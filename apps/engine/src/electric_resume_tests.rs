//! OTTO-11096: what a resume does at the engine's own boundary, where a test can hold a read open
//! and count storage requests. `conformance-electric-resume.test.ts` proves the product behaviour
//! across a real process restart; these prove the parts a black-box client cannot see.
//!
//! The store here keeps a stream the way durable-streams does: its literal wire bytes (`item,`), an
//! offset that is a byte position, and reads that run from the offset to the tail. Forgetting the
//! registry stands in for a restart, because the Engine's claims and streams are the part that
//! survives one and the registry is the part that does not.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use axum::Router;
use axum::body::{Body, to_bytes};
use axum::extract::{Request, State};
use axum::http::{HeaderMap, Method, StatusCode};
use axum::response::{IntoResponse, Response};
use tokio::sync::watch;
use tower::ServiceExt;

use crate::ds::{DsClient, Envelope};
use crate::engine::Engine;
use crate::http::router;
use crate::schema::Schema;
use crate::store_identity::{StoreIdentityV1, StreamScope};

#[derive(Default)]
struct Wire {
    bytes: Vec<u8>,
    closed: bool,
}

#[derive(Clone)]
struct ByteStore {
    streams: Arc<std::sync::Mutex<HashMap<String, Wire>>>,
    /// Reads of a shape stream from its beginning: one per key-set rebuild.
    rebuild_reads: Arc<AtomicUsize>,
    /// While true, those reads park until it is false again.
    hold_rebuilds: Arc<watch::Sender<bool>>,
    /// The rebuild reads that had arrived when the last hold was released. Counted then, because a
    /// request that lost the race to read afterwards is a retry at an older offset, not a resume.
    reads_held: Arc<AtomicUsize>,
    /// This many rebuild reads answer 503 before the store recovers.
    failing_rebuilds: Arc<AtomicUsize>,
    /// This many change-log HEADs answer 503 before the store recovers.
    failing_heads: Arc<AtomicUsize>,
}

impl Default for ByteStore {
    fn default() -> Self {
        Self {
            streams: Arc::default(),
            rebuild_reads: Arc::default(),
            hold_rebuilds: Arc::new(watch::channel(false).0),
            reads_held: Arc::default(),
            failing_rebuilds: Arc::default(),
            failing_heads: Arc::default(),
        }
    }
}

fn format_offset(bytes: usize) -> String {
    format!("{:016}_{bytes:016}", 0)
}

async fn byte_store(State(store): State<ByteStore>, request: Request) -> Response {
    let path = request.uri().path().to_string();
    let query = request.uri().query().unwrap_or_default().to_string();
    let method = request.method().clone();
    let closing = request.headers().contains_key("stream-closed");
    let body = to_bytes(request.into_body(), 64 * 1024 * 1024).await.unwrap();
    if method == Method::PUT {
        store.streams.lock().unwrap().entry(path).or_default();
        return StatusCode::CREATED.into_response();
    }
    if method == Method::POST {
        let mut streams = store.streams.lock().unwrap();
        let wire = streams.entry(path).or_default();
        if closing {
            wire.closed = true;
        } else if !body.is_empty() {
            for item in serde_json::from_slice::<Vec<&serde_json::value::RawValue>>(&body).unwrap() {
                wire.bytes.extend_from_slice(item.get().as_bytes());
                wire.bytes.push(b',');
            }
        }
        return ([("stream-next-offset", format_offset(wire.bytes.len()))], StatusCode::OK).into_response();
    }
    if method == Method::DELETE {
        store.streams.lock().unwrap().remove(&path);
        return StatusCode::NO_CONTENT.into_response();
    }
    if method == Method::HEAD {
        if path.contains("/changes/")
            && store.failing_heads.fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1)).is_ok()
        {
            return StatusCode::SERVICE_UNAVAILABLE.into_response();
        }
        let streams = store.streams.lock().unwrap();
        let Some(wire) = streams.get(&path) else { return StatusCode::NOT_FOUND.into_response() };
        let mut response = ([("stream-next-offset", format_offset(wire.bytes.len()))], "").into_response();
        if wire.closed {
            response.headers_mut().insert("stream-closed", "true".parse().unwrap());
        }
        return response;
    }
    let offset = query.split('&').find_map(|pair| pair.strip_prefix("offset=")).unwrap_or("-1").to_string();
    if offset == "-1" && path.contains("/shape/") {
        store.rebuild_reads.fetch_add(1, Ordering::SeqCst);
        let _ = store.hold_rebuilds.subscribe().wait_for(|held| !held).await;
        let failing = store.failing_rebuilds.fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1));
        if failing.is_ok() {
            return StatusCode::SERVICE_UNAVAILABLE.into_response();
        }
    }
    let start = if offset == "-1" { 0 } else { crate::changelog::offset_bytes(&offset).unwrap() as usize };
    let streams = store.streams.lock().unwrap();
    let Some(wire) = streams.get(&path) else { return StatusCode::NOT_FOUND.into_response() };
    let tail = wire.bytes.len();
    let mut headers = HeaderMap::new();
    headers.insert("content-type", "application/json".parse().unwrap());
    headers.insert("stream-next-offset", format_offset(tail.max(start)).parse().unwrap());
    headers.insert("stream-up-to-date", "true".parse().unwrap());
    if wire.closed {
        headers.insert("stream-closed", "true".parse().unwrap());
    }
    if query.contains("live=") && start >= tail {
        return (StatusCode::NO_CONTENT, headers).into_response();
    }
    let body = if start >= tail {
        "[]".to_string()
    } else {
        format!("[{}]", std::str::from_utf8(&wire.bytes[start..tail - 1]).unwrap())
    };
    (headers, body).into_response()
}

struct Stack {
    store: ByteStore,
    url: String,
}

async fn stack() -> Stack {
    let store = ByteStore::default();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let app = Router::new().fallback(byte_store).with_state(store.clone());
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    Stack { store, url }
}

struct Source {
    engine: Engine,
    app: Router,
    ds: DsClient,
}

/// One Engine on `scope` — what one row of the sources table gets: its own registry and its own
/// shape ids, which start again from `s1`.
async fn source(stack: &Stack, scope: &str) -> Source {
    let scope = StreamScope::new(
        format!("source-{scope}"),
        StoreIdentityV1::in_process_test_identity(),
        format!("query-{scope}"),
    )
    .unwrap();
    let ds = DsClient::new_for_in_process_test_with_scope(&stack.url, scope);
    let engine = Engine::new_for_in_process_test(ds.clone());
    let schema: Schema = serde_json::from_value(serde_json::json!({
        "tables": { "items": {
            "columns": { "id": { "type": "text" }, "label": { "type": "text" } },
            "primaryKey": "id",
        } }
    }))
    .unwrap();
    engine.define_schema(&schema).await.unwrap();
    Source { app: router(engine.clone()), engine, ds }
}

fn upsert(key: &str, label: &str) -> Envelope {
    serde_json::from_value(serde_json::json!({
        "type": "public.items",
        "key": key,
        "value": { "id": key, "label": label },
        "headers": { "operation": "upsert" },
    }))
    .unwrap()
}

fn delete(key: &str) -> Envelope {
    serde_json::from_value(serde_json::json!({
        "type": "public.items",
        "key": key,
        "headers": { "operation": "delete" },
    }))
    .unwrap()
}

struct Answer {
    status: StatusCode,
    headers: HeaderMap,
    body: serde_json::Value,
}

impl Answer {
    fn header(&self, name: &str) -> String {
        self.headers.get(name).unwrap().to_str().unwrap().to_string()
    }

    /// `(operation, key)` of each change message.
    fn changes(&self) -> Vec<(String, String)> {
        self.body
            .as_array()
            .unwrap()
            .iter()
            .filter(|message| message.get("key").is_some())
            .map(|message| {
                (
                    message["headers"]["operation"].as_str().unwrap().to_string(),
                    message["key"].as_str().unwrap().to_string(),
                )
            })
            .collect()
    }

    fn must_refetch(&self) -> bool {
        self.status == StatusCode::CONFLICT && self.body[0]["headers"]["control"] == "must-refetch"
    }
}

async fn get(app: &Router, uri: String) -> Answer {
    let response = app.clone().oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap()).await.unwrap();
    let status = response.status();
    let headers = response.headers().clone();
    let bytes = to_bytes(response.into_body(), 64 * 1024 * 1024).await.unwrap();
    Answer { status, headers, body: serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null) }
}

async fn snapshot(source: &Source) -> (String, String) {
    let answer = get(&source.app, "/v1/shape?table=items&offset=-1".to_string()).await;
    assert_eq!(answer.status, StatusCode::OK);
    (answer.header("electric-handle"), answer.header("electric-offset"))
}

fn at(handle: &str, offset: &str) -> String {
    format!("/v1/shape?table=items&handle={}&offset={offset}", urlencode(handle))
}

fn urlencode(value: &str) -> String {
    url::form_urlencoded::byte_serialize(value.as_bytes()).collect()
}

/// A restart, from the registry's point of view: claims and streams stay, handles are forgotten.
fn forget_handles(source: &Source) {
    super::close_handles(&source.engine);
}

fn registered(source: &Source) -> usize {
    source.engine.electric_handles.entries.lock().unwrap().len()
}

async fn claims(source: &Source, shape: &str) -> usize {
    let response = source
        .app
        .clone()
        .oneshot(Request::builder().uri(format!("/shapes/{shape}")).body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), 64 * 1024).await.unwrap();
    serde_json::from_slice::<serde_json::Value>(&body).unwrap()["subscriptions"].as_u64().unwrap() as usize
}

/// Long enough for requests that have finished their storage round-trips to reach the shared read.
async fn let_requests_settle() {
    tokio::time::sleep(Duration::from_millis(300)).await;
}

impl Stack {
    fn hold_rebuilds(&self) {
        self.store.rebuild_reads.store(0, Ordering::SeqCst);
        self.store.hold_rebuilds.send_replace(true);
    }

    /// Wait until `count` rebuild reads have arrived at the held store, or every one of `requests`
    /// has already been answered without one, which the caller's assertions then judge.
    async fn rebuild_reads_arrived(&self, count: usize, requests: &[tokio::task::JoinHandle<Answer>]) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while self.store.rebuild_reads.load(Ordering::SeqCst) < count
            && !requests.iter().all(tokio::task::JoinHandle::is_finished)
        {
            assert!(tokio::time::Instant::now() < deadline, "{count} rebuild read(s) never arrived");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    /// Let `requests`, already running against a held store, pile up on the first read; release them.
    async fn finish(&self, requests: Vec<tokio::task::JoinHandle<Answer>>) -> Vec<Answer> {
        self.rebuild_reads_arrived(1, &requests).await;
        let_requests_settle().await;
        self.store.reads_held.store(self.store.rebuild_reads.load(Ordering::SeqCst), Ordering::SeqCst);
        self.store.hold_rebuilds.send_replace(false);
        let mut answers = Vec::new();
        for request in requests {
            answers.push(tokio::time::timeout(Duration::from_secs(10), request).await.unwrap().unwrap());
        }
        answers
    }
}

#[tokio::test]
async fn a_forgotten_handle_resumes_as_of_its_own_offset() {
    let stack = stack().await;
    let source = source(&stack, "alpha").await;
    let (handle, empty) = snapshot(&source).await;
    source
        .ds
        .append("shape/s1", &[upsert("k1", "one"), upsert("k2", "two"), upsert("k3", "雪 \"three\"")])
        .await
        .unwrap();
    let first = get(&source.app, at(&handle, &empty)).await;
    assert_eq!(first.status, StatusCode::OK);
    let offset = first.header("electric-offset");
    forget_handles(&source);

    // Past the client's offset, before it returns. Rebuilt at the tail, the key set has neither key.
    source
        .ds
        .append("shape/s1", &[delete("k2"), delete("k3"), upsert("k3", "again"), upsert("k4", "four")])
        .await
        .unwrap();

    let resumed = get(&source.app, at(&handle, &offset)).await;
    assert_eq!(resumed.status, StatusCode::OK, "{:?}", resumed.body);
    assert_eq!(resumed.header("electric-handle"), handle);
    assert_eq!(
        resumed.changes(),
        [("delete", "k2"), ("delete", "k3"), ("insert", "k3"), ("insert", "k4")]
            .map(|(op, key)| (op.to_string(), key.to_string())),
    );
}

#[tokio::test]
async fn resumes_from_two_sources_whose_shapes_share_an_id_never_cross() {
    let stack = stack().await;
    let alpha = source(&stack, "alpha").await;
    let beta = source(&stack, "beta").await;
    let (alpha_handle, empty) = snapshot(&alpha).await;
    let (beta_handle, _) = snapshot(&beta).await;
    assert_eq!(alpha_handle.split('h').next(), Some("s1"));
    assert_eq!(beta_handle.split('h').next(), Some("s1"), "both sources must mint the same shape id");

    // Equal byte lengths, so both clients stand at the SAME logical path and the SAME offset string.
    alpha.ds.append("shape/s1", &[upsert("a", "x"), upsert("shared", "x")]).await.unwrap();
    beta.ds.append("shape/s1", &[upsert("b", "x"), upsert("other_", "x")]).await.unwrap();
    let alpha_first = get(&alpha.app, at(&alpha_handle, &empty)).await;
    let beta_first = get(&beta.app, at(&beta_handle, &empty)).await;
    let offset = alpha_first.header("electric-offset");
    assert_eq!(offset, beta_first.header("electric-offset"));
    forget_handles(&alpha);
    forget_handles(&beta);

    alpha.ds.append("shape/s1", &[upsert("a", "y"), upsert("b", "y")]).await.unwrap();
    beta.ds.append("shape/s1", &[upsert("a", "y"), upsert("b", "y")]).await.unwrap();

    // Overlap the two rebuilds: a process-wide (path, offset) key would hand one source the other's.
    stack.hold_rebuilds();
    let spawn = |app: Router, uri: String| tokio::spawn(async move { get(&app, uri).await });
    let answers = stack
        .finish(vec![
            spawn(alpha.app.clone(), at(&alpha_handle, &offset)),
            spawn(beta.app.clone(), at(&beta_handle, &offset)),
        ])
        .await;
    assert_eq!(answers[0].status, StatusCode::OK, "{:?}", answers[0].body);
    assert_eq!(answers[1].status, StatusCode::OK, "{:?}", answers[1].body);
    let expect = |ops: [(&str, &str); 2]| ops.map(|(op, key)| (op.to_string(), key.to_string())).to_vec();
    assert_eq!(answers[0].changes(), expect([("update", "a"), ("insert", "b")]));
    assert_eq!(answers[1].changes(), expect([("insert", "a"), ("update", "b")]));
    assert_eq!(answers[0].body[0]["value"]["label"], "y");

    // A handle is only ever good on the source that minted it, even with an identical definition,
    // shape id and offset: the stream behind it is a different stream.
    forget_handles(&alpha);
    forget_handles(&beta);
    assert!(get(&beta.app, at(&alpha_handle, &offset)).await.must_refetch());
    assert!(get(&alpha.app, at(&beta_handle, &offset)).await.must_refetch());
    assert_eq!(registered(&alpha), 0);
    assert_eq!(registered(&beta), 0);
}

#[tokio::test]
async fn concurrent_resumes_of_one_handle_are_one_rebuild_one_entry_one_claim() {
    let stack = stack().await;
    let source = source(&stack, "alpha").await;
    let (handle, empty) = snapshot(&source).await;
    source.ds.append("shape/s1", &[upsert("k1", "one")]).await.unwrap();
    let first = get(&source.app, at(&handle, &empty)).await;
    let offset = first.header("electric-offset");
    forget_handles(&source);
    source.ds.append("shape/s1", &[upsert("k1", "changed")]).await.unwrap();

    stack.hold_rebuilds();
    let requests = (0..8)
        .map(|_| {
            let (app, uri) = (source.app.clone(), at(&handle, &offset));
            tokio::spawn(async move { get(&app, uri).await })
        })
        .collect();
    let answers = stack.finish(requests).await;
    for answer in &answers {
        assert_eq!(answer.status, StatusCode::OK, "{:?}", answer.body);
        assert_eq!(answer.header("electric-handle"), handle);
    }
    assert_eq!(registered(&source), 1);
    assert_eq!(claims(&source, "s1").await, 1);
    assert_eq!(stack.store.reads_held.load(Ordering::SeqCst), 1, "every resume must share one rebuild");
}

#[tokio::test]
async fn different_clients_at_one_offset_share_a_rebuild_and_keep_their_own_claims() {
    let stack = stack().await;
    let source = source(&stack, "alpha").await;
    let (a, empty) = snapshot(&source).await;
    let (b, _) = snapshot(&source).await;
    assert_ne!(a, b);
    source.ds.append("shape/s1", &[upsert("k1", "one")]).await.unwrap();
    let offset = get(&source.app, at(&a, &empty)).await.header("electric-offset");
    forget_handles(&source);
    source.ds.append("shape/s1", &[upsert("k1", "changed")]).await.unwrap();

    stack.hold_rebuilds();
    let requests = [&a, &b]
        .map(|handle| {
            let (app, uri) = (source.app.clone(), at(handle, &offset));
            tokio::spawn(async move { get(&app, uri).await })
        })
        .into_iter()
        .collect();
    let answers = stack.finish(requests).await;
    for answer in &answers {
        assert_eq!(answer.status, StatusCode::OK, "{:?}", answer.body);
        assert_eq!(answer.changes(), [("update".to_string(), "k1".to_string())]);
    }
    assert_eq!(registered(&source), 2);
    assert_eq!(claims(&source, "s1").await, 2);
    assert_eq!(stack.store.reads_held.load(Ordering::SeqCst), 1, "same shape and offset must share one rebuild");
}

#[tokio::test]
async fn a_cancelled_resume_hands_over_to_the_next_and_leaves_no_stale_state() {
    let stack = stack().await;
    let source = source(&stack, "alpha").await;
    let (handle, empty) = snapshot(&source).await;
    source.ds.append("shape/s1", &[upsert("k1", "one")]).await.unwrap();
    let offset = get(&source.app, at(&handle, &empty)).await.header("electric-offset");
    forget_handles(&source);

    stack.hold_rebuilds();
    let (app, uri) = (source.app.clone(), at(&handle, &offset));
    let leader = tokio::spawn(async move { get(&app, uri).await });
    stack.rebuild_reads_arrived(1, std::slice::from_ref(&leader)).await;
    let (app, uri) = (source.app.clone(), at(&handle, &offset));
    let waiter = tokio::spawn(async move { get(&app, uri).await });
    let_requests_settle().await;
    leader.abort();
    stack.rebuild_reads_arrived(2, std::slice::from_ref(&waiter)).await;
    let answers = stack.finish(vec![waiter]).await;
    assert_eq!(answers[0].status, StatusCode::OK, "{:?}", answers[0].body);
    assert_eq!(registered(&source), 1);
    assert_eq!(claims(&source, "s1").await, 1);
}

/// The moment after a restart is when storage is most contended. A client whose handle and offset are
/// good must not be told to throw away its rows because a read failed: it retries, and resumes.
#[tokio::test]
async fn a_storage_failure_during_a_resume_is_retried_with_the_same_handle() {
    let stack = stack().await;
    let source = source(&stack, "alpha").await;
    let (handle, empty) = snapshot(&source).await;
    source.ds.append("shape/s1", &[upsert("k1", "one")]).await.unwrap();
    let offset = get(&source.app, at(&handle, &empty)).await.header("electric-offset");
    forget_handles(&source);
    source.ds.append("shape/s1", &[delete("k1")]).await.unwrap();

    stack.store.failing_rebuilds.store(1, Ordering::SeqCst);
    let failed = get(&source.app, at(&handle, &offset)).await;
    assert_eq!(failed.status, StatusCode::SERVICE_UNAVAILABLE, "{:?}", failed.body);
    assert_eq!(failed.header("retry-after"), "1");
    assert_eq!(registered(&source), 0);
    assert_eq!(claims(&source, "s1").await, 0, "a failed resume gives its claim back");

    let resumed = get(&source.app, at(&handle, &offset)).await;
    assert_eq!(resumed.status, StatusCode::OK, "{:?}", resumed.body);
    assert_eq!(resumed.changes(), [("delete".to_string(), "k1".to_string())]);
    assert_eq!(claims(&source, "s1").await, 1);
}

/// A reactivation that cannot finish yet leaves the shape where it was: the client comes back.
#[tokio::test]
async fn a_deferred_reactivation_during_a_resume_is_retried_with_the_same_handle() {
    let stack = stack().await;
    let source = source(&stack, "alpha").await;
    let (handle, offset) = snapshot(&source).await;
    forget_handles(&source);
    let unreadable = crate::changelog::LogPosition { segment: 0, offset: "-1".into() };
    source.engine.park_dormant_shape_for_test("s1", "public.items", unreadable).await;
    stack.store.failing_heads.store(1, Ordering::SeqCst);

    let failed = get(&source.app, at(&handle, &offset)).await;
    assert_eq!(failed.status, StatusCode::SERVICE_UNAVAILABLE, "{:?}", failed.body);
    assert_eq!(failed.header("retry-after"), "1");
    assert!(failed.body["message"].as_str().unwrap().contains("reactivation was deferred"), "{:?}", failed.body);

    let resumed = get(&source.app, at(&handle, &offset)).await;
    assert_eq!(resumed.status, StatusCode::OK, "{:?}", resumed.body);
    assert_eq!(resumed.header("electric-handle"), handle);
}

/// A draining engine's answer to a resume it cannot finish is the one every new long-poll gets.
#[tokio::test]
async fn a_resume_interrupted_by_shutdown_is_retried_on_the_next_process() {
    let stack = stack().await;
    let source = source(&stack, "alpha").await;
    let (handle, empty) = snapshot(&source).await;
    source.ds.append("shape/s1", &[upsert("k1", "one")]).await.unwrap();
    let offset = get(&source.app, at(&handle, &empty)).await.header("electric-offset");
    forget_handles(&source);

    stack.hold_rebuilds();
    let (app, uri) = (source.app.clone(), at(&handle, &offset));
    let resume = tokio::spawn(async move { get(&app, uri).await });
    stack.rebuild_reads_arrived(1, std::slice::from_ref(&resume)).await;
    source.engine.shutdown_token().begin();
    let answer = tokio::time::timeout(Duration::from_secs(10), resume).await.unwrap().unwrap();
    stack.store.hold_rebuilds.send_replace(false);
    assert_eq!(answer.status, StatusCode::SERVICE_UNAVAILABLE, "{:?}", answer.body);
    assert_eq!(answer.header("retry-after"), "1");
}

#[tokio::test]
async fn an_idle_evicted_handle_takes_its_own_claim_back() {
    let stack = stack().await;
    let source = source(&stack, "alpha").await;
    let (handle, offset) = snapshot(&source).await;
    let subscription = source.engine.electric_handles.entries.lock().unwrap()[&handle].subscription.clone();
    // What the evictor does: the cursor state goes and the handle's own claim is released.
    forget_handles(&source);
    source.engine.release_subscription("s1", Some(&subscription)).await;
    assert_eq!(claims(&source, "s1").await, 0);

    let resumed = get(&source.app, at(&handle, &offset)).await;
    assert_eq!(resumed.status, StatusCode::OK, "{:?}", resumed.body);
    assert_eq!(claims(&source, "s1").await, 1);
    assert_eq!(source.engine.subscription_owner(&subscription).await.as_deref(), Some("s1"));
}

#[tokio::test]
async fn a_stream_closed_under_a_resume_answers_409_and_gives_the_claim_back() {
    let stack = stack().await;
    let source = source(&stack, "alpha").await;
    let (handle, _) = snapshot(&source).await;
    source.ds.append("shape/s1", &[upsert("k1", "one")]).await.unwrap();
    let (other, offset) = snapshot(&source).await;
    assert_eq!(claims(&source, "s1").await, 2);
    forget_handles(&source);

    // The shape still resolves and its stream passes the join's HEAD; it closes behind that check.
    stack.hold_rebuilds();
    let (app, uri) = (source.app.clone(), at(&handle, &offset));
    let resume = tokio::spawn(async move { get(&app, uri).await });
    stack.rebuild_reads_arrived(1, std::slice::from_ref(&resume)).await;
    source.ds.close_stream("shape/s1").await.unwrap();
    let answers = stack.finish(vec![resume]).await;
    assert!(answers[0].must_refetch(), "{} {:?}", answers[0].status, answers[0].body);
    assert_eq!(registered(&source), 0);
    assert_eq!(claims(&source, "s1").await, 1, "the refused resume must give its claim back: {other} still holds one");
}
