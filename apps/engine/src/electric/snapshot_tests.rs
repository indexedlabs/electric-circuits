//! `GET /v1/shape?offset=-1` through the real router, engine lifecycle and catalog writer, over a
//! scripted durable-streams store (OTTO-11109). The store only supplies an append log, HEAD tails
//! and a gate that holds shape-stream reads; every assertion is on the HTTP answer or on how many
//! times the store was asked to fold a shape from the beginning.
//!
//! Every test runs on a paused clock: `quiesce()` returns only once every task is parked, which is
//! how "all N requests have arrived" is established without a sleep.

use super::*;
use crate::ds::{BodyGate, DsClient, EnvelopeHeaders, ScriptedStore};
use axum::body::{Body, Bytes};
use axum::extract::Request;
use std::sync::atomic::Ordering::SeqCst;
use tower::ServiceExt;

const SNAPSHOT: &str = "/v1/shape?table=items&offset=-1";

/// One engine (one source in sources-table mode) over its own store.
struct Source {
    engine: Engine,
    store: Arc<ScriptedStore>,
    ds: DsClient,
    app: axum::Router,
    shape: String,
    stream: String,
    reads: std::sync::Mutex<tokio::sync::mpsc::UnboundedReceiver<(String, String, bool)>>,
    folds: std::sync::atomic::AtomicUsize,
}

impl Source {
    async fn new(base: &str) -> Self {
        Self::with_ds(base, |ds| ds).await
    }

    /// A source whose shape folds read under the memory budget, so [`Self::hold_after_read`] can
    /// hold a fold page after the store has fixed its contents.
    async fn budgeted(base: &str) -> Self {
        let config = crate::config::Config::resolve(|key| {
            (key == "ELECTRIC_CIRCUITS_MEMORY_BUDGET_BYTES").then(|| (1u64 << 30).to_string())
        })
        .unwrap();
        Self::with_ds(base, |ds| ds.with_memory_budget(&config)).await
    }

    async fn with_ds(base: &str, ds: impl FnOnce(DsClient) -> DsClient) -> Self {
        let store =
            Arc::new(ScriptedStore { read_appended: true, streams: Some(Default::default()), ..Default::default() });
        let engine = Engine::new_for_in_process_test(ds(DsClient::with_test_store(base.into(), store.clone())));
        let schema: crate::schema::Schema = serde_json::from_value(serde_json::json!({
            "tables": { "items": { "columns": {
                "id": { "type": "text" }, "n": { "type": "int" }, "f": { "type": "float" },
                "b": { "type": "bool" }, "s": { "type": "text" }, "z": { "type": "text" }
            }, "primaryKey": "id" } }
        }))
        .unwrap();
        engine.define_schema(&schema).await.unwrap();
        // The shape every `/v1/shape?table=items` request shares, created up front so its stream
        // can be given rows before the first snapshot.
        let (record, _) =
            engine.create_shape_as(&"public.items".into(), None, None, false, true, Some("seed".into())).await.unwrap();
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        *store.read_observer.lock().unwrap() = Some(tx);
        Source {
            app: crate::http::router(engine.clone()),
            ds: DsClient::with_test_store(base.into(), store.clone()),
            engine,
            store,
            shape: record.id,
            stream: record.stream_path,
            reads: std::sync::Mutex::new(rx),
            folds: Default::default(),
        }
    }

    async fn append(&self, envelopes: Vec<Envelope>) {
        self.ds.append(&self.stream, &envelopes).await.unwrap();
    }

    /// How many times the shape's stream was read from its beginning: one per fold.
    fn folds(&self) -> usize {
        let mut reads = self.reads.lock().unwrap();
        while let Ok((path, offset, live)) = reads.try_recv() {
            if path.ends_with(&self.stream) && offset == "-1" && !live {
                self.folds.fetch_add(1, SeqCst);
            }
        }
        self.folds.load(SeqCst)
    }

    /// Hold every catch-up read of the shape's stream until [`Self::open`].
    fn hold(&self) -> Arc<BodyGate> {
        let gate = Arc::new(BodyGate::default());
        *self.store.read_gate.lock().unwrap() = Some((self.stream.clone(), false, gate.clone()));
        gate
    }

    fn open(&self, gate: &BodyGate) {
        *self.store.read_gate.lock().unwrap() = None;
        gate.released.notify_waiters();
    }

    /// Hold every budgeted read AFTER the store has decided what the page contains: a write
    /// appended while it is held is not in that page.
    fn hold_after_read(&self) -> Arc<BodyGate> {
        let gate = Arc::new(BodyGate::default());
        *self.store.body_gate.lock().unwrap() = Some(gate.clone());
        gate
    }

    fn open_after_read(&self, gate: &BodyGate) {
        *self.store.body_gate.lock().unwrap() = None;
        gate.released.notify_waiters();
    }

    fn spawn_get(&self, uri: &str) -> tokio::task::JoinHandle<(StatusCode, HeaderMap, Bytes)> {
        tokio::spawn(get(self.app.clone(), uri.to_string()))
    }
}

async fn get(app: axum::Router, uri: String) -> (StatusCode, HeaderMap, Bytes) {
    let response = app.oneshot(Request::get(uri).body(Body::empty()).unwrap()).await.unwrap();
    let status = response.status();
    let headers = response.headers().clone();
    (status, headers, axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap())
}

/// Returns once every other task is parked (the paused clock only advances when nothing can run).
async fn quiesce() {
    tokio::time::sleep(Duration::from_millis(1)).await;
}

fn upsert(key: &str, value: serde_json::Value) -> Envelope {
    Envelope {
        type_: "change".into(),
        key: key.into(),
        value: Some(value),
        old: None,
        headers: EnvelopeHeaders {
            operation: "upsert".into(),
            txid: Some("7".into()),
            offset: None,
            lsn: None,
            seq: None,
            last: None,
        },
    }
}

fn delete(key: &str) -> Envelope {
    let mut env = upsert(key, serde_json::Value::Null);
    env.value = None;
    env.headers.operation = "delete".into();
    env
}

/// Inserts, an update, a deleted key, numbers, booleans, nulls and escaped strings — but no key
/// deleted and then inserted again (OTTO-11109 leaves that fold quirk to its own issue).
fn rows() -> Vec<Envelope> {
    vec![
        upsert("a", serde_json::json!({"b": true, "f": 1.5, "id": "a", "n": 1, "s": "plain", "z": null})),
        upsert(
            "q\"k",
            serde_json::json!({
                "b": false, "f": -0.25, "id": "q\"k", "n": 12345678901_i64,
                "s": "tab\tquote\"back\\slash é ☃", "z": null
            }),
        ),
        upsert("a", serde_json::json!({"b": false, "f": 2.0, "id": "a", "n": 2, "s": "updated", "z": "x"})),
        upsert("gone", serde_json::json!({"b": null, "f": null, "id": "gone", "n": 3, "s": "", "z": null})),
        delete("gone"),
    ]
}

/// The body `/v1/shape` answered for [`rows`] before OTTO-11109 (engine ac68597).
const ROWS_BODY: &str = concat!(
    r#"[{"headers":{"operation":"insert"},"key":"a","value":{"b":"false","f":"2.0","id":"a","n":"2","s":"updated","z":"x"}},"#,
    r#"{"headers":{"operation":"insert"},"key":"q\"k","value":{"b":"false","f":"-0.25","id":"q\"k","n":"12345678901","s":"tab\tquote\"back\\slash é ☃","z":null}},"#,
    r#"{"headers":{"control":"up-to-date","global_last_seen_lsn":"0"}}]"#,
);

fn ops(body: &[u8]) -> Vec<(String, String)> {
    let messages: Vec<serde_json::Value> = serde_json::from_slice(body).unwrap();
    messages
        .iter()
        .filter_map(|m| Some((m["headers"]["operation"].as_str()?.to_string(), m["key"].as_str()?.to_string())))
        .collect()
}

#[tokio::test(start_paused = true)]
async fn snapshot_body_is_byte_identical_to_the_unshared_build() {
    let source = Source::new("scripted://body").await;
    source.append(rows()).await;
    let (status, headers, body) = get(source.app.clone(), SNAPSHOT.into()).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(std::str::from_utf8(&body).unwrap(), ROWS_BODY);
    assert_eq!(headers.get(axum::http::header::CONTENT_TYPE).unwrap(), "application/json");
    let tail = source.store.appended.lock().unwrap().iter().filter(|(p, _, _)| p.ends_with(&source.stream)).count();
    assert_eq!(headers.get("electric-offset").unwrap().to_str().unwrap(), format!("0000000000000000_{tail:016}"));
    assert!(headers.get("electric-handle").unwrap().to_str().unwrap().starts_with(&format!("{}h", source.shape)));
    assert!(headers.contains_key("electric-up-to-date"));
    assert!(headers.contains_key("electric-schema"));
}

#[tokio::test(start_paused = true)]
async fn concurrent_snapshots_of_one_shape_await_one_fold() {
    let source = Source::new("scripted://concurrent").await;
    source.append(rows()).await;
    let gate = source.hold();
    let requests: Vec<_> = (0..5).map(|_| source.spawn_get(SNAPSHOT)).collect();
    quiesce().await;
    assert_eq!(gate.arrived.load(SeqCst), 1, "five concurrent snapshots of one shape must share one fold");
    source.open(&gate);
    for request in requests {
        let (status, _, body) = request.await.unwrap();
        assert_eq!(status, StatusCode::OK);
        assert_eq!(std::str::from_utf8(&body).unwrap(), ROWS_BODY);
    }
    assert_eq!(source.folds(), 1);
    let metrics = &source.engine.metrics;
    assert_eq!(metrics.electric_snapshot_builds.load(SeqCst), 1);
    assert_eq!(metrics.electric_snapshot_joins.load(SeqCst), 4);
    get(source.app.clone(), SNAPSHOT.into()).await;
    assert_eq!(metrics.electric_snapshot_reuses.load(SeqCst), 1);
}

#[tokio::test(start_paused = true)]
async fn a_finished_snapshot_is_reused_until_the_tail_moves_or_it_expires() {
    let source = Source::new("scripted://reuse").await;
    source.append(rows()).await;
    let (_, first, body) = get(source.app.clone(), SNAPSHOT.into()).await;
    assert_eq!(std::str::from_utf8(&body).unwrap(), ROWS_BODY);
    let (status, second, body) = get(source.app.clone(), SNAPSHOT.into()).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(source.folds(), 1, "a snapshot at an unchanged tail must reuse the finished build");
    assert_eq!(std::str::from_utf8(&body).unwrap(), ROWS_BODY);
    assert_eq!(first.get("electric-offset"), second.get("electric-offset"));
    assert_ne!(first.get("electric-handle"), second.get("electric-handle"), "every client gets its own handle");

    source.append(vec![upsert("c", serde_json::json!({"id": "c"}))]).await;
    let (status, third, body) = get(source.app.clone(), SNAPSHOT.into()).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(source.folds(), 2, "a new tail must trigger a new build");
    assert_eq!(
        ops(&body),
        [("insert".into(), "a".into()), ("insert".into(), "q\"k".into()), ("insert".into(), "c".into())]
    );
    assert_ne!(third.get("electric-offset"), second.get("electric-offset"));

    // The kept body is counted in the memory probe while it is kept, and released at its expiry
    // by the build's own task — not only skipped by the next request — so an idle shape does not
    // pin its body (these bytes are outside the memory budget).
    let kept = crate::electric::snapshot_cache_heap_bytes(&source.engine);
    assert!(kept > 0, "a kept body must be reported in the memory probe");
    assert!(source.engine.mem_bytes().await.bytes_electric_adapter >= kept);
    tokio::time::sleep(Duration::from_secs(31)).await;
    assert!(
        source.engine.electric_handles.snapshots.lock().unwrap().is_empty(),
        "a kept body must be released when it expires"
    );
    assert_eq!(crate::electric::snapshot_cache_heap_bytes(&source.engine), 0);
    let (status, fourth, _) = get(source.app.clone(), SNAPSHOT.into()).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(source.folds(), 3, "a finished build is kept for a short time only");
    assert_eq!(fourth.get("electric-offset"), third.get("electric-offset"));
}

#[tokio::test(start_paused = true)]
async fn two_sources_with_the_same_shape_id_never_share_a_snapshot() {
    let alpha = Source::new("scripted://alpha").await;
    let beta = Source::new("scripted://beta").await;
    assert_eq!(alpha.stream, beta.stream, "the cross-source case needs one stream path in both sources");
    alpha.append(rows()).await;
    beta.append(vec![upsert("beta-only", serde_json::json!({"id": "beta-only"}))]).await;
    let (alpha_gate, beta_gate) = (alpha.hold(), beta.hold());
    let alpha_requests: Vec<_> = (0..3).map(|_| alpha.spawn_get(SNAPSHOT)).collect();
    let beta_requests: Vec<_> = (0..3).map(|_| beta.spawn_get(SNAPSHOT)).collect();
    quiesce().await;
    assert_eq!(alpha_gate.arrived.load(SeqCst), 1, "each source folds its own shape once");
    assert_eq!(beta_gate.arrived.load(SeqCst), 1, "each source folds its own shape once");
    beta.open(&beta_gate);
    for request in beta_requests {
        let (status, _, body) = request.await.unwrap();
        assert_eq!(status, StatusCode::OK);
        assert_eq!(ops(&body), [("insert".into(), "beta-only".into())]);
    }
    alpha.open(&alpha_gate);
    for request in alpha_requests {
        let (status, _, body) = request.await.unwrap();
        assert_eq!(status, StatusCode::OK);
        assert_eq!(std::str::from_utf8(&body).unwrap(), ROWS_BODY);
    }
    // Finished builds are not shared either: each source's next snapshot reuses its own.
    let (_, _, body) = get(beta.app.clone(), SNAPSHOT.into()).await;
    assert_eq!(ops(&body), [("insert".into(), "beta-only".into())]);
    let (_, _, body) = get(alpha.app.clone(), SNAPSHOT.into()).await;
    assert_eq!(std::str::from_utf8(&body).unwrap(), ROWS_BODY);
    assert_eq!((alpha.folds(), beta.folds()), (1, 1));
}

#[tokio::test(start_paused = true)]
async fn a_cancelled_first_requester_does_not_fail_the_others() {
    let source = Source::new("scripted://cancel-first").await;
    source.append(rows()).await;
    let gate = source.hold();
    let first = source.spawn_get(SNAPSHOT);
    quiesce().await;
    let others: Vec<_> = (0..3).map(|_| source.spawn_get(SNAPSHOT)).collect();
    quiesce().await;
    first.abort();
    quiesce().await;
    assert_eq!(gate.arrived.load(SeqCst), 1, "later requesters must wait on the first requester's build");
    source.open(&gate);
    for request in others {
        let (status, _, body) = request.await.unwrap();
        assert_eq!(status, StatusCode::OK);
        assert_eq!(std::str::from_utf8(&body).unwrap(), ROWS_BODY);
    }
    assert_eq!(source.folds(), 1, "cancelling the first requester must not restart the build");
}

/// Decision: a build every requester abandoned still finishes and is kept for reuse, because
/// clients that time out during a reconnect storm come straight back for the same shape.
#[tokio::test(start_paused = true)]
async fn a_build_every_requester_abandoned_finishes_and_serves_the_next_one() {
    let source = Source::new("scripted://cancel-all").await;
    source.append(rows()).await;
    let gate = source.hold();
    let requests: Vec<_> = (0..2).map(|_| source.spawn_get(SNAPSHOT)).collect();
    quiesce().await;
    for request in &requests {
        request.abort();
    }
    quiesce().await;
    source.open(&gate);
    quiesce().await;
    let (status, _, body) = get(source.app.clone(), SNAPSHOT.into()).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(std::str::from_utf8(&body).unwrap(), ROWS_BODY);
    assert_eq!(source.folds(), 1, "the abandoned build must finish and be reused");
}

#[tokio::test(start_paused = true)]
async fn a_shape_retired_mid_build_tells_every_waiter_to_refetch() {
    let source = Source::new("scripted://retired").await;
    source.append(rows()).await;
    let gate = source.hold();
    let requests: Vec<_> = (0..3).map(|_| source.spawn_get(SNAPSHOT)).collect();
    quiesce().await;
    assert_eq!(gate.arrived.load(SeqCst), 1, "three concurrent snapshots must share one fold");
    source.engine.purge_shape_durable(&source.shape).await.unwrap();
    source.open(&gate);
    for request in requests {
        let (status, headers, body) = request.await.unwrap();
        assert_eq!(status, StatusCode::CONFLICT);
        assert_eq!(headers.get("electric-offset").unwrap(), "-1");
        assert!(headers.get("electric-handle").is_none());
        assert!(std::str::from_utf8(&body).unwrap().contains("must-refetch"));
    }
    assert_eq!(source.folds(), 1);
    // The failed build is not kept: the refetch gets the replacement shape.
    let (status, headers, _) = get(source.app.clone(), SNAPSHOT.into()).await;
    assert_eq!(status, StatusCode::OK);
    assert!(!headers.get("electric-handle").unwrap().to_str().unwrap().starts_with(&format!("{}h", source.shape)));
}

#[tokio::test(start_paused = true)]
async fn every_client_of_a_shared_build_gets_its_own_handle_and_key_set() {
    let source = Source::new("scripted://handles").await;
    source.append(rows()).await;
    let gate = source.hold();
    let requests: Vec<_> = (0..2).map(|_| source.spawn_get(SNAPSHOT)).collect();
    quiesce().await;
    assert_eq!(gate.arrived.load(SeqCst), 1, "two concurrent snapshots must share one fold");
    source.open(&gate);
    let mut clients = Vec::new();
    for request in requests {
        let (status, headers, _) = request.await.unwrap();
        assert_eq!(status, StatusCode::OK);
        let handle = headers.get("electric-handle").unwrap().to_str().unwrap().to_string();
        let offset = headers.get("electric-offset").unwrap().to_str().unwrap().to_string();
        clients.push((handle, offset));
    }
    assert_ne!(clients[0].0, clients[1].0, "a shared build still mints one handle per client");
    assert_eq!(clients[0].1, clients[1].1);

    source
        .append(vec![
            upsert("a", serde_json::json!({"id": "a", "n": 3})),
            upsert("c", serde_json::json!({"id": "c"})),
            delete("q\"k"),
        ])
        .await;
    let expected: Vec<(String, String)> =
        vec![("update".into(), "a".into()), ("insert".into(), "c".into()), ("delete".into(), "q\"k".into())];
    for (handle, offset) in &clients {
        let uri = format!("/v1/shape?table=items&handle={handle}&offset={offset}&live=true");
        let (status, headers, body) = get(source.app.clone(), uri).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(ops(&body), expected, "each client classifies against its own key set");
        assert_eq!(headers.get("electric-handle").unwrap().to_str().unwrap(), handle);
    }
}

/// Shutdown begins a drain in which requests the engine has already accepted are still served, so
/// a build in flight finishes for its waiters. Engine close (after the drain) ends a build that is
/// still running; its waiters are then told to retry.
#[tokio::test(start_paused = true)]
async fn a_draining_engine_serves_snapshots_until_it_closes() {
    let source = Source::new("scripted://shutdown").await;
    source.append(rows()).await;
    let gate = source.hold();
    let requests: Vec<_> = (0..3).map(|_| source.spawn_get(SNAPSHOT)).collect();
    quiesce().await;
    assert_eq!(gate.arrived.load(SeqCst), 1);
    source.engine.shutdown_token().begin();
    // A NEW snapshot must create or join its shape, a control mutation, and control admission
    // closes when shutdown begins (unchanged by OTTO-11109): it is told to retry elsewhere.
    let (status, _, body) = get(source.app.clone(), SNAPSHOT.into()).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body:?}");
    source.open(&gate);
    for request in requests {
        let (status, _, body) = tokio::time::timeout(Duration::from_secs(5), request).await.unwrap().unwrap();
        assert_eq!(status, StatusCode::OK, "a draining engine still serves the snapshots it accepted");
        assert_eq!(std::str::from_utf8(&body).unwrap(), ROWS_BODY);
    }
}

#[tokio::test(start_paused = true)]
async fn engine_close_tells_the_waiters_of_an_unfinished_build_to_retry() {
    let source = Source::new("scripted://close").await;
    source.append(rows()).await;
    let gate = source.hold();
    let request = source.spawn_get(SNAPSHOT);
    quiesce().await;
    assert_eq!(gate.arrived.load(SeqCst), 1);
    source.engine.close(Duration::from_secs(1)).await;
    let (status, headers, _) = tokio::time::timeout(Duration::from_secs(5), request).await.unwrap().unwrap();
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "a closed engine's waiters must be told to retry");
    assert_eq!(headers.get(axum::http::header::RETRY_AFTER).unwrap(), "1");
    assert!(source.engine.electric_handles.snapshots.lock().unwrap().is_empty(), "an abandoned build is not kept");
}

/// A request that arrives while a build is in flight, after a write the build's fold has already
/// read past, must not be answered with that build: its first response would be older than the
/// stream was when it asked (a client that writes, drains and subscribes would miss its write).
#[tokio::test(start_paused = true)]
async fn a_request_joining_a_build_that_missed_a_newer_write_gets_a_fresh_build() {
    let source = Source::budgeted("scripted://late-joiner").await;
    source.append(rows()).await;
    let gate = source.hold_after_read();
    let first = source.spawn_get(SNAPSHOT);
    quiesce().await;
    assert_eq!(source.folds(), 1, "the first build has read its page");
    source.append(vec![upsert("c", serde_json::json!({"id": "c"}))]).await;
    let late = source.spawn_get(SNAPSHOT);
    quiesce().await;
    source.open_after_read(&gate);

    let (status, first_headers, body) = first.await.unwrap();
    assert_eq!(status, StatusCode::OK);
    assert_eq!(std::str::from_utf8(&body).unwrap(), ROWS_BODY, "the first request predates the write");
    let (status, late_headers, body) = late.await.unwrap();
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        ops(&body),
        [("insert".into(), "a".into()), ("insert".into(), "q\"k".into()), ("insert".into(), "c".into())],
        "a request must see every write appended before it arrived"
    );
    assert_ne!(late_headers.get("electric-offset"), first_headers.get("electric-offset"));
    assert_eq!(source.folds(), 2);
}
