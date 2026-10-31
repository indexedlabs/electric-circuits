//! Public positioned reads of the segmented change log (external-consumer contract).

#[path = "support/changes_consumer.rs"]
mod changes_consumer;

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::Router;
use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::{Method, StatusCode};
use axum::response::{IntoResponse, Response};
use electric_circuits_engine::changelog::{CONTROL_TYPE, rotation_envelope};
use electric_circuits_engine::ds::{DsClient, Envelope, EnvelopeHeaders};
use electric_circuits_engine::engine::Engine;
use electric_circuits_engine::http::router;
use tower::ServiceExt;

use changes_consumer::{ChangePage, ReferenceConsumer};

#[derive(Clone)]
struct FakePage {
    status: StatusCode,
    headers: Vec<(&'static str, &'static str)>,
    body: String,
    delay: Duration,
}

impl FakePage {
    fn ok(body: impl Into<String>) -> Self {
        Self {
            status: StatusCode::OK,
            headers: vec![("stream-next-offset", "0000000000000000_0000000000000100")],
            body: body.into(),
            delay: Duration::ZERO,
        }
    }

    fn gone(status: StatusCode) -> Self {
        Self { status, headers: vec![], body: String::new(), delay: Duration::ZERO }
    }
}

/// Minimal, FakeDs-backed durable-streams fixture. Keys are logical stream path + the exact
/// positioned-read query, so the assertions exercise the route's forwarding rather than a fake
/// reader's interpretation of offsets or rotation.
#[derive(Clone, Default)]
struct FakeDs {
    pages: Arc<Mutex<BTreeMap<(String, String, bool), FakePage>>>,
}

impl FakeDs {
    fn page(&self, path: &str, offset: &str, live: bool, page: FakePage) {
        self.pages.lock().unwrap().insert((path.to_string(), offset.to_string(), live), page);
    }
}

async fn durable_streams(State(ds): State<FakeDs>, request: Request) -> Response {
    let path =
        request.uri().path().trim_start_matches('/').rsplit_once("/queries/test-query/").map_or_else(
            || request.uri().path().trim_start_matches('/').to_string(),
            |(_, logical)| logical.to_string(),
        );
    match *request.method() {
        Method::GET => {
            let query = request.uri().query().unwrap_or("");
            let offset = query.split('&').find_map(|part| part.strip_prefix("offset=")).unwrap_or("-1");
            let live = query.split('&').any(|part| part == "live=long-poll");
            let page = ds
                .pages
                .lock()
                .unwrap()
                .get(&(path, offset.to_string(), live))
                .cloned()
                .unwrap_or_else(|| FakePage::gone(StatusCode::NOT_FOUND));
            if !page.delay.is_zero() {
                tokio::time::sleep(page.delay).await;
            }
            let mut response = (page.status, page.body).into_response();
            for (name, value) in page.headers {
                response.headers_mut().insert(name, value.parse().unwrap());
            }
            response
        }
        Method::HEAD | Method::PUT | Method::POST | Method::DELETE => StatusCode::OK.into_response(),
        _ => StatusCode::METHOD_NOT_ALLOWED.into_response(),
    }
}

async fn engine() -> (Engine, FakeDs) {
    let ds = FakeDs::default();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = Router::new().fallback(durable_streams).with_state(ds.clone());
    tokio::spawn(async move {
        let _ = axum::serve(listener, server).await;
    });
    (Engine::new_for_in_process_test(DsClient::new_for_in_process_test(format!("http://{address}"))), ds)
}

fn transaction(key: &str, lsn: &str, seq: u64, last: bool) -> Envelope {
    Envelope {
        type_: "public.items".to_string(),
        key: key.to_string(),
        value: Some(serde_json::json!({ "id": key })),
        old: None,
        headers: EnvelopeHeaders {
            operation: "insert".to_string(),
            txid: Some(format!("tx-{lsn}")),
            offset: None,
            lsn: Some(lsn.to_string()),
            seq: Some(seq),
            last: last.then_some(true),
            commit_ts: None,
        },
    }
}

fn page(envelopes: &[Envelope]) -> String {
    serde_json::to_string(envelopes).unwrap()
}

async fn get(app: Router, uri: &str) -> Response {
    app.oneshot(axum::http::Request::builder().uri(uri).body(Body::empty()).unwrap()).await.unwrap()
}

#[tokio::test]
async fn returns_the_unmodified_change_log_page_and_stream_headers() {
    let (engine, ds) = engine().await;
    let first = transaction("one", "0/10", 0, true);
    let second = transaction("two", "0/20", 0, true);
    ds.page(
        "changes/0",
        "-1",
        false,
        FakePage {
            headers: vec![("stream-next-offset", "0000000000000000_0000000000000100"), ("stream-up-to-date", "true")],
            ..FakePage::ok(page(&[first, second]))
        },
    );

    let response = get(router(engine), "/changes/0?offset=-1").await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["stream-next-offset"], "0000000000000000_0000000000000100");
    assert_eq!(response.headers()["stream-up-to-date"], "true");
    let bytes = axum::body::to_bytes(response.into_body(), 64 * 1024).await.unwrap();
    let response_page: Vec<Envelope> = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(response_page.len(), 2);
    assert_eq!(response_page[0].key, "one");
    assert_eq!(response_page[1].key, "two");
    assert_eq!(response_page[0].headers.last, Some(true));
    assert_eq!(response_page[1].headers.last, Some(true));
    assert_eq!(response_page[0].headers.lsn.as_deref(), Some("0/10"));
    assert_eq!(response_page[1].headers.seq, Some(0));
}

#[tokio::test]
async fn a_closed_drained_segment_exposes_its_pointer_and_the_successor_page() {
    let (engine, ds) = engine().await;
    let pointer = rotation_envelope(1);
    ds.page(
        "changes/0",
        "tail",
        false,
        FakePage {
            headers: vec![("stream-next-offset", "tail"), ("stream-closed", "true")],
            ..FakePage::ok(page(&[pointer]))
        },
    );
    let next = transaction("after-rotation", "0/30", 0, true);
    ds.page("changes/1", "-1", false, FakePage::ok(page(&[next])));

    let closed = get(router(engine.clone()), "/changes/0?offset=tail").await;
    assert_eq!(closed.status(), StatusCode::OK);
    assert_eq!(closed.headers()["stream-closed"], "true");
    let body = axum::body::to_bytes(closed.into_body(), 64 * 1024).await.unwrap();
    let page: Vec<Envelope> = serde_json::from_slice(&body).unwrap();
    assert_eq!(page.len(), 1);
    assert_eq!(page[0].type_, CONTROL_TYPE);
    assert_eq!(page[0].value.as_ref().unwrap()["next"], "changes/1");

    let successor = get(router(engine), "/changes/1?offset=-1").await;
    assert_eq!(successor.status(), StatusCode::OK);
    let body = axum::body::to_bytes(successor.into_body(), 64 * 1024).await.unwrap();
    let page: Vec<Envelope> = serde_json::from_slice(&body).unwrap();
    assert_eq!(page.into_iter().map(|env| env.key).collect::<Vec<_>>(), vec!["after-rotation"]);
}

#[tokio::test]
async fn missing_deleted_and_stale_generation_positions_have_distinct_terminal_results() {
    let (engine, ds) = engine().await;
    ds.page("changes/7", "-1", false, FakePage::gone(StatusCode::NOT_FOUND));
    ds.page("changes/3", "-1", false, FakePage::gone(StatusCode::GONE));
    ds.page("changes/0", "-1", false, FakePage::ok(page(&[transaction("current", "0/40", 0, true)])));
    let app = router(engine);

    assert_eq!(get(app.clone(), "/changes/7?offset=-1").await.status(), StatusCode::NOT_FOUND);
    assert_eq!(get(app.clone(), "/changes/3?offset=-1").await.status(), StatusCode::GONE);

    let stale = get(app, "/changes/0?offset=-1&generation=previous-query-generation").await;
    assert_eq!(stale.status(), StatusCode::GONE, "a cursor from a prior generation must re-sync, not look missing");
    let body = axum::body::to_bytes(stale.into_body(), 64 * 1024).await.unwrap();
    assert!(String::from_utf8_lossy(&body).contains("stale-generation"), "gone reason must be named");
}

#[tokio::test]
async fn position_names_the_current_generation_tail_and_known_segments() {
    let (engine, _ds) = engine().await;
    let response = get(router(engine), "/changes/position").await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), 64 * 1024).await.unwrap();
    let position: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(position["generation"], "test-query");
    assert_eq!(position["position"], serde_json::json!({ "segment": 0, "offset": "-1" }));
    assert_eq!(position["segments"], serde_json::json!({}));
}

#[tokio::test]
async fn an_external_consumer_pin_is_explicit_monotone_and_listed() {
    let (engine, _ds) = engine().await;
    let app = router(engine);
    let pin = app
        .clone()
        .oneshot(
            axum::http::Request::builder()
                .method(Method::PUT)
                .uri("/consumers/kernel")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"segment":0,"offset":"tail"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(pin.status(), StatusCode::OK);
    let listed = get(app.clone(), "/consumers").await;
    let bytes = axum::body::to_bytes(listed.into_body(), 64 * 1024).await.unwrap();
    assert_eq!(serde_json::from_slice::<serde_json::Value>(&bytes).unwrap()[0]["position"]["offset"], "tail");
    let backwards = app
        .oneshot(
            axum::http::Request::builder()
                .method(Method::PUT)
                .uri("/consumers/kernel")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"segment":0,"offset":"-1"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(backwards.status(), StatusCode::CONFLICT);
}

#[tokio::test]
async fn live_long_poll_uses_the_shared_deadline_and_returns_up_to_date() {
    // This binary is the first route test to ask for a live deadline. Set it before the request;
    // the config caches it exactly as shape reads do.
    unsafe { std::env::set_var("ELECTRIC_LIVE_TIMEOUT_MS", "100") };
    let (engine, ds) = engine().await;
    ds.page(
        "changes/0",
        "tail",
        true,
        FakePage {
            status: StatusCode::NO_CONTENT,
            headers: vec![("stream-next-offset", "tail"), ("stream-up-to-date", "true")],
            body: String::new(),
            delay: Duration::from_secs(10),
        },
    );

    let started = Instant::now();
    let response = get(router(engine), "/changes/0?offset=tail&live=long-poll").await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["stream-up-to-date"], "true");
    assert!(started.elapsed() < Duration::from_secs(2), "change-log long poll must use ELECTRIC_LIVE_TIMEOUT_MS");
}

#[tokio::test]
async fn reference_consumer_holds_a_chunked_transaction_until_last_and_crosses_only_closed_and_drained() {
    let (engine, ds) = engine().await;
    let first = transaction("first", "0/50", 0, false);
    let last = transaction("last", "0/50", 1, true);
    ds.page("changes/0", "-1", false, FakePage::ok(page(&[first])));
    ds.page(
        "changes/0",
        "page-1",
        false,
        FakePage {
            headers: vec![("stream-next-offset", "page-2"), ("stream-closed", "true")],
            ..FakePage::ok(page(&[last, rotation_envelope(1)]))
        },
    );
    let app = router(engine);
    let mut consumer = ReferenceConsumer::default();

    let first_page = get(app.clone(), "/changes/0?offset=-1").await;
    let first_page = ChangePage::from_response(first_page).await.unwrap();
    assert_eq!(consumer.consume(first_page), None, "an unmarked chunk is held, never emitted");
    assert!(consumer.transactions().is_empty());

    let second_page = get(app, "/changes/0?offset=page-1").await;
    let second_page = ChangePage::from_response(second_page).await.unwrap();
    assert_eq!(consumer.consume(second_page), Some(1), "a reader crosses only after draining a closed segment");
    assert_eq!(consumer.transactions().len(), 1);
    assert_eq!(
        consumer.transactions()[0].iter().map(|env| env.key.as_str()).collect::<Vec<_>>(),
        vec!["first", "last"]
    );

    // At-least-once delivery may replay both pages. `(lsn, seq)` highwater prevents a second
    // completed transaction while retaining the original held-run rule.
    let replay = ChangePage {
        envelopes: vec![transaction("first", "0/50", 0, false), transaction("last", "0/50", 1, true)],
        closed: false,
    };
    assert_eq!(consumer.consume(replay), None);
    assert_eq!(consumer.transactions().len(), 1);
}
