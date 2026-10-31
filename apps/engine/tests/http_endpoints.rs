//! Integration tests for the fleet HTTP surface added to the engine router: `/v1/health` (state
//! machine + exact body + status codes + cache headers), `GET /` (200 empty), the
//! `OPTIONS /v1/shape` CORS preflight, and the liveness/readiness split (`/health` vs `/ready`).
//! The router is driven in-process via `Service::oneshot`; no Postgres or durable-streams server is
//! needed (the health phase is set at Engine construction).

use axum::Router;
use axum::body::Body;
use axum::extract::{Request as AxumRequest, State};
use axum::http::{Method, Request, StatusCode};
use axum::response::{IntoResponse, Response};
use electric_circuits_engine::ds::DsClient;
use electric_circuits_engine::engine::Engine;
use electric_circuits_engine::http::router;
use electric_circuits_engine::schema::Schema;
use tokio::sync::oneshot;
use tower::ServiceExt; // for `oneshot`

fn library_engine() -> Engine {
    Engine::new_for_in_process_test(DsClient::new_for_in_process_test("http://127.0.0.1:1"))
}

#[derive(Clone, Default)]
struct FeedDs;

async fn feed_ds_handler(State(_): State<FeedDs>, request: AxumRequest) -> Response {
    match *request.method() {
        Method::HEAD => [("stream-next-offset", "tip")].into_response(),
        Method::GET => ([("stream-next-offset", "tip"), ("stream-up-to-date", "1")], "[]").into_response(),
        Method::PUT | Method::POST | Method::DELETE => StatusCode::OK.into_response(),
        _ => StatusCode::METHOD_NOT_ALLOWED.into_response(),
    }
}

async fn feed_engine() -> (Engine, oneshot::Sender<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (stop, wait) = oneshot::channel();
    tokio::spawn(async move {
        let server = axum::serve(listener, Router::new().fallback(feed_ds_handler).with_state(FeedDs));
        tokio::select! { _ = server => {}, _ = wait => {} }
    });
    let engine = Engine::new_for_in_process_test(DsClient::new_for_in_process_test(format!("http://{address}")));
    let schema: Schema = serde_json::from_value(serde_json::json!({
        "tables": { "items": { "columns": { "id": {"type":"int"} }, "primaryKey": "id" } }
    }))
    .unwrap();
    engine.define_schema(&schema).await.unwrap();
    (engine, stop)
}

async fn body_string(res: axum::response::Response) -> String {
    let bytes = axum::body::to_bytes(res.into_body(), 64 * 1024).await.unwrap();
    String::from_utf8(bytes.to_vec()).unwrap()
}

#[tokio::test]
async fn health_active_in_library_mode() {
    let res = router(library_engine())
        .oneshot(Request::builder().uri("/v1/health").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(res.headers().get("cache-control").unwrap(), "no-cache, no-store, must-revalidate");
    assert_eq!(res.headers().get("content-type").unwrap(), "application/json");
    assert_eq!(body_string(res).await, r#"{"status":"active"}"#);
}

#[tokio::test]
async fn health_waiting_returns_202_in_pg_mode_before_setup() {
    // new_pg starts `waiting`; without setup_postgres it stays there.
    let engine = Engine::new_pg_for_in_process_test(
        DsClient::new_for_in_process_test("http://127.0.0.1:1"),
        "postgres://x/y".into(),
    );
    assert_eq!(engine.health_status(), "waiting");
    let res = router(engine).oneshot(Request::builder().uri("/v1/health").body(Body::empty()).unwrap()).await.unwrap();
    assert_eq!(res.status(), StatusCode::ACCEPTED);
    assert_eq!(body_string(res).await, r#"{"status":"waiting"}"#);
}

#[tokio::test]
async fn root_returns_200_empty() {
    let res = router(library_engine()).oneshot(Request::builder().uri("/").body(Body::empty()).unwrap()).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert!(body_string(res).await.is_empty());
}

/// The native client contract is served by the engine itself. These routes must be registered on
/// the Axum surface (rather than only on the TypeScript gateway); malformed/library-mode requests
/// may fail validation, but a missing route is a contract failure.
#[tokio::test]
async fn native_v1_routes_are_registered_on_the_engine() {
    let cases = [
        ("POST", "/v1/shapes", r#"{"table":"items"}"#),
        ("GET", "/v1/shapes/s1", ""),
        ("DELETE", "/v1/shapes/s1", ""),
        ("POST", "/v1/subsets/query", r#"{"table":"items"}"#),
        ("POST", "/v1/subset-feeds", r#"{"table":"items"}"#),
        ("POST", "/v1/aggregates", r#"{"table":"items","fn":"count"}"#),
    ];
    for (method, uri, body) in cases {
        let request = Request::builder()
            .method(method)
            .uri(uri)
            .header("content-type", "application/json")
            .body(Body::from(body))
            .unwrap();
        let res = router(library_engine()).oneshot(request).await.unwrap();
        // A missing GET shape is a legitimate 404 from the handler. For the mutating/query
        // routes, a 404 would instead mean the path was never registered.
        if method != "GET" {
            assert_ne!(res.status(), StatusCode::NOT_FOUND, "{method} {uri} must be an engine route");
        }
    }
}

#[tokio::test]
async fn native_openapi_document_describes_the_public_routes() {
    let res = router(library_engine())
        .oneshot(Request::builder().uri("/v1/openapi.json").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(res.headers().get("content-type").unwrap(), "application/json");
    let document: serde_json::Value = serde_json::from_str(&body_string(res).await).unwrap();
    assert_eq!(document["openapi"], "3.0.3");
    for path in ["/v1/shapes", "/v1/shapes/{id}", "/v1/subsets/query", "/v1/subset-feeds", "/v1/aggregates"] {
        assert!(document["paths"].get(path).is_some(), "missing documented path {path}");
    }
    let predicate = &document["components"]["schemas"]["Predicate"];
    assert!(predicate.is_object());
    assert!(predicate["oneOf"].is_array(), "predicate schema must preserve its alternatives");
    assert!(
        serde_json::to_string(predicate).unwrap().contains("#/components/schemas/Predicate"),
        "recursive predicate references must be present in the generated document"
    );
    let leaf = predicate["oneOf"]
        .as_array()
        .unwrap()
        .iter()
        .find(|variant| variant["properties"].get("value").is_some())
        .expect("predicate schema must include its leaf value");
    assert_eq!(leaf["properties"]["value"], serde_json::json!({}));
    let create = &document["paths"]["/v1/shapes"]["post"];
    assert_eq!(
        create["requestBody"]["content"]["application/json"]["schema"]["$ref"],
        "#/components/schemas/ShapeRequest"
    );
    assert_eq!(
        create["responses"]["200"]["content"]["application/json"]["schema"]["$ref"],
        "#/components/schemas/ShapeCreatedResponse"
    );
    let get = &document["paths"]["/v1/shapes/{id}"]["get"];
    assert_eq!(
        get["responses"]["200"]["content"]["application/json"]["schema"]["$ref"],
        "#/components/schemas/ShapeMetadataResponse"
    );
    let delete = &document["paths"]["/v1/shapes/{id}"]["delete"];
    assert!(!delete["parameters"].as_array().unwrap().iter().any(|p| p["name"] == "purge"));
    assert!(delete["responses"].get("404").is_none());
    assert!(delete["responses"].get("503").is_none());
    let feed = &document["paths"]["/v1/subset-feeds"]["post"];
    assert_eq!(
        feed["requestBody"]["content"]["application/json"]["schema"]["$ref"],
        "#/components/schemas/SubsetFeedRequest"
    );
    assert!(document["components"]["schemas"]["SubsetFeedRequest"]["properties"].get("changesOnly").is_none());
    let aggregate = &document["components"]["schemas"]["AggregateRequest"];
    assert_eq!(aggregate["properties"]["fn"]["$ref"], "#/components/schemas/AggregateFunction");
    assert!(document["paths"]["/v1/subsets/query"]["post"]["responses"].get("503").is_some());
    for path in ["/v1/shapes", "/v1/subsets/query", "/v1/subset-feeds", "/v1/aggregates"] {
        let responses = &document["paths"][path]["post"]["responses"];
        for status in ["400", "415", "422"] {
            assert_eq!(
                responses[status]["content"]["application/json"]["schema"]["$ref"],
                "#/components/schemas/ErrorResponse"
            );
        }
    }
}

#[tokio::test]
async fn malformed_native_json_uses_documented_error_contract() {
    let cases = [
        ("{}", Some("application/json"), StatusCode::UNPROCESSABLE_ENTITY),
        ("{", Some("application/json"), StatusCode::BAD_REQUEST),
        (r#"{"table":"items"}"#, Some("text/plain"), StatusCode::UNSUPPORTED_MEDIA_TYPE),
    ];
    for (body, content_type, expected) in cases {
        let mut builder = Request::builder().method(Method::POST).uri("/v1/shapes");
        if let Some(content_type) = content_type {
            builder = builder.header("content-type", content_type);
        }
        let res = router(library_engine()).oneshot(builder.body(Body::from(body)).unwrap()).await.unwrap();
        assert_eq!(res.status(), expected);
        assert!(res.headers().get("content-type").unwrap().to_str().unwrap().starts_with("application/json"));
        let body: serde_json::Value = serde_json::from_str(&body_string(res).await).unwrap();
        assert!(body["error"].as_str().is_some_and(|message| !message.is_empty()));
    }
}

/// Semantic request errors are client-correctable native API errors, not internal failures. The
/// router must preserve both the documented status and the JSON error contract before shape
/// creation can allocate a subscription or touch durable-streams state.
#[tokio::test]
async fn native_shape_rejects_unknown_schema_names_with_documented_error_contract() {
    let (engine, stop) = feed_engine().await;
    let cases = [
        (r#"{"table":"missing"}"#, serde_json::json!({ "error": "unknown table 'public.missing'" })),
        (
            r#"{"table":"items","columns":["missing"]}"#,
            serde_json::json!({ "error": "unknown column 'missing' on table 'public.items'" }),
        ),
    ];
    for (body, expected) in cases {
        let res = router(engine.clone())
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/v1/shapes")
                    .header("content-type", "application/json")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = res.status();
        let body: serde_json::Value = serde_json::from_str(&body_string(res).await).unwrap();
        assert_eq!(body, expected);
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }
    let _ = stop.send(());
}

#[tokio::test]
async fn electric_shape_extractor_rejection_keeps_compatibility_body() {
    let res = router(library_engine())
        .oneshot(Request::builder().uri("/v1/shape").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    assert!(
        res.headers()
            .get("content-type")
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.starts_with("text/plain")),
        "Electric extractor failures retain their existing response media type"
    );
    let body = body_string(res).await;
    assert!(body.contains("Failed to deserialize query string"));
}

#[tokio::test(flavor = "multi_thread")]
async fn subset_feed_route_forces_changes_only() {
    let (engine, stop) = feed_engine().await;
    let res = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        router(engine.clone()).oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/v1/subset-feeds")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"table":"items","changesOnly":false,"subscription":"feed-test"}"#))
                .unwrap(),
        ),
    )
    .await
    .expect("subset feed request must complete")
    .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body: serde_json::Value = serde_json::from_str(&body_string(res).await).unwrap();
    let id = body["shapeId"].as_str().unwrap();

    let rows = router(engine)
        .oneshot(Request::builder().uri(format!("/shapes/{id}/rows")).body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(rows.status(), StatusCode::OK);
    let rows: serde_json::Value = serde_json::from_str(&body_string(rows).await).unwrap();
    assert_eq!(rows["changesOnly"], true, "the subset-feed endpoint must override changesOnly=false");
    let _ = stop.send(());
}

#[tokio::test]
async fn options_shape_is_cors_preflight() {
    let res = router(library_engine())
        .oneshot(Request::builder().method("OPTIONS").uri("/v1/shape").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::NO_CONTENT);
    assert_eq!(res.headers().get("access-control-allow-methods").unwrap(), "GET, POST, HEAD, DELETE, OPTIONS");
}

/// `GET /ready` is the probe a load balancer gates on: 200 only when the engine is actually able
/// to serve. Library mode has nothing to wait for, so it is ready from construction.
#[tokio::test]
async fn ready_is_200_active_in_library_mode() {
    let res =
        router(library_engine()).oneshot(Request::builder().uri("/ready").body(Body::empty()).unwrap()).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(res.headers().get("cache-control").unwrap(), "no-cache, no-store, must-revalidate");
    assert_eq!(body_string(res).await, r#"{"status":"active"}"#);
}

/// External change-log consumers are observability-only: their position and lag are available to a
/// source-status wrapper, but neither a live pin nor an abandoned one changes the engine's health.
#[tokio::test]
async fn change_log_position_and_consumer_listing_do_not_affect_readiness() {
    let (engine, stop) = feed_engine().await;
    let expected_position = engine.changes_position();
    let app = router(engine);

    let position =
        app.clone().oneshot(Request::builder().uri("/changes/position").body(Body::empty()).unwrap()).await.unwrap();
    assert_eq!(position.status(), StatusCode::OK);
    let position: serde_json::Value = serde_json::from_str(&body_string(position).await).unwrap();
    assert_eq!(position["position"], serde_json::to_value(expected_position).unwrap());
    assert!(position["segments"].is_object(), "the wrapper receives the known retained segments");

    let pin = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::PUT)
                .uri("/consumers/kernel")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"segment":0,"offset":"tip"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(pin.status(), StatusCode::OK);
    let consumers =
        app.clone().oneshot(Request::builder().uri("/consumers").body(Body::empty()).unwrap()).await.unwrap();
    assert_eq!(consumers.status(), StatusCode::OK);
    let consumers: serde_json::Value = serde_json::from_str(&body_string(consumers).await).unwrap();
    assert_eq!(
        consumers,
        serde_json::json!([{
            "id": "kernel", "position": { "segment": 0, "offset": "tip" }, "pinned_segments": [0], "lag_segments": 0
        }])
    );

    let ready = app.oneshot(Request::builder().uri("/ready").body(Body::empty()).unwrap()).await.unwrap();
    assert_eq!(ready.status(), StatusCode::OK, "consumer state is not readiness state");
    let _ = stop.send(());
}

/// Postgres mode before `setup_postgres`: NOT ready (503 `waiting`), while `/v1/health` answers its
/// fleet-parity 202 for the same phase. The two probes are deliberately different contracts.
#[tokio::test]
async fn ready_is_503_waiting_before_postgres_is_up() {
    let engine = Engine::new_pg_for_in_process_test(
        DsClient::new_for_in_process_test("http://127.0.0.1:1"),
        "postgres://x/y".into(),
    );
    assert_eq!(engine.readiness_status(), "waiting");
    let res = router(engine).oneshot(Request::builder().uri("/ready").body(Body::empty()).unwrap()).await.unwrap();
    assert_eq!(res.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body_string(res).await, r#"{"status":"waiting"}"#);
}

/// A degraded engine is live but not ready — and `/health` must NOT go with it, or a kubelet would
/// restart a pod whose problem a restart is the documented fix for only when an operator says so.
#[tokio::test]
async fn degraded_is_not_ready_but_is_still_live() {
    let engine = library_engine();
    engine.force_degraded();
    assert_eq!(engine.readiness_status(), "degraded");

    let res =
        router(engine.clone()).oneshot(Request::builder().uri("/ready").body(Body::empty()).unwrap()).await.unwrap();
    assert_eq!(res.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body_string(res).await, r#"{"status":"degraded"}"#);

    let res = router(engine).oneshot(Request::builder().uri("/health").body(Body::empty()).unwrap()).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK, "liveness must not follow readiness");
    assert_eq!(body_string(res).await, "ok");
}

/// The first thing a `SIGTERM` does: `/ready` turns 503 `shutting_down` so a load balancer drains
/// the pod BEFORE anything is wound down. Liveness and `/v1/health` are untouched — the process is
/// still perfectly able to answer what it already accepted.
#[tokio::test]
async fn shutdown_makes_ready_503_before_anything_else_changes() {
    let engine = library_engine();
    engine.shutdown_token().begin();
    assert_eq!(engine.readiness_status(), "shutting_down");
    assert_eq!(engine.health_status(), "active", "shutdown must not rewrite the boot phase");

    let res =
        router(engine.clone()).oneshot(Request::builder().uri("/ready").body(Body::empty()).unwrap()).await.unwrap();
    assert_eq!(res.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body_string(res).await, r#"{"status":"shutting_down"}"#);

    let res =
        router(engine.clone()).oneshot(Request::builder().uri("/health").body(Body::empty()).unwrap()).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    let res = router(engine).oneshot(Request::builder().uri("/v1/health").body(Body::empty()).unwrap()).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK, "the fleet healthcheck is unchanged by shutdown");
    assert_eq!(body_string(res).await, r#"{"status":"active"}"#);
}

/// A NEW `live=true` request during the drain is answered `503` + `Retry-After: 1`, not an empty
/// 204. Electric's client re-polls a 204 immediately, so 204 would turn the drain window into a
/// tight poll loop for every live subscriber; a 5xx is what it backs off on.
#[tokio::test]
async fn a_new_live_poll_during_the_drain_is_told_to_come_back() {
    let engine = library_engine();
    engine.shutdown_token().begin();
    let res = router(engine)
        .oneshot(
            Request::builder().uri("/v1/shape?table=items&handle=h1&offset=0_0&live=true").body(Body::empty()).unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(res.headers().get("retry-after").unwrap(), "1");
    assert!(body_string(res).await.contains("shutting down"));
}

#[tokio::test]
async fn an_electric_snapshot_while_control_admission_is_closed_is_retryable() {
    let (engine, stop) = feed_engine().await;
    engine.close_control_admission();
    let res = router(engine)
        .oneshot(Request::builder().uri("/v1/shape?table=items&offset=-1").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(res.headers().get("retry-after").unwrap(), "1");
    assert!(body_string(res).await.contains("control admission is closed"));
    let _ = stop.send(());
}

/// ...and a NON-live request is not turned away for being non-live: the drain window exists so
/// requests the engine has already accepted still get served. (A snapshot is the exception on both
/// counts: a new one needs control admission, which the flip closes, and one still waiting on a
/// shared build is told to retry — see `apps/engine/src/electric/snapshot_tests.rs`.)
#[tokio::test]
async fn a_non_live_request_during_the_drain_is_served_normally() {
    let engine = library_engine();
    engine.shutdown_token().begin();
    let res = router(engine)
        .oneshot(Request::builder().uri("/v1/shape?table=items&offset=-1").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_ne!(res.status(), StatusCode::SERVICE_UNAVAILABLE, "only live polls are turned away");
}

#[tokio::test]
async fn legacy_health_still_ok() {
    let res =
        router(library_engine()).oneshot(Request::builder().uri("/health").body(Body::empty()).unwrap()).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(body_string(res).await, "ok");
}

/// `DELETE /table/{table}/rows` is registered and validates its input: an unknown table is a 400
/// with an `error` body (not a 404/405, which would mean the route or method is missing).
#[tokio::test]
async fn delete_table_rows_rejects_unknown_table() {
    let res = router(library_engine())
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/table/nope/rows")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"keys":[{"id":1}]}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    assert!(body_string(res).await.contains("unknown table"));
}

const INTROSPECTION_ROUTES: &[&str] = &["/trace", "/graph", "/graph/node", "/state", "/state/node"];

/// `ELECTRIC_CIRCUITS_TRACE=0` (introspection off) removes the visualizer/introspection surface entirely
/// — the routes are never registered, so `/trace` can never gain a subscriber and the hot path
/// keeps its zero-subscriber fast path. Everything else keeps serving.
#[tokio::test]
async fn introspection_disabled_unregisters_viz_routes() {
    use electric_circuits_engine::http::router_with_introspection;
    for route in INTROSPECTION_ROUTES {
        let res = router_with_introspection(library_engine(), false)
            .oneshot(Request::builder().uri(*route).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::NOT_FOUND, "{route} should be unregistered");
    }
    // The rest of the surface is untouched.
    let res = router_with_introspection(library_engine(), false)
        .oneshot(Request::builder().uri("/health").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
}

/// Default (`router`, introspection on): the same routes respond (200, or 400 for the two that
/// require a query param — anything but 404 proves registration).
#[tokio::test]
async fn introspection_enabled_by_default() {
    for route in INTROSPECTION_ROUTES {
        let res = router(library_engine())
            .oneshot(Request::builder().uri(*route).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_ne!(res.status(), StatusCode::NOT_FOUND, "{route} should be registered");
    }
}

/// A degraded engine has lost membership effects it cannot re-derive, so every route that would
/// answer WITH membership must refuse (503 + the typed error body) rather than serve what the
/// engine knows is wrong — while the observability surface stays up, because that is what an
/// operator needs to see the failure and decide to restart.
#[tokio::test]
async fn degraded_refuses_the_membership_routes_and_keeps_observability_up() {
    let engine = library_engine();
    engine.force_degraded();
    let call = async |method: &str, uri: &str, body: &'static str| {
        let req = Request::builder()
            .method(method)
            .uri(uri)
            .header("content-type", "application/json")
            .body(Body::from(body))
            .unwrap();
        router(engine.clone()).oneshot(req).await.unwrap()
    };

    for (method, uri, body) in [
        ("POST", "/shapes", r#"{"table":"t"}"#),
        ("POST", "/aggregate", r#"{"table":"t","fn":"count"}"#),
        ("POST", "/query", r#"{"table":"t"}"#),
        ("GET", "/shapes/s1", ""),
        ("GET", "/shapes/s1/rows", ""),
        ("GET", "/shapes/s1/log", ""),
        ("GET", "/v1/shape?table=t&offset=-1", ""),
    ] {
        let res = call(method, uri, body).await;
        assert_eq!(res.status(), StatusCode::SERVICE_UNAVAILABLE, "{method} {uri} must refuse");
        assert_eq!(
            body_string(res).await,
            r#"{"error":"degraded: subquery membership effects were lost; restart required"}"#,
            "{method} {uri} body"
        );
    }

    // `/v1/health` reports the state (503 + `degraded`), and the barrier endpoint still answers so
    // the held `pendingFlips` and the `flipFailures` count are readable.
    let res = call("GET", "/v1/health", "").await;
    assert_eq!(res.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body_string(res).await, r#"{"status":"degraded"}"#);

    let res = call("GET", "/replication/lsn", "").await;
    assert_eq!(res.status(), StatusCode::OK);
    let v: serde_json::Value = serde_json::from_str(&body_string(res).await).unwrap();
    assert_eq!(v["flipFailures"], 1);

    for uri in ["/metrics", "/memory", "/subqueries", "/graph", "/state", "/health"] {
        assert_eq!(call("GET", uri, "").await.status(), StatusCode::OK, "{uri} must stay up");
    }
}
