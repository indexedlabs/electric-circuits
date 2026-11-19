//! A claim's identity on a single shape: the `claim` a create answers, a renewal names and a
//! release names (ADR-0008, "A claim has an identity").
//!
//! A subscription id is the caller's to choose, so two sessions that use one id (a device id, a
//! stored id a later launch reuses) share one claim on one shape, exactly as a group's sessions
//! do. Every test drives the native routes over an engine with no Postgres: a changes-only feed
//! reads no snapshot, so its create, its renewal and its release all answer here. The group's
//! half is in `group_rejoin_tests.rs`.
use super::*;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use tower::ServiceExt;

const DEADLINE: Duration = Duration::from_secs(10);

/// An engine over a scripted store that knows `items`, at this shape cap (`0` = none).
async fn engine_with_cap(max_shapes: usize) -> Engine {
    let store = Arc::new(crate::ds::ScriptedStore { read_appended: true, ..Default::default() });
    let mut engine =
        Engine::new_for_in_process_test(DsClient::with_test_store("scripted://claim-identity".into(), store));
    engine.retention = Arc::new(RetentionConfig { max_shapes, ..RetentionConfig::default() });
    let schema: Schema = serde_json::from_value(serde_json::json!({"tables": {
        "items": {"columns": {"id": {"type": "int"}, "n": {"type": "int"}}, "primaryKey": "id"}
    }}))
    .unwrap();
    engine.define_schema(&schema).await.unwrap();
    engine
}

struct Answer {
    status: StatusCode,
    body: serde_json::Value,
}

async fn send(engine: &Engine, request: Request<Body>) -> Answer {
    let response = tokio::time::timeout(DEADLINE, crate::http::router(engine.clone()).oneshot(request))
        .await
        .expect("the route must answer")
        .unwrap();
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), 1 << 20).await.unwrap();
    Answer { status, body: serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null) }
}

/// `POST /v1/subset-feeds` for `items` with `n >= at_least`, by a caller that holds `claim`.
async fn feed(engine: &Engine, at_least: i64, subscription: &str, claim: Option<&str>) -> Answer {
    let path = match claim {
        Some(claim) => format!("/v1/subset-feeds?claim={claim}"),
        None => "/v1/subset-feeds".to_string(),
    };
    let body = serde_json::json!({
        "table": "items",
        "where": {"col": "n", "op": "gte", "value": at_least},
        "subscription": subscription,
    });
    let request =
        Request::post(path).header("content-type", "application/json").body(Body::from(body.to_string())).unwrap();
    send(engine, request).await
}

/// The identity of the claim a create granted. A string when the engine answered none, so that a
/// test of what a release does with it fails on the release and not here.
fn claim_of(answer: &Answer) -> String {
    assert_eq!(answer.status, StatusCode::OK, "{}", answer.body);
    answer.body["claim"].as_str().unwrap_or("no-claim-answered").to_string()
}

fn shape_of(answer: &Answer) -> String {
    assert_eq!(answer.status, StatusCode::OK, "{}", answer.body);
    answer.body["shapeId"].as_str().expect("a create answers its shape").to_string()
}

async fn release(engine: &Engine, shape: &str, subscription: &str, claim: Option<&str>) -> StatusCode {
    let claim = claim.map(|claim| format!("&claim={claim}")).unwrap_or_default();
    let request =
        Request::delete(format!("/v1/shapes/{shape}?subscription={subscription}{claim}")).body(Body::empty()).unwrap();
    send(engine, request).await.status
}

/// How many claims the shape holds, as `GET /v1/shapes/{id}` reports it; `None` once it is gone.
async fn held(engine: &Engine, shape: &str) -> Option<u64> {
    let answer = send(engine, Request::get(format!("/v1/shapes/{shape}")).body(Body::empty()).unwrap()).await;
    (answer.status == StatusCode::OK).then(|| answer.body["subscriptions"].as_u64().unwrap())
}

/// Two sessions share one subscription id on one feed. Session A's release, retried after session
/// B took the claim again, names A's claim and so changes nothing.
#[tokio::test(flavor = "multi_thread")]
async fn a_retried_release_of_an_earlier_claim_leaves_the_claim_a_later_session_holds() {
    // A cap of one: another feed needs this one's room, and gets it only when nothing holds it.
    let engine = engine_with_cap(1).await;
    let created = feed(&engine, 1, "device", None).await;
    let (shape, claim_a) = (shape_of(&created), claim_of(&created));
    assert_eq!(release(&engine, &shape, "device", Some(&claim_a)).await, StatusCode::OK);
    assert_eq!(held(&engine, &shape).await, Some(0), "A's release dropped A's claim");
    // Session B creates the identical feed under the same id: the same shape, claimed again.
    let again = feed(&engine, 1, "device", None).await;
    assert_eq!(shape_of(&again), shape);
    let claim_b = claim_of(&again);

    assert_eq!(release(&engine, &shape, "device", Some(&claim_a)).await, StatusCode::OK, "a stale release succeeds");

    assert_eq!(held(&engine, &shape).await, Some(1), "B's claim is still held");
    assert_ne!(claim_a, claim_b, "each session's claim has its own identity");
    // Held, so the cap may not evict it: one more feed is refused.
    let refused = feed(&engine, 2, "another", None).await;
    assert_eq!(refused.status, StatusCode::SERVICE_UNAVAILABLE, "{}", refused.body);
    assert_eq!(held(&engine, &shape).await, Some(1));

    // B's own release names B's claim, and drops it; the cap may then take the feed's room.
    assert_eq!(release(&engine, &shape, "device", Some(&claim_b)).await, StatusCode::OK);
    assert_eq!(held(&engine, &shape).await, Some(0));
    assert_eq!(feed(&engine, 2, "another", None).await.status, StatusCode::OK);
    assert_eq!(held(&engine, &shape).await, None, "the unheld feed was evicted for the new one");
    engine.close(Duration::from_secs(1)).await;
}

/// A is still held when B creates the same feed under the same id, so B's create is a renewal of
/// a held claim. B named no claim, so the claim is B's from then on: A's release, arriving late,
/// names a claim nobody holds any more.
#[tokio::test(flavor = "multi_thread")]
async fn a_late_release_of_a_claim_a_later_session_took_over_changes_nothing() {
    let engine = engine_with_cap(0).await;
    let created = feed(&engine, 1, "device", None).await;
    let (shape, claim_a) = (shape_of(&created), claim_of(&created));
    let claim_b = claim_of(&feed(&engine, 1, "device", None).await);

    assert_eq!(release(&engine, &shape, "device", Some(&claim_a)).await, StatusCode::OK);

    assert_eq!(held(&engine, &shape).await, Some(1), "B's claim is still held");
    assert_ne!(claim_a, claim_b, "a create that names no claim is a new session's");
    assert_eq!(release(&engine, &shape, "device", Some(&claim_b)).await, StatusCode::OK);
    assert_eq!(held(&engine, &shape).await, Some(0));
    engine.close(Duration::from_secs(1)).await;
}

/// A session renews by repeating its create and naming the claim it holds. The renewal keeps the
/// claim's identity, so the release the session sends later still names it.
#[tokio::test(flavor = "multi_thread")]
async fn a_renewal_that_names_its_claim_keeps_it_and_the_sessions_release_still_drops_it() {
    let engine = engine_with_cap(0).await;
    let created = feed(&engine, 1, "device", None).await;
    let (shape, claim) = (shape_of(&created), claim_of(&created));

    for _ in 0..3 {
        let renewed = feed(&engine, 1, "device", Some(&claim)).await;
        assert_eq!(renewed.body["claim"], claim.as_str(), "a renewal by the claim's holder keeps its identity");
    }

    assert_eq!(held(&engine, &shape).await, Some(1), "a renewal is one claim, not four");
    assert_eq!(release(&engine, &shape, "device", Some(&claim)).await, StatusCode::OK);
    assert_eq!(held(&engine, &shape).await, Some(0), "the release names the claim the renewals kept");
    engine.close(Duration::from_secs(1)).await;
}

/// A release that names no claim is the release an older client sends. It does what it did before
/// claims had identities: it drops whatever claim the subscription holds, whoever took it.
#[tokio::test(flavor = "multi_thread")]
async fn a_release_that_names_no_claim_releases_whatever_claim_the_subscription_holds() {
    let engine = engine_with_cap(0).await;
    let shape = shape_of(&feed(&engine, 1, "device", None).await);
    assert_eq!(release(&engine, &shape, "device", None).await, StatusCode::OK);
    assert_eq!(held(&engine, &shape).await, Some(0));
    // A later session takes the claim again, and the first one's release is retried.
    feed(&engine, 1, "device", None).await;
    assert_eq!(held(&engine, &shape).await, Some(1));

    assert_eq!(release(&engine, &shape, "device", None).await, StatusCode::OK);

    assert_eq!(held(&engine, &shape).await, Some(0), "an unnamed release drops the later session's claim, as before");
    // And another subscription's claim on the same shape is never the one released.
    feed(&engine, 1, "device", None).await;
    let other = claim_of(&feed(&engine, 1, "tablet", None).await);
    assert_eq!(release(&engine, &shape, "device", Some(&other)).await, StatusCode::OK);
    assert_eq!(held(&engine, &shape).await, Some(2), "a claim's identity belongs to its own subscription");
    engine.close(Duration::from_secs(1)).await;
}
