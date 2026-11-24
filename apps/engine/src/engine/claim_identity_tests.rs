//! A claim's identity on a single shape: the `claim` a create answers, a renewal names and a
//! release names (ADR-0008, "A claim has an identity").
//!
//! A subscription id is the caller's to choose, so two sessions that use one id (a device id, a
//! stored id a later launch reuses) share one claim on one shape, exactly as a group's sessions
//! do. Every test drives the native routes over an engine with no Postgres: a changes-only feed
//! reads no snapshot, so its create, its renewal and its release all answer here.
use super::*;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use tower::ServiceExt;

const DEADLINE: Duration = Duration::from_secs(10);

/// An engine over a scripted store that knows `items`.
async fn engine() -> Engine {
    let store = Arc::new(crate::ds::ScriptedStore { read_appended: true, ..Default::default() });
    let engine = Engine::new_for_in_process_test(DsClient::with_test_store("scripted://claim-identity".into(), store));
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
    let engine = engine().await;
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

    // B's own release names B's claim, and drops it.
    assert_eq!(release(&engine, &shape, "device", Some(&claim_b)).await, StatusCode::OK);
    assert_eq!(held(&engine, &shape).await, Some(0));
    engine.close(Duration::from_secs(1)).await;
}

/// A is still held when B creates the same feed under the same id, so B's create is a renewal of
/// a held claim. B named no claim, so the claim is B's from then on: A's release, arriving late,
/// names a claim nobody holds any more.
#[tokio::test(flavor = "multi_thread")]
async fn a_late_release_of_a_claim_a_later_session_took_over_changes_nothing() {
    let engine = engine().await;
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
    let engine = engine().await;
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
    let engine = engine().await;
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

/// A group under `scope` the way a completed load leaves it: two member shapes on one stream, one
/// claim per member, and the group's claim given an identity by its public member's create.
async fn engine_with_group() -> (Engine, GroupRecord, String) {
    const STREAM: &str = "shape/group-claim";
    let store = Arc::new(crate::ds::ScriptedStore {
        read_appended: true,
        streams: Some(Default::default()),
        ..Default::default()
    });
    let engine = Engine::new_for_in_process_test(DsClient::with_test_store("scripted://group-claim".into(), store));
    engine.ds.ensure_stream(STREAM).await.unwrap();
    let members = [("member-0", "scope", "items"), ("member-1", "scope:member:1", "notes")];
    let now = crate::changelog::now_secs();
    let mut st = engine.state.lock().await;
    for (id, subscription, table) in members {
        st.shapes.insert(
            id.to_string(),
            ShapeRecord {
                id: id.to_string(),
                table: TableRef::parse(table).unwrap(),
                stream_path: STREAM.to_string(),
                changes_only: true,
                where_json: None,
                columns: None,
                family_key: None,
                is_subquery: false,
                aggregate: None,
                fingerprint: None,
                backfill_rows: None,
                backfill_bytes: None,
            },
        );
        let (ready_tx, ready) = tokio::sync::watch::channel(ShareOutcome::Ready);
        drop(ready_tx);
        st.feed_shares
            .insert(id.to_string(), FeedShare { sig: format!("group-{id}"), subs: Default::default(), ready });
        st.subscribe(id, subscription.to_string(), now);
    }
    let created = st.grant_claim("scope", &ClaimAsk::Keep, true).id;
    let group = GroupRecord {
        id: STREAM.into(),
        stream_path: STREAM.into(),
        subscription: "scope".into(),
        members: members
            .iter()
            .map(|(id, subscription, table)| GroupMember {
                key: table.to_string(),
                shape_id: id.to_string(),
                table: table.to_string(),
                where_: None,
                columns: None,
                subscription: subscription.to_string(),
                is_subquery: false,
            })
            .collect(),
        lsn: "0/0".into(),
        snapshot: String::new(),
        horizon: "0/0".into(),
        replay_fence: "0".into(),
    };
    st.group_claims.reserve("scope", STREAM).unwrap();
    st.group_claims.reserve_private_members("scope", ["scope:member:1".to_string()]);
    st.group_claims.complete(group.clone());
    drop(st);
    (engine, group, created)
}

/// A group's claim is one claim with one identity, held on its public subscription, and a load
/// renews it as a create renews a feed's: naming the identity keeps it, naming none (a later
/// session's load) is answered a new one. A release a member's id carries for the group then
/// counts only against the identity it names.
#[tokio::test(flavor = "multi_thread")]
async fn a_late_release_of_an_earlier_group_claim_leaves_the_group_a_later_load_took() {
    let (engine, group, claim_a) = engine_with_group().await;
    let kept = engine.renew_group_claim_as(&group, &ClaimAsk::Native(Some(claim_a.clone()))).await.unwrap();
    assert_eq!(kept.as_deref(), Some(claim_a.as_str()), "a load by the claim's holder keeps its identity");
    // Session B loads the same group under the same subscription, naming no claim.
    let claim_b = engine.renew_group_claim_as(&group, &ClaimAsk::Native(None)).await.unwrap().expect("renewed");
    assert_ne!(claim_a, claim_b, "a load that names no claim is a new session's");

    let stale = format!("/v1/shapes/member-0?subscription=scope&claim={claim_a}");
    assert_eq!(send(&engine, Request::delete(stale).body(Body::empty()).unwrap()).await.status, StatusCode::OK);

    assert!(engine.group_record("scope").await.is_some(), "B's group is still held");
    assert_eq!(held(&engine, "member-0").await, Some(1));
    assert_eq!(held(&engine, "member-1").await, Some(1));

    let own = format!("/v1/shapes/member-0?subscription=scope&claim={claim_b}");
    assert_eq!(send(&engine, Request::delete(own).body(Body::empty()).unwrap()).await.status, StatusCode::OK);
    assert!(engine.group_record("scope").await.is_none(), "B's release names B's claim, and drops the group");
    engine.close(Duration::from_secs(1)).await;
}
