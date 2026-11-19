//! `POST /v1/subsets/rejoin`: claim or renew the group a subscription already holds, without
//! reading a row.
//!
//! Every test drives the route itself over an engine that has **no Postgres**: a load could not
//! answer here, so a `200` is proof that the re-join read nothing. The group is left the way a
//! completed load leaves it (member shapes on one stream, one claim per member, the durable
//! record), and the store is the scripted one, so every catalog record the engine writes is seen.
use super::*;
use crate::ds::ScriptedStore;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use tower::ServiceExt;

const DEADLINE: Duration = Duration::from_secs(10);
const SUBSCRIPTION: &str = "scope";
const GROUP_STREAM: &str = "shape/group-rejoin";

fn store() -> Arc<ScriptedStore> {
    Arc::new(ScriptedStore { read_appended: true, streams: Some(Default::default()), ..Default::default() })
}

/// `events.record_id = <id>`: the first member's predicate.
fn on_record(id: i64) -> PredicateJson {
    serde_json::from_value(serde_json::json!({"col": "record_id", "op": "eq", "value": id})).unwrap()
}

/// An engine holding one group under `scope`: `events` filtered to record 1, and `notes`.
async fn engine_with_group(store: &Arc<ScriptedStore>) -> (Engine, GroupRecord) {
    let ds = DsClient::with_test_store("scripted://group-rejoin".into(), store.clone());
    let engine = Engine::new_for_in_process_test(ds);
    // No clock-driven sweeps: nothing here may lapse a lease or write a record of its own.
    engine.retention_started.store(true, Ordering::SeqCst);
    let compiled = crate::subquery::tests::restart_schemas();
    engine.state.lock().await.tables = compiled.clone();
    *engine.tables_shared.write().unwrap() = compiled;
    engine.ds.ensure_stream(GROUP_STREAM).await.unwrap();

    let members =
        [("member-0", SUBSCRIPTION, "events", Some(on_record(1))), ("member-1", "scope:member:1", "notes", None)];
    let leased = crate::changelog::now_secs() - 20;
    let mut st = engine.state.lock().await;
    for (id, subscription, table, where_json) in &members {
        st.shapes.insert(
            id.to_string(),
            ShapeRecord {
                id: id.to_string(),
                table: TableRef::from(*table),
                stream_path: GROUP_STREAM.to_string(),
                changes_only: true,
                where_json: where_json.clone(),
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
        st.subscribe(id, subscription.to_string(), leased);
    }
    let group = GroupRecord {
        id: GROUP_STREAM.into(),
        stream_path: GROUP_STREAM.into(),
        subscription: SUBSCRIPTION.into(),
        members: members
            .iter()
            .map(|(id, subscription, table, where_json)| GroupMember {
                key: table.to_string(),
                shape_id: id.to_string(),
                table: TableRef::from(*table).to_string(),
                where_: where_json.clone(),
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
    st.group_claims.reserve(SUBSCRIPTION, GROUP_STREAM).unwrap();
    st.group_claims.reserve_private_members(SUBSCRIPTION, ["scope:member:1".to_string()]);
    st.group_claims.complete(group.clone());
    drop(st);
    (engine, group)
}

/// The body a load of the fixture's group sends, which is the body its re-join sends.
fn request(subscription: &str) -> serde_json::Value {
    serde_json::json!({
        "subscription": subscription,
        "members": [
            {"key": "events", "table": "events", "where": {"col": "record_id", "op": "eq", "value": 1}},
            {"key": "notes", "table": "notes"},
        ],
    })
}

struct Answer {
    status: StatusCode,
    retry_after: Option<String>,
    body: serde_json::Value,
}

async fn post(engine: &Engine, path: &str, body: serde_json::Value) -> Answer {
    let request =
        Request::post(path).header("content-type", "application/json").body(Body::from(body.to_string())).unwrap();
    let response = tokio::time::timeout(DEADLINE, crate::http::router(engine.clone()).oneshot(request))
        .await
        .expect("the route must answer")
        .unwrap();
    let status = response.status();
    let retry_after = response.headers().get("retry-after").map(|value| value.to_str().unwrap().to_string());
    let bytes = axum::body::to_bytes(response.into_body(), 1 << 20).await.unwrap();
    Answer { status, retry_after, body: serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null) }
}

async fn rejoin(engine: &Engine, body: serde_json::Value) -> Answer {
    post(engine, "/v1/subsets/rejoin", body).await
}

/// Each member's lease time, in member order; `None` for a claim nobody holds.
async fn leases(engine: &Engine, group: &GroupRecord) -> Vec<Option<u64>> {
    let st = engine.state.lock().await;
    group
        .members
        .iter()
        .map(|member| st.feed_shares[&member.shape_id].subs.get(&member.subscription).copied())
        .collect()
}

/// Every catalog record the engine has appended, in order.
fn catalog(store: &ScriptedStore) -> Vec<serde_json::Value> {
    store
        .appended
        .lock()
        .unwrap()
        .iter()
        .filter(|(path, _, _)| path.contains("catalog"))
        .flat_map(|(_, _, body)| serde_json::from_slice::<Vec<serde_json::Value>>(body).unwrap_or_default())
        .collect()
}

fn kinds(events: &[serde_json::Value]) -> Vec<String> {
    events.iter().map(|event| event["t"].as_str().unwrap_or_default().to_string()).collect()
}

/// What the engine holds that a create would have added to: shapes, group records, reservations.
async fn registered(engine: &Engine) -> (usize, usize, bool) {
    let st = engine.state.lock().await;
    (st.shapes.len(), st.group_claims.records_iter().count(), st.group_claims.is_reserved("nobody"))
}

#[tokio::test(flavor = "multi_thread")]
async fn a_rejoin_takes_a_released_group_again_and_reads_no_rows() {
    let store = store();
    let (engine, group) = engine_with_group(&store).await;
    engine.release_group_lease(SUBSCRIPTION, &group.id).await.unwrap();
    assert_eq!(leases(&engine, &group).await, vec![None, None], "the release gave every claim back");
    let before = catalog(&store).len();

    let answer = rejoin(&engine, request(SUBSCRIPTION)).await;

    assert_eq!(answer.status, StatusCode::OK, "{}", answer.body);
    // A load's answer less the snapshot.
    let mut fields: Vec<&str> = answer.body.as_object().unwrap().keys().map(String::as_str).collect();
    fields.sort_unstable();
    assert_eq!(
        fields,
        ["claim", "groupId", "leaseSeconds", "offset", "results", "shapeId", "streamPath", "streamUrl", "subscription"],
        "no rows, lsn, snapshot or horizon: the re-join read nothing"
    );
    assert_eq!(answer.body["groupId"], group.id);
    assert_eq!(answer.body["shapeId"], group.id);
    assert_eq!(answer.body["subscription"], SUBSCRIPTION);
    assert_eq!(answer.body["leaseSeconds"], engine.lease_seconds());
    assert_eq!(answer.body["streamPath"], GROUP_STREAM);
    assert_eq!(answer.body["streamUrl"], engine.stream_url(GROUP_STREAM));
    assert_eq!(answer.body["offset"], engine.stream_tail(GROUP_STREAM).await.unwrap().unwrap());
    assert_eq!(
        answer.body["results"],
        serde_json::json!([
            {"key": "events", "table": "public.events", "shapeId": "member-0", "subscription": "scope"},
            {"key": "notes", "table": "public.notes", "shapeId": "member-1", "subscription": "scope:member:1"},
        ]),
        "the members in request order"
    );

    // The claims are held again, and each new claim was durable before the answer (ADR-0008).
    assert!(leases(&engine, &group).await.iter().all(Option::is_some), "every member is claimed again");
    assert_eq!(engine.subscription_owner(SUBSCRIPTION).await.as_deref(), Some("member-0"));
    assert_eq!(kinds(&catalog(&store)[before..]), ["joined", "joined"]);

    // The control: this engine has no Postgres, so the load that names the same group cannot
    // answer. The re-join above did, because it asked Postgres nothing.
    let load = post(&engine, "/v1/subsets/load", request(SUBSCRIPTION)).await;
    assert_ne!(load.status, StatusCode::OK, "a load reads the members' rows: {}", load.body);
    engine.close(Duration::from_secs(1)).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_rejoin_renews_the_lease_of_a_group_that_is_held() {
    let store = store();
    let (engine, group) = engine_with_group(&store).await;
    let touched =
        std::time::Instant::now().checked_sub(Duration::from_secs(60)).expect("the host has been up a minute");
    for member in &group.members {
        engine
            .lives
            .lock()
            .unwrap()
            .insert(member.shape_id.clone(), ShapeLife { last_read: touched, state: LifeState::Active });
    }
    let before = leases(&engine, &group).await;
    let records = catalog(&store).len();

    let answer = rejoin(&engine, request(SUBSCRIPTION)).await;

    assert_eq!(answer.status, StatusCode::OK, "{}", answer.body);
    assert_eq!(answer.body["groupId"], group.id);
    let after = leases(&engine, &group).await;
    assert!(
        after.iter().zip(&before).all(|(new, old)| new.unwrap() > old.unwrap()),
        "every member's lease moves: {before:?} -> {after:?}"
    );
    // A renewal is a touch: it is the only sign of life the group's reader gives the engine.
    for member in &group.members {
        let idle = engine.lives.lock().unwrap()[&member.shape_id].last_read.elapsed();
        assert!(idle < Duration::from_secs(30), "{} was last touched {idle:?} ago", member.shape_id);
    }
    // A renewal promises nothing new, so its records are queued; wait for them like any reader.
    engine.catalog_tx.wait_durable().await.unwrap();
    assert_eq!(kinds(&catalog(&store)[records..]), ["joined", "joined"], "one lease record per member");
    assert_eq!(engine.state.lock().await.shapes.len(), 2, "a renewal mints nothing");
    engine.close(Duration::from_secs(1)).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_rejoin_of_a_subscription_with_no_group_says_so_and_creates_nothing() {
    let store = store();
    let (engine, group) = engine_with_group(&store).await;
    let registered_before = registered(&engine).await;
    let leases_before = leases(&engine, &group).await;
    let records = catalog(&store).len();

    let answer = rejoin(&engine, request("nobody")).await;

    assert_eq!(answer.status, StatusCode::PRECONDITION_FAILED, "{}", answer.body);
    assert_eq!(answer.retry_after, None, "no such group is not a retry");
    assert!(answer.body["error"].as_str().is_some_and(|error| error.contains("nobody")), "{}", answer.body);
    assert_eq!(registered(&engine).await, registered_before, "no shape, group or reservation was made");
    assert_eq!(engine.subscription_owner("nobody").await, None, "no claim was taken");
    assert_eq!(leases(&engine, &group).await, leases_before, "another subscription's group is untouched");
    engine.catalog_tx.wait_durable().await.unwrap();
    assert_eq!(catalog(&store).len(), records, "nothing was written");
    engine.close(Duration::from_secs(1)).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_rejoin_that_names_other_members_says_no_such_group_and_leaves_the_group() {
    let store = store();
    let (engine, group) = engine_with_group(&store).await;
    engine.release_group_lease(SUBSCRIPTION, &group.id).await.unwrap();
    let records = catalog(&store).len();

    let other_predicate = serde_json::json!({
        "subscription": SUBSCRIPTION,
        "members": [
            {"key": "events", "table": "events", "where": {"col": "record_id", "op": "eq", "value": 2}},
            {"key": "notes", "table": "notes"},
        ],
    });
    let fewer = serde_json::json!({
        "subscription": SUBSCRIPTION,
        "members": [{"key": "events", "table": "events", "where": {"col": "record_id", "op": "eq", "value": 1}}],
    });
    let reordered = serde_json::json!({
        "subscription": SUBSCRIPTION,
        "members": [
            {"key": "notes", "table": "notes"},
            {"key": "events", "table": "events", "where": {"col": "record_id", "op": "eq", "value": 1}},
        ],
    });
    for (what, body) in [("another predicate", other_predicate), ("fewer members", fewer), ("another order", reordered)]
    {
        let answer = rejoin(&engine, body).await;
        assert_eq!(answer.status, StatusCode::PRECONDITION_FAILED, "{what}: {}", answer.body);
    }

    // Unlike a load, which replaces a group nobody holds when asked for other members, the
    // re-join changed nothing: the group is still there, unclaimed, and nothing was written.
    assert_eq!(engine.group_record(SUBSCRIPTION).await.map(|record| record.id), Some(group.id.clone()));
    assert_eq!(leases(&engine, &group).await, vec![None, None]);
    assert_eq!(engine.state.lock().await.shapes.len(), 2);
    engine.catalog_tx.wait_durable().await.unwrap();
    assert_eq!(catalog(&store).len(), records, "nothing was written");
    // And its own members still re-join it.
    assert_eq!(rejoin(&engine, request(SUBSCRIPTION)).await.status, StatusCode::OK);
    engine.close(Duration::from_secs(1)).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_rejoin_of_a_group_whose_stream_is_closed_takes_no_claim() {
    let store = store();
    let (engine, group) = engine_with_group(&store).await;
    engine.release_group_lease(SUBSCRIPTION, &group.id).await.unwrap();
    // The stream closed under the group (its retirement began in another process, say): the
    // group can no longer serve a reader, so there is nothing to re-join.
    engine.ds.close_stream(GROUP_STREAM).await.unwrap();

    let answer = rejoin(&engine, request(SUBSCRIPTION)).await;

    assert_eq!(answer.status, StatusCode::PRECONDITION_FAILED, "{}", answer.body);
    assert_eq!(leases(&engine, &group).await, vec![None, None], "no claim on a group that cannot be read");
    engine.close(Duration::from_secs(1)).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_rejoin_while_a_load_of_the_subscription_is_in_flight_is_retried() {
    let store = store();
    let (engine, _) = engine_with_group(&store).await;
    // A load under `loading` has reserved its claim and is still creating its members.
    engine.reserve_group_with_id("loading", &[], "shape/loading").await.unwrap();

    let answer = rejoin(&engine, request("loading")).await;

    assert_eq!(answer.status, StatusCode::SERVICE_UNAVAILABLE, "{}", answer.body);
    assert_eq!(answer.retry_after.as_deref(), Some("1"));
    assert!(engine.group_reservation_for_test("loading").await, "the load's reservation is untouched");
    engine.close(Duration::from_secs(1)).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_rejoin_never_answers_a_group_whose_teardown_has_begun() {
    let store = store();
    let (engine, group) = engine_with_group(&store).await;
    engine.release_group_lease(SUBSCRIPTION, &group.id).await.unwrap();
    // A teardown owns the group from the moment it is recorded here.
    let release = GroupRelease::new();
    engine.group_releases.lock().unwrap().insert(group.id.clone(), release.clone());
    let asked = tokio::spawn({
        let engine = engine.clone();
        async move { rejoin(&engine, request(SUBSCRIPTION)).await }
    });
    // The teardown finishes: the record goes, then its waiters are told.
    engine.take_group(SUBSCRIPTION, &group.id).await;
    release.complete(&Ok(()));
    engine.group_releases.lock().unwrap().remove(&group.id);

    let answer = asked.await.unwrap();

    assert_eq!(answer.status, StatusCode::PRECONDITION_FAILED, "{}", answer.body);
    assert_eq!(leases(&engine, &group).await, vec![None, None], "no claim was taken on the dying group");
    engine.close(Duration::from_secs(1)).await;
}

/// A request that names the claim its caller holds: the `claim` query parameter.
fn with_claim(path: &str, claim: Option<&str>) -> String {
    match claim {
        Some(claim) if path.contains('?') => format!("{path}&claim={claim}"),
        Some(claim) => format!("{path}?claim={claim}"),
        None => path.to_string(),
    }
}

/// A re-join by a caller that holds `claim` (a renewal), or holds none (a new session).
async fn rejoin_as(engine: &Engine, claim: Option<&str>) -> Answer {
    post(engine, &with_claim("/v1/subsets/rejoin", claim), request(SUBSCRIPTION)).await
}

/// The identity of the claim an answer granted. A string when the engine answered none, so that a
/// test of what a release does with it fails on the release and not here.
fn claim_of(answer: &Answer) -> String {
    assert_eq!(answer.status, StatusCode::OK, "{}", answer.body);
    answer.body["claim"].as_str().unwrap_or("no-claim-answered").to_string()
}

/// `DELETE` of the group under its subscription, naming `claim` when there is one.
async fn release_as(engine: &Engine, claim: Option<&str>) -> StatusCode {
    let path = with_claim("/v1/shapes/shape%2Fgroup-rejoin?subscription=scope", claim);
    let request = Request::delete(path).body(Body::empty()).unwrap();
    crate::http::router(engine.clone()).oneshot(request).await.unwrap().status()
}

/// How many claims each member holds, as `GET /v1/shapes/{id}` reports it.
async fn held(engine: &Engine, group: &GroupRecord) -> Vec<u64> {
    let mut counts = Vec::new();
    for member in &group.members {
        let request = Request::get(format!("/v1/shapes/{}", member.shape_id)).body(Body::empty()).unwrap();
        let response = crate::http::router(engine.clone()).oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK, "{} is registered", member.shape_id);
        let bytes = axum::body::to_bytes(response.into_body(), 1 << 20).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        counts.push(body["subscriptions"].as_u64().unwrap());
    }
    counts
}

/// The fixture's engine at a shape cap of its group's two members, each last touched a minute
/// ago: one more shape needs the group's room, and gets it only when nothing holds the group.
async fn engine_at_the_cap(store: &Arc<ScriptedStore>) -> (Engine, GroupRecord) {
    let (mut engine, group) = engine_with_group(store).await;
    engine.retention = Arc::new(RetentionConfig { max_shapes: 2, ..RetentionConfig::default() });
    let touched =
        std::time::Instant::now().checked_sub(Duration::from_secs(60)).expect("the host has been up a minute");
    for member in &group.members {
        engine
            .lives
            .lock()
            .unwrap()
            .insert(member.shape_id.clone(), ShapeLife { last_read: touched, state: LifeState::Active });
    }
    (engine, group)
}

/// What a create at the cap does before it is refused or admitted: evict what the cap may evict
/// to fit one more shape. Answers how many of the group's members are left.
async fn members_left_after_a_create_at_the_cap(engine: &Engine) -> usize {
    engine.make_room_for_group(1).await;
    engine.state.lock().await.shapes.len()
}

/// The race this identity closes. A group's sessions share one subscription, so a release names
/// the claim it was issued for: session A's, retried after session B re-joined, changes nothing.
#[tokio::test(flavor = "multi_thread")]
async fn a_retried_release_of_an_earlier_claim_leaves_the_claim_a_later_session_holds() {
    let store = store();
    let (engine, group) = engine_at_the_cap(&store).await;
    // Session A takes the group's claim and stops cleanly.
    let claim_a = claim_of(&rejoin_as(&engine, None).await);
    assert_eq!(release_as(&engine, Some(&claim_a)).await, StatusCode::OK);
    assert_eq!(held(&engine, &group).await, [0, 0], "A's release dropped A's claim");
    // Session B, the next launch, re-joins under the same subscription.
    let claim_b = claim_of(&rejoin_as(&engine, None).await);
    assert_eq!(held(&engine, &group).await, [1, 1]);
    let records = catalog(&store).len();

    // A's release arrives again: a retry of a request whose answer was lost.
    assert_eq!(release_as(&engine, Some(&claim_a)).await, StatusCode::OK, "a stale release is still a success");

    assert_eq!(held(&engine, &group).await, [1, 1], "B's claim is still held");
    assert_ne!(claim_a, claim_b, "each session's claim has its own identity");
    engine.catalog_tx.wait_durable().await.unwrap();
    assert_eq!(kinds(&catalog(&store)[records..]), Vec::<String>::new(), "the stale release recorded nothing");
    // Held, so the cap may not evict it: a create at the cap finds no room.
    assert_eq!(members_left_after_a_create_at_the_cap(&engine).await, 2, "a held group is not evicted");
    assert_eq!(engine.group_record(SUBSCRIPTION).await.map(|record| record.id), Some(group.id.clone()));

    // B's own release names B's claim, and drops it.
    assert_eq!(release_as(&engine, Some(&claim_b)).await, StatusCode::OK);
    assert_eq!(held(&engine, &group).await, [0, 0], "the release that names the held claim drops it");
    // Nothing holds the group now, so the cap may take its room.
    assert_eq!(members_left_after_a_create_at_the_cap(&engine).await, 0, "an unheld group is evicted at the cap");
    engine.close(Duration::from_secs(1)).await;
}

/// The same race without the first release landing: A is still held when B re-joins, so B's
/// re-join is a renewal of a held claim. B named no claim, so the claim is B's from then on and
/// A's release, arriving late, names a claim nobody holds any more.
#[tokio::test(flavor = "multi_thread")]
async fn a_late_release_of_a_claim_a_later_session_took_over_changes_nothing() {
    let store = store();
    let (engine, group) = engine_with_group(&store).await;
    let claim_a = claim_of(&rejoin_as(&engine, None).await);
    // B is a new session: it holds no claim to name.
    let claim_b = claim_of(&rejoin_as(&engine, None).await);

    assert_eq!(release_as(&engine, Some(&claim_a)).await, StatusCode::OK);

    assert_eq!(held(&engine, &group).await, [1, 1], "B's claim is still held");
    assert_ne!(claim_a, claim_b, "a re-join that names no claim is a new session's");
    assert_eq!(release_as(&engine, Some(&claim_b)).await, StatusCode::OK);
    assert_eq!(held(&engine, &group).await, [0, 0]);
    engine.close(Duration::from_secs(1)).await;
}

/// A session renews by naming the claim it holds, and the renewal keeps that claim's identity:
/// the release the session sends later, however many renewals after, still names it.
#[tokio::test(flavor = "multi_thread")]
async fn a_renewal_that_names_its_claim_keeps_it_and_the_sessions_release_still_drops_it() {
    let store = store();
    let (engine, group) = engine_with_group(&store).await;
    let claim = claim_of(&rejoin_as(&engine, None).await);
    engine.catalog_tx.wait_durable().await.unwrap();
    let records = catalog(&store).len();

    for _ in 0..3 {
        let renewed = rejoin_as(&engine, Some(&claim)).await;
        assert_eq!(renewed.body["claim"], claim.as_str(), "a renewal by the claim's holder keeps its identity");
    }
    // A renewal that keeps the identity records a lease and nothing else, as before.
    engine.catalog_tx.wait_durable().await.unwrap();
    let renewals = &catalog(&store)[records..];
    assert_eq!(renewals.len(), 6, "one lease record per member and renewal");
    assert!(renewals.iter().all(|record| record.get("claim").is_none()), "{renewals:?}");

    assert_eq!(release_as(&engine, Some(&claim)).await, StatusCode::OK);
    assert_eq!(held(&engine, &group).await, [0, 0], "the release names the claim the renewals kept");
    engine.close(Duration::from_secs(1)).await;
}

/// A renewal that names a claim the subscription no longer holds is another session's: it takes
/// the claim over under a new identity, so the superseded session cannot release it either.
#[tokio::test(flavor = "multi_thread")]
async fn a_renewal_that_names_a_superseded_claim_is_answered_a_new_one() {
    let store = store();
    let (engine, group) = engine_with_group(&store).await;
    let claim_a = claim_of(&rejoin_as(&engine, None).await);
    let claim_b = claim_of(&rejoin_as(&engine, None).await);

    let back = claim_of(&rejoin_as(&engine, Some(&claim_a)).await);

    assert!(back != claim_a && back != claim_b, "a superseded claim is not given back: {back}");
    assert_eq!(release_as(&engine, Some(&claim_b)).await, StatusCode::OK);
    assert_eq!(held(&engine, &group).await, [1, 1], "B's identity was replaced, so its release names nothing held");
    assert_eq!(release_as(&engine, Some(&back)).await, StatusCode::OK);
    assert_eq!(held(&engine, &group).await, [0, 0]);
    engine.close(Duration::from_secs(1)).await;
}

/// A claim's identity is part of the durable record of the claim: an engine that restarts keeps
/// telling a stale release from the held claim's own.
#[tokio::test(flavor = "multi_thread")]
async fn the_record_of_a_new_claim_names_its_identity() {
    let store = store();
    let (engine, group) = engine_with_group(&store).await;
    engine.release_group_lease(SUBSCRIPTION, &group.id).await.unwrap();
    let records = catalog(&store).len();

    let claim = claim_of(&rejoin_as(&engine, None).await);

    // Durable before the answer, like the claim itself: no wait here.
    let joined = &catalog(&store)[records..];
    assert_eq!(kinds(joined), ["joined", "joined"]);
    let public = joined.iter().find(|record| record["subscription"] == SUBSCRIPTION).unwrap();
    assert_eq!(public["claim"], claim.as_str(), "{public}");
    engine.close(Duration::from_secs(1)).await;
}

/// A claim is named with the subscription it belongs to, and by a well-formed identity.
#[tokio::test(flavor = "multi_thread")]
async fn a_malformed_claim_is_refused_and_changes_nothing() {
    let store = store();
    let (engine, group) = engine_with_group(&store).await;
    let delete = |path: &'static str| {
        let engine = engine.clone();
        async move {
            let request = Request::delete(path).body(Body::empty()).unwrap();
            crate::http::router(engine).oneshot(request).await.unwrap().status()
        }
    };

    assert_eq!(delete("/v1/shapes/shape%2Fgroup-rejoin?subscription=scope&claim=").await, StatusCode::BAD_REQUEST);
    assert_eq!(
        delete("/v1/shapes/member-0?claim=abc").await,
        StatusCode::BAD_REQUEST,
        "a claim without its subscription"
    );
    let long = "c".repeat(65);
    assert_eq!(rejoin_as(&engine, Some(&long)).await.status, StatusCode::BAD_REQUEST);
    assert_eq!(held(&engine, &group).await, [1, 1], "a refused request released nothing");
    engine.close(Duration::from_secs(1)).await;
}

/// A release that names no claim is the release an older client or gateway sends, and it does what
/// it did before claims had identities: it drops whatever claim the subscription holds. A group's
/// sessions share the subscription (the next launch re-joins under the one the last one loaded
/// with), so such a release, arriving after a later session re-joined, releases that session's
/// claim. This pins what that costs. The group, its members and its stream are kept, so its
/// reader misses nothing, and the session's next renewal takes the claim again; until then
/// nothing holds the group, which is what lets the shape cap evict it
/// (`conformance-group-rejoin.test.ts`).
#[tokio::test(flavor = "multi_thread")]
async fn a_release_that_names_no_claim_drops_a_later_sessions_claim_until_its_next_rejoin() {
    let store = store();
    let (engine, group) = engine_with_group(&store).await;
    let release = || async {
        let request =
            Request::delete("/v1/shapes/shape%2Fgroup-rejoin?subscription=scope").body(Body::empty()).unwrap();
        crate::http::router(engine.clone()).oneshot(request).await.unwrap().status()
    };
    // The earlier session stops cleanly; the later one re-joins.
    assert_eq!(release().await, StatusCode::OK);
    assert_eq!(rejoin(&engine, request(SUBSCRIPTION)).await.status, StatusCode::OK);
    assert!(leases(&engine, &group).await.iter().all(Option::is_some));

    // The earlier session's release arrives again.
    assert_eq!(release().await, StatusCode::OK);

    assert_eq!(leases(&engine, &group).await, vec![None, None], "the later session's claim is gone");
    assert_eq!(engine.group_record(SUBSCRIPTION).await.map(|record| record.id), Some(group.id.clone()));
    assert_eq!(engine.state.lock().await.shapes.len(), 2, "the members stay registered and live");
    assert!(engine.stream_tail(GROUP_STREAM).await.unwrap().is_some(), "the stream stays open");
    // The later session's next renewal is a re-join of the same group.
    let renewed = rejoin(&engine, request(SUBSCRIPTION)).await;
    assert_eq!(renewed.status, StatusCode::OK, "{}", renewed.body);
    assert_eq!(renewed.body["streamUrl"], engine.stream_url(GROUP_STREAM));
    assert!(leases(&engine, &group).await.iter().all(Option::is_some));
    engine.close(Duration::from_secs(1)).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_rejoin_checks_its_request_as_a_load_does() {
    let store = store();
    let (engine, group) = engine_with_group(&store).await;
    let leases_before = leases(&engine, &group).await;
    let member = serde_json::json!({"key": "notes", "table": "notes"});
    let cases = [
        ("no subscription", serde_json::json!({"members": [member]})),
        ("an empty subscription", serde_json::json!({"subscription": "", "members": [member]})),
        ("no members", serde_json::json!({"subscription": SUBSCRIPTION, "members": []})),
        ("a table twice", serde_json::json!({"subscription": SUBSCRIPTION, "members": [member, member]})),
        ("an unknown table", serde_json::json!({"subscription": SUBSCRIPTION, "members": [{"table": "nowhere"}]})),
        (
            "an unknown column",
            serde_json::json!({"subscription": SUBSCRIPTION, "members": [{"table": "notes", "columns": ["nope"]}]}),
        ),
    ];
    for (what, body) in cases {
        let answer = rejoin(&engine, body).await;
        assert_eq!(answer.status, StatusCode::BAD_REQUEST, "{what}: {}", answer.body);
        assert!(answer.body["error"].is_string(), "{what}: a JSON error body, got {}", answer.body);
    }
    assert_eq!(leases(&engine, &group).await, leases_before, "a refused request renews nothing");
    engine.close(Duration::from_secs(1)).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_degraded_engine_refuses_a_rejoin_as_it_refuses_a_load() {
    let store = store();
    let (engine, _) = engine_with_group(&store).await;
    engine.force_degraded();

    let answer = rejoin(&engine, request(SUBSCRIPTION)).await;

    assert_eq!(answer.status, StatusCode::SERVICE_UNAVAILABLE, "{}", answer.body);
    engine.close(Duration::from_secs(1)).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn the_openapi_document_describes_the_rejoin() {
    let store = store();
    let (engine, _) = engine_with_group(&store).await;
    let response = crate::http::router(engine.clone())
        .oneshot(Request::get("/v1/openapi.json").body(Body::empty()).unwrap())
        .await
        .unwrap();
    let bytes = axum::body::to_bytes(response.into_body(), 4 << 20).await.unwrap();
    let document: serde_json::Value = serde_json::from_slice(&bytes).unwrap();

    let operation = &document["paths"]["/v1/subsets/rejoin"]["post"];
    assert!(operation.is_object(), "POST /v1/subsets/rejoin is documented");
    let mut statuses: Vec<&str> = operation["responses"].as_object().unwrap().keys().map(String::as_str).collect();
    statuses.sort_unstable();
    assert_eq!(statuses, ["200", "400", "412", "415", "422", "503"]);
    let answer = &document["components"]["schemas"]["SubsetRejoinResponse"]["properties"];
    let mut fields: Vec<&str> = answer.as_object().unwrap().keys().map(String::as_str).collect();
    fields.sort_unstable();
    assert_eq!(
        fields,
        ["claim", "groupId", "leaseSeconds", "offset", "results", "shapeId", "streamPath", "streamUrl", "subscription"]
    );
    let release = &document["paths"]["/v1/shapes/{id}"]["delete"]["parameters"];
    let named: Vec<&str> = release.as_array().unwrap().iter().map(|p| p["name"].as_str().unwrap()).collect();
    assert_eq!(named, ["id", "subscription", "claim"], "a release names the claim it was issued for");
    for path in ["/v1/shapes", "/v1/subset-feeds", "/v1/aggregates", "/v1/subsets/load", "/v1/subsets/rejoin"] {
        let parameters = &document["paths"][path]["post"]["parameters"];
        assert_eq!(parameters[0]["name"], "claim", "{path} takes the claim its caller holds");
        assert_eq!(parameters[0]["in"], "query", "{path}");
    }
    for schema in ["ShapeCreatedResponse", "SubsetLoadResponse"] {
        let properties = &document["components"]["schemas"][schema]["properties"];
        assert!(properties["claim"].is_object(), "{schema} answers the claim's identity");
    }
    engine.close(Duration::from_secs(1)).await;
}
