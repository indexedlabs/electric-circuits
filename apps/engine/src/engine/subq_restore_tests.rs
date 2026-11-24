//! The catalog restore keeps subquery shapes when the LAST checkpoint names a registry state that
//! verifies, and drops them exactly as before when it does not.
//!
//! Each test is a sequence of engine processes over one store. A process that "stopped gracefully"
//! is modelled by what such a stop leaves durable — the shapes' `Created` records, their streams,
//! the registry state and the final `Offset` naming it — written through the real catalog writer
//! and `subq_state`; the process under test then folds that catalog from storage and restores it.
use super::*;
use crate::ds::ScriptedStore;
use crate::subq_state::{RegistryState, StatePointer};
use crate::subquery::tests::{create_settled, live_events, on_visible_record, restart_schemas};
use std::time::Duration;

const DEADLINE: Duration = Duration::from_secs(10);

fn store() -> Arc<ScriptedStore> {
    Arc::new(ScriptedStore { read_appended: true, streams: Some(Default::default()), ..Default::default() })
}

/// One engine process over `store`, with the three tables of `restart_schemas` compiled. It is a
/// Postgres-mode engine — only those restore from a checkpoint — that never connects: nothing here
/// reads Postgres, which is the point of restoring from saved state.
async fn process(store: &Arc<ScriptedStore>) -> (Engine, HashMap<TableRef, TableSchema>) {
    let ds = DsClient::with_test_store("scripted://subq-restore".into(), store.clone());
    let engine = Engine::new_pg_for_in_process_test(ds, "postgres://postgres@127.0.0.1:1/unreachable".into());
    // No clock-driven sweeps: nothing here may lapse a lease or write a record of its own.
    engine.retention_started.store(true, Ordering::SeqCst);
    let compiled = restart_schemas();
    engine.state.lock().await.tables = compiled.clone();
    *engine.tables_shared.write().unwrap() = compiled.clone();
    engine.subqueries.lock().await.set_schemas(Arc::new(compiled.clone()));
    (engine, compiled)
}

fn created(id: &str, table: &str, stream_path: &str, where_json: &PredicateJson, changes_only: bool) -> CatalogEvent {
    CatalogEvent::Created {
        rec: ShapeRecord {
            id: id.to_string(),
            table: TableRef::from(table),
            stream_path: stream_path.to_string(),
            changes_only,
            where_json: Some(where_json.clone()),
            columns: None,
            family_key: None,
            is_subquery: true,
            aggregate: None,
            fingerprint: None,
            backfill_rows: None,
            backfill_bytes: None,
        },
        sig: Some(format!("sig-{id}")),
        subscription: format!("sub-{id}"),
        at: crate::changelog::now_secs(),
    }
}

fn checkpoint() -> (LogPosition, Option<(u64, u64)>) {
    (LogPosition { segment: 0, offset: "0000000000000000_0000000000000000".into() }, Some((0x10, 1)))
}

fn offset(subq_state: Option<StatePointer>) -> CatalogEvent {
    let (pos, highwater) = checkpoint();
    CatalogEvent::Offset { pos, highwater, groups: Default::default(), subq_state }
}

/// What a process that held `s1` (materialized) and `s2` (changes-only) left behind when it
/// stopped gracefully: their records and streams, and the registry state the final `Offset` names.
/// Returns that state and its pointer.
async fn stopped_gracefully(store: &Arc<ScriptedStore>) -> (RegistryState, StatePointer) {
    let (engine, _) = process(store).await;
    for path in ["changes/0", "shape/s1", "shape/s2"] {
        engine.ds.ensure_stream(path).await.unwrap();
    }
    let state = {
        let mut registry = engine.subqueries.lock().await;
        create_settled(&mut registry, "s1", "events", "shape/s1", &live_events(), false, &[1, 2], &["10", "11"]).await;
        create_settled(&mut registry, "s2", "events", "shape/s2", &live_events(), true, &[1, 2], &[]).await;
        registry.export_state().unwrap()
    };
    let (pos, highwater) = checkpoint();
    let pointer = crate::subq_state::seal(&state, crate::subq_state::new_stream_name(), &pos, highwater).unwrap();
    engine.catalog_tx.seed_durable_offset(LogPosition::start(), None).await;
    for event in [
        created("s1", "events", "shape/s1", &live_events(), false),
        created("s2", "events", "shape/s2", &live_events(), true),
        offset(Some(pointer.clone())),
    ] {
        engine.catalog_tx.send_durable(event).await.unwrap();
    }
    crate::subq_state::write(&engine.ds, &state, &pointer, &pos, highwater).await.unwrap();
    // The process is gone. It never ran a sequencer, so its close writes nothing more.
    engine.close(DEADLINE).await;
    (state, pointer)
}

/// Boot a process over `store`: fold the catalog from storage and restore it.
async fn boot(store: &Arc<ScriptedStore>) -> Engine {
    let (engine, compiled) = process(store).await;
    let fold = engine.fold_catalog().await.unwrap();
    tokio::time::timeout(DEADLINE, engine.apply_catalog(fold, &compiled, RestoreMode::Resume))
        .await
        .expect("restore deadline")
        .unwrap();
    assert!(engine.catalog_tx.drain(DEADLINE).await);
    engine
}

fn catalog(store: &ScriptedStore) -> Vec<serde_json::Value> {
    store
        .appended
        .lock()
        .unwrap()
        .iter()
        .filter(|(path, _, _)| path.ends_with(CATALOG_STREAM))
        .flat_map(|(_, _, bytes)| serde_json::from_slice::<Vec<serde_json::Value>>(bytes).unwrap())
        .collect()
}

fn dropped(store: &ScriptedStore) -> Vec<String> {
    catalog(store).iter().filter(|ev| ev["t"] == "dropped").map(|ev| ev["id"].as_str().unwrap().to_string()).collect()
}

/// Storage operations on streams whose path ends with `suffix` (`close:…`, `delete:…`).
fn retirements(store: &ScriptedStore, suffix: &str) -> Vec<String> {
    store
        .operations
        .lock()
        .unwrap()
        .iter()
        .filter(|op| (op.starts_with("close:") || op.starts_with("delete:")) && op.ends_with(suffix))
        .map(|op| op.split(':').next().unwrap().to_string())
        .collect()
}

/// The registry state the last `Offset` in the durable catalog names, if it names one.
fn last_pointer(store: &ScriptedStore) -> Option<serde_json::Value> {
    catalog(store).iter().rev().find(|ev| ev["t"] == "offset").and_then(|ev| ev.get("subq_state").cloned())
}

/// **The contract.** A catalog whose last checkpoint names a registry state that verifies restores
/// its subquery shapes: registered, sharing, leased, their key sets and inner sets as they were —
/// and no drop is recorded and no stream is closed or deleted. (Before: every subquery shape was
/// `Dropped` here and its stream retired, so each client reloaded its whole scope.)
#[tokio::test(flavor = "multi_thread")]
async fn a_valid_state_pointer_restores_subquery_shapes_without_recording_a_drop() {
    let store = store();
    let (state, pointer) = stopped_gracefully(&store).await;

    let engine = boot(&store).await;

    {
        let st = engine.state.lock().await;
        assert!(st.shapes.contains_key("s1") && st.shapes.contains_key("s2"), "both records are installed");
        assert_eq!(st.subscription_owner("sub-s1"), Some(&"s1".to_string()), "with their subscriptions");
        assert!(st.feed_shares.contains_key("s2"), "and their sharing entries");
    }
    assert!(engine.lives.lock().unwrap().contains_key("s1"), "and they are retained like any live shape");
    assert_eq!(dropped(&store), Vec::<String>::new(), "no drop is recorded");
    for path in ["shape/s1", "shape/s2"] {
        assert_eq!(retirements(&store, path), Vec::<String>::new(), "{path} is neither closed nor deleted");
    }
    // The registry is what the stopped process held: same inner set, same key sets.
    let restored = engine.subqueries.lock().await.export_state().unwrap();
    assert_eq!(restored.nodes.len(), 1);
    assert_eq!(restored.nodes[0].contributors, state.nodes[0].contributors);
    let keys = |state: &RegistryState| -> Vec<(String, Vec<String>)> {
        state.shapes.iter().map(|s| (s.shape_id.clone(), s.keys.iter().map(|k| k.to_string()).collect())).collect()
    };
    assert_eq!(keys(&restored), keys(&state));
    assert_eq!(keys(&restored)[0], ("s1".to_string(), vec!["10".to_string(), "11".to_string()]));

    // The state is used ONCE: before the sequencer reads, a checkpoint that names nothing is
    // durable, and the stream is deleted.
    assert_eq!(last_pointer(&store), None, "the restore invalidated the pointer it consumed");
    assert_eq!(retirements(&store, &pointer.stream), vec!["delete"]);
    engine.close(DEADLINE).await;
}

/// A state that does not verify is not used at all: the shapes are dropped and their streams
/// retired, as every restart did before.
#[tokio::test(flavor = "multi_thread")]
async fn a_state_pointer_that_does_not_verify_drops_the_shapes_as_before() {
    let store = store();
    let (_, pointer) = stopped_gracefully(&store).await;
    // One more record on the stream than the checkpoint named.
    let (writer, _) = process(&store).await;
    writer
        .ds
        .append_json(&pointer.stream, &[serde_json::json!({"k": "group", "path": "shape/group-9", "position": 1})])
        .await
        .unwrap();

    let engine = boot(&store).await;

    assert!(engine.state.lock().await.shapes.is_empty());
    assert!(engine.subqueries.lock().await.shapes.is_empty());
    let mut ids = dropped(&store);
    ids.sort();
    assert_eq!(ids, vec!["s1", "s2"]);
    for path in ["shape/s1", "shape/s2"] {
        assert_eq!(retirements(&store, path), vec!["close", "delete"], "{path} is retired: closed, then deleted");
    }
    assert_eq!(retirements(&store, &pointer.stream), vec!["delete"], "the unusable state is removed too");
    engine.close(DEADLINE).await;
}

/// A crash is a stop that wrote nothing. The process that restored the state has already
/// invalidated it, so the boot after its crash finds no state and drops the shapes — never the
/// registry of two boots ago replayed over streams that have moved on since.
#[tokio::test(flavor = "multi_thread")]
async fn a_crash_after_a_restored_boot_drops_the_shapes() {
    let store = store();
    stopped_gracefully(&store).await;
    let restored = boot(&store).await;
    assert_eq!(restored.state.lock().await.shapes.len(), 2);
    // No `close`: the process dies here.

    let after_crash = boot(&store).await;

    assert!(after_crash.state.lock().await.shapes.is_empty());
    let mut ids = dropped(&store);
    ids.sort();
    assert_eq!(ids, vec!["s1", "s2"]);
    after_crash.close(DEADLINE).await;
    restored.close(DEADLINE).await;
}

/// A graceful stop of a process that restored the shapes writes the registry again — even if it
/// stops before the sequencer has read anything — so the next boot restores them again.
#[tokio::test(flavor = "multi_thread")]
async fn a_graceful_stop_after_a_restored_boot_keeps_the_shapes_for_the_next_one() {
    let store = store();
    stopped_gracefully(&store).await;
    let restored = boot(&store).await;
    assert_eq!(last_pointer(&store), None);
    assert_eq!(restored.close(DEADLINE).await, crate::shutdown::ShutdownOutcome::Complete);
    assert!(last_pointer(&store).is_some(), "the stop named a new state from its final checkpoint");

    let next = boot(&store).await;

    assert_eq!(next.state.lock().await.shapes.len(), 2);
    assert_eq!(next.subqueries.lock().await.shapes.len(), 2);
    assert_eq!(dropped(&store), Vec::<String>::new());
    next.close(DEADLINE).await;
}

/// A shape the state does not hold — it was created after the registry was written — is dropped
/// on its own; the shapes the state does hold are restored.
#[tokio::test(flavor = "multi_thread")]
async fn a_shape_missing_from_the_state_is_dropped_and_the_rest_restored() {
    let store = store();
    stopped_gracefully(&store).await;
    let (late, _) = process(&store).await;
    late.catalog_tx.seed_durable_offset(LogPosition::start(), None).await;
    late.ds.ensure_stream("shape/s3").await.unwrap();
    late.catalog_tx.send_durable(created("s3", "notes", "shape/s3", &on_visible_record(), false)).await.unwrap();

    let engine = boot(&store).await;

    let mut installed: Vec<String> = engine.state.lock().await.shapes.keys().cloned().collect();
    installed.sort();
    assert_eq!(installed, vec!["s1", "s2"]);
    assert_eq!(dropped(&store), vec!["s3"]);
    assert_eq!(retirements(&store, "shape/s3"), vec!["close", "delete"]);
    engine.close(DEADLINE).await;
}

/// A group whose members are subquery shapes is restored whole: its claim, its members, and the
/// position its stream had reached. (Before: a group with a subquery member was dropped as one.)
#[tokio::test(flavor = "multi_thread")]
async fn a_group_with_subquery_members_is_restored_whole() {
    let store = store();
    let group_path = "shape/group-1";
    let (first, _) = process(&store).await;
    for path in ["changes/0", group_path] {
        first.ds.ensure_stream(path).await.unwrap();
    }
    let mut state = {
        let mut registry = first.subqueries.lock().await;
        create_settled(&mut registry, "s1", "events", group_path, &live_events(), true, &[1, 2], &[]).await;
        create_settled(&mut registry, "s2", "notes", group_path, &on_visible_record(), true, &[1, 2], &[]).await;
        registry.export_state().unwrap()
    };
    state.group_positions = vec![(group_path.to_string(), 0x44)];
    let (pos, highwater) = checkpoint();
    let pointer = crate::subq_state::seal(&state, crate::subq_state::new_stream_name(), &pos, highwater).unwrap();
    let member = |id: &str, table: &str, where_: PredicateJson| crate::engine::group::GroupMember {
        key: format!("public.{table}"),
        shape_id: id.to_string(),
        table: format!("public.{table}"),
        where_: Some(where_),
        columns: None,
        subscription: format!("sub-{id}"),
        is_subquery: true,
    };
    let group = GroupRecord {
        id: group_path.to_string(),
        stream_path: group_path.to_string(),
        subscription: "sub-group".to_string(),
        members: vec![member("s1", "events", live_events()), member("s2", "notes", on_visible_record())],
        lsn: "0/10".into(),
        snapshot: "1:1:".into(),
        horizon: "0/10".into(),
        replay_fence: "-1".into(),
    };
    first.catalog_tx.seed_durable_offset(LogPosition::start(), None).await;
    for event in [
        created("s1", "events", group_path, &live_events(), true),
        created("s2", "notes", group_path, &on_visible_record(), true),
        CatalogEvent::GroupCreated { group },
        offset(Some(pointer.clone())),
    ] {
        first.catalog_tx.send_durable(event).await.unwrap();
    }
    crate::subq_state::write(&first.ds, &state, &pointer, &pos, highwater).await.unwrap();
    first.close(DEADLINE).await;

    let engine = boot(&store).await;

    assert!(engine.has_group_record("sub-group").await, "the group's claim is restored");
    assert_eq!(engine.state.lock().await.shapes.len(), 2, "with both members");
    assert_eq!(engine.subqueries.lock().await.shapes.len(), 2);
    assert_eq!(engine.group_lanes.positions(), vec![(group_path.to_string(), 0x44)]);
    assert!(catalog(&store).iter().all(|ev| ev["t"] != "dropped" && ev["t"] != "groupDropped"), "nothing is dropped");
    assert_eq!(retirements(&store, group_path), Vec::<String>::new(), "the group's stream is untouched");
    engine.close(DEADLINE).await;
}

fn fold_of(events: Vec<CatalogEvent>) -> CatalogFold {
    let mut fold = CatalogFold::default();
    for (i, event) in events.into_iter().enumerate() {
        fold.apply_once(&format!("e{i}"), event);
    }
    fold
}

fn pointer(stream: &str) -> StatePointer {
    StatePointer { stream: stream.into(), version: 1, records: 2, bytes: 10, sha256: "00".into() }
}

/// Only the LAST `Offset` names the state a restart may use: the registry moves with every change
/// the sequencer processes, so a later checkpoint that names none means there is none.
#[test]
fn only_the_last_checkpoint_names_the_registry_state() {
    let named = fold_of(vec![offset(None), offset(Some(pointer("meta/subq-state/a")))]);
    assert_eq!(named.subq_state, Some(pointer("meta/subq-state/a")));
    assert_eq!(named.superseded_subq_state, None);

    let invalidated = fold_of(vec![offset(Some(pointer("meta/subq-state/a"))), offset(None)]);
    assert_eq!(invalidated.subq_state, None, "a later checkpoint without a state invalidates it");
    assert_eq!(
        invalidated.superseded_subq_state,
        Some(pointer("meta/subq-state/a")),
        "and its stream is remembered so the boot can delete it"
    );

    let replaced = fold_of(vec![
        offset(Some(pointer("meta/subq-state/a"))),
        offset(None),
        offset(Some(pointer("meta/subq-state/b"))),
    ]);
    assert_eq!(replaced.subq_state, Some(pointer("meta/subq-state/b")));
    assert_eq!(replaced.superseded_subq_state, Some(pointer("meta/subq-state/a")));
}

/// The pointer is optional on the wire both ways: a checkpoint without one keeps the old form
/// (so an older engine reads it), and an old checkpoint folds to "no state".
#[test]
fn the_checkpoint_wire_form_carries_the_state_pointer_only_when_there_is_one() {
    let bare = serde_json::to_value(offset(None)).unwrap();
    assert!(bare.get("subq_state").is_none(), "{bare}");
    let named = serde_json::to_value(offset(Some(pointer("meta/subq-state/a")))).unwrap();
    assert_eq!(named["subq_state"]["stream"], "meta/subq-state/a");
    assert_eq!(named["subq_state"]["sha256"], "00");
    let old: CatalogEvent =
        serde_json::from_value(serde_json::json!({"t": "offset", "pos": {"segment": 0, "offset": "5"}})).unwrap();
    assert!(matches!(old, CatalogEvent::Offset { subq_state: None, .. }));
}
