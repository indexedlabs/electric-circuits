//! Socket-free crash cuts through the real catalog writer, lifecycle and sequencer.
//! These are focused controls/regressions, not PostgreSQL or storage qualification.
//! The store supplies atomic appends, explicit failures and retained bytes; it never supplies
//! an engine outcome. The independent oracle is a primary-key map of scripted source writes.
use super::*;
use crate::ds::{AppendFault, BodyGate, ScriptedStore};
use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

const DEADLINE: Duration = Duration::from_secs(10);
type Rows = BTreeMap<String, serde_json::Value>;

async fn bounded<T>(label: &str, future: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(DEADLINE, future).await.unwrap_or_else(|_| panic!("{label}: diagnostic deadline"))
}

fn store() -> Arc<ScriptedStore> {
    Arc::new(ScriptedStore { read_appended: true, streams: Some(Default::default()), ..Default::default() })
}

fn retained(store: &ScriptedStore) -> Arc<ScriptedStore> {
    Arc::new(ScriptedStore {
        read_appended: true,
        streams: Some(std::sync::Mutex::new(store.streams.as_ref().unwrap().lock().unwrap().clone())),
        appended: std::sync::Mutex::new(store.appended.lock().unwrap().clone()),
        ..Default::default()
    })
}

fn events(store: &ScriptedStore, suffix: &str) -> Vec<serde_json::Value> {
    store
        .appended
        .lock()
        .unwrap()
        .iter()
        .filter(|(path, _, _)| path.ends_with(suffix))
        .flat_map(|(_, _, bytes)| serde_json::from_slice::<Vec<serde_json::Value>>(bytes).unwrap())
        .collect()
}

fn materialize(store: &ScriptedStore, path: &str) -> Rows {
    let mut rows = Rows::new();
    for value in events(store, path) {
        let env: Envelope = serde_json::from_value(value).unwrap();
        match env.headers.operation.as_str() {
            "upsert" => {
                rows.insert(env.key, env.value.expect("absolute upsert contains a row"));
            }
            "delete" => {
                rows.remove(&env.key);
            }
            other => panic!("shape output must be absolute, got {other}"),
        }
    }
    rows
}

fn checkpoints(store: &ScriptedStore) -> Vec<(LogPosition, Option<(u64, u64)>)> {
    events(store, CATALOG_STREAM)
        .into_iter()
        .filter_map(|ev| match serde_json::from_value::<CatalogEvent>(ev).unwrap() {
            CatalogEvent::Offset { pos, highwater, .. } => Some((pos, highwater)),
            _ => None,
        })
        .collect()
}

fn assert_checkpoint_order(store: &ScriptedStore) {
    for pair in checkpoints(store).windows(2) {
        assert!(pair[1].0 >= pair[0].0, "durable position regressed: {pair:?}");
        if pair[1].0 == pair[0].0 {
            assert!(pair[1].1 >= pair[0].1, "highwater regressed at equal position: {pair:?}");
        }
    }
}

// Client acknowledgements, never reconstructed from the engine's registry or catalog fold.
// One interrupted lifecycle request may have landed without its answer; only that exact
// request is allowed either outcome. All previously acknowledged clients remain mandatory.
#[derive(Clone)]
struct Feed {
    stream: String,
    min_id: Option<u64>,
    subscriptions: BTreeSet<String>,
    active: bool,
}

#[derive(Clone)]
enum PendingCall {
    Create(String, Feed),
    Release(String, String),
    Purge(String),
}

#[derive(Clone, Default)]
struct Oracle {
    rows: Rows,
    feeds: BTreeMap<String, Feed>,
    removed: BTreeSet<String>,
    pending: Option<PendingCall>,
}

impl Oracle {
    fn pending_purge(&self, id: &str) -> bool {
        matches!(&self.pending, Some(PendingCall::Purge(pending)) if pending == id)
    }

    fn retained_ids(&self) -> BTreeSet<String> {
        self.feeds.keys().filter(|id| !self.pending_purge(id)).cloned().collect()
    }
}

struct Run {
    engine: Engine,
    store: Arc<ScriptedStore>,
    reads: mpsc::UnboundedReceiver<(String, String, bool)>,
    oracle: Arc<std::sync::Mutex<Oracle>>,
}

impl Run {
    async fn new(store: Arc<ScriptedStore>, oracle: Arc<std::sync::Mutex<Oracle>>) -> Self {
        let ds = DsClient::with_test_store("scripted://crash-consistency".into(), store.clone());
        let mut engine = Engine::new_for_in_process_test(ds);
        // Drive retention explicitly; no clock-dependent catalog records in a cut sweep.
        engine.retention_started.store(true, Ordering::SeqCst);
        engine.retention =
            Arc::new(crate::retention::RetentionConfig { idle_timeout: Duration::ZERO, ..(*engine.retention).clone() });
        let schema: Schema = serde_json::from_value(serde_json::json!({"tables": {
            "public.items": {"columns":{"id":{"type":"int"},"value":{"type":"text"}},"primaryKey":"id"}
        }}))
        .unwrap();
        let compiled = compile_schema(&schema).unwrap();
        engine.state.lock().await.tables = compiled.clone();
        *engine.tables_shared.write().unwrap() = compiled.clone();
        engine.subqueries.lock().await.set_schemas(Arc::new(compiled));
        let (tx, reads) = mpsc::unbounded_channel();
        *store.read_observer.lock().unwrap() = Some(tx);
        Self { engine, store, reads, oracle }
    }

    async fn create(&self, id: &str, min_id: Option<u64>, share: bool) {
        let subscription = format!("client-{id}");
        let mut feed = Feed {
            stream: format!("shape/{id}"),
            min_id,
            active: true,
            subscriptions: if share { BTreeSet::from([subscription.clone()]) } else { BTreeSet::new() },
        };
        self.oracle.lock().unwrap().pending = Some(PendingCall::Create(id.into(), feed.clone()));
        let predicate =
            min_id.map(|id| serde_json::from_value(serde_json::json!({"col":"id","op":"gte","value":id})).unwrap());
        let (record, claimed) = self
            .engine
            .create_shape_as(&"public.items".into(), predicate, None, false, share, Some(subscription.clone()))
            .await
            .unwrap();
        assert_eq!(record.id, id);
        assert_eq!(claimed, subscription);
        feed.stream = record.stream_path;
        let mut oracle = self.oracle.lock().unwrap();
        oracle.feeds.insert(id.into(), feed);
        oracle.pending = None;
    }

    async fn release(&self, id: &str) {
        let subscription = self.oracle.lock().unwrap().feeds[id].subscriptions.first().unwrap().clone();
        self.oracle.lock().unwrap().pending = Some(PendingCall::Release(id.into(), subscription.clone()));
        self.engine.release_subscription_durable(id, Some(&subscription)).await.unwrap();
        let mut oracle = self.oracle.lock().unwrap();
        oracle.feeds.get_mut(id).unwrap().subscriptions.remove(&subscription);
        oracle.pending = None;
    }

    async fn deactivate(&self, id: &str) {
        self.engine.deactivate_shape(id).await.unwrap();
        self.oracle.lock().unwrap().feeds.get_mut(id).unwrap().active = false;
    }

    async fn reactivate(&self, id: &str) {
        bounded("reactivation", self.engine.ensure_active(id)).await.unwrap();
        self.oracle.lock().unwrap().feeds.get_mut(id).unwrap().active = true;
    }

    async fn purge(&self, id: &str) {
        self.oracle.lock().unwrap().pending = Some(PendingCall::Purge(id.into()));
        self.engine.purge_shape_durable(id).await.unwrap();
        let mut oracle = self.oracle.lock().unwrap();
        oracle.feeds.remove(id);
        oracle.removed.insert(id.into());
        oracle.pending = None;
    }

    async fn check_clients(&self) {
        let oracle = self.oracle.lock().unwrap().clone();
        let required = oracle.retained_ids();
        let mut allowed: BTreeSet<_> = oracle.feeds.keys().cloned().collect();
        let mut feeds = oracle.feeds.clone();
        if let Some(PendingCall::Create(id, feed)) = &oracle.pending {
            allowed.insert(id.clone());
            feeds.insert(id.clone(), feed.clone());
        }
        let state = self.engine.state.lock().await;
        let actual: BTreeSet<_> = state.shapes.keys().cloned().collect();
        assert!(required.is_subset(&actual), "lost acknowledged shapes: expected {required:?}, got {actual:?}");
        assert!(actual.is_subset(&allowed), "unexpected restored shapes: allowed {allowed:?}, got {actual:?}");
        assert!(oracle.removed.is_disjoint(&actual), "acknowledged removals resurrected: {:?}", oracle.removed);
        let lives: BTreeSet<_> = self.engine.lives.lock().unwrap().keys().cloned().collect();
        assert_eq!(lives, actual, "retained shapes must have lifecycle entries");
        for (id, feed) in feeds {
            if !actual.contains(&id) || oracle.pending_purge(&id) {
                continue;
            }
            let subscriptions: BTreeSet<_> =
                state.feed_shares.get(&id).map(|share| share.subs.keys().cloned().collect()).unwrap_or_default();
            let mut after_pending = feed.subscriptions.clone();
            if let Some(PendingCall::Release(shape, subscription)) = &oracle.pending
                && shape == &id
            {
                after_pending.remove(subscription);
            }
            assert!(
                subscriptions == feed.subscriptions || subscriptions == after_pending,
                "lost acknowledged subscriptions for {id}: expected {:?}, got {subscriptions:?}",
                feed.subscriptions
            );
        }
    }

    async fn probe_after_restart(&mut self) {
        self.check_clients().await;
        // Wake the independently expected feeds, not whatever restore happened to retain.
        let retained = self.oracle.lock().unwrap().retained_ids();
        for id in retained {
            self.reactivate(&id).await;
        }
        self.drain().await;
        self.check().await;
        // A new row matching both predicates must arrive through every retained feed.
        self.write(0x100, 42, Some("committed after restart")).await;
    }

    async fn boot(&mut self) {
        let fold = self.engine.fold_catalog().await.expect("self-written catalog must decode");
        self.engine
            .init_change_log(fold.current_segment, fold.segment_starts.clone(), &fold.start_pos())
            .await
            .expect("self-written checkpoint must name an existing segment");
        let compiled = self.engine.tables_shared.read().unwrap().clone();
        bounded("restore", self.engine.apply_catalog(fold, &compiled, RestoreMode::Resume)).await.unwrap();
        self.check_clients().await;
    }

    async fn fresh(&mut self) {
        self.engine.init_change_log(0, Default::default(), &LogPosition::start()).await.unwrap();
        self.engine.catalog_tx.wait_durable().await.unwrap();
    }

    async fn read_at(&mut self, position: &LogPosition) {
        bounded("next sequencer read", async {
            loop {
                let (path, offset, live) = self.reads.recv().await.expect("read observer closed");
                if live && path.ends_with(&position.path()) && offset == position.offset {
                    break;
                }
            }
        })
        .await;
    }

    async fn processed(&self) -> LogPosition {
        self.engine.table_offset(&"public.items".into()).await.expect("sequencer exists")
    }

    async fn drain(&mut self) {
        let current = self.engine.changes.state().current();
        let head = self.engine.ds.head(&segment_path(current)).await.unwrap().unwrap();
        let offset = head.next_offset.unwrap();
        // Empty live reads park at the start sentinel; HEAD still reports the provider's zero tail.
        let offset = if offset == "0000000000000000_0000000000000000" { "-1".into() } else { offset };
        let target = LogPosition { segment: current, offset };
        let sequencer_exists = self.engine.state.lock().await.sequencer.is_some();
        assert!(
            sequencer_exists || self.oracle.lock().unwrap().retained_ids().is_empty(),
            "acknowledged feeds lost their sequencer"
        );
        if sequencer_exists {
            // A read at the new tail is issued only after the preceding page's output and
            // receipts complete. A command barrier alone can overtake an unread page.
            self.read_at(&target).await;
            assert_eq!(self.processed().await, target);
        }
        self.engine.catalog_tx.wait_durable().await.unwrap();
    }

    async fn check(&self) {
        self.engine.catalog_tx.wait_durable().await.unwrap();
        assert_checkpoint_order(&self.store);
        let fold = self.engine.fold_catalog().await.unwrap();
        assert!(self.engine.ds.head(&fold.start_pos.path()).await.unwrap().is_some(), "checkpoint segment was deleted");
        self.check_clients().await;
        let expected = self.oracle.lock().unwrap().clone();
        for (id, feed) in &expected.feeds {
            if !feed.active || expected.pending_purge(id) {
                continue;
            }
            assert!(
                self.engine.lives.lock().unwrap().get(id).is_some_and(|life| matches!(life.state, LifeState::Active)),
                "expected active client {id} is not maintained"
            );
            let wanted: Rows = expected
                .rows
                .iter()
                .filter(|(_, row)| feed.min_id.is_none_or(|min| row["id"].as_u64().unwrap() >= min))
                .map(|(key, row)| (key.clone(), row.clone()))
                .collect();
            assert_eq!(materialize(&self.store, &feed.stream), wanted, "shape {id} diverged");
        }
    }

    async fn write(&mut self, lsn: u64, id: u64, value: Option<&str>) {
        let old = self.oracle.lock().unwrap().rows.get(&id.to_string()).cloned();
        let row = value.map(|value| serde_json::json!({"id":id,"value":value}));
        let env = Envelope {
            type_: "public.items".into(),
            key: id.to_string(),
            value: row.clone(),
            old,
            headers: EnvelopeHeaders {
                operation: if row.is_some() { "update" } else { "delete" }.into(),
                txid: Some(lsn.to_string()),
                lsn: Some(format!("0/{lsn:X}")),
                seq: Some(0),
                last: Some(true),
                offset: None,
            },
        };
        // Match an ingestor reconnect: retry the identical committed source transaction after
        // an unavailable/ambiguous append. Never manufacture a new LSN or sequence.
        if let Err(error) = self.engine.changes.append_commit(std::slice::from_ref(&env)).await {
            assert!(crate::ds::is_unavailable(&error), "unexpected source append refusal: {error:#}");
            self.engine.changes.append_commit(&[env]).await.unwrap();
        }
        {
            let mut oracle = self.oracle.lock().unwrap();
            if let Some(row) = row {
                oracle.rows.insert(id.to_string(), row);
            } else {
                oracle.rows.remove(&id.to_string());
            }
        }
        self.drain().await;
        self.check().await;
    }

    async fn crash(self) -> Arc<ScriptedStore> {
        // No begin()/close(): abort and join every owned task BEFORE releasing callbacks/Engine.
        // Dropping an Engine handle alone does not kill tasks holding its clones.
        bounded("abort engine tasks", self.engine.shutdown.join_tasks()).await;
        self.engine.ds.clear_gone_reconciler();
        bounded("crash membership cleanup", self.engine.membership.shutdown()).await;
        let saved = retained(&self.store);
        // Destruct telemetry outside the async worker, as Engine::close does.
        let telemetry = self.engine.telemetry.lock().unwrap().take();
        if let Some(telemetry) = telemetry {
            bounded("crash telemetry cleanup", tokio::task::spawn_blocking(move || telemetry.shutdown()))
                .await
                .unwrap();
        }
        saved
    }

    async fn stop(&self) {
        assert_eq!(
            bounded("graceful close", self.engine.close(DEADLINE)).await,
            crate::shutdown::ShutdownOutcome::Complete
        );
        assert_checkpoint_order(&self.store);
    }
}

async fn workload(run: &mut Run) {
    run.fresh().await;
    run.create("s1", None, true).await;
    run.check().await;
    run.create("s2", Some(2), true).await;
    run.check().await;
    run.write(0x10, 1, Some("one")).await;
    run.write(0x20, 2, Some("two")).await;
    run.release("s2").await;
    run.deactivate("s2").await;
    assert!(matches!(run.engine.lives.lock().unwrap()["s2"].state, LifeState::Dormant { .. }));
    run.check().await;
    run.engine.changes.force_rotate().await;
    if run.engine.changes.state().current() == 0 {
        // A failed pointer append leaves the writer on the open predecessor; next rotation retries.
        run.engine.changes.force_rotate().await;
    }
    assert_eq!(run.engine.changes.state().current(), 1);
    run.drain().await;
    run.check().await;
    run.write(0x30, 2, Some("changed while dormant")).await;
    run.engine.sweep_change_log().await;
    assert!(run.engine.ds.head("changes/0").await.unwrap().is_some(), "dormant resume pins its segment");
    run.reactivate("s2").await;
    run.check().await;
    run.engine.sweep_change_log().await;
    run.engine.catalog_tx.wait_durable().await.unwrap();
    assert!(
        run.engine.ds.head("changes/0").await.unwrap().is_none(),
        "unpin permits deletion behind durable checkpoint"
    );
    run.check().await;
    run.write(0x40, 1, None).await;
    run.purge("s2").await;
    run.check().await;
}

#[tokio::test]
async fn crashsim_workload_control() {
    let mut run = Run::new(store(), Default::default()).await;
    bounded("workload", workload(&mut run)).await;
    run.stop().await;
    let kinds: Vec<_> =
        events(&run.store, CATALOG_STREAM).iter().map(|ev| ev["t"].as_str().unwrap().to_string()).collect();
    for kind in
        ["created", "dormant", "reactivated", "changesRotated", "changesSegmentDeleted", "dropped", "retired", "offset"]
    {
        assert!(kinds.iter().any(|seen| seen == kind), "missing cut surface: {kind}: {kinds:?}");
    }
}

#[tokio::test]
async fn crashsim_kill_at_every_catalog_append() {
    catalog_cut_sweep(false).await;
}

#[tokio::test]
async fn crashsim_sigterm_at_every_catalog_append() {
    catalog_cut_sweep(true).await;
}

async fn catalog_cut_sweep(graceful: bool) {
    // Determine cuts from the workload itself; don't silently stop exercising new catalog writes.
    let mut baseline = Run::new(store(), Default::default()).await;
    bounded("baseline workload", workload(&mut baseline)).await;
    let count = events(&baseline.store, CATALOG_STREAM).len();
    assert!(count >= 12, "the sweep must exercise the entire lifecycle");
    baseline.stop().await;
    eprintln!("catalog cut sweep: graceful={graceful}, cuts={count}");
    for cut in 0..count {
        eprintln!("catalog cut {cut}/{count}, graceful={graceful}");
        let storage = store();
        let gate = Arc::new(BodyGate::default());
        *storage.append_fault.lock().unwrap() = Some((CATALOG_STREAM.into(), cut, AppendFault::Hold(gate.clone())));
        let oracle = Arc::new(std::sync::Mutex::new(Oracle::default()));
        let mut run = Run::new(storage.clone(), oracle.clone()).await;
        // Keep the request future local so cancellation and engine-task abortion are explicit.
        let mut work = Box::pin(workload(&mut run));
        bounded("catalog cut arrival", async {
            tokio::select! {
                _ = gate.entered.notified() => {},
                _ = &mut work => panic!("workload finished before catalog cut {cut}/{count}"),
            }
        })
        .await;
        assert_checkpoint_order(&storage);
        // Freeze bytes before dropping the request guard (which can enqueue rollback work).
        let crashed_bytes = retained(&storage);
        drop(work);
        let saved = if graceful {
            run.engine.shutdown.begin();
            gate.released.notify_one();
            run.stop().await;
            retained(&storage)
        } else {
            let _ = run.crash().await;
            crashed_bytes
        };
        let mut restarted = Run::new(saved, oracle).await;
        bounded(&format!("restart after catalog append {cut}/{count}"), async {
            restarted.boot().await;
            restarted.probe_after_restart().await;
            restarted.stop().await;
        })
        .await;
    }
}

#[tokio::test]
async fn crashsim_catalog_503_at_every_append_retries_without_reordering() {
    for ambiguous in [false, true] {
        // Include equal-position highwater-only updates and a segment boundary.
        let writes = [
            (LogPosition::start(), None),
            (LogPosition::start(), Some((16, 1))),
            (LogPosition::start(), Some((16, 2))),
            (LogPosition::start_of(1), Some((32, 0))),
        ];
        for cut in 0..writes.len() {
            let storage = store();
            let run = Run::new(storage.clone(), Default::default()).await;
            *storage.append_fault.lock().unwrap() = Some((
                CATALOG_STREAM.into(),
                cut,
                if ambiguous { AppendFault::CommitThenUnavailable } else { AppendFault::Unavailable },
            ));
            for (pos, highwater) in &writes {
                run.engine.catalog_tx.send(CatalogEvent::Offset {
                    pos: pos.clone(),
                    highwater: *highwater,
                    groups: Default::default(),
                });
            }
            bounded("catalog retry drain", run.engine.catalog_tx.wait_durable()).await.unwrap();
            assert_checkpoint_order(&storage);
            let fold = run.engine.fold_catalog().await.unwrap();
            assert_eq!((fold.start_pos, fold.start_highwater), writes[3]);
            let logged = events(&storage, CATALOG_STREAM);
            assert_eq!(logged.len(), writes.len() + usize::from(ambiguous));
            if ambiguous {
                assert_eq!(logged[cut]["eid"], logged[cut + 1]["eid"], "ambiguous retry must retain eid");
            }
            assert_eq!(run.engine.metrics.catalog_append_retries.load(Ordering::Relaxed), 1);
            run.stop().await;
        }
    }
}

#[tokio::test]
async fn crashsim_boot_requests_are_refused_at_each_phase() {
    use tower::ServiceExt;
    // Boundaries are held explicitly, before connecting, after schema install, after catalog
    // fold, after segment resolution, and after restore while ACTIVE has not been published.
    for phase in 0..5 {
        let storage = store();
        let mut seed = Run::new(storage.clone(), Default::default()).await;
        seed.fresh().await;
        seed.create("s1", None, true).await;
        seed.release("s1").await;
        seed.deactivate("s1").await;
        seed.stop().await;
        let storage = retained(&storage);
        let mut run = Run::new(storage.clone(), Default::default()).await;
        let compiled = run.engine.tables_shared.read().unwrap().clone();
        // A PG Engine activates the production boot admission gate. All requests must fail
        // BEFORE SQL; this address is deliberately unusable and is never contacted.
        run.engine.close(DEADLINE).await;
        run.engine = Engine::new_pg_for_in_process_test(
            DsClient::with_test_store("scripted://boot-cuts".into(), storage.clone()),
            "postgres://unused.invalid/unused".into(),
        );
        run.engine.retention_started.store(true, Ordering::SeqCst);
        if phase > 0 {
            run.engine.health.store(HEALTH_STARTING, Ordering::Release);
            *run.engine.tables_shared.write().unwrap() = compiled.clone();
            run.engine.state.lock().await.tables = compiled.clone();
        }
        if phase > 1 {
            let fold = run.engine.fold_catalog().await.unwrap();
            if phase > 2 {
                run.engine
                    .init_change_log(fold.current_segment, fold.segment_starts.clone(), &fold.start_pos())
                    .await
                    .unwrap();
            }
            if phase > 3 {
                run.engine.apply_catalog(fold, &compiled, RestoreMode::Resume).await.unwrap();
            }
        }
        let before = events(&storage, CATALOG_STREAM);
        for (method, path, body) in [
            ("POST", "/v1/shapes", r#"{"table":"public.items","changesOnly":true}"#),
            ("POST", "/v1/aggregates", r#"{"table":"public.items","fn":"count"}"#),
            ("POST", "/v1/subsets/query", r#"{"table":"public.items","limit":10}"#),
            ("GET", "/shapes/s1/rows", ""),
            ("GET", "/v1/shape?table=public.items&offset=-1", ""),
        ] {
            let response = bounded(
                "boot request",
                crate::http::router(run.engine.clone()).oneshot(
                    axum::http::Request::builder()
                        .method(method)
                        .uri(path)
                        .header("content-type", "application/json")
                        .body(axum::body::Body::from(body))
                        .unwrap(),
                ),
            )
            .await
            .unwrap();
            assert_eq!(response.status(), axum::http::StatusCode::SERVICE_UNAVAILABLE, "phase {phase}, {path}");
            assert!(response.headers().contains_key("retry-after"), "phase {phase}, {path}: refusal must be retryable");
        }
        let error = run.engine.ensure_active("s1").await.unwrap_err();
        assert!(error.downcast_ref::<DeploymentNotReady>().is_some(), "phase {phase}: {error:#}");
        run.engine.catalog_tx.wait_durable().await.unwrap();
        assert_eq!(events(&storage, CATALOG_STREAM), before, "requests during phase {phase} wrote catalog state");
        if phase < 4 {
            assert!(run.engine.state.lock().await.sequencer.is_none());
        }
        run.stop().await;
    }
}

fn transaction_row(seq: u64, last: bool, table: &str) -> Envelope {
    Envelope {
        type_: table.into(),
        key: seq.to_string(),
        value: Some(serde_json::json!({"id":seq,"value":"x".repeat(1024)})),
        old: None,
        headers: EnvelopeHeaders {
            operation: "insert".into(),
            txid: Some("77".into()),
            lsn: Some("0/70".into()),
            seq: Some(seq),
            last: last.then_some(true),
            offset: None,
        },
    }
}

#[tokio::test]
async fn crashsim_held_over_cap_survives_restart_and_drains_marker_in_two_pages() {
    for graceful in [false, true] {
        let dir = std::env::temp_dir().join(format!("crashsim-held-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&dir).unwrap();
        let config = crate::txn_buffer::TxnBufferConfig::resolve(|key| match key {
            "ELECTRIC_CIRCUITS_HELD_MEMORY_BYTES" => Some("1024".into()),
            "ELECTRIC_CIRCUITS_TXN_SPILL_DIR" => Some(dir.to_string_lossy().into_owned()),
            _ => None,
        })
        .unwrap();
        let storage = store();
        let oracle = Arc::new(std::sync::Mutex::new(Oracle::default()));
        let mut run = Run::new(storage, oracle.clone()).await;
        run.engine.set_txn_config(config.clone());
        run.fresh().await;
        run.create("s1", None, true).await;
        let prefix: Vec<_> = (0..32).map(|seq| transaction_row(seq, false, "public.items")).collect();
        run.engine.changes.append_commit(&prefix).await.unwrap();
        let held_tail = LogPosition { segment: 0, offset: "0000000000000000_0000000000000001".into() };
        run.read_at(&held_tail).await;
        assert_eq!(run.processed().await, LogPosition::start());
        assert!(materialize(&run.store, "shape/s1").is_empty(), "uncommitted prefix escaped");
        assert_eq!(run.engine.metrics.sequencer_held_spills.load(Ordering::Relaxed), 1);
        assert_eq!(run.engine.metrics.sequencer_held_bytes.load(Ordering::Relaxed), 0);
        assert_eq!(std::fs::read_dir(&dir).unwrap().count(), 1);
        let saved = if graceful {
            run.stop().await;
            retained(&run.store)
        } else {
            run.crash().await
        };
        assert_eq!(std::fs::read_dir(&dir).unwrap().count(), 0, "scratch survives engine stop");
        let mut restarted = Run::new(saved, oracle.clone()).await;
        restarted.engine.set_txn_config(config);
        restarted.boot().await;
        restarted.read_at(&held_tail).await;
        assert!(materialize(&restarted.store, "shape/s1").is_empty());
        let mut marker = transaction_row(32, true, crate::replication::SOURCE_FENCE_ENVELOPE);
        marker.key = "after-large-transaction".into();
        assert!(restarted.engine.source_drain_receipt(&marker.key).is_none());
        oracle.lock().unwrap().rows = prefix.iter().map(|env| (env.key.clone(), env.value.clone().unwrap())).collect();
        restarted.engine.changes.append_commit(&[marker]).await.unwrap();
        // Fixed terminal: exactly one more page completes the two-page transaction and receipt.
        restarted.drain().await;
        assert_eq!(restarted.engine.source_drain_receipt("after-large-transaction").unwrap().commit_lsn, "0/70");
        restarted.check().await;
        assert_eq!(events(&restarted.store, "shape/s1").len(), 32);
        assert_eq!(std::fs::read_dir(&dir).unwrap().count(), 0);
        restarted.write(0x100, 42, Some("after held transaction restart")).await;
        restarted.stop().await;
        std::fs::remove_dir(&dir).unwrap();
    }
}

#[tokio::test]
async fn crashsim_untracked_transaction_advances_without_shape_output() {
    let mut run = Run::new(store(), Default::default()).await;
    run.fresh().await;
    run.create("s1", None, true).await;
    run.engine.changes.append_commit(&[transaction_row(0, true, "public.untracked")]).await.unwrap();
    run.drain().await;
    run.check().await;
    assert!(materialize(&run.store, "shape/s1").is_empty());
    run.stop().await;
    assert_eq!(checkpoints(&run.store).last().unwrap().1, Some((0x70, 0)));
}

#[tokio::test]
async fn crashsim_partial_shape_fanout_replays_absolute_effects() {
    for fault in [AppendFault::Unavailable, AppendFault::CommitThenUnavailable] {
        let mut run = Run::new(store(), Default::default()).await;
        run.fresh().await;
        for id in ["s1", "s2"] {
            run.create(id, None, false).await;
        }
        *run.store.append_fault.lock().unwrap() = Some(("shape/s2".into(), 0, fault));
        // id=2 matches both independent feeds; one stream can land before its peer retries.
        run.write(0x20, 2, Some("both feeds")).await;
        assert_eq!(materialize(&run.store, "shape/s1"), materialize(&run.store, "shape/s2"));
        run.stop().await;
    }
}

#[tokio::test]
async fn crashsim_slow_partial_fanout_never_checkpoints_and_crash_replays() {
    let oracle = Arc::new(std::sync::Mutex::new(Oracle::default()));
    let mut run = Run::new(store(), oracle.clone()).await;
    run.fresh().await;
    for id in ["s1", "s2"] {
        run.create(id, None, false).await;
    }
    let gate = Arc::new(BodyGate::default());
    *run.store.append_fault.lock().unwrap() = Some(("shape/s2".into(), 0, AppendFault::Hold(gate.clone())));
    let row = transaction_row(2, true, "public.items");
    oracle.lock().unwrap().rows.insert(row.key.clone(), row.value.clone().unwrap());
    run.engine.changes.append_commit(&[row]).await.unwrap();
    bounded("held shape append", gate.entered.notified()).await;
    assert_eq!(run.processed().await, LogPosition::start(), "partially delivered transaction advanced");
    assert!(checkpoints(&run.store).iter().all(|(pos, _)| pos == &LogPosition::start()));
    assert!(materialize(&run.store, "shape/s2").is_empty());
    let saved = run.crash().await;
    let mut restarted = Run::new(saved, oracle).await;
    restarted.boot().await;
    restarted.probe_after_restart().await;
    restarted.stop().await;
}

#[tokio::test]
async fn crashsim_missing_dormant_segment_retires_instead_of_serving_stale_rows() {
    let mut run = Run::new(store(), Default::default()).await;
    run.fresh().await;
    run.create("s1", None, true).await;
    run.write(0x10, 1, Some("before dormancy")).await;
    run.release("s1").await;
    run.deactivate("s1").await;
    run.engine.changes.force_rotate().await;
    run.drain().await;
    // External loss is deliberately distinct from the engine's safe deletion planner.
    run.engine.ds.delete_stream("changes/0").await.unwrap();
    let result = bounded("expired dormant resume", run.engine.ensure_active("s1")).await;
    assert!(result.is_err(), "a lost resume prefix must demand recreation");
    assert!(!run.engine.state.lock().await.shapes.contains_key("s1"));
    run.engine.catalog_tx.wait_durable().await.unwrap();
    assert!(run.engine.ds.head("shape/s1").await.unwrap().is_none());
    let fold = run.engine.fold_catalog().await.unwrap();
    assert!(!fold.recs.contains_key("s1"));
    assert!(fold.pending_retirements().is_empty());
    run.stop().await;
}

async fn lose_live_segment(status: u16) -> Run {
    let mut run = Run::new(
        Arc::new(ScriptedStore {
            read_appended: true,
            streams: Some(Default::default()),
            missing_status: status,
            ..Default::default()
        }),
        Default::default(),
    )
    .await;
    run.fresh().await;
    run.create("s1", None, true).await;
    let command = run.engine.state.lock().await.sequencer.as_ref().unwrap().cmd_tx.clone();
    let (done, ack) = tokio::sync::oneshot::channel();
    command.send(SequencerCmd::PauseReads { done }).unwrap();
    bounded("pause before loss", ack).await.unwrap();
    while run.reads.try_recv().is_ok() {} // discard arrivals before the externally controlled loss
    run.engine.changes.force_rotate().await;
    run.engine.ds.delete_stream("changes/0").await.unwrap();
    let (done, ack) = tokio::sync::oneshot::channel();
    command.send(SequencerCmd::ResumeReads { done }).unwrap();
    bounded("resume after loss", ack).await.unwrap();
    run.read_at(&LogPosition::start()).await;
    // Arrival and the terminal response handler share one poll on this current-thread runtime.
    assert_eq!(run.processed().await, LogPosition::start());
    assert_eq!(run.engine.health_status(), "degraded");
    run
}

async fn assert_replay_refused(run: &Run) {
    use tower::ServiceExt;
    for (method, path, body) in [
        ("GET", "/shapes/s1/rows", ""),
        ("POST", "/v1/shapes", r#"{"table":"public.items","changesOnly":true}"#),
        ("POST", "/v1/aggregates", r#"{"table":"public.items","fn":"count"}"#),
        ("POST", "/v1/subsets/query", r#"{"table":"public.items","limit":10}"#),
        ("GET", "/shapes/s1/log?offset=-1&live=true", ""),
        ("GET", "/v1/shape?table=public.items&offset=-1", ""),
        ("GET", "/v1/shape?table=public.items&offset=0&handle=s1&live=true", ""),
    ] {
        let response = bounded(
            "terminal replay refusal",
            crate::http::router(run.engine.clone()).oneshot(
                axum::http::Request::builder()
                    .method(method)
                    .uri(path)
                    .header("content-type", "application/json")
                    .body(axum::body::Body::from(body))
                    .unwrap(),
            ),
        )
        .await
        .unwrap();
        assert_eq!(
            response.status(),
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            "terminal replay failure admitted {method} {path}"
        );
        assert!(
            !response.headers().contains_key("retry-after"),
            "restart-only refusal advertised a retry: {method} {path}"
        );
    }
    let refusal = run.engine.ensure_not_degraded().unwrap_err();
    assert!(refusal.downcast_ref::<DeploymentNotReady>().is_none(), "terminal replay is not boot admission");
    assert!(refusal.downcast_ref::<ReplayFailed>().is_some());
    assert!(refusal.to_string().contains("restart required"));
    assert!(run.engine.ensure_active("s1").await.is_err());
    assert!(run.engine.read_shape_stream("shape/s1", "-1", true).await.is_err());
}

async fn assert_latched_stop(mut run: Run) {
    assert!(run.engine.shutdown.restart_requested(), "terminal replay must request a host restart");
    assert_eq!(bounded("fatal restart notification", run.engine.shutdown.wait_for_restart()).await, 75);
    assert_eq!(run.engine.readiness_status(), "shutting_down");
    while let Ok((path, _, live)) = run.reads.try_recv() {
        assert!(!(live && path.ends_with("changes/0")), "latched reader retried its gone segment");
    }
    assert_eq!(run.processed().await, LogPosition::start());
    tokio::time::resume();
    run.stop().await;
}

#[tokio::test(start_paused = true)]
async fn crashsim_missing_live_segment_withdraws_readiness() {
    for (index, status) in [404, 410].into_iter().enumerate() {
        if index > 0 {
            tokio::time::pause();
        }
        let run = lose_live_segment(status).await;
        assert_replay_refused(&run).await;
        assert_latched_stop(run).await;
    }
}

#[tokio::test(start_paused = true)]
async fn crashsim_missing_live_segment_is_not_managed_boot_retry() {
    for (index, status) in [404, 410].into_iter().enumerate() {
        if index > 0 {
            tokio::time::pause();
        }
        let run = lose_live_segment(status).await;
        *run.engine.managed_deployment.lock().unwrap() = Some(ManagedDeploymentState {
            config: crate::deployment::ManagedDeploymentConfig {
                revision: "018f0f46-93d0-7cf0-8b74-4bf0866ec285".into(),
                initial_active: false,
            },
            coordination_key: "a".repeat(64),
            role: ManagedRole::Active,
            ownership: None,
        });
        assert_replay_refused(&run).await;
        assert_latched_stop(run).await;
    }
}

#[tokio::test(start_paused = true)]
async fn crashsim_missing_live_segment_counts_failure_once() {
    let run = lose_live_segment(410).await;
    assert_eq!(
        run.engine.metrics.snapshot()["counters"]["sequencer_stream_gone_failures_total"],
        1,
        "a terminal gone segment must be visible in metrics"
    );
    assert!(
        run.engine
            .prometheus_text()
            .lines()
            .any(|line| line.starts_with("engine_sequencer_stream_gone_failures_total") && line.ends_with(" 1")),
        "gone-segment cause is missing from Prometheus"
    );
    let metrics = run.engine.metrics.clone();
    metrics.reset(); // resetting a window must not erase a restart-only diagnosis
    assert_latched_stop(run).await;
    assert_eq!(metrics.snapshot()["counters"]["sequencer_stream_gone_failures_total"], 1);
}

async fn replay_failed_diagnostic(run: &Run) -> serde_json::Value {
    use tower::ServiceExt;
    let response = crate::http::router(run.engine.clone())
        .oneshot(axum::http::Request::builder().uri("/replication/lsn").body(axum::body::Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), axum::http::StatusCode::OK);
    let bytes = axum::body::to_bytes(response.into_body(), 64 * 1024).await.unwrap();
    serde_json::from_slice::<serde_json::Value>(&bytes).unwrap()["replayFailed"].clone()
}

#[tokio::test]
async fn crashsim_healthy_replay_diagnostic_is_false() {
    let mut run = Run::new(store(), Default::default()).await;
    run.fresh().await;
    assert_eq!(replay_failed_diagnostic(&run).await, false, "healthy replay must report a boolean cause");
    run.stop().await;
}

#[tokio::test(start_paused = true)]
async fn crashsim_terminal_replay_diagnostic_is_true() {
    let run = lose_live_segment(404).await;
    assert_eq!(replay_failed_diagnostic(&run).await, true, "terminal replay must expose its cause");
    assert_latched_stop(run).await;
}

#[tokio::test]
async fn crashsim_terminal_refusal_survives_shutdown_admission() {
    let mut run = Run::new(store(), Default::default()).await;
    run.fresh().await;
    run.create("s1", None, true).await;
    // Explicitly arrange the exit window so the refusal contract can fail independently
    // of the missing restart hook. No healthy boot retry should mask a terminal cause.
    run.engine.sequencer_read_failed.store(true, Ordering::SeqCst);
    run.engine.shutdown.begin();
    assert_replay_refused(&run).await;
    run.stop().await;
}

async fn closed_tail_snapshot() -> (Arc<ScriptedStore>, Arc<std::sync::Mutex<Oracle>>, LogPosition) {
    let mut seed = Run::new(store(), Default::default()).await;
    seed.fresh().await;
    seed.create("s1", None, true).await;
    let command = seed.engine.state.lock().await.sequencer.as_ref().unwrap().cmd_tx.clone();
    let (done, ack) = tokio::sync::oneshot::channel();
    command.send(SequencerCmd::PauseReads { done }).unwrap();
    bounded("pause before rotation", ack).await.unwrap();
    for _ in 0..2 {
        // Empty segments do not rotate. An untracked transaction has no shape effect,
        // but makes each rotation real and leaves the saved tail beyond its pointer.
        seed.engine.changes.append_commit(&[transaction_row(0, true, "public.untracked")]).await.unwrap();
        seed.engine.changes.force_rotate().await;
    }
    assert_eq!(seed.engine.changes.state().current(), 2);
    let head = seed.engine.ds.head("changes/0").await.unwrap().unwrap();
    assert!(head.closed);
    let tail = LogPosition { segment: 0, offset: head.next_offset.unwrap() };
    assert_ne!(tail.offset, "-1", "checkpoint must be beyond the rotation pointer");
    seed.engine
        .catalog_tx
        .send_durable(CatalogEvent::Offset { pos: tail.clone(), highwater: None, groups: Default::default() })
        .await
        .unwrap();
    assert!(seed.engine.ds.head("changes/2").await.unwrap().is_some());
    let oracle = seed.oracle.clone();
    (seed.crash().await, oracle, tail)
}

async fn lose_successor_after_boot_check(status: u16) -> (Run, LogPosition) {
    let (mut saved, oracle, tail) = closed_tail_snapshot().await;
    if status != 503 {
        Arc::get_mut(&mut saved).unwrap().missing_status = status;
    }
    let restarted = Run::new(saved, oracle).await;
    let fold = restarted.engine.fold_catalog().await.unwrap();
    restarted
        .engine
        .init_change_log(fold.current_segment, fold.segment_starts.clone(), &fold.start_pos())
        .await
        .unwrap();
    // The chain was intact at boot validation. Lose the successor before the reader
    // starts, so this still exercises the runtime fallback after boot gains its check.
    if status == 503 {
        *restarted.store.head_fault.lock().unwrap() = Some(("changes/1".into(), 503));
    } else {
        restarted.engine.ds.delete_stream("changes/1").await.unwrap();
    }
    let (tx, mut heads) = mpsc::unbounded_channel();
    *restarted.store.head_observer.lock().unwrap() = Some(tx);
    let compiled = restarted.engine.tables_shared.read().unwrap().clone();
    restarted.engine.apply_catalog(fold, &compiled, RestoreMode::Resume).await.unwrap();
    restarted.check_clients().await;
    bounded("successor HEAD answered", async {
        loop {
            let (path, observed) = heads.recv().await.unwrap();
            if path.ends_with("changes/1") {
                assert_eq!(observed, status);
                break;
            }
        }
    })
    .await;
    (restarted, tail)
}

#[tokio::test]
async fn crashsim_boot_refuses_a_gap_between_checkpoint_and_writer() {
    for status in [404, 410, 503] {
        let (mut saved, _, tail) = closed_tail_snapshot().await;
        Arc::get_mut(&mut saved).unwrap().missing_status = status;
        let ds = DsClient::with_test_store("scripted://boot-gap".into(), saved.clone());
        if status == 503 {
            *saved.head_fault.lock().unwrap() = Some(("changes/1".into(), 503));
        } else {
            ds.delete_stream("changes/1").await.unwrap();
        }
        // Exercise the real PG boot admission gate without opening a connection. This is
        // the segment-validation phase, before restore or ACTIVE publication.
        let engine = Engine::new_pg_for_in_process_test(ds, "postgres://unused.invalid/unused".into());
        engine.health.store(HEALTH_STARTING, Ordering::Release);
        let fold = engine.fold_catalog().await.unwrap();
        assert_eq!(fold.start_pos(), tail);
        let error = engine
            .init_change_log(fold.current_segment, fold.segment_starts.clone(), &tail)
            .await
            .expect_err("boot must validate every segment needed for replay");
        assert!(format!("{error:#}").contains("changes/1"), "missing context: {error:#}");
        assert_eq!(engine.health_status(), "starting", "failed boot must never publish active");
        assert!(engine.ensure_not_degraded().unwrap_err().is::<DeploymentNotReady>());
        assert!(engine.state.lock().await.sequencer.is_none(), "validation must precede replay");
        assert!(!engine.shutdown.restart_requested(), "boot must not enter a runtime exit-75 loop");
        if status == 503 {
            assert_eq!(crate::pg::boot_disposition(&error), crate::pg::BootFailure::Retryable);
            engine.init_change_log(fold.current_segment, fold.segment_starts.clone(), &tail).await.unwrap();
            assert_eq!(engine.changes.state().current(), 2);
        } else {
            assert_eq!(crate::pg::boot_disposition(&error), crate::pg::BootFailure::Fatal);
            assert!(format!("{error:#}").contains("refusing to boot"));
        }
        assert_eq!(engine.close(DEADLINE).await, crate::shutdown::ShutdownOutcome::Complete);
        assert_eq!(checkpoints(&saved).last().unwrap().0, tail, "failed boot must not rewrite its checkpoint");
    }
}

#[tokio::test]
async fn crashsim_false_live_get_loss_restarts_and_boots_intact_chain() {
    for get_status in [404, 410] {
        let mut run = Run::new(store(), Default::default()).await;
        run.fresh().await;
        run.create("s1", None, true).await;
        run.write(0x10, 1, Some("before false GET loss")).await;
        let position = run.processed().await;
        let command = run.engine.state.lock().await.sequencer.as_ref().unwrap().cmd_tx.clone();
        let (done, ack) = tokio::sync::oneshot::channel();
        command.send(SequencerCmd::PauseReads { done }).unwrap();
        bounded("pause before false GET loss", ack).await.unwrap();
        while run.reads.try_recv().is_ok() {}
        *run.store.read_fault.lock().unwrap() = Some(("changes/0".into(), get_status));
        let (done, ack) = tokio::sync::oneshot::channel();
        command.send(SequencerCmd::ResumeReads { done }).unwrap();
        bounded("resume with false GET loss", ack).await.unwrap();
        run.read_at(&position).await;
        // This current-thread fixture returns the fault and runs its handler in the
        // same poll as read arrival; a suspending response would need a completion fence.
        assert!(
            run.engine.shutdown.restart_requested(),
            "GET {get_status} must request restart even with intact storage"
        );
        assert_eq!(bounded("false GET loss restart", run.engine.shutdown.wait_for_restart()).await, 75);
        assert_eq!(run.engine.health_status(), "degraded");
        assert_eq!(run.engine.readiness_status(), "shutting_down");
        assert_eq!(run.processed().await, position, "a failed GET must preserve the replay boundary");
        assert_eq!(replay_failed_diagnostic(&run).await, true);
        assert_replay_refused(&run).await;
        assert_eq!(run.engine.metrics.snapshot()["counters"]["sequencer_stream_gone_failures_total"], 1);
        assert!(run.engine.ds.head("changes/0").await.unwrap().is_some(), "the GET fault must not delete storage");
        run.stop().await;
        assert_eq!(checkpoints(&run.store).last().unwrap().0, position);

        let mut restarted = Run::new(retained(&run.store), run.oracle.clone()).await;
        restarted.boot().await;
        restarted.probe_after_restart().await;
        assert_eq!(restarted.engine.health_status(), "active");
        assert!(!restarted.engine.shutdown.restart_requested());
        restarted.stop().await;
    }
}

#[tokio::test]
async fn crashsim_successor_lost_after_boot_check_is_terminal() {
    for status in [404, 410] {
        let (run, tail) = lose_successor_after_boot_check(status).await;
        assert_eq!(run.processed().await, tail, "loss must not skip to the existing writer segment");
        assert_eq!(run.engine.health_status(), "degraded", "confirmed missing successor must withdraw readiness");
        assert!(run.engine.shutdown.restart_requested(), "successor loss must request restart");
        assert_eq!(bounded("successor restart notification", run.engine.shutdown.wait_for_restart()).await, 75);
        assert_eq!(run.engine.readiness_status(), "shutting_down");
        assert_eq!(replay_failed_diagnostic(&run).await, true);
        assert_replay_refused(&run).await;
        let metrics = run.engine.metrics.clone();
        assert_eq!(metrics.snapshot()["counters"]["sequencer_stream_gone_failures_total"], 1);
        run.stop().await;
        assert_eq!(metrics.snapshot()["counters"]["sequencer_stream_gone_failures_total"], 1);
        assert_eq!(checkpoints(&run.store).last().unwrap().0, tail, "shutdown must preserve the replay boundary");
    }
}

#[tokio::test]
async fn crashsim_transient_successor_head_retries_and_delivers() {
    let (mut run, _) = lose_successor_after_boot_check(503).await;
    assert_eq!(run.engine.health_status(), "active");
    assert!(!run.engine.shutdown.restart_requested());
    run.drain().await;
    run.write(0x100, 42, Some("after transient HEAD failure")).await;
    assert_eq!(run.engine.health_status(), "active");
    assert!(!run.engine.shutdown.restart_requested());
    assert_eq!(run.engine.metrics.snapshot()["counters"]["sequencer_stream_gone_failures_total"], 0);
    run.stop().await;
}

#[tokio::test]
async fn crashsim_reactivating_shape_pins_segment_until_replay_finishes() {
    let mut run = Run::new(store(), Default::default()).await;
    run.fresh().await;
    run.create("s1", None, true).await;
    run.write(0x10, 1, Some("old")).await;
    run.release("s1").await;
    run.deactivate("s1").await;
    run.engine.changes.force_rotate().await;
    run.drain().await;
    run.write(0x20, 1, Some("new")).await;
    // Make the retained segment older than the configured window without a wall-clock sleep.
    run.engine.changes.state().adopt(1, BTreeMap::from([(0, 1), (1, 2)]), Some(run.engine.changes_position().offset));
    let gate = Arc::new(BodyGate::default());
    *run.store.read_gate.lock().unwrap() = Some(("changes/0".into(), false, gate.clone()));
    let mut resume = Box::pin(run.engine.ensure_active("s1"));
    bounded("replay read held", async {
        tokio::select! {
            _ = gate.entered.notified() => {},
            result = &mut resume => panic!("resume bypassed read gate: {result:?}"),
        }
    })
    .await;
    assert!(matches!(run.engine.lives.lock().unwrap()["s1"].state, LifeState::Reactivating { .. }));
    run.engine.sweep_change_log().await;
    assert!(run.engine.ds.head("changes/0").await.unwrap().is_some(), "sweep deleted a held replay's segment");
    assert!(run.engine.state.lock().await.shapes.contains_key("s1"));
    *run.store.read_gate.lock().unwrap() = None;
    gate.released.notify_one();
    bounded("replay released", &mut resume).await.unwrap();
    drop(resume);
    run.oracle.lock().unwrap().feeds.get_mut("s1").unwrap().active = true;
    run.check().await;
    run.engine.sweep_change_log().await;
    run.engine.catalog_tx.wait_durable().await.unwrap();
    assert!(run.engine.ds.head("changes/0").await.unwrap().is_none());
    run.check().await;
    run.stop().await;
}

#[tokio::test]
async fn crashsim_503_at_every_workload_write() {
    let mut baseline = Run::new(store(), Default::default()).await;
    bounded("fault sweep baseline", workload(&mut baseline)).await;
    let count = baseline.store.operations.lock().unwrap().iter().filter(|op| *op == "append").count();
    baseline.stop().await;
    assert!(count >= 20, "write sweep must cover catalog, source and shape appends");
    eprintln!("write fault sweep: {count} appends, transient and ambiguous outcomes");
    for ambiguous in [false, true] {
        for cut in 0..count {
            let mut run = Run::new(store(), Default::default()).await;
            *run.store.append_fault.lock().unwrap() = Some((
                String::new(),
                cut,
                if ambiguous { AppendFault::CommitThenUnavailable } else { AppendFault::Unavailable },
            ));
            bounded(&format!("write fault {cut}/{count}, ambiguous={ambiguous}"), workload(&mut run)).await;
            assert!(run.store.append_fault.lock().unwrap().is_none(), "fault {cut} never fired");
            run.stop().await;
            let oracle = run.oracle.clone();
            let saved = retained(&run.store);
            let mut restarted = Run::new(saved, oracle).await;
            restarted.boot().await;
            restarted.probe_after_restart().await;
            restarted.stop().await;
        }
    }
}

/// Negative controls for the oracle itself: a restored registry cannot choose which client
/// promises get checked. Catch only the named semantic assertion, after a successful boot.
#[tokio::test]
async fn crashsim_oracle_rejects_lost_acknowledged_clients() {
    for (mutation, assertion) in [
        ("registry", "lost acknowledged shapes"),
        ("lifecycle", "retained shapes must have lifecycle entries"),
        ("subscription", "lost acknowledged subscriptions"),
    ] {
        let mut seed = Run::new(store(), Default::default()).await;
        bounded("oracle control workload", workload(&mut seed)).await;
        seed.stop().await;
        let mut restored = Run::new(retained(&seed.store), seed.oracle.clone()).await;
        restored.boot().await;
        restored.drain().await;
        match mutation {
            "registry" => restored.engine.state.lock().await.shapes.clear(),
            "lifecycle" => restored.engine.lives.lock().unwrap().clear(),
            "subscription" => restored.engine.state.lock().await.feed_shares.get_mut("s1").unwrap().subs.clear(),
            _ => unreachable!(),
        }
        let cleanup = restored.engine.clone();
        let outcome =
            bounded("oracle must reject corruption", tokio::spawn(async move { restored.check().await })).await;
        assert_eq!(
            bounded("oracle control cleanup", cleanup.close(DEADLINE)).await,
            crate::shutdown::ShutdownOutcome::Complete
        );
        let panic = outcome.expect_err(&format!("oracle accepted lost acknowledged {mutation}"));
        assert!(panic.is_panic(), "oracle task was cancelled, not a semantic failure");
        let payload = panic.into_panic();
        let message = payload
            .downcast_ref::<String>()
            .map(String::as_str)
            .or_else(|| payload.downcast_ref::<&str>().copied())
            .unwrap_or("non-string panic");
        assert!(message.contains(assertion), "wrong oracle failure for {mutation}: {message}");
    }
}
