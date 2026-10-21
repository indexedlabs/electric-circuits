//! The one `subset query finished` event each `/v1/subsets/query` page logs: its outcome and where
//! its time went (the engine-state lock, a pool slot, a dial, the settled snapshot, the `SELECT`), so
//! an operator can tell a saturated pool from a slow database under a burst of pages.
//!
//! The event is emitted from a guard: a page that ends normally reports `ok` or `error`, and a page
//! whose request is dropped mid-await (a client disconnect or an upstream timeout) reports
//! `cancelled`, naming the `phase` it was dropped in and crediting that phase with the time it had
//! run. Without the guard the slowest pages, the ones a caller gave up on, would never be logged.

use std::time::{Duration, Instant};

use crate::pg::{CheckoutTiming, QueryPhase, SubsetQueryTiming};
use crate::table_ref::TableRef;

/// The log target, so the events can be filtered on their own.
pub(super) const TARGET: &str = "electric_circuits_engine::subset_query";

/// Collects one page's timings as it runs and logs them exactly once.
pub(super) struct SubsetQueryLog {
    table: String,
    started: Instant,
    /// Taking the engine-state lock; `None` until the lock is held.
    pub(super) state_wait: Option<Duration>,
    /// Pool occupancy and cap when the page asked for a connection; `None` until it asks.
    pub(super) pool: Option<(usize, usize)>,
    pub(super) checkout: CheckoutTiming,
    pub(super) query: SubsetQueryTiming,
    pub(super) rows: usize,
    logged: bool,
}

impl SubsetQueryLog {
    pub(super) fn start(table: &TableRef) -> Self {
        SubsetQueryLog {
            table: table.to_string(),
            started: Instant::now(),
            state_wait: None,
            pool: None,
            checkout: CheckoutTiming::default(),
            query: SubsetQueryTiming::default(),
            rows: 0,
            logged: false,
        }
    }

    /// Record that the engine-state lock is held, the first phase.
    pub(super) fn state_held(&mut self) {
        self.state_wait = Some(self.started.elapsed());
    }

    /// Log the page's outcome: `ok`, or `error` with the phase it failed in and a value-free
    /// description of why (see [`Failure`]).
    pub(super) fn finish<T>(mut self, result: &anyhow::Result<T>) {
        match result {
            Ok(_) => self.emit("ok", None, None),
            Err(e) => {
                let phase = self.failed_phase();
                self.emit("error", Some(phase), Some(Failure::of(e)));
            }
        }
    }

    /// The last phase a failed page began: a Postgres phase, else `prepare` (compiling the
    /// predicate and columns, after the lock), else the state lock itself.
    fn failed_phase(&self) -> &'static str {
        match self.query.last.or(self.checkout.last) {
            Some(phase) => phase.name(),
            None if self.state_wait.is_some() => "prepare",
            None => "state_wait",
        }
    }

    /// The phase a dropped page was in, with its elapsed time credited to that phase's field.
    fn credit_in_progress(&mut self) -> &'static str {
        if self.state_wait.is_none() {
            self.state_wait = Some(self.started.elapsed());
            return "state_wait";
        }
        let Some((phase, since)) = self.checkout.in_progress.take().or(self.query.in_progress.take()) else {
            return "between_phases";
        };
        let elapsed = since.elapsed();
        match phase {
            QueryPhase::PoolWait => self.checkout.slot_wait = elapsed,
            QueryPhase::Connect => self.checkout.connect = Some(elapsed),
            QueryPhase::Snapshot => self.query.snapshot = elapsed,
            QueryPhase::Select => self.query.select = elapsed,
        }
        phase.name()
    }

    fn emit(&mut self, outcome: &'static str, phase: Option<&'static str>, failure: Option<Failure>) {
        self.logged = true;
        let (pool_in_use, pool_size) = self.pool.unwrap_or_default();
        let (error_kind, sqlstate) = failure.map_or((None, None), |f| (Some(f.kind), f.sqlstate));
        tracing::info!(
            target: TARGET,
            table = %self.table,
            outcome,
            phase,
            error_kind,
            sqlstate = sqlstate.as_deref(),
            rows = self.rows as u64,
            state_wait_ms = ms(self.state_wait.unwrap_or_default()),
            pool_wait_ms = ms(self.checkout.slot_wait),
            dialed = self.checkout.connect.is_some(),
            connect_ms = ms(self.checkout.connect.unwrap_or_default()),
            pool_in_use = pool_in_use as u64,
            pool_size = pool_size as u64,
            snapshot_ms = ms(self.query.snapshot),
            select_ms = ms(self.query.select),
            total_ms = ms(self.started.elapsed()),
            "subset query finished"
        );
    }
}

impl Drop for SubsetQueryLog {
    fn drop(&mut self) {
        if !self.logged {
            let phase = self.credit_in_progress();
            self.emit("cancelled", Some(phase), None);
        }
    }
}

/// Why a page failed, without its message. Messages can quote a caller's predicate values (a
/// non-numeric string compared to an integer column fails with the string in the text, and so do
/// Postgres cast errors), so the event carries a fixed category and, for a Postgres error, its
/// SQLSTATE: enough to tell a settle wait, a refused connection (`53300`) or a cancelled statement
/// (`57014`) apart.
struct Failure {
    kind: &'static str,
    sqlstate: Option<String>,
}

impl Failure {
    fn of(e: &anyhow::Error) -> Self {
        if let Some(unsettled) = e.downcast_ref::<crate::pg::SnapshotUnsettled>() {
            let kind = match unsettled.cause {
                crate::pg::UnsettledCause::TimedOut => "settle_timed_out",
                crate::pg::UnsettledCause::Rejected => "settle_rejected",
                crate::pg::UnsettledCause::Shared => "settle_shared",
                crate::pg::UnsettledCause::Overflowed => "settle_overflowed",
            };
            return Failure { kind, sqlstate: None };
        }
        if let Some(pg) = e.chain().find_map(|cause| cause.downcast_ref::<tokio_postgres::Error>()) {
            let kind = if pg.is_closed() { "postgres_closed" } else { "postgres" };
            return Failure { kind, sqlstate: pg.code().map(|code| code.code().to_owned()) };
        }
        Failure { kind: "other", sqlstate: None }
    }
}

fn ms(d: Duration) -> u64 {
    u64::try_from(d.as_millis()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use std::io::Write;
    use std::sync::{Arc, Mutex};

    use super::*;

    #[derive(Clone, Default)]
    struct Captured(Arc<Mutex<Vec<u8>>>);

    impl Write for Captured {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    /// Captures the JSON events logged on this thread while it lives.
    struct Capture {
        captured: Captured,
        _guard: tracing::subscriber::DefaultGuard,
    }

    impl Capture {
        fn start() -> Self {
            let captured = Captured::default();
            let writer = captured.clone();
            let subscriber = tracing_subscriber::fmt()
                .json()
                .with_writer(move || writer.clone())
                .with_max_level(tracing::Level::INFO)
                .finish();
            Capture { captured, _guard: tracing::subscriber::set_default(subscriber) }
        }

        /// The raw lines of the `subset query finished` events so far.
        fn lines(&self) -> Vec<String> {
            let bytes = self.captured.0.lock().unwrap().clone();
            String::from_utf8(bytes)
                .unwrap()
                .lines()
                .filter(|line| serde_json::from_str::<serde_json::Value>(line).unwrap()["target"] == TARGET)
                .map(str::to_owned)
                .collect()
        }

        /// Those events' fields.
        fn events(&self) -> Vec<serde_json::Value> {
            self.lines()
                .iter()
                .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap()["fields"].clone())
                .collect()
        }
    }

    /// The `subset query finished` events `run` logs, as JSON field maps.
    fn events(run: impl FnOnce()) -> Vec<serde_json::Value> {
        let capture = Capture::start();
        run();
        capture.events()
    }

    fn table() -> TableRef {
        "public.items".into()
    }

    #[test]
    fn a_finished_page_logs_its_phases_once() {
        let logged = events(|| {
            let mut log = SubsetQueryLog::start(&table());
            log.state_wait = Some(Duration::from_millis(3));
            log.pool = Some((7, 20));
            log.checkout.slot_wait = Duration::from_millis(40);
            log.checkout.connect = Some(Duration::from_millis(9));
            log.query.snapshot = Duration::from_millis(12);
            log.query.select = Duration::from_millis(150);
            log.rows = 5;
            log.finish(&anyhow::Ok(()));
        });
        assert_eq!(logged.len(), 1, "{logged:?}");
        let event = &logged[0];
        assert_eq!(event["message"], "subset query finished");
        assert_eq!(event["table"], "public.items");
        assert_eq!(event["outcome"], "ok");
        for absent in ["error_kind", "sqlstate", "phase"] {
            assert!(event.get(absent).is_none_or(serde_json::Value::is_null), "{absent} in {event}");
        }
        for (field, value) in [
            ("rows", 5),
            ("state_wait_ms", 3),
            ("pool_wait_ms", 40),
            ("connect_ms", 9),
            ("pool_in_use", 7),
            ("pool_size", 20),
            ("snapshot_ms", 12),
            ("select_ms", 150),
        ] {
            assert_eq!(event[field], value, "{field} in {event}");
        }
        assert_eq!(event["dialed"], true);
    }

    #[test]
    fn a_settle_failure_names_its_phase_and_cause() {
        let logged = events(|| {
            let mut log = SubsetQueryLog::start(&table());
            log.state_held();
            log.pool = Some((20, 20));
            log.checkout.slot_wait = Duration::from_millis(30);
            log.checkout.last = Some(QueryPhase::PoolWait);
            log.query.snapshot = Duration::from_millis(1500);
            log.query.last = Some(QueryPhase::Snapshot);
            let unsettled =
                crate::pg::SnapshotUnsettled { cause: crate::pg::UnsettledCause::TimedOut, pending: vec![7] };
            let failed: anyhow::Result<()> = Err(anyhow::Error::new(unsettled));
            log.finish(&failed);
        });
        assert_eq!(logged.len(), 1, "{logged:?}");
        let event = &logged[0];
        assert_eq!(event["outcome"], "error");
        assert_eq!(event["phase"], "snapshot");
        assert_eq!(event["error_kind"], "settle_timed_out");
        assert_eq!(event["snapshot_ms"], 1500);
        assert_eq!(event["pool_wait_ms"], 30);
        assert_eq!(event["dialed"], false);
    }

    /// Through the real `Engine::query_subset` and predicate compiler: a non-numeric string compared
    /// to an integer column fails with the string in its message, and none of it reaches the event.
    #[tokio::test]
    async fn a_rejected_predicate_logs_a_value_free_error_from_the_real_query_path() {
        use crate::ds::DsClient;
        use crate::engine::Engine;
        use crate::schema::{TableDef, TableSchema};

        let engine = Engine::new_for_in_process_test(DsClient::new_for_in_process_test("http://127.0.0.1:1"));
        let def: TableDef =
            serde_json::from_value(serde_json::json!({"columns":{"id":{"type":"int"}},"primaryKey":"id"})).unwrap();
        engine.state.lock().await.tables.insert(table(), TableSchema::from_def(&table(), &def).unwrap());
        let predicate =
            serde_json::from_value(serde_json::json!({"col":"id","op":"eq","value":"private-sentinel"})).unwrap();

        let capture = Capture::start();
        let page = engine.query_subset(&table(), Some(predicate), None, None, None, None).await;
        let error = format!("{:#}", page.err().expect("a string is not an integer literal"));
        assert!(error.contains("private-sentinel"), "the compiler must have quoted the value: {error}");
        let lines = capture.lines();
        assert_eq!(lines.len(), 1, "{lines:?}");
        assert!(!lines[0].contains("private-sentinel"), "the predicate value leaked: {}", lines[0]);
        let event = &capture.events()[0];
        assert_eq!(event["outcome"], "error");
        assert_eq!(event["phase"], "prepare");
        assert_eq!(event["error_kind"], "other");
        drop(capture);
        engine.close(Duration::from_secs(5)).await;
    }

    #[test]
    fn a_page_dropped_before_the_lock_credits_the_state_wait() {
        let logged = events(|| {
            let log = SubsetQueryLog::start(&table());
            std::thread::sleep(Duration::from_millis(15));
            drop(log);
        });
        assert_eq!(logged.len(), 1, "{logged:?}");
        assert_eq!(logged[0]["outcome"], "cancelled");
        assert_eq!(logged[0]["phase"], "state_wait");
        assert!(logged[0]["state_wait_ms"].as_u64().unwrap() >= 15, "{}", logged[0]);
    }

    /// A page dropped while every pool slot is taken: the real checkout is left mid-wait, and the
    /// event credits that wait to `pool_wait_ms` rather than reading 0.
    #[tokio::test]
    async fn a_page_dropped_waiting_for_a_slot_credits_the_pool_wait() {
        let pool = crate::pg::Pool::new("postgres://unused/test".into(), 1);
        let held = pool.hold_slot_for_test().await;
        let mut log = SubsetQueryLog::start(&table());
        log.state_held();
        log.pool = Some((pool.in_use(), pool.size()));
        let waited = tokio::time::timeout(Duration::from_millis(30), pool.get_timed(&mut log.checkout)).await;
        assert!(waited.is_err(), "the full pool keeps the page waiting");
        let logged = events(|| drop(log));
        assert_eq!(logged.len(), 1, "{logged:?}");
        let event = &logged[0];
        assert_eq!(event["outcome"], "cancelled");
        assert_eq!(event["phase"], "pool_wait");
        assert!(event["pool_wait_ms"].as_u64().unwrap() >= 25, "{event}");
        assert_eq!((event["pool_in_use"].as_u64(), event["pool_size"].as_u64()), (Some(1), Some(1)));
        drop(held);
        pool.close();
    }
}
