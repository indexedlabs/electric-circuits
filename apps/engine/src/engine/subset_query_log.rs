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

    /// Log the page's outcome: `ok`, or `error` with the error's top-level message. Only the top
    /// level: deeper causes can quote a caller's predicate values (a Postgres cast error does), and
    /// those stay out of the logs.
    pub(super) fn finish<T>(mut self, result: &anyhow::Result<T>) {
        match result {
            Ok(_) => self.emit("ok", None, None),
            Err(e) => self.emit("error", Some(e.to_string()), None),
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

    fn emit(&mut self, outcome: &'static str, error: Option<String>, phase: Option<&'static str>) {
        self.logged = true;
        let (pool_in_use, pool_size) = self.pool.unwrap_or_default();
        tracing::info!(
            target: TARGET,
            table = %self.table,
            outcome,
            phase,
            error = error.as_deref(),
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
            self.emit("cancelled", None, Some(phase));
        }
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

    /// The `subset query finished` events `run` logs, as JSON field maps.
    fn events(run: impl FnOnce()) -> Vec<serde_json::Value> {
        let captured = Captured::default();
        let writer = captured.clone();
        let subscriber = tracing_subscriber::fmt()
            .json()
            .with_writer(move || writer.clone())
            .with_max_level(tracing::Level::INFO)
            .finish();
        tracing::subscriber::with_default(subscriber, run);
        let bytes = captured.0.lock().unwrap().clone();
        String::from_utf8(bytes)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
            .filter(|event| event["target"] == TARGET)
            .map(|event| event["fields"].clone())
            .collect()
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
        for absent in ["error", "phase"] {
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
    fn a_failed_page_keeps_the_phases_it_ran_and_only_its_top_level_error() {
        let logged = events(|| {
            let mut log = SubsetQueryLog::start(&table());
            log.state_held();
            log.pool = Some((20, 20));
            log.checkout.slot_wait = Duration::from_millis(30);
            log.query.snapshot = Duration::from_millis(1500);
            let failed: anyhow::Result<()> =
                Err(anyhow::anyhow!("invalid input syntax for type uuid: \"secret\"").context("subset select"));
            log.finish(&failed);
        });
        assert_eq!(logged.len(), 1, "{logged:?}");
        let event = &logged[0];
        assert_eq!(event["outcome"], "error");
        assert_eq!(event["error"], "subset select", "a predicate value never reaches the log");
        assert_eq!(event["snapshot_ms"], 1500);
        assert_eq!(event["pool_wait_ms"], 30);
        assert_eq!(event["dialed"], false);
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
        let waited = tokio::time::timeout(Duration::from_millis(25), pool.get_timed(&mut log.checkout)).await;
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
