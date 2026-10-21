//! The one `subset query finished` event each `/v1/subsets/query` page logs: its outcome and where
//! its time went (the engine-state lock, a pool slot, a dial, the settled snapshot, the `SELECT`), so
//! an operator can tell a saturated pool from a slow database under a burst of pages.
//!
//! The event is emitted from a guard: a page that ends normally reports `ok` or `error`, and a page
//! whose request is dropped mid-await (a client disconnect or an upstream timeout) reports
//! `cancelled` with the phases it reached. Without the guard the slowest pages, the ones a caller
//! gave up on, would never be logged.

use std::time::{Duration, Instant};

use crate::pg::{CheckoutTiming, SubsetQueryTiming};
use crate::table_ref::TableRef;

/// The log target, so the events can be filtered on their own.
pub(super) const TARGET: &str = "electric_circuits_engine::subset_query";

/// Collects one page's timings as it runs and logs them exactly once.
pub(super) struct SubsetQueryLog {
    table: String,
    started: Instant,
    /// Taking the engine-state lock and cloning the schemas.
    pub(super) state_wait: Duration,
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
            state_wait: Duration::ZERO,
            pool: None,
            checkout: CheckoutTiming::default(),
            query: SubsetQueryTiming::default(),
            rows: 0,
            logged: false,
        }
    }

    /// Log the page's outcome: `ok`, or `error` with the error chain.
    pub(super) fn finish<T>(mut self, result: &anyhow::Result<T>) {
        match result {
            Ok(_) => self.emit("ok", None),
            Err(e) => self.emit("error", Some(format!("{e:#}"))),
        }
    }

    fn emit(&mut self, outcome: &'static str, error: Option<String>) {
        self.logged = true;
        let (pool_in_use, pool_size) = self.pool.unwrap_or_default();
        tracing::info!(
            target: TARGET,
            table = %self.table,
            outcome,
            error = error.as_deref(),
            rows = self.rows as u64,
            state_wait_ms = ms(self.state_wait),
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
            self.emit("cancelled", None);
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
            log.state_wait = Duration::from_millis(3);
            log.pool = Some((7, 20));
            log.checkout =
                CheckoutTiming { slot_wait: Duration::from_millis(40), connect: Some(Duration::from_millis(9)) };
            log.query = SubsetQueryTiming { snapshot: Duration::from_millis(12), select: Duration::from_millis(150) };
            log.rows = 5;
            log.finish(&anyhow::Ok(()));
        });
        assert_eq!(logged.len(), 1, "{logged:?}");
        let event = &logged[0];
        assert_eq!(event["message"], "subset query finished");
        assert_eq!(event["table"], "public.items");
        assert_eq!(event["outcome"], "ok");
        assert!(event.get("error").is_none_or(serde_json::Value::is_null), "{event}");
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
    fn a_failed_page_keeps_the_phases_it_ran_and_its_cause() {
        let logged = events(|| {
            let mut log = SubsetQueryLog::start(&table());
            log.pool = Some((20, 20));
            log.checkout.slot_wait = Duration::from_millis(30);
            log.query.snapshot = Duration::from_millis(1500);
            let failed: anyhow::Result<()> =
                Err(anyhow::anyhow!("snapshot did not settle").context("subset query-back"));
            log.finish(&failed);
        });
        assert_eq!(logged.len(), 1, "{logged:?}");
        let event = &logged[0];
        assert_eq!(event["outcome"], "error");
        assert_eq!(event["error"], "subset query-back: snapshot did not settle");
        assert_eq!(event["snapshot_ms"], 1500);
        assert_eq!(event["pool_wait_ms"], 30);
        assert_eq!(event["dialed"], false);
    }

    #[test]
    fn a_dropped_page_logs_as_cancelled() {
        let logged = events(|| {
            let mut log = SubsetQueryLog::start(&table());
            log.checkout.slot_wait = Duration::from_millis(80);
            drop(log);
        });
        assert_eq!(logged.len(), 1, "{logged:?}");
        assert_eq!(logged[0]["outcome"], "cancelled");
        assert_eq!(logged[0]["pool_wait_ms"], 80);
    }
}
