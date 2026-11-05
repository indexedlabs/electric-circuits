//! The one `group load finished` event each `/v1/subsets/load` logs: its outcome and where its time
//! went (renewing or creating members, the stream tail, the pool, the settled snapshot, each member's
//! `SELECT`, completing the group), so an operator can tell a slow member from a saturated pool.
//!
//! Like `subset_query_log`, the event comes from a guard, so a load whose request is dropped mid-await
//! still reports `cancelled` and the phase it was in. Member predicates never reach the event.

use std::time::{Duration, Instant};

use axum::http::StatusCode;

use super::subset_query_log::{Failure, ms};
use crate::pg::{QueryPhase, SubsetBatchTiming};

/// The log target, so the events can be filtered on their own.
const TARGET: &str = "electric_circuits_engine::group_load";

/// A step of a group load outside the batch read, which times itself in [`SubsetBatchTiming`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum GroupLoadPhase {
    /// Checking the request and its members against the engine's schemas.
    Validate,
    /// Probing and renewing the group this subscription already holds.
    Renew,
    /// Reserving the claim and creating the member shapes.
    Create,
    /// Reading the group stream's tail.
    Tail,
    /// Planning and reading the members in one snapshot.
    Query,
    /// Making the group record durable.
    Complete,
}

impl GroupLoadPhase {
    fn name(self) -> &'static str {
        match self {
            GroupLoadPhase::Validate => "validate",
            GroupLoadPhase::Renew => "renew",
            GroupLoadPhase::Create => "create",
            GroupLoadPhase::Tail => "tail",
            GroupLoadPhase::Query => "query",
            GroupLoadPhase::Complete => "complete",
        }
    }
}

/// Collects one group load's timings as it runs and logs them exactly once.
pub(crate) struct GroupLoadLog {
    tables: String,
    member_count: usize,
    started: Instant,
    /// The group's stream, once the load knows which it is.
    group: Option<String>,
    current: Option<(GroupLoadPhase, Instant)>,
    last: Option<GroupLoadPhase>,
    validate: Duration,
    renew: Duration,
    create: Duration,
    tail: Duration,
    complete: Duration,
    /// The batch read's own pieces.
    pub(crate) batch: SubsetBatchTiming,
    /// Why the batch read failed, value-free.
    batch_failure: Option<Failure>,
    logged: bool,
}

impl GroupLoadLog {
    pub(crate) fn start<'a>(tables: impl Iterator<Item = &'a str>) -> Self {
        let tables: Vec<&str> = tables.collect();
        GroupLoadLog {
            member_count: tables.len(),
            tables: tables.join(","),
            started: Instant::now(),
            group: None,
            current: None,
            last: None,
            validate: Duration::ZERO,
            renew: Duration::ZERO,
            create: Duration::ZERO,
            tail: Duration::ZERO,
            complete: Duration::ZERO,
            batch: SubsetBatchTiming::default(),
            batch_failure: None,
            logged: false,
        }
    }

    /// Name the group's stream.
    pub(crate) fn group(&mut self, stream_path: &str) {
        self.group = Some(stream_path.to_owned());
    }

    /// End the phase under way and begin `phase`. A phase the load comes back to, because it reread
    /// a group that was being released, accumulates.
    pub(crate) fn phase(&mut self, phase: GroupLoadPhase) {
        self.end_phase();
        self.current = Some((phase, Instant::now()));
        self.last = Some(phase);
    }

    fn end_phase(&mut self) {
        let Some((phase, since)) = self.current.take() else { return };
        let elapsed = since.elapsed();
        match phase {
            GroupLoadPhase::Validate => self.validate += elapsed,
            GroupLoadPhase::Renew => self.renew += elapsed,
            GroupLoadPhase::Create => self.create += elapsed,
            GroupLoadPhase::Tail => self.tail += elapsed,
            GroupLoadPhase::Complete => self.complete += elapsed,
            // The batch times its own pieces.
            GroupLoadPhase::Query => {}
        }
    }

    /// Record why the batch read failed, while the typed error is still at hand.
    pub(crate) fn batch_failed(&mut self, error: &anyhow::Error) {
        self.batch_failure = Some(Failure::of(error));
    }

    /// Log the load's outcome: `ok`, or `error` with the HTTP status it answered.
    pub(crate) fn finish(mut self, status: Result<(), StatusCode>) {
        self.end_phase();
        match status {
            Ok(()) => self.emit("ok", None, None),
            Err(status) => {
                let phase = self.phase_name();
                self.emit("error", Some(phase), Some(status));
            }
        }
    }

    /// The last phase begun, naming the batch's own step when it was reading.
    fn phase_name(&self) -> &'static str {
        match self.last {
            Some(GroupLoadPhase::Query) => {
                self.batch.query.last.or(self.batch.checkout.last).map_or("plan", QueryPhase::name)
            }
            Some(phase) => phase.name(),
            None => "start",
        }
    }

    /// Credit a dropped batch's step under way to its field.
    fn credit_batch_in_progress(&mut self) {
        let Some((phase, since)) = self.batch.checkout.in_progress.take().or(self.batch.query.in_progress.take())
        else {
            return;
        };
        let elapsed = since.elapsed();
        match phase {
            QueryPhase::PoolWait => self.batch.checkout.slot_wait = elapsed,
            QueryPhase::Connect => self.batch.checkout.connect = Some(elapsed),
            QueryPhase::Snapshot => self.batch.query.snapshot = elapsed,
            QueryPhase::Select => self.batch.query.select = elapsed,
        }
    }

    fn emit(&mut self, outcome: &'static str, phase: Option<&'static str>, status: Option<StatusCode>) {
        self.logged = true;
        let (pool_in_use, pool_size) = self.batch.pool.unwrap_or_default();
        let (error_kind, sqlstate) = self.batch_failure.take().map_or((None, None), |f| (Some(f.kind), f.sqlstate));
        let reads = self.batch.reads.lock().unwrap();
        let rows: usize = reads.members.iter().map(|member| member.rows).sum();
        // One string field, so a log search can read every member of a load from its one line.
        let members = serde_json::Value::Array(
            reads
                .members
                .iter()
                .map(|member| {
                    serde_json::json!({
                        "table": member.table,
                        "rows": member.rows,
                        "started_ms": ms(member.started),
                        "select_ms": ms(member.select),
                    })
                })
                .collect(),
        );
        tracing::info!(
            target: TARGET,
            group = self.group.as_deref(),
            tables = %self.tables,
            member_count = self.member_count as u64,
            outcome,
            phase,
            status = status.map(|status| u64::from(status.as_u16())),
            error_kind,
            sqlstate = sqlstate.as_deref(),
            rows = rows as u64,
            validate_ms = ms(self.validate),
            renew_ms = ms(self.renew),
            create_ms = ms(self.create),
            tail_ms = ms(self.tail),
            plan_ms = ms(self.batch.plan),
            pool_wait_ms = ms(self.batch.checkout.slot_wait),
            dialed = self.batch.checkout.connect.is_some(),
            connect_ms = ms(self.batch.checkout.connect.unwrap_or_default()),
            pool_in_use = pool_in_use as u64,
            pool_size = pool_size as u64,
            snapshot_ms = ms(self.batch.query.snapshot),
            select_ms = ms(self.batch.query.select),
            connections = reads.connections as u64,
            helper_wait_ms = ms(reads.helper_wait),
            members = %members,
            complete_ms = ms(self.complete),
            total_ms = ms(self.started.elapsed()),
            "group load finished"
        );
    }
}

impl Drop for GroupLoadLog {
    fn drop(&mut self) {
        if !self.logged {
            self.end_phase();
            self.credit_batch_in_progress();
            let phase = self.phase_name();
            self.emit("cancelled", Some(phase), None);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;
    use std::sync::{Arc, Mutex};

    use super::*;
    use crate::pg::BatchMemberTiming;

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

    /// The `group load finished` events `run` logs, as JSON field maps.
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

    fn log() -> GroupLoadLog {
        GroupLoadLog::start(["public.record", "public.assignment"].into_iter())
    }

    #[test]
    fn a_finished_load_logs_its_phases_and_members_once() {
        let logged = events(|| {
            let mut log = log();
            log.group("shape/g7");
            log.create = Duration::from_millis(11);
            log.tail = Duration::from_millis(2);
            log.complete = Duration::from_millis(9);
            log.batch.pool = Some((4, 20));
            log.batch.query.snapshot = Duration::from_millis(30);
            log.batch.query.select = Duration::from_millis(440);
            {
                let mut reads = log.batch.reads.lock().unwrap();
                reads.connections = 2;
                reads.helper_wait = Duration::from_millis(6);
                reads.members.push(BatchMemberTiming {
                    table: "public.record".into(),
                    rows: 3,
                    started: Duration::ZERO,
                    select: Duration::from_millis(435),
                });
                reads.members.push(BatchMemberTiming {
                    table: "public.assignment".into(),
                    rows: 4,
                    started: Duration::from_millis(7),
                    select: Duration::from_millis(213),
                });
            }
            log.finish(Ok(()));
        });
        assert_eq!(logged.len(), 1, "{logged:?}");
        let event = &logged[0];
        assert_eq!(event["message"], "group load finished");
        assert_eq!(event["outcome"], "ok");
        assert_eq!(event["group"], "shape/g7");
        assert_eq!(event["tables"], "public.record,public.assignment");
        for (field, value) in [
            ("member_count", 2),
            ("rows", 7),
            ("create_ms", 11),
            ("tail_ms", 2),
            ("complete_ms", 9),
            ("pool_in_use", 4),
            ("pool_size", 20),
            ("snapshot_ms", 30),
            ("select_ms", 440),
            ("connections", 2),
            ("helper_wait_ms", 6),
        ] {
            assert_eq!(event[field], value, "{field} in {event}");
        }
        let members: serde_json::Value = serde_json::from_str(event["members"].as_str().unwrap()).unwrap();
        assert_eq!(
            members,
            serde_json::json!([
                {"table": "public.record", "rows": 3, "started_ms": 0, "select_ms": 435},
                {"table": "public.assignment", "rows": 4, "started_ms": 7, "select_ms": 213},
            ])
        );
    }

    /// A member's failure names the batch step and its SQLSTATE, and no predicate value reaches it.
    #[test]
    fn a_failed_load_names_its_phase_status_and_cause() {
        let logged = events(|| {
            let mut log = log();
            log.phase(GroupLoadPhase::Query);
            log.batch.query.last = Some(QueryPhase::Select);
            let unsettled =
                crate::pg::SnapshotUnsettled { cause: crate::pg::UnsettledCause::TimedOut, pending: vec![7] };
            log.batch_failed(&anyhow::Error::new(unsettled).context("private-sentinel"));
            log.finish(Err(StatusCode::SERVICE_UNAVAILABLE));
        });
        assert_eq!(logged.len(), 1, "{logged:?}");
        let event = &logged[0];
        assert_eq!(event["outcome"], "error");
        assert_eq!(event["phase"], "select");
        assert_eq!(event["status"], 503);
        assert_eq!(event["error_kind"], "settle_timed_out");
        assert!(!event.to_string().contains("private-sentinel"), "{event}");
    }

    #[test]
    fn a_load_dropped_mid_phase_credits_that_phase() {
        let logged = events(|| {
            let mut log = log();
            log.phase(GroupLoadPhase::Create);
            std::thread::sleep(Duration::from_millis(15));
            drop(log);
        });
        assert_eq!(logged.len(), 1, "{logged:?}");
        assert_eq!(logged[0]["outcome"], "cancelled");
        assert_eq!(logged[0]["phase"], "create");
        assert!(logged[0]["create_ms"].as_u64().unwrap() >= 15, "{}", logged[0]);
    }

    #[test]
    fn a_load_dropped_during_a_select_credits_the_select() {
        let logged = events(|| {
            let mut log = log();
            log.phase(GroupLoadPhase::Query);
            log.batch.query.last = Some(QueryPhase::Select);
            log.batch.query.in_progress = Some((QueryPhase::Select, Instant::now()));
            std::thread::sleep(Duration::from_millis(15));
            drop(log);
        });
        assert_eq!(logged.len(), 1, "{logged:?}");
        assert_eq!(logged[0]["outcome"], "cancelled");
        assert_eq!(logged[0]["phase"], "select");
        assert!(logged[0]["select_ms"].as_u64().unwrap() >= 15, "{}", logged[0]);
    }
}
