//! Engine shutdown: one token and task set per Engine in both hosting modes.
//!
//! `SIGTERM`/`SIGINT` must not be a kill: the ingestor may be half-way through appending a commit,
//! the sequencer may be half-way through fanning a transaction out, and the durable checkpoint that
//! decides where the next boot resumes is written lazily. A hard stop costs a replay (correct, but
//! wasteful) — and, worse, leaves a long-poll holding the pod's termination grace for its full
//! window.
//!
//! So shutdown is **cooperative and ordered**:
//!
//! 1. the token flips. `GET /ready` answers `503 {"status":"shutting_down"}` from that instant, and
//!    the server KEEPS ACCEPTING for `ELECTRIC_CIRCUITS_SHUTDOWN_DRAIN_SECS` so a load balancer
//!    actually gets to poll it and take the pod out of rotation. A probe that connection-refuses
//!    also reads as "not ready", but only after the LB has already sent requests into a closing
//!    socket; answering 503 for one probe interval first is what makes the drain graceful rather
//!    than merely fast;
//! 2. the HTTP server stops accepting and lets its in-flight requests finish — the `/v1/shape`
//!    `live=true` long-poll and the engine's own durable-streams long-polls join this token in their
//!    selects, so they return in milliseconds instead of pinning the shutdown for their full
//!    timeout;
//! 3. each **party** (the replication ingestor, the sequencer) reaches its own safe point and
//!    finishes: the ingestor completes a commit it is APPENDING, then records its position
//!    LOCALLY — the wire acknowledgement to Postgres rides the replication client's own status
//!    interval (1 s) and its stop path does not force a final one, so a commit appended in the last
//!    second or so is re-delivered after the restart and dropped by the sequencer's `(lsn, seq)`
//!    highwater. Stopping mid-transaction, before the commit, is the same story with nothing
//!    appended at all. Either way the slot is never advanced BY the shutdown. The sequencer
//!    flushes a completed batch and writes a final checkpoint. An interrupted reliable append
//!    preserves the earlier replay boundary; shutdown never checkpoints an undelivered batch;
//! 4. Engine close drains the catalog, joins circuit threads and remaining tasks, and clears owned callbacks.
//!    A source worker then drops its runtime; the standalone process reports the shutdown outcome.
//!
//! **The drain window.** Stopping to READ is immediate; finishing what was already read is not an
//! abort. A commit the sequencer has processed can still have subquery effects in flight — a flip's
//! query-back on a flip worker, a batch on an emission lane — and those are what decide whether the
//! subquery registry can be written to storage for the next boot (`crate::subq_state`). So the
//! tasks that carry them ([`ShutdownToken::spawn_draining`]) and the reliable appends they and the
//! sequencer make keep running after the token flips, until the drain ends: when Engine close has
//! seen its parties finish ([`ShutdownToken::end_drain`]), or [`ShutdownToken::set_drain_budget`]
//! after the flip, whichever is first. The budget comes out of the grace period and defaults to
//! zero, which is the old behaviour: everything stops at the flip.
//!
//! The whole thing is bounded (`ELECTRIC_CIRCUITS_SHUTDOWN_GRACE_SECS`, default 25 s — under a
//! typical Kubernetes `terminationGracePeriodSeconds: 30`), and a **second** signal during the grace
//! period stops waiting and exits non-zero. Streams are never closed or retired on shutdown: a
//! restored shape continues its stream, exactly as after a crash.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

/// Exit code when shutdown was forced rather than completed: a second signal arrived, or the grace
/// period elapsed with a party still outstanding. Distinct from `0` (clean) and from
/// [`crate::pg::EXIT_CONFIG`] / the circuit-rebuild `75`.
pub const EXIT_SHUTDOWN_FORCED: i32 = 70;
/// Grace elapsed far enough for background tasks to stop, but the durable catalog did not land its
/// final checkpoint/receipt. Exit is non-zero so an orchestrator never treats replay-required state
/// as a clean handoff.
pub const EXIT_SHUTDOWN_INCOMPLETE: i32 = 71;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShutdownOutcome {
    Complete,
    Forced,
    CatalogIncomplete,
}

impl ShutdownOutcome {
    pub const fn exit_code(self) -> i32 {
        match self {
            Self::Complete => 0,
            Self::Forced => EXIT_SHUTDOWN_FORCED,
            Self::CatalogIncomplete => EXIT_SHUTDOWN_INCOMPLETE,
        }
    }
}

/// How long the whole grace period may take, by default. Below a typical Kubernetes
/// `terminationGracePeriodSeconds: 30`, so the engine always gets to finish on its own terms rather
/// than being `SIGKILL`ed part-way.
pub const DEFAULT_GRACE: Duration = Duration::from_secs(25);

/// How long the durable catalog writer is given to drain after the sequencer's final checkpoint.
pub const CATALOG_DRAIN: Duration = Duration::from_secs(10);

/// How long the HTTP server keeps accepting after the token flips, answering `GET /ready` with 503,
/// so a load balancer's readiness probe observes the drain before the socket closes. Two seconds
/// covers a default Kubernetes `periodSeconds: 1..2` probe; raise it to match a slower probe. Comes
/// out of the overall grace period, not on top of it.
pub const DEFAULT_READY_DRAIN: Duration = Duration::from_secs(2);

/// How long deferred subquery work — flip query-backs, emission-lane appends, the sequencer's own
/// flush — may keep running after the token flips (the drain window; see the module docs). Long
/// enough for the query-backs of the last commits read to land on a healthy store, which is what
/// lets the subquery registry be written for the next boot; short enough to leave the catalog
/// drain ([`CATALOG_DRAIN`]) its share of the grace when storage is down and nothing will land.
pub const DEFAULT_SUBQUERY_DRAIN: Duration = Duration::from_secs(5);

struct Inner {
    metrics: Arc<crate::metrics::Metrics>,
    tx: tokio::sync::watch::Sender<bool>,
    /// Parties registered and not yet finished, by name — so a shutdown that runs out of grace can
    /// say **who** it was waiting for.
    outstanding: std::sync::Mutex<BTreeSet<&'static str>>,
    /// When the token flipped. The grace period is measured from HERE, not from when the HTTP
    /// server finally stopped, so the readiness-drain window comes out of the same budget.
    began_at: std::sync::Mutex<Option<std::time::Instant>>,
    tasks: std::sync::Mutex<Option<tokio::task::JoinSet<()>>>,
    restart_notify: Option<Arc<tokio::sync::Notify>>,
    restart_reason: std::sync::atomic::AtomicI32,
    /// Flipped when the drain window is closed explicitly (see the module docs).
    drain_ended: tokio::sync::watch::Sender<bool>,
    /// How long after the token flips the drain window closes on its own, in milliseconds.
    drain_budget_ms: std::sync::atomic::AtomicU64,
    /// Tasks spawned with [`ShutdownToken::spawn_draining`] that have not finished.
    draining: std::sync::atomic::AtomicUsize,
}

/// A cheap, cloneable handle to one Engine's shutdown state.
#[derive(Clone)]
pub struct ShutdownToken {
    inner: Arc<Inner>,
}

impl Default for ShutdownToken {
    fn default() -> Self {
        ShutdownToken::new()
    }
}

impl ShutdownToken {
    pub(crate) fn metrics(&self) -> &Arc<crate::metrics::Metrics> {
        &self.inner.metrics
    }

    pub fn new() -> Self {
        Self::with_supervisor(None)
    }

    pub(crate) fn with_supervisor(restart_notify: Option<Arc<tokio::sync::Notify>>) -> Self {
        Self::with_metrics(restart_notify, Arc::new(crate::metrics::Metrics::default()))
    }

    pub(crate) fn with_metrics(
        restart_notify: Option<Arc<tokio::sync::Notify>>,
        metrics: Arc<crate::metrics::Metrics>,
    ) -> Self {
        ShutdownToken {
            inner: Arc::new(Inner {
                metrics,
                tx: tokio::sync::watch::channel(false).0,
                outstanding: std::sync::Mutex::new(BTreeSet::new()),
                began_at: std::sync::Mutex::new(None),
                tasks: std::sync::Mutex::new(Some(tokio::task::JoinSet::new())),
                restart_notify,
                restart_reason: std::sync::atomic::AtomicI32::new(0),
                drain_ended: tokio::sync::watch::channel(false).0,
                drain_budget_ms: std::sync::atomic::AtomicU64::new(0),
                draining: std::sync::atomic::AtomicUsize::new(0),
            }),
        }
    }

    #[cfg(test)]
    pub(crate) fn restart_requested(&self) -> bool {
        self.inner.restart_reason.load(std::sync::atomic::Ordering::Acquire) != 0
    }

    /// Request a restart from the host. Library code never terminates its caller's process;
    /// the standalone binary owns exit policy and supervised sources notify their owner.
    pub(crate) fn restart_or_exit(&self, code: i32) {
        self.inner.restart_reason.store(code, std::sync::atomic::Ordering::Release);
        self.begin();
        self.inner.tx.send_replace(true);
    }

    /// Wait for a fatal restart request and return its exit code, including an already pending
    /// request. Ordinary shutdown does not resolve this future. Cancellation has no side effects;
    /// embedders may observe the code without exiting, while the standalone host exits explicitly.
    pub async fn wait_for_restart(&self) -> i32 {
        let mut rx = self.inner.tx.subscribe();
        let _ = rx.wait_for(|_| self.inner.restart_reason.load(std::sync::atomic::Ordering::Acquire) != 0).await;
        self.inner.restart_reason.load(std::sync::atomic::Ordering::Acquire)
    }

    /// Track work on the caller's runtime, including work started by host HTTP handlers.
    /// Closing the task set rejects later spawns, then aborts and joins all remaining work.
    pub(crate) fn spawn<F, T>(&self, future: F) -> tokio::sync::oneshot::Receiver<T>
    where
        F: std::future::Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let mut guard = self.inner.tasks.lock().unwrap();
        if let Some(tasks) = guard.as_mut() {
            while let Some(result) = tasks.try_join_next() {
                if let Err(error) = result {
                    tracing::error!(%error, "Engine task failed");
                }
            }
            tasks.spawn(async move {
                let _ = tx.send(future.await);
            });
        }
        rx
    }

    pub(crate) fn spawn_background<F>(&self, future: F)
    where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        let shutdown = self.clone();
        self.spawn(async move {
            tokio::select! {
                biased;
                _ = shutdown.wait() => {},
                _ = future => {},
            }
        });
    }

    /// How long deferred subquery work may keep running after the token flips (see the module
    /// docs). Set once, before anything is spawned, by whoever built the Engine from a config.
    pub(crate) fn set_drain_budget(&self, budget: Duration) {
        let ms = u64::try_from(budget.as_millis()).unwrap_or(u64::MAX);
        self.inner.drain_budget_ms.store(ms, std::sync::atomic::Ordering::Release);
    }

    /// Close the drain window now. Idempotent; only meaningful once the token has flipped.
    pub(crate) fn end_drain(&self) {
        self.inner.drain_ended.send_replace(true);
    }

    /// Resolves once shutdown has begun AND its drain window has closed: at [`Self::end_drain`], or
    /// the drain budget after the flip. Immediately, if both already happened. With no budget this
    /// is [`Self::wait`].
    pub(crate) async fn wait_drain_end(&self) {
        self.wait().await;
        let budget = Duration::from_millis(self.inner.drain_budget_ms.load(std::sync::atomic::Ordering::Acquire));
        let left = budget.saturating_sub(self.elapsed().unwrap_or_default());
        if left.is_zero() {
            return;
        }
        let mut ended = self.inner.drain_ended.subscribe();
        tokio::select! {
            _ = ended.wait_for(|ended| *ended) => {}
            _ = tokio::time::sleep(left) => {}
        }
    }

    /// Whether shutdown has begun and its drain window has closed: nothing that waits on
    /// [`Self::wait_drain_end`] is still running.
    pub(crate) fn drain_over(&self) -> bool {
        if !self.is_shutting_down() {
            return false;
        }
        let budget = Duration::from_millis(self.inner.drain_budget_ms.load(std::sync::atomic::Ordering::Acquire));
        *self.inner.drain_ended.borrow() || self.elapsed().unwrap_or_default() >= budget
    }

    /// [`Self::spawn_background`] for work that must be allowed to FINISH what it holds when the
    /// token flips: it is cancelled when the drain window closes, not when shutdown begins.
    pub(crate) fn spawn_draining<F>(&self, future: F)
    where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        struct Draining(Arc<Inner>);
        impl Drop for Draining {
            fn drop(&mut self) {
                self.0.draining.fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
            }
        }
        let shutdown = self.clone();
        self.inner.draining.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        let guard = Draining(self.inner.clone());
        self.spawn(async move {
            let _guard = guard;
            tokio::select! {
                biased;
                _ = shutdown.wait_drain_end() => {},
                _ = future => {},
            }
        });
    }

    /// Wait until every [`Self::spawn_draining`] task has stopped, or `timeout` elapses. Called by
    /// Engine close after [`Self::end_drain`], so nothing is still inside the subquery registry
    /// when the membership circuit is shut down under it.
    pub(crate) async fn wait_for_drained(&self, timeout: Duration) -> bool {
        let deadline = std::time::Instant::now() + timeout;
        while self.inner.draining.load(std::sync::atomic::Ordering::Acquire) > 0 {
            if std::time::Instant::now() >= deadline {
                return false;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        true
    }

    pub(crate) async fn join_tasks(&self) {
        let tasks = self.inner.tasks.lock().unwrap().take();
        if let Some(mut tasks) = tasks {
            tasks.abort_all();
            while let Some(result) = tasks.join_next().await {
                if let Err(error) = result
                    && !error.is_cancelled()
                {
                    tracing::error!(%error, "Engine task failed during close");
                }
            }
        }
    }

    /// Flip the token. Idempotent; returns `true` only for the call that actually transitioned, so
    /// the caller can tell a first signal from a second one.
    pub fn begin(&self) -> bool {
        let first = !self.is_shutting_down();
        if first {
            // `send_replace`, not `send`: a `watch::Sender` with no live receiver refuses `send`
            // AND leaves the value alone, and this token normally has none (every waiter subscribes
            // on demand inside `wait`). `send_replace` always stores, then wakes whoever is there.
            *self.inner.began_at.lock().unwrap() = Some(std::time::Instant::now());
            self.inner.tx.send_replace(true);
            if let Some(notify) = &self.inner.restart_notify {
                notify.notify_one();
            }
            self.metrics().shutdown_in_progress.store(1, std::sync::atomic::Ordering::Relaxed);
        }
        first
    }

    pub fn is_shutting_down(&self) -> bool {
        *self.inner.tx.borrow()
    }

    /// How long ago the token flipped (`None` if it has not). The grace period is measured against
    /// this, so the readiness-drain window and the tasks' wind-down share one budget.
    pub fn elapsed(&self) -> Option<Duration> {
        self.inner.began_at.lock().unwrap().map(|t| t.elapsed())
    }

    /// Resolves as soon as shutdown has begun — **immediately** if it already has, which is what
    /// makes this safe to put in a `tokio::select!` that may be entered after the flip.
    pub async fn wait(&self) {
        let mut rx = self.inner.tx.subscribe();
        // `wait_for` evaluates the current value first, so an already-flipped token returns at once.
        let _ = rx.wait_for(|flipped| *flipped).await;
    }

    /// Register a component Engine close must wait for. The returned guard removes it
    /// on drop, so a party that returns — or panics — never wedges the shutdown.
    pub fn party(&self, name: &'static str) -> ShutdownParty {
        self.inner.outstanding.lock().unwrap().insert(name);
        ShutdownParty { inner: self.inner.clone(), name }
    }

    /// Parties registered and not finished, right now.
    pub fn outstanding(&self) -> Vec<&'static str> {
        self.inner.outstanding.lock().unwrap().iter().copied().collect()
    }

    /// Wait until every registered party has finished, or `timeout` elapses. Returns whether they
    /// all finished. Polled rather than notified: this runs once, on the way out, and a 10 ms poll
    /// is not worth a wakeup protocol (same call as `CatalogWriter::drain`).
    pub async fn wait_for_parties(&self, timeout: Duration) -> bool {
        let deadline = std::time::Instant::now() + timeout;
        loop {
            if self.inner.outstanding.lock().unwrap().is_empty() {
                return true;
            }
            if std::time::Instant::now() >= deadline {
                return false;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}

/// The message the grace watchdog logs before it forces the exit.
///
/// Split out from the watchdog itself because the watchdog's body is `sleep` then
/// `std::process::exit`, which cannot be unit-tested — but the property that matters *can* be: a
/// forced exit must NAME who it was still waiting for, or an operator is left with "it did not
/// shut down" and no next step.
pub fn grace_expiry_message(outstanding: &[&'static str], grace: Duration) -> String {
    let who = if outstanding.is_empty() {
        // No party is registered before the ingestor is spawned, so this is the shape of the boot
        // hanging on something it cannot interrupt — say that rather than printing an empty list.
        "no registered party (the boot had not finished: a connect, a catalog fold or a backfill \
         was still in flight)"
            .to_string()
    } else {
        outstanding.join(", ")
    };
    format!(
        "the {grace:?} shutdown grace elapsed with work still in flight — waiting on: {who}. \
         Exiting {EXIT_SHUTDOWN_FORCED}. Nothing is corrupted: an unacknowledged commit is \
         re-delivered and the last checkpoint stands. Raise ELECTRIC_CIRCUITS_SHUTDOWN_GRACE_SECS \
         if this is a commit that legitimately needs longer."
    )
}

/// Drop guard for a registered shutdown party (see [`ShutdownToken::party`]).
pub struct ShutdownParty {
    inner: Arc<Inner>,
    name: &'static str,
}

impl Drop for ShutdownParty {
    fn drop(&mut self) {
        self.inner.outstanding.lock().unwrap().remove(self.name);
    }
}

/// Resolve the grace period from an env getter. Pure, and **fallible**: an unparseable value is
/// boot-fatal rather than silently replaced, exactly like the large-transaction knobs — a grace
/// period the operator meant to set and that silently did not apply is the worst of both worlds.
pub fn resolve_grace(get: impl Fn(&str) -> Option<String>) -> anyhow::Result<Duration> {
    let secs = secs_var(&get, "ELECTRIC_CIRCUITS_SHUTDOWN_GRACE_SECS", DEFAULT_GRACE.as_secs())?;
    if secs == 0 {
        anyhow::bail!(
            "ELECTRIC_CIRCUITS_SHUTDOWN_GRACE_SECS must be > 0 (it bounds the graceful shutdown; 0 \
             would forbid finishing an in-flight commit)"
        );
    }
    Ok(Duration::from_secs(secs))
}

/// Resolve the readiness-drain window (see [`DEFAULT_READY_DRAIN`]). `0` is legal and means "stop
/// accepting at once" — correct when nothing in front of the engine polls `/ready`.
pub fn resolve_ready_drain(get: impl Fn(&str) -> Option<String>) -> anyhow::Result<Duration> {
    Ok(Duration::from_secs(secs_var(&get, "ELECTRIC_CIRCUITS_SHUTDOWN_DRAIN_SECS", DEFAULT_READY_DRAIN.as_secs())?))
}

/// Resolve the subquery drain window (see [`DEFAULT_SUBQUERY_DRAIN`]) for a shutdown bounded by
/// `grace`. `0` is legal and means "abandon deferred subquery work at the signal": correct, and it
/// costs every subquery shape at the next boot unless the engine happened to be idle.
///
/// The window comes out of the grace, so an explicit value must be less than it. Left unset it is
/// the default or a fifth of the grace, whichever is shorter — a deployment that configured a
/// short grace must not be refused at boot for a knob it never set, and the rest of the grace
/// still has to cover the final checkpoint and the catalog drain.
pub fn resolve_subquery_drain(get: impl Fn(&str) -> Option<String>, grace: Duration) -> anyhow::Result<Duration> {
    const NAME: &str = "ELECTRIC_CIRCUITS_SHUTDOWN_SUBQUERY_DRAIN_SECS";
    if get(NAME).is_none_or(|raw| raw.trim().is_empty()) {
        return Ok(DEFAULT_SUBQUERY_DRAIN.min(grace / 5));
    }
    let drain = Duration::from_secs(secs_var(&get, NAME, DEFAULT_SUBQUERY_DRAIN.as_secs())?);
    if drain >= grace {
        anyhow::bail!(
            "{NAME} ({}s) must be less than ELECTRIC_CIRCUITS_SHUTDOWN_GRACE_SECS ({}s): the drain comes OUT of \
             the grace, and the final checkpoint still has to reach the catalog after it",
            drain.as_secs(),
            grace.as_secs(),
        );
    }
    Ok(drain)
}

fn secs_var(get: impl Fn(&str) -> Option<String>, name: &str, default: u64) -> anyhow::Result<u64> {
    let Some(raw) = get(name) else { return Ok(default) };
    let raw = raw.trim();
    if raw.is_empty() {
        return Ok(default);
    }
    raw.parse().map_err(|_| anyhow::anyhow!("{name} must be a whole number of seconds, got '{raw}'"))
}

/// The first of `SIGTERM` / `SIGINT`. On non-unix only `ctrl_c` exists.
pub async fn first_signal() -> &'static str {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut term = match signal(SignalKind::terminate()) {
            Ok(s) => s,
            Err(e) => {
                tracing::error!("cannot install the SIGTERM handler ({e}); only SIGINT will be honoured");
                let _ = tokio::signal::ctrl_c().await;
                return "SIGINT";
            }
        };
        tokio::select! {
            _ = term.recv() => "SIGTERM",
            _ = tokio::signal::ctrl_c() => "SIGINT",
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
        "SIGINT"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn begin_is_idempotent_and_reports_the_transition() {
        let t = ShutdownToken::new();
        assert!(!t.is_shutting_down());
        assert!(t.begin(), "the first begin is the transition");
        assert!(!t.begin(), "a second signal is not a transition");
        assert!(t.is_shutting_down());
    }

    /// The subquery drain window comes out of the grace: an explicit value has to fit inside it,
    /// and the default shrinks to fit a short grace rather than refusing a boot that set neither.
    #[test]
    fn the_subquery_drain_window_fits_inside_the_grace() {
        let set =
            |value: &'static str| move |name: &str| (name.ends_with("SUBQUERY_DRAIN_SECS")).then(|| value.to_string());
        let unset = |_: &str| None;
        assert_eq!(resolve_subquery_drain(unset, Duration::from_secs(25)).unwrap(), DEFAULT_SUBQUERY_DRAIN);
        assert_eq!(resolve_subquery_drain(unset, Duration::from_secs(5)).unwrap(), Duration::from_secs(1));
        assert_eq!(resolve_subquery_drain(set("0"), Duration::from_secs(25)).unwrap(), Duration::ZERO);
        assert_eq!(resolve_subquery_drain(set("12"), Duration::from_secs(25)).unwrap(), Duration::from_secs(12));
        assert!(resolve_subquery_drain(set("25"), Duration::from_secs(25)).is_err(), "all of the grace");
        assert!(resolve_subquery_drain(set("soon"), Duration::from_secs(25)).is_err(), "not a number");
    }

    async fn is_pending(future: impl std::future::Future<Output = ()>) -> bool {
        tokio::pin!(future);
        std::future::poll_fn(|cx| std::task::Poll::Ready(future.as_mut().poll(cx).is_pending())).await
    }

    /// With no budget the drain window is empty: it closes the instant shutdown begins, which is
    /// the behaviour every engine had before the window existed.
    #[tokio::test]
    async fn without_a_budget_the_drain_window_closes_when_shutdown_begins() {
        let t = ShutdownToken::new();
        assert!(is_pending(t.wait_drain_end()).await, "no shutdown, no end");
        assert!(!t.drain_over());
        t.begin();
        tokio::time::timeout(Duration::from_secs(1), t.wait_drain_end()).await.expect("closed at the flip");
        assert!(t.drain_over());
    }

    /// With a budget, deferred work outlives the signal: the window stays open until Engine close
    /// ends it (or the budget runs out), and only then does a waiter resolve.
    #[tokio::test]
    async fn a_budgeted_drain_window_stays_open_until_it_is_ended() {
        let t = ShutdownToken::new();
        t.set_drain_budget(Duration::from_secs(3600));
        t.begin();
        assert!(is_pending(t.wait_drain_end()).await, "the signal alone does not close the window");
        assert!(!t.drain_over());
        t.end_drain();
        tokio::time::timeout(Duration::from_secs(1), t.wait_drain_end()).await.expect("closed by end_drain");
        assert!(t.drain_over());
    }

    /// The budget is a bound of its own: nobody has to end the window for it to close.
    #[tokio::test(start_paused = true)]
    async fn the_drain_window_closes_when_its_budget_runs_out() {
        let t = ShutdownToken::new();
        t.set_drain_budget(Duration::from_secs(5));
        t.begin();
        let started = tokio::time::Instant::now();
        t.wait_drain_end().await;
        assert!(started.elapsed() <= Duration::from_secs(5), "no later than the budget");
    }

    /// A draining task keeps running after the signal and is stopped — and accounted for — when
    /// the window closes; a background task is stopped by the signal itself.
    #[tokio::test]
    async fn a_draining_task_outlives_the_signal_and_stops_with_the_window() {
        let t = ShutdownToken::new();
        t.set_drain_budget(Duration::from_secs(3600));
        let (finish, finished) = tokio::sync::oneshot::channel::<()>();
        let (landed, mut did_land) = tokio::sync::mpsc::unbounded_channel::<&'static str>();
        let draining = landed.clone();
        t.spawn_draining(async move {
            let _ = finished.await;
            let _ = draining.send("draining");
            std::future::pending::<()>().await;
        });
        t.spawn_background(async move {
            std::future::pending::<()>().await;
            let _ = landed.send("background");
        });
        t.begin();
        finish.send(()).unwrap();
        assert_eq!(did_land.recv().await, Some("draining"), "work held at the signal is finished");
        assert!(!t.wait_for_drained(Duration::from_millis(20)).await, "and the task is still accounted for");
        t.end_drain();
        assert!(t.wait_for_drained(Duration::from_secs(1)).await, "it stops when the window closes");
        assert_eq!(did_land.recv().await, None, "the background task was dropped at the signal, unfinished");
    }

    /// The property every `select!` arm depends on: a token flipped BEFORE the wait is awaited must
    /// still resolve immediately, or a long-poll entered just after the flip would hang out the
    /// whole grace period.
    #[tokio::test]
    async fn wait_returns_immediately_when_already_flipped() {
        let t = ShutdownToken::new();
        t.begin();
        tokio::time::timeout(Duration::from_millis(50), t.wait())
            .await
            .expect("an already-flipped token must not block");
    }

    #[tokio::test]
    async fn wait_wakes_on_a_later_flip() {
        let t = ShutdownToken::new();
        let t2 = t.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            t2.begin();
        });
        tokio::time::timeout(Duration::from_secs(2), t.wait()).await.expect("the flip must wake the waiter");
    }

    #[tokio::test]
    async fn parties_gate_the_wait_and_are_named() {
        let t = ShutdownToken::new();
        let p = t.party("ingestor");
        assert_eq!(t.outstanding(), vec!["ingestor"]);
        assert!(!t.wait_for_parties(Duration::from_millis(30)).await, "an outstanding party must block");
        drop(p);
        assert!(t.outstanding().is_empty());
        assert!(t.wait_for_parties(Duration::from_millis(30)).await);
    }

    /// The forced-exit message must name the parties — and must not print an empty list when the
    /// process is stuck somewhere that registers no party at all (a boot that never finished).
    #[test]
    fn the_forced_exit_names_who_it_waited_for() {
        let m = grace_expiry_message(&["replication ingestor", "sequencer"], Duration::from_secs(25));
        assert!(m.contains("replication ingestor, sequencer"), "{m}");
        assert!(m.contains("Exiting 70"), "{m}");
        let m = grace_expiry_message(&[], Duration::from_secs(25));
        assert!(m.contains("no registered party"), "{m}");
        assert!(m.contains("the boot had not finished"), "{m}");
    }

    #[test]
    fn grace_defaults_and_refuses_nonsense() {
        assert_eq!(resolve_grace(|_| None).unwrap(), DEFAULT_GRACE);
        assert_eq!(resolve_grace(|_| Some("  ".into())).unwrap(), DEFAULT_GRACE);
        assert_eq!(resolve_grace(|_| Some("5".into())).unwrap(), Duration::from_secs(5));
        assert!(resolve_grace(|_| Some("0".into())).is_err(), "0 would forbid finishing a commit");
        assert!(resolve_grace(|_| Some("soon".into())).is_err());
        assert!(resolve_grace(|_| Some("-1".into())).is_err());
    }

    /// The drain window may legitimately be 0 (nothing in front of the engine polls `/ready`), which
    /// is the one place it differs from the grace period.
    #[test]
    fn ready_drain_defaults_and_allows_zero() {
        assert_eq!(resolve_ready_drain(|_| None).unwrap(), DEFAULT_READY_DRAIN);
        assert_eq!(resolve_ready_drain(|_| Some("0".into())).unwrap(), Duration::ZERO);
        assert_eq!(resolve_ready_drain(|_| Some("7".into())).unwrap(), Duration::from_secs(7));
        assert!(resolve_ready_drain(|_| Some("later".into())).is_err());
    }

    #[test]
    fn shutdown_outcomes_have_distinct_process_statuses() {
        assert_eq!(ShutdownOutcome::Complete.exit_code(), 0);
        assert_eq!(ShutdownOutcome::Forced.exit_code(), EXIT_SHUTDOWN_FORCED);
        assert_eq!(ShutdownOutcome::CatalogIncomplete.exit_code(), EXIT_SHUTDOWN_INCOMPLETE);
        assert_ne!(ShutdownOutcome::CatalogIncomplete.exit_code(), 0);
    }

    /// The grace period is measured from the FLIP, so the readiness-drain window and the tasks'
    /// wind-down share one budget rather than stacking.
    #[tokio::test]
    async fn elapsed_is_measured_from_the_flip() {
        let t = ShutdownToken::new();
        assert!(t.elapsed().is_none());
        t.begin();
        assert!(t.elapsed().is_some());
    }
}

#[cfg(test)]
mod ownership_tests {
    use super::*;

    #[tokio::test]
    async fn close_joins_tasks_and_refuses_late_spawns() {
        let token = ShutdownToken::new();
        let owned = Arc::new(());
        let weak = Arc::downgrade(&owned);
        let (started, ready) = tokio::sync::oneshot::channel();
        token.spawn(async move {
            let _owned = owned;
            let _ = started.send(());
            std::future::pending::<()>().await;
        });
        ready.await.unwrap();
        token.begin();
        token.join_tasks().await;
        assert!(weak.upgrade().is_none());
        let late = token.spawn(async { panic!("closed task set must never poll new work") });
        assert!(late.await.is_err());
    }

    #[tokio::test]
    async fn background_work_ends_when_its_engine_stops() {
        let token = ShutdownToken::new();
        let (started, ready) = tokio::sync::oneshot::channel();
        let (held, released) = tokio::sync::oneshot::channel::<()>();
        token.spawn_background(async move {
            let _held = held;
            let _ = started.send(());
            std::future::pending::<()>().await;
        });
        ready.await.unwrap();
        token.begin();
        assert!(released.await.is_err());
        token.join_tasks().await;
    }

    #[tokio::test]
    async fn supervised_failure_signals_only_the_source_owner() {
        let healthy = ShutdownToken::new();
        let notify = Arc::new(tokio::sync::Notify::new());
        let failed = ShutdownToken::with_supervisor(Some(notify.clone()));
        failed.restart_or_exit(74);
        notify.notified().await;
        assert!(failed.is_shutting_down());
        assert!(failed.restart_requested());
        assert_eq!(failed.wait_for_restart().await, 74);
        assert!(!healthy.is_shutting_down());
        assert!(!healthy.restart_requested());
    }

    #[test]
    fn review_1_library_restart_must_not_exit_its_host() {
        const CHILD: &str = "CIRCUITS_REVIEW_1_RESTART_CHILD";
        if std::env::var_os(CHILD).is_some() {
            let token = ShutdownToken::new();
            let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
            runtime.block_on(async {
                let restart = token.wait_for_restart();
                tokio::pin!(restart);
                token.begin();
                std::future::poll_fn(|cx| {
                    assert!(
                        std::future::Future::poll(restart.as_mut(), cx).is_pending(),
                        "ordinary shutdown is not fatal"
                    );
                    std::task::Poll::Ready(())
                })
                .await;
                token.restart_or_exit(75);
                assert!(token.is_shutting_down());
                assert!(token.restart_requested());
                assert_eq!(tokio::time::timeout(Duration::from_secs(2), restart).await.unwrap(), 75);
                assert_eq!(token.wait_for_restart().await, 75, "late observers also receive the exit code");
            });
            return;
        }
        // A subprocess turns the old process::exit into an assertion failure in this test,
        // instead of terminating the entire lib test runner as the CI failure did.
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "shutdown::ownership_tests::review_1_library_restart_must_not_exit_its_host",
                "--nocapture",
            ])
            .env(CHILD, "1")
            .output()
            .unwrap();
        assert!(String::from_utf8_lossy(&output.stdout).contains("running 1 test"), "child must select the regression");
        assert!(
            output.status.success(),
            "a library restart must return control to its host, got {:?}: {}",
            output.status.code(),
            String::from_utf8_lossy(&output.stderr)
        );
    }
}
