use std::ops::{Deref, DerefMut};
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;
use tokio::sync::{Mutex, MutexGuard};

pub(crate) const MARKER_WAIT: usize = 0;
pub(crate) const MEMBERSHIP: usize = 1;
pub(crate) const SEQUENCER_WAIT: usize = 2;
pub(crate) const SEQUENCER_HOLD: usize = 3;
pub(crate) const FLIP_WAIT: usize = 4;
pub(crate) const FLIP_HOLD: usize = 5;
pub(crate) const FLIP_BATCH: usize = 6;
pub(crate) const FLIP_ONE: usize = 7;
pub(crate) const SAMPLER_WAIT: usize = 8;
pub(crate) const SAMPLER_HOLD: usize = 9;
const CATEGORIES: usize = 10;
/// Upper bounds of the duration buckets, in nanoseconds; the last bucket is everything above 5 s.
const BUCKET_BOUNDS: [u64; 5] = [1_000_000, 10_000_000, 100_000_000, 1_000_000_000, 5_000_000_000];
const BUCKET_LABELS: [&str; 6] = ["le_1ms", "le_10ms", "le_100ms", "le_1s", "le_5s", "gt_5s"];
static NANOS: [AtomicU64; CATEGORIES] = [const { AtomicU64::new(0) }; CATEGORIES];
static CALLS: [AtomicU64; CATEGORIES] = [const { AtomicU64::new(0) }; CATEGORIES];
static MAX_NANOS: [AtomicU64; CATEGORIES] = [const { AtomicU64::new(0) }; CATEGORIES];
static BUCKETS: [[AtomicU64; 6]; CATEGORIES] = [const { [const { AtomicU64::new(0) }; 6] }; CATEGORIES];
static ACTIVE_FLIPS: AtomicU64 = AtomicU64::new(0);
static PEAK_FLIPS: AtomicU64 = AtomicU64::new(0);
static SKIP_MARKER_WAIT: OnceLock<bool> = OnceLock::new();
static SEQUENCER_YIELD: OnceLock<bool> = OnceLock::new();
static SLICE_ROWS: OnceLock<usize> = OnceLock::new();
static SAMPLER_PERIOD: OnceLock<u64> = OnceLock::new();

/// `STALL_REPRO_SEQUENCER_YIELD=1`: the sequencer yields to the runtime after every release of
/// the registry lock, so ready flip workers and HTTP handlers run before it re-queues.
pub(crate) fn sequencer_yield() -> bool {
    *SEQUENCER_YIELD.get_or_init(|| std::env::var("STALL_REPRO_SEQUENCER_YIELD").is_ok_and(|value| value == "1"))
}

/// `STALL_REPRO_LOCK_SLICE_ROWS=N`: the per-commit membership flush releases and re-takes the
/// registry lock every N delta rows of a table instead of holding it for the whole table. `0`
/// (default) keeps one acquisition per table per commit.
pub(crate) fn slice_rows() -> usize {
    *SLICE_ROWS
        .get_or_init(|| std::env::var("STALL_REPRO_LOCK_SLICE_ROWS").ok().and_then(|v| v.parse().ok()).unwrap_or(0))
}

/// `STALL_REPRO_MEM_SAMPLER_MS`: period of the 500 ms OTel memory sampler; `0` disables it.
pub(crate) fn sampler_period_ms() -> u64 {
    *SAMPLER_PERIOD
        .get_or_init(|| std::env::var("STALL_REPRO_MEM_SAMPLER_MS").ok().and_then(|v| v.parse().ok()).unwrap_or(500))
}

pub(crate) fn unsafe_skip_marker_wait() -> bool {
    *SKIP_MARKER_WAIT
        .get_or_init(|| std::env::var("STALL_REPRO_UNSAFE_SKIP_MARKER_WAIT").is_ok_and(|value| value == "1"))
}

pub(crate) struct Timer {
    category: usize,
    started: Instant,
}

impl Timer {
    pub(crate) fn start(category: usize) -> Self {
        if category == FLIP_BATCH {
            let active = ACTIVE_FLIPS.fetch_add(1, Ordering::Relaxed) + 1;
            PEAK_FLIPS.fetch_max(active, Ordering::Relaxed);
        }
        Self { category, started: Instant::now() }
    }
}

impl Drop for Timer {
    fn drop(&mut self) {
        let nanos = self.started.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64;
        NANOS[self.category].fetch_add(nanos, Ordering::Relaxed);
        CALLS[self.category].fetch_add(1, Ordering::Relaxed);
        MAX_NANOS[self.category].fetch_max(nanos, Ordering::Relaxed);
        let bucket = BUCKET_BOUNDS.iter().position(|bound| nanos <= *bound).unwrap_or(BUCKET_BOUNDS.len());
        BUCKETS[self.category][bucket].fetch_add(1, Ordering::Relaxed);
        if self.category == FLIP_BATCH {
            ACTIVE_FLIPS.fetch_sub(1, Ordering::Relaxed);
        }
    }
}

pub(crate) struct MeasuredGuard<'guard, Value> {
    guard: MutexGuard<'guard, Value>,
    _timer: Timer,
}

impl<Value> Deref for MeasuredGuard<'_, Value> {
    type Target = Value;
    fn deref(&self) -> &Self::Target {
        &self.guard
    }
}

impl<Value> DerefMut for MeasuredGuard<'_, Value> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.guard
    }
}

pub(crate) async fn lock<Value>(mutex: &Mutex<Value>, wait: usize, hold: usize) -> MeasuredGuard<'_, Value> {
    let waiting = Timer::start(wait);
    let guard = mutex.lock().await;
    drop(waiting);
    MeasuredGuard { guard, _timer: Timer::start(hold) }
}

pub(crate) async fn flip_lock<Value>(mutex: &Mutex<Value>) -> MeasuredGuard<'_, Value> {
    lock(mutex, FLIP_WAIT, FLIP_HOLD).await
}

pub(crate) fn snapshot() -> serde_json::Value {
    let labels = [
        "marker_wait",
        "membership",
        "sequencer_registry_wait",
        "sequencer_registry_hold",
        "flip_registry_wait",
        "flip_registry_hold",
        "flip_batch",
        "flip_one",
        "sampler_registry_wait",
        "sampler_registry_hold",
    ];
    let mut values = serde_json::Map::new();
    for (category, label) in labels.into_iter().enumerate() {
        let mut value = serde_json::json!({
            "calls": CALLS[category].load(Ordering::Relaxed),
            "wall_seconds": NANOS[category].load(Ordering::Relaxed) as f64 / 1_000_000_000.0,
            "max_seconds_since_boot": MAX_NANOS[category].load(Ordering::Relaxed) as f64 / 1_000_000_000.0,
        });
        for (bucket, bucket_label) in BUCKET_LABELS.into_iter().enumerate() {
            value[bucket_label] = BUCKETS[category][bucket].load(Ordering::Relaxed).into();
        }
        values.insert(label.to_owned(), value);
    }
    serde_json::json!({
        "categories": values,
        "active_flip_batches": ACTIVE_FLIPS.load(Ordering::Relaxed),
        "peak_flip_batches": PEAK_FLIPS.load(Ordering::Relaxed),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn measured_guard_preserves_mutation_and_release() {
        let mutex = Mutex::new(7_u64);
        let calls = CALLS[SEQUENCER_WAIT].load(Ordering::Relaxed);
        {
            let mut guard = lock(&mutex, SEQUENCER_WAIT, SEQUENCER_HOLD).await;
            *guard += 1;
            assert!(mutex.try_lock().is_err());
        }
        assert_eq!(*mutex.try_lock().expect("measurement guard released"), 8);
        assert!(CALLS[SEQUENCER_WAIT].load(Ordering::Relaxed) > calls);
    }
}
