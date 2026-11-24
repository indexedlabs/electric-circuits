use std::ops::{Deref, DerefMut};
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
static NANOS: [AtomicU64; 8] = [const { AtomicU64::new(0) }; 8];
static CALLS: [AtomicU64; 8] = [const { AtomicU64::new(0) }; 8];
static ACTIVE_FLIPS: AtomicU64 = AtomicU64::new(0);
static PEAK_FLIPS: AtomicU64 = AtomicU64::new(0);

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
        "marker_wait", "membership", "sequencer_registry_wait", "sequencer_registry_hold",
        "flip_registry_wait", "flip_registry_hold", "flip_batch", "flip_one",
    ];
    let mut values = serde_json::Map::new();
    for (category, label) in labels.into_iter().enumerate() {
        values.insert(label.to_owned(), serde_json::json!({
            "calls": CALLS[category].load(Ordering::Relaxed),
            "wall_seconds": NANOS[category].load(Ordering::Relaxed) as f64 / 1_000_000_000.0,
        }));
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
