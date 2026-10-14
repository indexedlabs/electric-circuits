//! Process-wide admission for transient byte owners. Never grow a reservation while holding
//! another one: spill or discard/replay first. In particular, oversized admission needs an empty
//! budget. Guards, including queued requests, release their ownership on cancellation.

use std::collections::VecDeque;
use std::sync::{
    Arc, Mutex, OnceLock,
    atomic::{AtomicU64, Ordering},
};
use tokio::sync::Notify;

/// Boot policy shared by clones of a configuration, including supervised source configurations.
#[derive(Clone, Debug)]
pub struct MemoryBudgetConfig {
    explicit: Option<u64>,
    resolved: Arc<OnceLock<Arc<MemoryBudget>>>,
}

impl MemoryBudgetConfig {
    pub(crate) fn resolve(raw: Option<String>) -> anyhow::Result<Self> {
        let explicit = raw
            .map(|raw| {
                raw.trim().parse::<u64>().map_err(|_| {
                    anyhow::anyhow!(
                        "ELECTRIC_CIRCUITS_MEMORY_BUDGET_BYTES must be a nonnegative byte count, got '{raw}'"
                    )
                })
            })
            .transpose()?;
        Ok(Self { explicit, resolved: Arc::default() })
    }

    pub(crate) fn budget(&self) -> Arc<MemoryBudget> {
        self.resolved
            .get_or_init(|| {
                let cgroup = crate::mem::cgroup_memory();
                let limit = effective_limit(self.explicit, cgroup.version, cgroup.max_bytes);
                match limit {
                    Some(bytes) => tracing::info!(bytes, "process memory budget enabled"),
                    None => {
                        tracing::info!("process memory budget disabled: no explicit setting or readable cgroup limit")
                    }
                }
                MemoryBudget::new(limit)
            })
            .clone()
    }
}

fn effective_limit(explicit: Option<u64>, version: Option<&str>, cgroup: Option<u64>) -> Option<u64> {
    explicit.or_else(|| {
        // Linux v1's page-rounded LONG_MAX is an unlimited sentinel, not a finite limit.
        cgroup.filter(|v| version != Some("v1") || *v < (1u64 << 60)).map(|v| ((v as u128 * 3) / 5) as u64)
    })
}

#[derive(Debug, Default)]
struct State {
    used: u64,
    next: u64,
    queue: VecDeque<u64>,
}

#[derive(Debug)]
pub(crate) struct MemoryBudget {
    limit: Option<u64>,
    state: Mutex<State>,
    changed: Notify,
    pub held: AtomicU64,
    pub waits: AtomicU64,
}

impl MemoryBudget {
    pub fn new(limit: Option<u64>) -> Arc<Self> {
        Arc::new(Self {
            limit,
            state: Mutex::default(),
            changed: Notify::new(),
            held: AtomicU64::new(0),
            waits: AtomicU64::new(0),
        })
    }

    pub fn enabled(&self) -> bool {
        self.limit.is_some()
    }
    pub fn limit(&self) -> u64 {
        self.limit.unwrap_or(0)
    }

    fn fits(&self, used: u64, bytes: u64) -> bool {
        used == 0 || bytes <= self.limit().saturating_sub(used) && used <= self.limit()
    }

    pub async fn acquire(self: &Arc<Self>, bytes: u64) -> Permit {
        if !self.enabled() || bytes == 0 {
            return Permit::default();
        }
        let mut waiting = Waiting { budget: self.clone(), ticket: None };
        loop {
            let notified = self.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            {
                let mut state = self.state.lock().unwrap();
                let first = match waiting.ticket {
                    None => state.queue.is_empty(),
                    Some(ticket) => state.queue.front() == Some(&ticket),
                };
                if first && self.fits(state.used, bytes) {
                    if waiting.ticket.take().is_some() {
                        state.queue.pop_front();
                    }
                    state.used += bytes;
                    self.held.store(state.used, Ordering::Relaxed);
                    self.changed.notify_waiters();
                    return Permit { budget: Some(self.clone()), bytes };
                }
                if waiting.ticket.is_none() {
                    let ticket = state.next;
                    state.next = state.next.wrapping_add(1);
                    state.queue.push_back(ticket);
                    waiting.ticket = Some(ticket);
                    self.waits.fetch_add(1, Ordering::Relaxed);
                }
            }
            notified.await;
        }
    }
}

struct Waiting {
    budget: Arc<MemoryBudget>,
    ticket: Option<u64>,
}
impl Drop for Waiting {
    fn drop(&mut self) {
        if let Some(ticket) = self.ticket {
            self.budget.state.lock().unwrap().queue.retain(|t| *t != ticket);
            self.budget.changed.notify_waiters();
        }
    }
}

#[derive(Debug, Default)]
pub(crate) struct Permit {
    budget: Option<Arc<MemoryBudget>>,
    bytes: u64,
}
impl Permit {
    pub fn bytes(&self) -> u64 {
        self.bytes
    }
}
impl Drop for Permit {
    fn drop(&mut self) {
        if let Some(budget) = &self.budget {
            let mut state = budget.state.lock().unwrap();
            state.used -= self.bytes;
            budget.held.store(state.used, Ordering::Relaxed);
            budget.changed.notify_waiters();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        future::{Future, poll_fn},
        pin::Pin,
        task::Poll,
    };

    async fn pending<F: Future>(mut future: Pin<&mut F>) {
        poll_fn(|cx| {
            assert!(future.as_mut().poll(cx).is_pending(), "admission must remain pending");
            Poll::Ready(())
        })
        .await;
    }

    #[tokio::test]
    async fn memory_budget_capacity_and_oversized_admission_are_exclusive() {
        let budget = MemoryBudget::new(Some(100));
        let small = budget.acquire(40).await;
        let rest = budget.acquire(60).await;
        assert_eq!(budget.held.load(Ordering::Relaxed), 100);
        let mut huge = Box::pin(budget.acquire(101));
        pending(huge.as_mut()).await;
        drop(small);
        pending(huge.as_mut()).await;
        assert_eq!(budget.waits.load(Ordering::Relaxed), 1);
        drop(rest);
        let huge = huge.await;
        assert_eq!(budget.held.load(Ordering::Relaxed), 101);
        let mut small = Box::pin(budget.acquire(1));
        pending(small.as_mut()).await;
        drop(huge);
        let small = small.await;
        assert_eq!(budget.held.load(Ordering::Relaxed), 1);
        drop(small);
        assert_eq!(budget.held.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn memory_budget_fifo_and_cancelled_head_do_not_strand_followers() {
        let budget = MemoryBudget::new(Some(100));
        let owner = budget.acquire(50).await;
        let mut large = Box::pin(budget.acquire(80));
        let mut small = Box::pin(budget.acquire(10));
        pending(large.as_mut()).await;
        pending(small.as_mut()).await;
        assert_eq!(budget.waits.load(Ordering::Relaxed), 2);
        drop(large);
        let small = small.await;
        assert_eq!(budget.held.load(Ordering::Relaxed), 60);
        drop((small, owner));
        assert_eq!(budget.held.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn memory_budget_disabled_and_zero_have_distinct_semantics() {
        let disabled = MemoryBudget::new(None);
        let _a = disabled.acquire(u64::MAX).await;
        let _b = disabled.acquire(u64::MAX).await;
        assert_eq!(disabled.held.load(Ordering::Relaxed), 0);
        let zero = MemoryBudget::new(Some(0));
        let owner = zero.acquire(1).await;
        let _empty = zero.acquire(0).await;
        let mut next = Box::pin(zero.acquire(2));
        pending(next.as_mut()).await;
        drop(owner);
        drop(next.await);
        assert_eq!(zero.held.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn memory_budget_resolution_obeys_override_finite_cgroup_and_missing_limit() {
        assert_eq!(effective_limit(None, Some("v2"), Some(100)), Some(60));
        assert_eq!(effective_limit(None, Some("v1"), Some(101)), Some(60));
        assert_eq!(effective_limit(None, Some("v2"), None), None);
        assert_eq!(effective_limit(None, Some("v1"), Some(9223372036854771712)), None);
        assert_eq!(effective_limit(Some(7), Some("v1"), Some(u64::MAX)), Some(7));
        assert_eq!(effective_limit(Some(0), Some("v2"), Some(100)), Some(0));
        assert_eq!(effective_limit(None, Some("v2"), Some(u64::MAX)), Some(11068046444225730969));
        for bad in ["-1", "128MiB", "wat", "18446744073709551616"] {
            assert!(MemoryBudgetConfig::resolve(Some(bad.into())).is_err(), "{bad}");
        }
        let config = MemoryBudgetConfig::resolve(Some("128".into())).unwrap();
        assert!(Arc::ptr_eq(&config.budget(), &config.clone().budget()));
    }
}
