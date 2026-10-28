//! Shape-group coordination primitives.
//!
//! A group is a single lease over several otherwise ordinary shape members.  The
//! coordinator owns the two invariants that cannot be expressed by a `ShapeRecord`:
//! a claim is reserved atomically before any member is created, and a group keeps one
//! durable member list so releasing the public claim can release every private member.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Per-engine group activity.  This is deliberately owned by the Engine rather than a
/// process-global counter: two engines may share a process during a handoff and their
/// transaction fences must not affect one another.
#[derive(Clone, Default)]
pub(crate) struct GroupFence {
    active: Arc<AtomicUsize>,
}

impl GroupFence {
    pub(crate) fn activate(&self) {
        self.active.fetch_add(1, Ordering::AcqRel);
    }

    pub(crate) fn deactivate(&self) {
        let _ = self.active.fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| n.checked_sub(1));
    }

    pub(crate) fn active(&self) -> bool {
        self.active.load(Ordering::Acquire) != 0
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub(crate) struct GroupMember {
    pub key: String,
    pub shape_id: String,
    pub table: String,
    #[serde(rename = "where", default)]
    pub where_: Option<crate::predicate::PredicateJson>,
    #[serde(default)]
    pub columns: Option<Vec<String>>,
    pub subscription: String,
    pub is_subquery: bool,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub(crate) struct GroupRecord {
    pub id: String,
    pub stream_path: String,
    pub subscription: String,
    pub members: Vec<GroupMember>,
    /// The one settled snapshot returned to all members.  These are opaque Postgres
    /// values at this layer, but keeping them beside the durable member list makes it
    /// impossible for a caller to accidentally report per-member fences.
    pub lsn: String,
    pub snapshot: String,
    pub horizon: String,
    pub replay_fence: String,
}

#[derive(Debug, thiserror::Error)]
#[error("group subscription '{subscription}' conflicts with {owner}")]
pub(crate) struct GroupConflict {
    pub subscription: String,
    pub owner: String,
}

#[derive(Debug, thiserror::Error)]
#[error("group stream '{stream_path}' is closed; reload the scope")]
pub(crate) struct GroupStreamClosed {
    pub stream_path: String,
}

/// The state transition used by the HTTP create path.  The reservation is made before
/// the first member touches the shape registry and is either completed with a record or
/// released on every error path.
#[derive(Default)]
pub(crate) struct GroupClaims {
    reserved: HashMap<String, String>,
    records: HashMap<String, GroupRecord>,
    private_members: HashMap<String, String>,
}

impl GroupClaims {
    pub(crate) fn reserve(&mut self, subscription: &str, owner_hint: &str) -> anyhow::Result<()> {
        if self.reserved.contains_key(subscription) || self.records.contains_key(subscription) {
            return Err(anyhow::Error::new(GroupConflict {
                subscription: subscription.to_string(),
                owner: owner_hint.to_string(),
            }));
        }
        self.reserved.insert(subscription.to_string(), owner_hint.to_string());
        Ok(())
    }

    pub(crate) fn complete(&mut self, record: GroupRecord) {
        self.reserved.remove(&record.subscription);
        for member in &record.members {
            if member.subscription != record.subscription {
                self.private_members.insert(member.subscription.clone(), record.subscription.clone());
            }
        }
        self.records.insert(record.subscription.clone(), record);
    }

    pub(crate) fn release(&mut self, subscription: &str) -> Option<GroupRecord> {
        self.reserved.remove(subscription);
        self.private_members.retain(|_, owner| owner != subscription);
        self.records.remove(subscription)
    }

    pub(crate) fn release_if_matches(&mut self, subscription: &str, group_id: &str) -> Option<GroupRecord> {
        if self.records.get(subscription).is_some_and(|record| record.id == group_id) {
            self.reserved.remove(subscription);
            self.private_members.retain(|_, owner| owner != subscription);
            self.records.remove(subscription)
        } else {
            None
        }
    }

    pub(crate) fn get(&self, subscription: &str) -> Option<&GroupRecord> {
        self.records.get(subscription)
    }

    pub(crate) fn records_iter(&self) -> impl Iterator<Item = &GroupRecord> {
        self.records.values()
    }

    pub(crate) fn is_reserved(&self, subscription: &str) -> bool {
        self.reserved.contains_key(subscription)
    }

    pub(crate) fn private_owner(&self, subscription: &str) -> Option<&String> {
        self.private_members.get(subscription)
    }

    pub(crate) fn reserve_private_members(&mut self, group: &str, members: impl IntoIterator<Item = String>) {
        for member in members {
            self.private_members.insert(member, group.to_string());
        }
    }
}

/// Completion shared by callers that race a group release. The release task is engine-owned, so
/// cancelling one HTTP request cannot strand another caller waiting for the same teardown.
pub(crate) struct GroupRelease {
    done: tokio::sync::Notify,
    result: std::sync::Mutex<Option<std::result::Result<(), String>>>,
}

impl GroupRelease {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self { done: tokio::sync::Notify::new(), result: std::sync::Mutex::new(None) })
    }

    pub(crate) fn complete(&self, result: &anyhow::Result<()>) {
        let result = result.as_ref().map(|_| ()).map_err(|error| format!("{error:#}"));
        *self.result.lock().unwrap() = Some(result);
        self.done.notify_waiters();
    }

    pub(crate) async fn wait(&self) -> anyhow::Result<()> {
        loop {
            let mut notified = std::pin::pin!(self.done.notified());
            notified.as_mut().enable();
            if let Some(result) = self.result.lock().unwrap().clone() {
                return result.map_err(anyhow::Error::msg);
            }
            notified.await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_group_claim_is_reserved_before_members_are_created() {
        let mut claims = GroupClaims::default();
        claims.reserve("scope-1", "group:scope-1").unwrap();
        let error = claims.reserve("scope-1", "shape:s1").unwrap_err();
        assert!(error.downcast_ref::<GroupConflict>().is_some());
        assert!(claims.is_reserved("scope-1"));
    }
}
