//! Shape-group coordination primitives.
//!
//! A group is a single lease over several otherwise ordinary shape members.  The
//! coordinator owns the two invariants that cannot be expressed by a `ShapeRecord`:
//! a claim is reserved atomically before any member is created, and a group keeps one
//! durable member list so releasing the public claim can release every private member.
//! The transaction barrier is deliberately small and independent of the Postgres code;
//! the sequencer and the HTTP adapter can use it without importing each other's types.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::ds::Envelope;

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

/// A per-group transaction barrier.  Every source transaction is registered with a
/// monotonically increasing commit position, then completed after all plain and deferred
/// member work has been staged.  `drain_ready` releases only the contiguous completed
/// prefix, so a delayed subquery flip from T cannot be appended after T+1.
#[derive(Default)]
pub(crate) struct TransactionBarrier {
    txns: BTreeMap<u64, (bool, Vec<Envelope>)>,
    ready: VecDeque<Envelope>,
}

impl TransactionBarrier {
    pub(crate) fn begin(&mut self, commit: u64) {
        self.txns.entry(commit).or_insert_with(|| (false, Vec::new()));
    }

    pub(crate) fn stage(&mut self, commit: u64, envelopes: impl IntoIterator<Item = Envelope>) {
        let entry = self.txns.entry(commit).or_insert_with(|| (false, Vec::new()));
        entry.1.extend(envelopes);
    }

    pub(crate) fn finish(&mut self, commit: u64) {
        if let Some((done, _)) = self.txns.get_mut(&commit) {
            *done = true;
        }
        self.drain_ready();
    }

    fn drain_ready(&mut self) {
        while let Some(done) = self.txns.first_key_value().map(|(_, (done, _))| *done) {
            if !done {
                break;
            }
            let (_, (_, envelopes)) = self.txns.pop_first().expect("first key still exists");
            self.ready.extend(envelopes);
        }
    }

    pub(crate) fn take_ready(&mut self) -> Vec<Envelope> {
        self.ready.drain(..).collect()
    }

    pub(crate) fn pending(&self) -> usize {
        self.txns.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn env(key: &str) -> Envelope {
        serde_json::from_value(serde_json::json!({
            "type": "public.record", "key": key,
            "headers": {"operation": "upsert"}
        }))
        .unwrap()
    }

    #[test]
    fn delayed_member_output_cannot_cross_a_group_transaction() {
        let mut barrier = TransactionBarrier::default();
        barrier.begin(10);
        barrier.begin(20);
        barrier.stage(20, [env("plain-t20")]);
        barrier.finish(20);
        assert!(barrier.take_ready().is_empty(), "T+1 waits for T");
        barrier.stage(10, [env("subquery-t10")]);
        barrier.finish(10);
        let keys: Vec<_> = barrier.take_ready().into_iter().map(|e| e.key).collect();
        assert_eq!(keys, ["subquery-t10", "plain-t20"]);
    }

    #[test]
    fn a_group_claim_is_reserved_before_members_are_created() {
        let mut claims = GroupClaims::default();
        claims.reserve("scope-1", "group:scope-1").unwrap();
        let error = claims.reserve("scope-1", "shape:s1").unwrap_err();
        assert!(error.downcast_ref::<GroupConflict>().is_some());
        assert!(claims.is_reserved("scope-1"));
    }
}
