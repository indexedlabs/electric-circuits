//! Shape-group coordination primitives.
//!
//! A group is a single lease over several otherwise ordinary shape members.  The
//! coordinator owns the two invariants that cannot be expressed by a `ShapeRecord`:
//! a claim is reserved atomically before any member is created, and a group keeps one
//! durable member list so releasing the public claim can release every private member.

use std::collections::HashMap;
use std::sync::Arc;

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

/// A load named a subscription whose group is still being assembled or compensated. A retry
/// after a lost response lands here, so it is retryable (`503` with `Retry-After`), not a `409`.
#[derive(Debug, thiserror::Error)]
#[error("group subscription '{subscription}' is still being loaded or released; retry")]
pub(crate) struct GroupLoadInFlight {
    pub subscription: String,
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
    /// Slots promised to a group load but not yet represented by a shape record.  A member
    /// creation moves one slot to `inflight_slots` while it is registering, so concurrent
    /// creates cannot consume capacity that the group still needs.
    reserved_slots: HashMap<String, usize>,
    inflight_slots: HashMap<String, usize>,
    /// Member slots that have become shape records while the group reservation is still being
    /// assembled. They are tracked only so a failed create can return the slot; `reserved_slots`
    /// deliberately excludes them because the shape record already counts against the cap.
    registered_slots: HashMap<String, usize>,
    records: HashMap<String, GroupRecord>,
    private_members: HashMap<String, String>,
    /// Completed group member shape id -> group id. Retention's under-lock recheck must be O(1),
    /// even when a deployment has many groups and members.
    shape_groups: HashMap<String, String>,
}

impl GroupClaims {
    pub(crate) fn reserve(&mut self, subscription: &str, owner_hint: &str) -> anyhow::Result<()> {
        self.reserve_with_slots(subscription, owner_hint, 0)
    }

    pub(crate) fn reserve_with_slots(
        &mut self,
        subscription: &str,
        owner_hint: &str,
        slots: usize,
    ) -> anyhow::Result<()> {
        if self.reserved.contains_key(subscription) || self.records.contains_key(subscription) {
            return Err(anyhow::Error::new(GroupConflict {
                subscription: subscription.to_string(),
                owner: owner_hint.to_string(),
            }));
        }
        self.reserved.insert(subscription.to_string(), owner_hint.to_string());
        if slots > 0 {
            self.reserved_slots.insert(subscription.to_string(), slots);
        }
        Ok(())
    }

    pub(crate) fn complete(&mut self, record: GroupRecord) {
        self.reserved.remove(&record.subscription);
        self.reserved_slots.remove(&record.subscription);
        self.inflight_slots.remove(&record.subscription);
        self.registered_slots.remove(&record.subscription);
        for member in &record.members {
            if member.subscription != record.subscription {
                self.private_members.insert(member.subscription.clone(), record.subscription.clone());
            }
            self.shape_groups.insert(member.shape_id.clone(), record.id.clone());
        }
        self.records.insert(record.subscription.clone(), record);
    }

    pub(crate) fn release(&mut self, subscription: &str) -> Option<GroupRecord> {
        self.reserved.remove(subscription);
        self.reserved_slots.remove(subscription);
        self.inflight_slots.remove(subscription);
        self.registered_slots.remove(subscription);
        self.private_members.retain(|_, owner| owner != subscription);
        let record = self.records.remove(subscription);
        if let Some(record) = &record {
            for member in &record.members {
                self.shape_groups.remove(&member.shape_id);
            }
        }
        record
    }

    /// Compensate one load attempt without being able to remove a replacement group that
    /// subsequently claimed the same subscription.  A cancellation can finish after its
    /// durable `GroupDropped` event has been queued, so the cleanup path must be bound to the
    /// group incarnation it started with.
    pub(crate) fn abort_if_matches(&mut self, subscription: &str, group_id: &str) -> Option<GroupRecord> {
        if self.records.get(subscription).is_some_and(|record| record.id == group_id) {
            return self.release_if_matches(subscription, group_id);
        }
        let legacy = format!("group {subscription}");
        if self.reserved.get(subscription).is_some_and(|owner| owner == group_id || owner == &legacy) {
            self.reserved.remove(subscription);
            self.reserved_slots.remove(subscription);
            self.inflight_slots.remove(subscription);
            self.registered_slots.remove(subscription);
            self.private_members.retain(|_, owner| owner != subscription);
        }
        None
    }

    pub(crate) fn release_reservation_if_matches(&mut self, subscription: &str, group_id: &str) {
        let legacy = format!("group {subscription}");
        if self.reserved.get(subscription).is_some_and(|owner| owner == group_id || owner == &legacy) {
            self.reserved.remove(subscription);
            self.reserved_slots.remove(subscription);
            self.inflight_slots.remove(subscription);
            self.registered_slots.remove(subscription);
            self.private_members.retain(|_, owner| owner != subscription);
        }
    }

    pub(crate) fn release_if_matches(&mut self, subscription: &str, group_id: &str) -> Option<GroupRecord> {
        if self.records.get(subscription).is_some_and(|record| record.id == group_id) {
            self.reserved.remove(subscription);
            self.reserved_slots.remove(subscription);
            self.inflight_slots.remove(subscription);
            self.registered_slots.remove(subscription);
            self.private_members.retain(|_, owner| owner != subscription);
            let record = self.records.remove(subscription);
            if let Some(record) = &record {
                for member in &record.members {
                    self.shape_groups.remove(&member.shape_id);
                }
            }
            record
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

    pub(crate) fn reservation_id(&self, subscription: &str) -> Option<&String> {
        self.reserved.get(subscription)
    }

    pub(crate) fn reserved_slots(&self) -> usize {
        // Registered members are already present in `EngineState::shapes`; only future and
        // in-flight members remain as reservation credit here.
        self.reserved_slots.values().sum::<usize>() + self.inflight_slots.values().sum::<usize>()
    }

    pub(crate) fn has_reserved_slot(&self, group: &str) -> bool {
        self.reserved_slots.get(group).is_some_and(|slots| *slots > 0)
    }

    pub(crate) fn begin_member_slot(&mut self, group: &str) -> bool {
        let Some(remaining) = self.reserved_slots.get_mut(group) else { return false };
        if *remaining == 0 {
            return false;
        }
        *remaining -= 1;
        *self.inflight_slots.entry(group.to_string()).or_default() += 1;
        true
    }

    pub(crate) fn finish_member_slot(&mut self, group: &str) -> bool {
        // The shape record is inserted under the same state lock immediately before this call,
        // so the slot is now represented by `shapes.len()` and must leave the reservation count.
        let Some(inflight) = self.inflight_slots.get_mut(group) else { return false };
        if *inflight == 0 {
            return false;
        }
        *inflight -= 1;
        if *inflight == 0 {
            self.inflight_slots.remove(group);
        }
        *self.registered_slots.entry(group.to_string()).or_default() += 1;
        true
    }

    pub(crate) fn restore_member_slot(&mut self, group: &str) -> bool {
        if let Some(inflight) = self.inflight_slots.get_mut(group)
            && *inflight > 0
        {
            *inflight -= 1;
            if *inflight == 0 {
                self.inflight_slots.remove(group);
            }
            *self.reserved_slots.entry(group.to_string()).or_default() += 1;
            return true;
        }
        self.restore_registered_member_slot(group)
    }

    /// Return the slot of a member record that was registered successfully but whose create then
    /// failed during backfill or durability. The record is removed by the create rollback, so the
    /// reservation must regain the capacity before the next member attempt.
    pub(crate) fn restore_registered_member_slot(&mut self, group: &str) -> bool {
        let Some(registered) = self.registered_slots.get_mut(group) else { return false };
        if *registered == 0 {
            return false;
        }
        *registered -= 1;
        if *registered == 0 {
            self.registered_slots.remove(group);
        }
        *self.reserved_slots.entry(group.to_string()).or_default() += 1;
        true
    }

    pub(crate) fn group_for_subscription(&self, subscription: &str) -> Option<String> {
        if let Some(owner) = self.reservation_id(subscription) {
            return Some(owner.clone());
        }
        let owner = self.private_members.get(subscription)?;
        self.is_reserved(owner).then(|| owner.clone())
    }

    /// Build the record-owned member map once for a retention snapshot.  Per-shape lookups then
    /// stay O(1), rather than scanning every group member under the engine state lock.
    pub(crate) fn record_shape_groups(&self) -> HashMap<String, String> {
        self.shape_groups.clone()
    }

    /// True for the public reservation and every private member reservation belonging to it.
    /// Member claims are recorded separately so a group can create ordinary shapes atomically,
    /// but their lease protection must last for the whole assembly window.
    pub(crate) fn in_active_reservation(&self, subscription: &str) -> bool {
        self.is_reserved(subscription)
            || self.private_members.get(subscription).is_some_and(|group| self.is_reserved(group))
    }

    pub(crate) fn private_owner(&self, subscription: &str) -> Option<&String> {
        self.private_members.get(subscription)
    }

    /// Whether a subscription on `shape_id` is owned by a group reservation or record.  The
    /// anonymous native release path uses this before selecting a claim, so it cannot bypass the
    /// public group's reservation/ownership guards by omitting `?subscription=`.
    pub(crate) fn owns_shape_claim(&self, subscription: &str, shape_id: &str) -> bool {
        self.in_active_reservation(subscription)
            || self.private_owner(subscription).is_some()
            || self
                .records
                .get(subscription)
                .is_some_and(|group| group.members.iter().any(|member| member.shape_id == shape_id))
    }

    pub(crate) fn reserve_private_members(&mut self, group: &str, members: impl IntoIterator<Item = String>) {
        for member in members {
            self.private_members.insert(member, group.to_string());
        }
    }

    /// Return the group identity owning a shape, including a reservation whose private member
    /// claims have been installed before its shapes are complete. Group records remain present
    /// while release retires members, so the retention snapshot protects that window too.
    pub(crate) fn group_for_shape(&self, shape_id: &str, subscriptions: &[String]) -> Option<String> {
        if let Some(group) = self.shape_groups.get(shape_id) {
            return Some(group.clone());
        }
        subscriptions.iter().find_map(|subscription| {
            if let Some(owner) = self.reservation_id(subscription) {
                return Some(owner.clone());
            }
            let owner = self.private_members.get(subscription)?;
            self.is_reserved(owner).then(|| owner.clone())
        })
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
            let notified = self.done.notified();
            tokio::pin!(notified);
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

    #[test]
    fn a_group_reservation_holds_and_accounts_for_member_slots() {
        let mut claims = GroupClaims::default();
        claims.reserve_with_slots("scope-1", "group:scope-1", 3).unwrap();
        assert_eq!(claims.reserved_slots(), 3);
        assert!(claims.begin_member_slot("scope-1"));
        assert_eq!(claims.reserved_slots(), 3, "an in-flight member still holds its slot");
        assert!(claims.finish_member_slot("scope-1"));
        assert_eq!(claims.reserved_slots(), 2);
        assert!(claims.restore_member_slot("scope-1"));
        assert_eq!(claims.reserved_slots(), 3, "a rolled-back registered member returns its slot");
        assert!(claims.begin_member_slot("scope-1"));
        assert!(claims.restore_member_slot("scope-1"));
        assert_eq!(claims.reserved_slots(), 3);
    }

    #[test]
    fn a_stale_release_cannot_take_a_replacement_group() {
        let mut claims = GroupClaims::default();
        claims.reserve("scope-1", "group:scope-1").unwrap();
        claims.complete(GroupRecord {
            id: "stream/one".into(),
            stream_path: "stream/one".into(),
            subscription: "scope-1".into(),
            members: Vec::new(),
            lsn: "0/0".into(),
            snapshot: "".into(),
            horizon: "0/0".into(),
            replay_fence: "0".into(),
        });
        assert!(claims.release_if_matches("scope-1", "stream/two").is_none());
        assert_eq!(claims.get("scope-1").map(|group| group.id.as_str()), Some("stream/one"));
    }

    #[test]
    fn an_old_abort_cannot_remove_a_replacement_group() {
        let mut claims = GroupClaims::default();
        claims.reserve("scope-1", "group:scope-1").unwrap();
        claims.complete(GroupRecord {
            id: "stream/one".into(),
            stream_path: "stream/one".into(),
            subscription: "scope-1".into(),
            members: Vec::new(),
            lsn: "0/0".into(),
            snapshot: "".into(),
            horizon: "0/0".into(),
            replay_fence: "0".into(),
        });
        assert!(claims.abort_if_matches("scope-1", "stream/two").is_none());
        assert_eq!(claims.get("scope-1").map(|group| group.id.as_str()), Some("stream/one"));
    }

    #[tokio::test]
    async fn concurrent_release_waiters_share_completion() {
        let release = GroupRelease::new();
        let waiter = {
            let release = release.clone();
            tokio::spawn(async move { release.wait().await })
        };
        tokio::task::yield_now().await;
        release.complete(&Ok(()));
        waiter.await.unwrap().unwrap();
    }
}
