//! Private, bounded runtime drain receipts. Deployment handoff provenance lives separately.

use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::ds::{Envelope, EnvelopeHeaders};
use crate::table_ref::TableRef;

pub(crate) const ENVELOPE_TYPE: &str = "__circuits.runtime_authority_fence";
pub(crate) const RECEIPT_CAPACITY: usize = 65_536;
const RECEIPT_TTL: Duration = Duration::from_secs(600);

pub(crate) fn marker_table() -> &'static TableRef {
    static TABLE: std::sync::OnceLock<TableRef> = std::sync::OnceLock::new();
    TABLE.get_or_init(|| TableRef::public("native_sync_authority_fence").expect("valid runtime marker table"))
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct AuthorityMarker {
    pub source_commit_id: String,
    pub user_id: String,
    pub generation: String,
}

impl AuthorityMarker {
    pub(crate) fn parse(source_commit_id: &str, user_id: &str, generation: &str) -> Option<Self> {
        for value in [source_commit_id, user_id] {
            if value.len() != 36 || Uuid::parse_str(value).ok()?.to_string() != value {
                return None;
            }
        }
        if generation.len() != 64
            || !generation.bytes().all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return None;
        }
        Some(Self { source_commit_id: source_commit_id.into(), user_id: user_id.into(), generation: generation.into() })
    }
}

#[derive(Clone, Debug)]
pub(crate) struct RuntimeFence {
    pub marker: AuthorityMarker,
    pub incarnation: Uuid,
}

impl RuntimeFence {
    pub(crate) fn into_envelope(self) -> Envelope {
        Envelope {
            type_: ENVELOPE_TYPE.into(),
            key: self.marker.source_commit_id.clone(),
            value: Some(serde_json::json!({
                "sourceCommitId": self.marker.source_commit_id,
                "userId": self.marker.user_id, "generation": self.marker.generation,
                "incarnation": self.incarnation.to_string(),
            })),
            old: None,
            headers: EnvelopeHeaders {
                operation: "runtime_authority_fence".into(),
                txid: None,
                offset: None,
                lsn: None,
                seq: None,
                last: None,
                commit_ts: None,
            },
        }
    }

    pub(crate) fn from_envelope(envelope: &Envelope) -> Option<Self> {
        if envelope.type_ != ENVELOPE_TYPE {
            return None;
        }
        let value = envelope.value.as_ref()?;
        let marker = AuthorityMarker::parse(
            value.get("sourceCommitId")?.as_str()?,
            value.get("userId")?.as_str()?,
            value.get("generation")?.as_str()?,
        )?;
        let incarnation = Uuid::parse_str(value.get("incarnation")?.as_str()?).ok()?;
        (envelope.key == marker.source_commit_id).then_some(Self { marker, incarnation })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RuntimeDrainReceipt {
    #[serde(flatten)]
    pub marker: AuthorityMarker,
    pub commit_lsn: String,
}

pub(crate) struct RuntimeReceipts {
    incarnation: Uuid,
    receipts: HashMap<AuthorityMarker, (RuntimeDrainReceipt, Instant)>,
    fifo: VecDeque<AuthorityMarker>,
}

impl Default for RuntimeReceipts {
    fn default() -> Self {
        Self { incarnation: Uuid::new_v4(), receipts: HashMap::new(), fifo: VecDeque::new() }
    }
}

impl RuntimeReceipts {
    pub(crate) fn incarnation(&self) -> Uuid {
        self.incarnation
    }

    pub(crate) fn invalidate(&mut self) {
        self.incarnation = Uuid::new_v4();
        self.receipts.clear();
        self.fifo.clear();
    }

    pub(crate) fn get(&mut self, marker: &AuthorityMarker, now: Instant) -> Option<RuntimeDrainReceipt> {
        self.expire(now);
        self.receipts.get(marker).map(|(receipt, _)| receipt.clone())
    }

    pub(crate) fn publish(
        &mut self,
        transaction_incarnation: Uuid,
        fences: Vec<RuntimeFence>,
        commit_lsn: &str,
        now: Instant,
    ) {
        if transaction_incarnation != self.incarnation {
            return;
        }
        self.expire(now);
        for fence in fences {
            // A queued old envelope cannot acquire the incarnation current when it is sequenced.
            if fence.incarnation != self.incarnation || self.receipts.contains_key(&fence.marker) {
                continue;
            }
            if self.fifo.len() == RECEIPT_CAPACITY {
                self.remove_oldest();
            }
            let receipt = RuntimeDrainReceipt { marker: fence.marker.clone(), commit_lsn: commit_lsn.into() };
            self.fifo.push_back(fence.marker.clone());
            self.receipts.insert(fence.marker, (receipt, now));
        }
    }

    fn expire(&mut self, now: Instant) {
        while self.fifo.front().is_some_and(|marker| {
            self.receipts.get(marker).is_none_or(|(_, inserted)| now.duration_since(*inserted) >= RECEIPT_TTL)
        }) {
            self.remove_oldest();
        }
    }

    fn remove_oldest(&mut self) {
        if let Some(marker) = self.fifo.pop_front() {
            self.receipts.remove(&marker);
        }
    }
}

/// Excess markers remain unknown; the sequencer must still process every application envelope.
#[derive(Default)]
pub(crate) struct PendingRuntimeFences {
    fences: Vec<RuntimeFence>,
    overflowed: bool,
}

impl PendingRuntimeFences {
    pub(crate) fn stage(&mut self, envelope: &Envelope, incarnation: Uuid) {
        if self.fences.len() == RECEIPT_CAPACITY {
            self.overflowed = true;
            return;
        }
        if let Some(fence) = RuntimeFence::from_envelope(envelope)
            && fence.incarnation == incarnation
        {
            self.fences.push(fence);
        }
    }

    pub(crate) fn overflowed(&self) -> bool {
        self.overflowed
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.fences.is_empty()
    }
    pub(crate) fn into_fences(self) -> Vec<RuntimeFence> {
        self.fences
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn marker(id: u128) -> AuthorityMarker {
        AuthorityMarker::parse(&Uuid::from_u128(id).to_string(), &Uuid::from_u128(1).to_string(), &"a".repeat(64))
            .unwrap()
    }

    fn fence(marker: AuthorityMarker, incarnation: Uuid) -> RuntimeFence {
        RuntimeFence { marker, incarnation }
    }

    #[test]
    fn validation_and_exact_tuple_isolation() {
        let mut store = RuntimeReceipts::default();
        let now = Instant::now();
        let original = marker(2);
        store.publish(store.incarnation(), vec![fence(original.clone(), store.incarnation())], "0/10", now);
        assert!(store.get(&original, now).is_some());
        for changed in [
            AuthorityMarker { user_id: Uuid::from_u128(3).to_string(), ..original.clone() },
            AuthorityMarker { generation: "b".repeat(64), ..original.clone() },
            marker(4),
        ] {
            assert!(store.get(&changed, now).is_none());
        }
        for generation in ["a".repeat(63), "a".repeat(65), "A".repeat(64), "g".repeat(64)] {
            assert!(AuthorityMarker::parse(&original.source_commit_id, &original.user_id, &generation).is_none());
        }
        assert!(AuthorityMarker::parse("bad", &original.user_id, &original.generation).is_none());
        assert!(AuthorityMarker::parse(&original.source_commit_id, "bad", &original.generation).is_none());
        assert!(
            AuthorityMarker::parse("ABCDEFAB-0000-0000-0000-000000000001", &original.user_id, &original.generation)
                .is_none()
        );
    }

    #[test]
    fn ttl_boundary_and_duplicate_do_not_refresh_age() {
        let mut store = RuntimeReceipts::default();
        let now = Instant::now();
        let marker = marker(2);
        let incarnation = store.incarnation();
        store.publish(incarnation, vec![fence(marker.clone(), incarnation)], "0/10", now);
        let before = now + RECEIPT_TTL - Duration::from_nanos(1);
        store.publish(incarnation, vec![fence(marker.clone(), incarnation)], "0/20", before);
        assert_eq!(store.get(&marker, before).unwrap().commit_lsn, "0/10");
        assert!(store.get(&marker, now + RECEIPT_TTL).is_none());
        assert!(store.get(&marker, now + RECEIPT_TTL + Duration::from_nanos(1)).is_none());
        assert!(store.fifo.is_empty());
    }

    #[test]
    fn fifo_and_transaction_staging_are_bounded() {
        let mut store = RuntimeReceipts::default();
        let incarnation = store.incarnation();
        let now = Instant::now();
        let mut pending = PendingRuntimeFences::default();
        for id in 1..=(RECEIPT_CAPACITY + 1) {
            pending.stage(&fence(marker(id as u128), incarnation).into_envelope(), incarnation);
        }
        assert_eq!(pending.fences.len(), RECEIPT_CAPACITY);
        assert!(pending.overflowed());
        store.publish(incarnation, pending.into_fences(), "0/10", now);
        assert_eq!(store.receipts.len(), RECEIPT_CAPACITY);
        assert!(store.get(&marker((RECEIPT_CAPACITY + 1) as u128), now).is_none());
        store.publish(incarnation, vec![fence(marker(1), incarnation)], "0/20", now);
        store.publish(incarnation, vec![fence(marker((RECEIPT_CAPACITY + 1) as u128), incarnation)], "0/30", now);
        assert!(store.get(&marker(1), now).is_none(), "duplicate must not move the FIFO entry");
        assert!(store.get(&marker(2), now).is_some());
        assert_eq!(store.receipts.len(), RECEIPT_CAPACITY);
        assert_eq!(store.fifo.len(), RECEIPT_CAPACITY);
    }

    #[test]
    fn reset_rejects_queued_old_envelopes_and_in_flight_publication() {
        let mut store = RuntimeReceipts::default();
        let old = store.incarnation();
        let now = Instant::now();
        let queued = fence(marker(2), old).into_envelope();
        store.invalidate();
        let current = store.incarnation();
        let mut pending = PendingRuntimeFences::default();
        pending.stage(&queued, current);
        assert!(pending.is_empty(), "old queued marker first processed after reset is rejected");
        store.publish(current, vec![fence(marker(2), old)], "0/10", now);
        store.publish(old, vec![fence(marker(3), old)], "0/20", now);
        assert!(store.get(&marker(2), now).is_none());
        assert!(store.get(&marker(3), now).is_none());
        store.publish(current, vec![fence(marker(4), current)], "0/30", now);
        assert!(store.get(&marker(4), now).is_some());
        store.invalidate();
        assert!(store.get(&marker(4), now).is_none());
    }
}
