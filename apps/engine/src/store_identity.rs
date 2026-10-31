//! Immutable Durable Streams identity and namespace scope for the coupled pilot engine.

use anyhow::{Result, bail};
use serde::{Deserialize, Serialize};

const IDENTIFIER_MIN: usize = 3;
const IDENTIFIER_MAX: usize = 48;

/// The deployment-owned identity expected from a Durable Streams store.
///
/// This is deliberately independent from the readiness response. Observed state may confirm this
/// value, but can never fill in a missing expected value.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoreIdentityV1 {
    pub store_id: String,
    pub store_generation: String,
    pub protocol_version: u32,
    pub layout_version: u32,
    pub durability_mode: String,
    pub wal_shard_count: u32,
    pub stream_lane_count: u32,
    pub filesystem_uuid: String,
}

impl StoreIdentityV1 {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        store_id: String,
        store_generation: String,
        protocol_version: u32,
        layout_version: u32,
        durability_mode: String,
        wal_shard_count: u32,
        stream_lane_count: u32,
        filesystem_uuid: String,
    ) -> Result<Self> {
        validate_canonical_uuid("store_id", &store_id)?;
        validate_canonical_uuid("store_generation", &store_generation)?;
        validate_canonical_uuid("filesystem_uuid", &filesystem_uuid)?;
        if protocol_version == 0 {
            bail!("protocol_version must be greater than zero");
        }
        if layout_version == 0 {
            bail!("layout_version must be greater than zero");
        }
        if durability_mode != "wal" {
            bail!("durability_mode must be exactly 'wal' for the coupled pilot");
        }
        if wal_shard_count == 0 {
            bail!("wal_shard_count must be greater than zero");
        }
        if stream_lane_count == 0 {
            bail!("stream_lane_count must be greater than zero");
        }
        Ok(Self {
            store_id,
            store_generation,
            protocol_version,
            layout_version,
            durability_mode,
            wal_shard_count,
            stream_lane_count,
            filesystem_uuid,
        })
    }

    #[doc(hidden)]
    pub fn in_process_test_identity() -> Self {
        Self::new(
            "2bc96d0b-9740-4f50-97c6-754b2b27d6b0".to_string(),
            "ff8b5fa6-e786-4994-8da0-f14e9e79f318".to_string(),
            1,
            1,
            "wal".to_string(),
            2,
            1,
            "253f14d5-cbee-4df8-9e3c-e44c6e41501b".to_string(),
        )
        .expect("fixed test identity is valid")
    }
}

/// Immutable scope used to qualify every logical Circuits stream name exactly once.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StreamScope {
    pub stack_namespace: String,
    pub store: StoreIdentityV1,
    pub query_generation: String,
}

/// The immutable event-zero binding for one coupled-pilot catalog namespace.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoreBound {
    pub store: StoreIdentityV1,
    pub stack_namespace: String,
    pub ingest_epoch: String,
    pub query_generation: String,
}

impl StoreBound {
    pub fn coupled_v1(scope: &StreamScope) -> Self {
        Self {
            store: scope.store.clone(),
            stack_namespace: scope.stack_namespace.clone(),
            ingest_epoch: "coupled-v1".to_string(),
            query_generation: scope.query_generation.clone(),
        }
    }
}

impl StreamScope {
    pub fn new(stack_namespace: String, store: StoreIdentityV1, query_generation: String) -> Result<Self> {
        validate_identifier("stack_namespace", &stack_namespace)?;
        validate_identifier("query_generation", &query_generation)?;
        Ok(Self { stack_namespace, store, query_generation })
    }

    /// Translate one validated logical stream path to its pilot physical path.
    pub fn qualify(&self, logical_path: &str) -> Result<String> {
        validate_logical_path(logical_path)?;
        Ok(format!(
            "circuits/v1/{}/stores/{}/queries/{}/{}",
            self.stack_namespace, self.store.store_generation, self.query_generation, logical_path
        ))
    }

    /// A short name for this scope, for a value that must not outlive it or cross into another.
    pub(crate) fn tag(&self) -> String {
        use sha2::{Digest, Sha256};
        let namespace = format!("{}/{}/{}", self.stack_namespace, self.store.store_generation, self.query_generation);
        format!("{:x}", Sha256::digest(namespace.as_bytes()))[..16].to_string()
    }

    #[doc(hidden)]
    pub fn in_process_test_scope() -> Self {
        Self::new("test-stack".to_string(), StoreIdentityV1::in_process_test_identity(), "test-query".to_string())
            .expect("fixed test scope is valid")
    }
}

pub fn validate_identifier(name: &str, value: &str) -> Result<()> {
    let bytes = value.as_bytes();
    if !(IDENTIFIER_MIN..=IDENTIFIER_MAX).contains(&bytes.len())
        || !bytes.first().is_some_and(u8::is_ascii_lowercase)
        || !bytes.last().is_some_and(u8::is_ascii_alphanumeric)
        || !bytes.iter().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || *b == b'-')
    {
        bail!("{name} must match ^[a-z][a-z0-9-]{{1,46}}[a-z0-9]$ (3-48 lowercase ASCII characters), got '{value}'");
    }
    Ok(())
}

pub fn validate_logical_path(path: &str) -> Result<()> {
    if path.is_empty()
        || path.starts_with('/')
        || path.contains('\\')
        || path.contains('?')
        || path.contains('#')
        || path.contains('%')
    {
        bail!("invalid logical Durable Streams path '{path}'");
    }
    for component in path.split('/') {
        if component.is_empty() || component == "." || component == ".." {
            bail!("invalid logical Durable Streams path '{path}'");
        }
    }
    Ok(())
}

fn validate_canonical_uuid(name: &str, value: &str) -> Result<()> {
    let parsed =
        uuid::Uuid::parse_str(value).map_err(|e| anyhow::anyhow!("{name} must be a canonical lowercase UUID: {e}"))?;
    if parsed.hyphenated().to_string() != value {
        bail!("{name} must be a canonical lowercase UUID, got '{value}'");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scopes_separate_identical_logical_paths() {
        let first = StreamScope::new(
            "first-stack".to_string(),
            StoreIdentityV1::in_process_test_identity(),
            "query-one".to_string(),
        )
        .unwrap();
        let second = StreamScope::new(
            "other-stack".to_string(),
            StoreIdentityV1::in_process_test_identity(),
            "query-one".to_string(),
        )
        .unwrap();
        assert_ne!(first.qualify("meta/catalog").unwrap(), second.qualify("meta/catalog").unwrap());
    }

    #[test]
    fn scope_and_path_reject_ambiguous_spellings() {
        for value in ["ab", "Upper-stack", "under_score", "stack/name", "stack%2fname", "stack-"] {
            assert!(validate_identifier("stack_namespace", value).is_err(), "{value}");
        }
        let scope = StreamScope::in_process_test_scope();
        for path in [
            "",
            "/meta/catalog",
            "meta//catalog",
            "meta/./catalog",
            "meta/../catalog",
            "meta\\catalog",
            "meta?a",
            "meta#a",
            "meta%2f",
        ] {
            assert!(scope.qualify(path).is_err(), "{path}");
        }
    }
}
