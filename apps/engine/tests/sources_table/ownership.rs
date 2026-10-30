//! OTTO-6003 lifecycle contracts. Each ignored test uses a fresh process because the current
//! adapter caches its TTL in process statics. The parent serializes children and owns fixture cleanup.

use super::*;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::path::PathBuf;
use std::time::Duration;

use anyhow::ensure;
use electric_circuits_engine::ds::DsClient;
use electric_circuits_engine::engine::{Engine, GroupLoadAwait, PostgresSetup};
use electric_circuits_engine::http::router_with_introspection;
use electric_circuits_engine::table_ref::TableSelector;
use tokio::sync::{Notify, watch};

#[path = "observability.rs"]
mod observability;

const SOURCES: [&str; 3] = ["alpha", "beta", "gamma"];
const RESTARTS: i64 = 50;
// Re-exec the test binary, not Cargo: the parent's herdr-heavy lock covers the entire run.
fn isolated(name: &str, case: Case) -> Result<()> {
    if isolated_contract(name, |command| {
        let command = command
            .env("ELECTRIC_HANDLE_TTL", "1")
            .env("ELECTRIC_CIRCUITS_SUBQ_STORAGE", "1")
            .env_remove("ELECTRIC_CIRCUITS_SUBQ_STORAGE_DIR")
            .env("ELECTRIC_CIRCUITS_SHAPE_IDLE_SECS", "0");
        if matches!(case, Case::GroupLapse | Case::GroupRestoreExpired | Case::GroupReleaseDuringAssembly) {
            command
                .env("ELECTRIC_CIRCUITS_SUBSCRIPTION_LEASE_SECS", "5")
                .env("ELECTRIC_CIRCUITS_RETENTION_SWEEP_SECS", "1");
        } else {
            command.env("ELECTRIC_CIRCUITS_SUBSCRIPTION_LEASE_SECS", "0");
        }
    })? {
        return Ok(());
    }
    tokio::runtime::Builder::new_multi_thread().worker_threads(2).enable_all().build()?.block_on(async {
        let fixture = Fixture::new().await?;
        let result = match case {
            Case::FailedRetry => {
                let mut errors = Vec::new();
                for result in [
                    failed_source_retry(&fixture).await,
                    self_stopped_sources_recover_without_starving_rows(&fixture).await,
                    changed_version_fetch_failure_does_not_retry_cached_rows(&fixture).await,
                ] {
                    if let Err(error) = result {
                        errors.push(format!("{error:#}"));
                    }
                }
                if errors.is_empty() { Ok(()) } else { anyhow::bail!("{}", errors.join("\n")) }
            }
            Case::Restarts => repeated_restarts(&fixture).await,
            Case::StaleHandle => stale_handle(&fixture).await,
            Case::ForeignHandle => foreign_handle(&fixture).await,
            Case::ReaderSnapshots => reader_snapshots(&fixture).await,
            Case::Eviction => every_live_engine_evicts(&fixture).await,
            Case::Single => single_engine_controls(&fixture).await,
            Case::GroupDelayedFlip => group_delayed_subquery_flip(&fixture).await,
            Case::GroupDrainReceipt => group_source_drain_receipt(&fixture).await,
            Case::GroupEqualKeyDelete => group_equal_key_delete(&fixture).await,
            Case::GroupConflict => group_claim_overlap_is_atomic(&fixture).await,
            Case::GroupFence => group_uses_one_snapshot_fence(&fixture).await,
            Case::GroupRetirement => group_retirement_recreates_after_close(&fixture).await,
            Case::GroupRestart => group_restart_restores_plain_and_drops_mixed(&fixture).await,
            Case::GroupRestoreExpired => group_restore_drops_expired_group_before_first_sweep(&fixture).await,
            Case::GroupClosedReload => group_closed_reload_waits_for_release(&fixture).await,
            Case::GroupRelease => group_release_reloads_same_subscription(&fixture).await,
            Case::GroupLapse => group_lapse_reloads_same_subscription(&fixture).await,
            Case::GroupOneSubscription => group_one_subscription_claims(&fixture).await,
            Case::GroupRetryRenews => group_retry_renews(&fixture).await,
            Case::GroupRenewalRelease => group_renewal_rechecks_a_racing_release(&fixture, false).await,
            Case::GroupRenewalReleaseOpen => group_renewal_rechecks_a_racing_release(&fixture, true).await,
            Case::GroupRenewalReplaced => group_renewal_follows_a_replacement(&fixture).await,
            Case::GroupReleaseDuringAssembly => group_release_during_assembly(&fixture).await,
            Case::GroupRetirementDuringAssembly => group_retirement_during_assembly(&fixture).await,
            Case::GroupMembersMax => group_members_max(&fixture).await,
            Case::GroupCompletionCuts => group_completion_cuts(&fixture).await,
            Case::GroupReleaseCancellation => group_release_cancellation(&fixture).await,
            Case::GroupRenewalCancellation => group_renewal_cancellation(&fixture).await,
            Case::GroupReconnectBurst => group_reconnect_burst(&fixture).await,
            Case::GroupAdmission => group_admission_gate(&fixture).await,
        };
        // Result assertions, rather than panics, let fixture cleanup run on semantic failures.
        let cleanup = fixture.cleanup().await;
        if let Err(error) = &cleanup {
            eprintln!("fixture cleanup failed: {error:#}");
        }
        result.and(cleanup)
    })
}

#[derive(Clone, Copy)]
enum Case {
    FailedRetry,
    Restarts,
    StaleHandle,
    ForeignHandle,
    ReaderSnapshots,
    Eviction,
    Single,
    GroupDelayedFlip,
    GroupDrainReceipt,
    GroupEqualKeyDelete,
    GroupConflict,
    GroupFence,
    GroupRetirement,
    GroupRestart,
    GroupRestoreExpired,
    GroupClosedReload,
    GroupRelease,
    GroupLapse,
    GroupOneSubscription,
    GroupRetryRenews,
    GroupRenewalRelease,
    GroupRenewalReleaseOpen,
    GroupRenewalReplaced,
    GroupReleaseDuringAssembly,
    GroupRetirementDuringAssembly,
    GroupMembersMax,
    GroupCompletionCuts,
    GroupReleaseCancellation,
    GroupRenewalCancellation,
    GroupReconnectBurst,
    GroupAdmission,
}

#[test]
#[ignore = "isolated PostgreSQL 18 required via ELECTRIC_CIRCUITS_TEST_PG_URL"]
fn sources_fifty_restarts_release_engines_threads_and_spill() -> Result<()> {
    isolated("ownership::sources_fifty_restarts_release_engines_threads_and_spill", Case::Restarts)
}

#[test]
#[ignore = "isolated PostgreSQL 18 required via ELECTRIC_CIRCUITS_TEST_PG_URL"]
fn replacement_refuses_the_stopped_sources_handle() -> Result<()> {
    isolated("ownership::replacement_refuses_the_stopped_sources_handle", Case::StaleHandle)
}

#[test]
#[ignore = "isolated PostgreSQL 18 required via ELECTRIC_CIRCUITS_TEST_PG_URL"]
fn a_source_refuses_another_sources_handle() -> Result<()> {
    isolated("ownership::a_source_refuses_another_sources_handle", Case::ForeignHandle)
}

#[test]
#[ignore = "isolated PostgreSQL 18 required via ELECTRIC_CIRCUITS_TEST_PG_URL; observes a real TTL sweep"]
fn reader_snapshots_release_handles_and_heap_across_restarts() -> Result<()> {
    isolated("ownership::reader_snapshots_release_handles_and_heap_across_restarts", Case::ReaderSnapshots)
}

#[test]
#[ignore = "isolated PostgreSQL 18 required via ELECTRIC_CIRCUITS_TEST_PG_URL; observes a real TTL sweep"]
fn idle_handles_are_evicted_on_every_live_source_after_restart() -> Result<()> {
    isolated("ownership::idle_handles_are_evicted_on_every_live_source_after_restart", Case::Eviction)
}

#[test]
#[ignore = "isolated PostgreSQL 18 required via ELECTRIC_CIRCUITS_TEST_PG_URL; observes a real TTL sweep"]
fn single_engine_keeps_eviction_and_degrade_reaping() -> Result<()> {
    isolated("ownership::single_engine_keeps_eviction_and_degrade_reaping", Case::Single)
}

#[test]
#[ignore = "isolated PostgreSQL 18 required via ELECTRIC_CIRCUITS_TEST_PG_URL"]
fn failed_source_retries_on_poll_without_row_change_or_refresh() -> Result<()> {
    isolated("ownership::failed_source_retries_on_poll_without_row_change_or_refresh", Case::FailedRetry)
}

#[test]
#[ignore = "isolated PostgreSQL 18 required via ELECTRIC_CIRCUITS_TEST_PG_URL"]
fn group_delayed_subquery_flip_is_ordered_after_plain_output() -> Result<()> {
    isolated("ownership::group_delayed_subquery_flip_is_ordered_after_plain_output", Case::GroupDelayedFlip)
}

#[test]
#[ignore = "isolated PostgreSQL 18 required via ELECTRIC_CIRCUITS_TEST_PG_URL"]
fn group_source_drain_receipt_covers_deferred_lane() -> Result<()> {
    isolated("ownership::group_source_drain_receipt_covers_deferred_lane", Case::GroupDrainReceipt)
}

#[test]
#[ignore = "isolated PostgreSQL 18 required via ELECTRIC_CIRCUITS_TEST_PG_URL"]
fn group_delete_names_table_for_equal_keys() -> Result<()> {
    isolated("ownership::group_delete_names_table_for_equal_keys", Case::GroupEqualKeyDelete)
}

#[test]
#[ignore = "isolated PostgreSQL 18 required via ELECTRIC_CIRCUITS_TEST_PG_URL"]
fn group_claim_overlap_is_atomic_and_returns_409() -> Result<()> {
    isolated("ownership::group_claim_overlap_is_atomic_and_returns_409", Case::GroupConflict)
}

#[test]
#[ignore = "isolated PostgreSQL 18 required via ELECTRIC_CIRCUITS_TEST_PG_URL"]
fn group_members_share_one_snapshot_horizon_and_replay_fence() -> Result<()> {
    isolated("ownership::group_members_share_one_snapshot_horizon_and_replay_fence", Case::GroupFence)
}

#[test]
#[ignore = "isolated PostgreSQL 18 required via ELECTRIC_CIRCUITS_TEST_PG_URL"]
fn group_retirement_reloads_and_closed_stream_is_recreated() -> Result<()> {
    isolated("ownership::group_retirement_reloads_and_closed_stream_is_recreated", Case::GroupRetirement)
}

#[test]
#[ignore = "isolated PostgreSQL 18 required via ELECTRIC_CIRCUITS_TEST_PG_URL"]
fn group_restart_restores_plain_and_drops_mixed_groups() -> Result<()> {
    isolated("ownership::group_restart_restores_plain_and_drops_mixed_groups", Case::GroupRestart)
}

#[test]
#[ignore = "isolated PostgreSQL 18 required via ELECTRIC_CIRCUITS_TEST_PG_URL"]
fn group_restore_drops_expired_lease_before_first_sweep() -> Result<()> {
    isolated("ownership::group_restore_drops_expired_lease_before_first_sweep", Case::GroupRestoreExpired)
}

#[test]
#[ignore = "isolated PostgreSQL 18 required via ELECTRIC_CIRCUITS_TEST_PG_URL"]
fn group_closed_stream_reload_waits_for_release() -> Result<()> {
    isolated("ownership::group_closed_stream_reload_waits_for_release", Case::GroupClosedReload)
}

#[test]
#[ignore = "isolated PostgreSQL 18 required via ELECTRIC_CIRCUITS_TEST_PG_URL"]
fn group_release_reloads_same_subscription_on_a_new_stream() -> Result<()> {
    isolated("ownership::group_release_reloads_same_subscription_on_a_new_stream", Case::GroupRelease)
}

#[test]
#[ignore = "isolated PostgreSQL 18 required via ELECTRIC_CIRCUITS_TEST_PG_URL"]
fn group_lapse_reloads_same_subscription_on_a_new_stream() -> Result<()> {
    isolated("ownership::group_lapse_reloads_same_subscription_on_a_new_stream", Case::GroupLapse)
}

#[test]
#[ignore = "isolated PostgreSQL 18 required via ELECTRIC_CIRCUITS_TEST_PG_URL"]
fn group_owns_one_subscription_across_every_member() -> Result<()> {
    isolated("ownership::group_owns_one_subscription_across_every_member", Case::GroupOneSubscription)
}

#[test]
#[ignore = "isolated PostgreSQL 18 required via ELECTRIC_CIRCUITS_TEST_PG_URL"]
fn group_retry_renews_after_a_dropped_response() -> Result<()> {
    isolated("ownership::group_retry_renews_after_a_dropped_response", Case::GroupRetryRenews)
}

#[test]
#[ignore = "isolated PostgreSQL 18 required via ELECTRIC_CIRCUITS_TEST_PG_URL"]
fn group_renewal_never_answers_a_release_that_raced_it() -> Result<()> {
    isolated("ownership::group_renewal_never_answers_a_release_that_raced_it", Case::GroupRenewalRelease)
}

#[test]
#[ignore = "isolated PostgreSQL 18 required via ELECTRIC_CIRCUITS_TEST_PG_URL"]
fn group_renewal_rechecks_a_release_that_has_not_closed_the_stream() -> Result<()> {
    isolated(
        "ownership::group_renewal_rechecks_a_release_that_has_not_closed_the_stream",
        Case::GroupRenewalReleaseOpen,
    )
}

#[test]
#[ignore = "isolated PostgreSQL 18 required via ELECTRIC_CIRCUITS_TEST_PG_URL"]
fn group_renewal_renews_a_same_members_replacement() -> Result<()> {
    isolated("ownership::group_renewal_renews_a_same_members_replacement", Case::GroupRenewalReplaced)
}

#[test]
#[ignore = "isolated PostgreSQL 18 required via ELECTRIC_CIRCUITS_TEST_PG_URL"]
fn group_release_during_assembly_is_rejected_and_claims_survive() -> Result<()> {
    isolated(
        "ownership::group_release_during_assembly_is_rejected_and_claims_survive",
        Case::GroupReleaseDuringAssembly,
    )
}

#[test]
#[ignore = "isolated PostgreSQL 18 required via ELECTRIC_CIRCUITS_TEST_PG_URL"]
fn group_engine_retirement_during_assembly_retires_member_and_fails_the_load() -> Result<()> {
    isolated(
        "ownership::group_engine_retirement_during_assembly_retires_member_and_fails_the_load",
        Case::GroupRetirementDuringAssembly,
    )
}

#[test]
#[ignore = "isolated PostgreSQL 18 required via ELECTRIC_CIRCUITS_TEST_PG_URL"]
fn group_rejects_more_than_eight_members_without_partial_state() -> Result<()> {
    isolated("ownership::group_rejects_more_than_eight_members_without_partial_state", Case::GroupMembersMax)
}

#[test]
#[ignore = "isolated PostgreSQL 18 required via ELECTRIC_CIRCUITS_TEST_PG_URL"]
fn group_completion_cuts_leave_no_claims_or_durable_group() -> Result<()> {
    isolated("ownership::group_completion_cuts_leave_no_claims_or_durable_group", Case::GroupCompletionCuts)
}

#[test]
#[ignore = "isolated PostgreSQL 18 required via ELECTRIC_CIRCUITS_TEST_PG_URL"]
fn group_release_cancelled_after_durability_still_completes() -> Result<()> {
    isolated("ownership::group_release_cancelled_after_durability_still_completes", Case::GroupReleaseCancellation)
}

#[test]
#[ignore = "isolated PostgreSQL 18 required via ELECTRIC_CIRCUITS_TEST_PG_URL"]
fn group_renewal_cancelled_at_each_renewal_await_keeps_the_group() -> Result<()> {
    isolated("ownership::group_renewal_cancelled_at_each_renewal_await_keeps_the_group", Case::GroupRenewalCancellation)
}

#[test]
#[ignore = "isolated PostgreSQL 18 required via ELECTRIC_CIRCUITS_TEST_PG_URL"]
fn thirty_group_reconnects_claim_and_renew_without_conflicts() -> Result<()> {
    isolated("ownership::thirty_group_reconnects_claim_and_renew_without_conflicts", Case::GroupReconnectBurst)
}

#[test]
#[ignore = "isolated PostgreSQL 18 required via ELECTRIC_CIRCUITS_TEST_PG_URL"]
fn group_admission_does_not_block_other_loads_or_plain_shapes() -> Result<()> {
    isolated("ownership::group_admission_does_not_block_other_loads_or_plain_shapes", Case::GroupAdmission)
}

#[derive(Clone)]
struct ObservedDs {
    store: FeedDs,
    changed: watch::Sender<u64>,
    retired: Arc<Mutex<Vec<(Method, String)>>>,
    closed: Arc<Mutex<HashSet<String>>>,
    delete_gates: Arc<Mutex<HashMap<String, Arc<Notify>>>>,
    /// Like `delete_gates`, for the `POST` that closes a stream: holds a release before its
    /// stream closes. `close_gate_arrived` fires when one is held.
    close_gates: Arc<Mutex<HashMap<String, Arc<Notify>>>>,
    close_gate_arrived: Arc<Notify>,
    fail_group_head_after_put: Arc<Mutex<bool>>,
    fail_next_group_head: Arc<Mutex<bool>>,
    /// Queued HEAD outcomes: `Some(status)` returns that status, while `None` passes through
    /// to the real store.  The latter lets a test target the post-renew probe without turning
    /// the pre-renew probe into a malformed 200 response.
    group_head_faults: Arc<Mutex<Vec<Option<u16>>>>,
    /// Hold one group-stream HEAD: `Some(n)` passes `n` HEADs through and holds the next until
    /// `group_head_release`, so a test can stop a renewal at a chosen storage probe.
    hold_group_head_after: Arc<Mutex<Option<usize>>>,
    group_head_arrived: Arc<Notify>,
    group_head_release: Arc<Notify>,
    hold_group_catalog: Arc<Mutex<bool>>,
    hold_group_drop_id: Arc<Mutex<Option<String>>>,
    group_catalog_arrived: Arc<Notify>,
    group_catalog_release: Arc<Notify>,
}

async fn observed_ds(State(ds): State<ObservedDs>, request: AxumRequest) -> Response {
    let method = request.method().clone();
    let path = request.uri().path().to_string();
    let mut request = request;
    let closing = method == Method::POST && request.headers().contains_key("stream-closed");
    let group_path = path.contains("shape/group-");
    if method == Method::PUT && group_path {
        let mut armed = ds.fail_group_head_after_put.lock().await;
        if *armed {
            *armed = false;
            *ds.fail_next_group_head.lock().await = true;
        }
    }
    if method == Method::HEAD && group_path {
        let hold = {
            let mut armed = ds.hold_group_head_after.lock().await;
            match armed.as_mut() {
                Some(0) => {
                    *armed = None;
                    true
                }
                Some(remaining) => {
                    *remaining -= 1;
                    false
                }
                None => false,
            }
        };
        if hold {
            ds.group_head_arrived.notify_one();
            ds.group_head_release.notified().await;
        }
        if let Some(Some(status)) = {
            let mut faults = ds.group_head_faults.lock().await;
            (!faults.is_empty()).then(|| faults.remove(0))
        } {
            return StatusCode::from_u16(status).unwrap_or(StatusCode::SERVICE_UNAVAILABLE).into_response();
        }
        let mut fail = ds.fail_next_group_head.lock().await;
        if *fail {
            *fail = false;
            return StatusCode::SERVICE_UNAVAILABLE.into_response();
        }
    }
    if method == Method::POST && path.ends_with("meta/catalog") {
        // Inspect the catalog batch so the gate holds the actual group lifecycle durability point,
        // after every member claim exists, rather than an earlier member Created append.
        let (parts, body) = request.into_parts();
        let bytes = to_bytes(body, 64 * 1024 * 1024).await.unwrap_or_default();
        let expected_group_drop = ds.hold_group_drop_id.lock().await.clone();
        let events = serde_json::from_slice::<Vec<serde_json::Value>>(&bytes).unwrap_or_default();
        let group_created = events.iter().any(|event| event["t"] == "groupCreated");
        let group_dropped = events.iter().any(|event| {
            event["t"] == "groupDropped"
                && expected_group_drop.as_deref().is_some_and(|group_id| event["group_id"].as_str() == Some(group_id))
        });
        let hold = if group_dropped {
            *ds.hold_group_drop_id.lock().await = None;
            true
        } else if group_created {
            let mut armed = ds.hold_group_catalog.lock().await;
            let hold = *armed;
            *armed = false;
            hold
        } else {
            false
        };
        if hold {
            ds.group_catalog_arrived.notify_one();
            ds.group_catalog_release.notified().await;
        }
        request = AxumRequest::from_parts(parts, Body::from(bytes));
    }
    let close_gate = if closing {
        ds.close_gates
            .lock()
            .await
            .iter()
            .find(|(suffix, _)| path.ends_with(suffix.as_str()))
            .map(|(_, gate)| gate.clone())
    } else {
        None
    };
    if let Some(gate) = close_gate {
        ds.close_gate_arrived.notify_one();
        gate.notified().await;
    }
    let delete_gate = if method == Method::DELETE {
        ds.delete_gates
            .lock()
            .await
            .iter()
            .find(|(suffix, _)| path.ends_with(suffix.as_str()))
            .map(|(_, gate)| gate.clone())
    } else {
        None
    };
    if let Some(gate) = delete_gate.as_ref() {
        ds.retired.lock().await.push((method.clone(), path.clone()));
        ds.changed.send_modify(|revision| *revision += 1);
        gate.notified().await;
    }
    let mut response = feed_ds_handler(State(ds.store.clone()), request).await;
    if closing {
        ds.closed.lock().await.insert(path.clone());
    }
    if method == Method::HEAD && ds.closed.lock().await.iter().any(|suffix| path.ends_with(suffix)) {
        response.headers_mut().insert("stream-closed", "true".parse().unwrap());
    }
    if closing || (method == Method::DELETE && delete_gate.is_none()) {
        ds.retired.lock().await.push((method.clone(), path.clone()));
    }
    if method == Method::DELETE {
        ds.closed.lock().await.remove(&path);
    }
    if method == Method::POST || method == Method::DELETE {
        if delete_gate.is_none() {
            ds.changed.send_modify(|revision| *revision += 1);
        }
    }
    response
}

struct Fixture {
    client: tokio_postgres::Client,
    pg_url: String,
    config: Config,
    sources_table: String,
    version_table: String,
    outer: String,
    inner: String,
    slots: Vec<String>,
    root: PathBuf,
    ds_url: String,
    ds: ObservedDs,
    ds_stop: oneshot::Sender<()>,
    ds_task: tokio::task::JoinHandle<()>,
}

impl Fixture {
    async fn new() -> Result<Self> {
        let pg_url = std::env::var("ELECTRIC_CIRCUITS_TEST_PG_URL")
            .context("ELECTRIC_CIRCUITS_TEST_PG_URL is required; missing PostgreSQL is not red evidence")?;
        let client = pg::connect(&pg_url).await?;
        let version: String = client.query_one("SHOW server_version_num", &[]).await?.get(0);
        ensure!(version.parse::<u32>()? / 10_000 == 18, "this contract requires PostgreSQL 18, got {version}");
        let suffix = uuid::Uuid::new_v4().simple().to_string();
        let sources_table = format!("otto_sources_{suffix}");
        let version_table = format!("otto_version_{suffix}");
        let outer = format!("otto_items_{suffix}");
        let inner = format!("otto_members_{suffix}");
        let root = std::env::temp_dir().join(format!("otto-6003-{suffix}"));
        std::fs::create_dir(&root)?;
        let secret = root.join("database-url");
        std::fs::write(&secret, &pg_url)?;
        FixtureObjects {
            slots: SOURCES.iter().map(|source| format!("otto_{source}_{suffix}")).collect(),
            publications: SOURCES.iter().map(|source| format!("otto_{source}_{suffix}_pub")).collect(),
            tables: vec![sources_table.clone(), version_table.clone(), outer.clone(), inner.clone()],
        }
        .record()?;
        client
            .batch_execute(&format!(
                "CREATE TABLE {sources_table} (
                source_id text PRIMARY KEY, plugin text NOT NULL, database_secret text NOT NULL,
                slot text NOT NULL, publication text NOT NULL, tables text[] NOT NULL,
                revision bigint NOT NULL, updated_at timestamptz NOT NULL);
             CREATE TABLE {version_table} (revision bigint NOT NULL);
             INSERT INTO {version_table} VALUES (1);
             CREATE TABLE {outer} (id bigint PRIMARY KEY, body text NOT NULL);
             CREATE TABLE {inner} (id bigint PRIMARY KEY);
             ALTER TABLE {outer} REPLICA IDENTITY FULL;
             ALTER TABLE {inner} REPLICA IDENTITY FULL;
             INSERT INTO {outer} VALUES (1, 'kept'), (2, 'excluded');
             INSERT INTO {inner} VALUES (1);"
            ))
            .await?;
        let mut slots = Vec::new();
        for source in SOURCES {
            let slot = format!("otto_{source}_{suffix}");
            let publication = format!("{slot}_pub");
            client.batch_execute(&format!("CREATE PUBLICATION {publication} FOR TABLE {outer}, {inner}")).await?;
            client.query_one("SELECT pg_create_logical_replication_slot($1, 'pgoutput')", &[&slot]).await?;
            client
                .execute(
                    &format!("INSERT INTO {sources_table} VALUES ($1, 'pgoutput', $2, $3, $4, $5, 1, now())"),
                    &[
                        &source,
                        &format!("file:{}", secret.display()),
                        &slot,
                        &publication,
                        &vec![format!("public.{outer}"), format!("public.{inner}")],
                    ],
                )
                .await?;
            slots.push(slot);
        }
        let ds = ObservedDs {
            store: FeedDs::default(),
            changed: watch::channel(0).0,
            retired: Arc::new(Mutex::new(Vec::new())),
            closed: Arc::new(Mutex::new(HashSet::new())),
            delete_gates: Arc::new(Mutex::new(HashMap::new())),
            close_gates: Arc::new(Mutex::new(HashMap::new())),
            close_gate_arrived: Arc::new(Notify::new()),
            fail_group_head_after_put: Arc::new(Mutex::new(false)),
            fail_next_group_head: Arc::new(Mutex::new(false)),
            group_head_faults: Arc::new(Mutex::new(Vec::new())),
            hold_group_head_after: Arc::new(Mutex::new(None)),
            group_head_arrived: Arc::new(Notify::new()),
            group_head_release: Arc::new(Notify::new()),
            hold_group_catalog: Arc::new(Mutex::new(false)),
            hold_group_drop_id: Arc::new(Mutex::new(None)),
            group_catalog_arrived: Arc::new(Notify::new()),
            group_catalog_release: Arc::new(Notify::new()),
        };
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let ds_url = format!("http://{}", listener.local_addr()?);
        let (ds_stop, stopped) = oneshot::channel();
        let app = Router::new().fallback(observed_ds).with_state(ds.clone());
        let ds_task = tokio::spawn(async move {
            tokio::select! { _ = axum::serve(listener, app) => {}, _ = stopped => {} }
        });
        let mut config = source_config(
            &pg_url,
            &sources_table,
            &version_table,
            &ds_url,
            &root.join("sources").to_string_lossy(),
            "otto-6003-admin",
        );
        // Include the optional counts circuit in the ownership regression.
        config.dbsp.counts = vec![(format!("public.{outer}").parse()?, vec!["body".into()])];
        electric_circuits_engine::config::set_globals(
            &config.instance_id,
            &config.stack_id,
            None,
            config.control_secret.as_deref(),
        );
        Ok(Self {
            client,
            pg_url,
            config,
            sources_table,
            version_table,
            outer,
            inner,
            slots,
            root,
            ds_url,
            ds,
            ds_stop,
            ds_task,
        })
    }

    async fn host(&self) -> Result<SourcesSupervisor> {
        let host = SourcesSupervisor::new(self.config.clone())?;
        host.refresh().await?;
        for source in SOURCES {
            self.ready(&host.router(), source, 1).await?;
        }
        Ok(host)
    }

    async fn ready(&self, app: &Router, source: &str, revision: i64) -> Result<()> {
        let (code, status) = json(app, Method::GET, &format!("/sources/{source}/status"), Body::empty()).await?;
        ensure!(
            code == StatusCode::OK && status["ready"] == true && status["revision"] == revision,
            "source {source} did not finish revision {revision}: {code} {status}"
        );
        Ok(())
    }

    async fn restart(&self, host: &SourcesSupervisor, revision: i64) -> Result<()> {
        self.client
            .execute(
                &format!("UPDATE {} SET revision = $1 WHERE source_id = 'alpha'", self.sources_table),
                &[&revision],
            )
            .await?;
        set_revision(&self.client, &self.version_table, revision).await?;
        // Refresh returns only after the old worker was joined and the replacement boot completed.
        host.refresh().await?;
        self.ready(&host.router(), "alpha", revision).await
    }

    async fn snapshot(&self, app: &Router, prefix: &str, subquery: bool) -> Result<(String, String)> {
        let mut query = url::form_urlencoded::Serializer::new(String::new());
        query
            .append_pair("table", &format!("public.{}", self.outer))
            .append_pair("columns", "id,body")
            .append_pair("offset", "-1");
        if subquery {
            query.append_pair("where", &format!("id IN (SELECT id FROM {})", self.inner));
        }
        let response = app
            .clone()
            .oneshot(axum::http::Request::get(format!("{prefix}/v1/shape?{}", query.finish())).body(Body::empty())?)
            .await?;
        let code = response.status();
        let headers = response.headers().clone();
        let bytes = to_bytes(response.into_body(), 64 * 1024).await?;
        ensure!(code == StatusCode::OK, "snapshot {prefix}: {code}: {}", String::from_utf8_lossy(&bytes));
        let messages: Vec<serde_json::Value> = serde_json::from_slice(&bytes)?;
        let actual: BTreeSet<_> = messages
            .iter()
            .filter_map(|message| message.get("value"))
            .map(|value| {
                (value["id"].as_str().unwrap_or("").to_string(), value["body"].as_str().unwrap_or("").to_string())
            })
            .collect();
        // Independently authored SQL; source rows stay fixed throughout this lifecycle contract.
        let sql = if subquery {
            format!("SELECT o.id::text, o.body FROM {} o JOIN {} i ON o.id = i.id", self.outer, self.inner)
        } else {
            format!("SELECT id::text, body FROM {}", self.outer)
        };
        let expected: BTreeSet<(String, String)> =
            self.client.query(&sql, &[]).await?.iter().map(|row| (row.get(0), row.get(1))).collect();
        ensure!(actual == expected && !actual.is_empty(), "snapshot {prefix}: actual={actual:?}, SQL={expected:?}");
        Ok((
            headers.get("electric-handle").context("missing handle")?.to_str()?.into(),
            headers.get("electric-offset").context("missing offset")?.to_str()?.into(),
        ))
    }

    async fn read_handle(
        &self,
        app: &Router,
        prefix: &str,
        handle: &(String, String),
    ) -> Result<(StatusCode, serde_json::Value)> {
        let query = url::form_urlencoded::Serializer::new(String::new())
            .append_pair("table", &format!("public.{}", self.outer))
            .append_pair("handle", &handle.0)
            .append_pair("offset", &handle.1)
            .finish();
        json(app, Method::GET, &format!("{prefix}/v1/shape?{query}"), Body::empty()).await
    }

    async fn cleanup(self) -> Result<()> {
        let _ = self.ds_stop.send(());
        self.ds_task.await?;
        // The parent cleans all recorded objects after process exit, including partial setup.
        Ok(())
    }
}

async fn stop(host: &SourcesSupervisor) {
    host.shutdown_token().begin();
    host.shutdown_all().await;
}

fn spill_folders(f: &Fixture) -> Result<BTreeSet<PathBuf>> {
    let root = std::env::temp_dir().join("electric-circuits-subq");
    let mut folders = BTreeSet::new();
    for source in SOURCES {
        let path = f.root.join("sources").join(source).join("subq");
        if path.exists() {
            for entry in std::fs::read_dir(path)? {
                let entry = entry?;
                if entry.path().is_dir() {
                    folders.insert(entry.path());
                }
            }
        }
    }
    if !root.exists() {
        return Ok(folders);
    }
    let prefix = format!("{}-", std::process::id());
    let automatic: Result<BTreeSet<_>> = std::fs::read_dir(root)?
        .filter_map(|entry| match entry {
            Ok(entry) if entry.file_name().to_string_lossy().starts_with(&prefix) => Some(Ok(entry.path())),
            Ok(_) => None,
            Err(error) => Some(Err(error.into())),
        })
        .collect();
    folders.extend(automatic?);
    Ok(folders)
}

#[cfg(target_os = "macos")]
fn thread_count() -> Result<usize> {
    let mut info = std::mem::MaybeUninit::<libc::proc_taskinfo>::uninit();
    let size = std::mem::size_of::<libc::proc_taskinfo>();
    // SAFETY: the writable buffer has the exact size/alignment required by PROC_PIDTASKINFO.
    // Only this process is queried; the struct is read only after a complete successful write.
    let written = unsafe {
        libc::proc_pidinfo(
            std::process::id() as libc::pid_t,
            libc::PROC_PIDTASKINFO,
            0,
            info.as_mut_ptr().cast(),
            size as libc::c_int,
        )
    };
    ensure!(written == size as libc::c_int, "proc_pidinfo: {} (returned {written})", std::io::Error::last_os_error());
    // SAFETY: proc_pidinfo reported initialization of the complete proc_taskinfo above.
    let info = unsafe { info.assume_init() };
    Ok(usize::try_from(info.pti_threadnum)?)
}

#[cfg(target_os = "linux")]
fn thread_count() -> Result<usize> {
    Ok(std::fs::read_dir("/proc/self/task")?.collect::<std::io::Result<Vec<_>>>()?.len())
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn thread_count() -> Result<usize> {
    anyhow::bail!("thread-count contract supports macOS and Linux")
}

async fn repeated_restarts(f: &Fixture) -> Result<()> {
    let host = f.host().await?;
    let app = host.router();
    let result = async {
        for source in SOURCES { f.snapshot(&app, &format!("/sources/{source}"), true).await?; }
        // Warm the host's blocking worker used by source-thread joins before measuring it.
        tokio::task::spawn_blocking(|| ()).await?;
        let baseline_threads = thread_count()?;
        let baseline_spill = spill_folders(f)?.len();
        ensure!(baseline_spill == SOURCES.len(), "expected one automatic membership spill directory per live Engine, got {baseline_spill}");
        let mut old_states = Vec::new();
        let mut failures = Vec::new();
        for revision in 2..=RESTARTS + 1 {
            let engine = host.engine_for_test("alpha").await.context("alpha missing before restart")?;
            old_states.push(engine.state_alive_probe_for_test());
            drop(engine);
            f.restart(&host, revision).await?;
            for source in SOURCES { f.snapshot(&app, &format!("/sources/{source}"), true).await?; }
            let alive = old_states.iter().filter(|probe| probe()).count();
            let threads = thread_count()?;
            let folders = spill_folders(f)?.len();
            println!("restart {}: old Engines alive={alive}; threads={threads} (baseline {baseline_threads}); spill folders={folders} (baseline {baseline_spill})", revision - 1);
            if alive != 0 || threads > baseline_threads || folders != baseline_spill {
                failures.push(format!("restart {}: {alive} old Engines alive, threads {threads}/{baseline_threads}, spill folders {folders}/{baseline_spill}", revision - 1));
            }
        }
        // Collect all 50 observations before failing, so one leak cannot shorten the workload.
        ensure!(failures.is_empty(), "50 restarts must release every old Engine and return threads/spill to baseline:\n{}", failures.join("\n"));
        Ok(())
    }.await;
    stop(&host).await;
    result
}

async fn stale_handle(f: &Fixture) -> Result<()> {
    let host = f.host().await?;
    let app = host.router();
    let result = async {
        // A plain shape is deliberately retained across restart. A missing subquery stream must
        // not accidentally make a process-global stale handle look correctly rejected.
        let old = f.snapshot(&app, "/sources/alpha", false).await?;
        f.restart(&host, 2).await?;
        let (code, body) = f.read_handle(&app, "/sources/alpha", &old).await?;
        ensure!(
            code == StatusCode::CONFLICT && body[0]["headers"]["control"] == "must-refetch",
            "replacement must refuse the old Engine's handle with 409 must-refetch, got {code}: {body}"
        );
        Ok(())
    }
    .await;
    stop(&host).await;
    result
}

async fn foreign_handle(f: &Fixture) -> Result<()> {
    let host = f.host().await?;
    let app = host.router();
    let result = async {
        let alpha = f.snapshot(&app, "/sources/alpha", false).await?;
        let beta = f.snapshot(&app, "/sources/beta", false).await?;
        for (prefix, handle) in [("/sources/alpha", &alpha), ("/sources/beta", &beta)] {
            let (code, body) = f.read_handle(&app, prefix, handle).await?;
            ensure!(code == StatusCode::OK, "own handle must work on {prefix}: {code} {body}");
        }
        let (code, body) = f.read_handle(&app, "/sources/beta", &alpha).await?;
        ensure!(
            code == StatusCode::CONFLICT && body[0]["headers"]["control"] == "must-refetch",
            "source beta must refuse source alpha's handle with 409 must-refetch, got {code}: {body}"
        );
        Ok(())
    }
    .await;
    stop(&host).await;
    result
}

async fn reader_snapshots(f: &Fixture) -> Result<()> {
    let host = f.host().await?;
    let app = host.router();
    let result = async {
        let initial = host.engine_for_test("alpha").await.context("alpha missing")?;
        let baseline = electric_circuits_engine::electric::registry_usage_for_test(&initial).await;
        ensure!(baseline == (0, 0), "fresh Engine registry must be empty: {baseline:?}");
        drop(initial);
        let mut stopped = Vec::new();
        let mut inherited_subscriptions = 0;
        for revision in 1..=3 {
            if revision == 3 {
                // Close preserves durable leases. With leases disabled in this fixture, measure
                // the restored baseline separately from handles minted by this live Engine.
                let live = host.engine_for_test("alpha").await.context("alpha missing")?;
                for shape in live.graph().await.shapes {
                    inherited_subscriptions += live.subscription_count(&shape.id).await;
                }
            }
            for _ in 0..16 {
                // The reader uses only the snapshot body and never resumes the returned handle.
                f.snapshot(&app, "/sources/alpha", false).await?;
            }
            if revision < 3 {
                let engine = host.engine_for_test("alpha").await.context("alpha missing")?;
                stopped.push(engine.state_alive_probe_for_test());
                drop(engine);
                f.restart(&host, revision + 1).await?;
            }
        }
        let live = host.engine_for_test("alpha").await.context("alpha replacement missing")?;
        let graph = live.graph().await;
        ensure!(graph.shapes.len() == 1, "reader snapshots should share one plain shape");
        let eviction = await_subscription_count(f, &app, &[("/sources/alpha".into(), graph.shapes[0].id.clone())], inherited_subscriptions).await;
        let usage = electric_circuits_engine::electric::registry_usage_for_test(&live).await;
        let alive = stopped.iter().filter(|probe| probe()).count();
        println!("reader snapshots: registry count/heap={usage:?}, baseline={baseline:?}, stopped Engines alive={alive}");
        ensure!(usage == baseline && alive == 0,
            "snapshot-only readers must return handle count and heap to baseline after TTL and drop stopped Engines: registry={usage:?}, baseline={baseline:?}, stopped alive={alive}; eviction={eviction:?}");
        eviction?;
        Ok(())
    }.await;
    let close_check = async {
        result?;
        for _ in 0..8 {
            f.snapshot(&app, "/sources/alpha", false).await?;
        }
        let lefts = || async {
            f.ds.store.streams.lock().await.values().flatten().filter(|event| event["t"] == "left").count()
        };
        let before = lefts().await;
        stop(&host).await;
        ensure!(lefts().await == before, "Engine close appended Left for live snapshot handles");
        Ok::<(), anyhow::Error>(())
    }
    .await;
    stop(&host).await;
    close_check
}

// Observe catalog writes to wait for releases without touching/renewing an idle Electric handle.
// The deadline diagnoses a missing eviction; event notification, not a sleep, drives observation.
async fn await_idle_release(f: &Fixture, app: &Router, shapes: &[(String, String)]) -> Result<()> {
    await_subscription_count(f, app, shapes, 0).await
}

async fn await_subscription_count(
    f: &Fixture,
    app: &Router,
    shapes: &[(String, String)],
    expected: usize,
) -> Result<()> {
    let mut changes = f.ds.changed.subscribe();
    let outcome = tokio::time::timeout(Duration::from_secs(75), async {
        loop {
            changes.borrow_and_update();
            let mut pending = Vec::new();
            for (prefix, id) in shapes {
                let (code, shape) = json(app, Method::GET, &format!("{prefix}/shapes/{id}"), Body::empty()).await?;
                ensure!(code == StatusCode::OK, "idle shape unexpectedly disappeared: {code} {shape}");
                if shape["subscriptions"] != expected {
                    pending.push((prefix.clone(), shape["subscriptions"].clone()));
                }
            }
            if pending.is_empty() {
                return Ok::<_, anyhow::Error>(());
            }
            changes.changed().await.context("DS observation channel closed")?;
        }
    })
    .await;
    if let Ok(result) = outcome {
        return result;
    }
    let mut remaining = Vec::new();
    for (prefix, id) in shapes {
        let (_, shape) = json(app, Method::GET, &format!("{prefix}/shapes/{id}"), Body::empty()).await?;
        if shape["subscriptions"] != expected {
            remaining.push(format!("{prefix}: subscriptions={}", shape["subscriptions"]));
        }
    }
    ensure!(
        remaining.is_empty(),
        "idle handles still own subscriptions after TTL=1s and a complete 60s sweep: {}",
        remaining.join(", ")
    );
    Ok(())
}

async fn every_live_engine_evicts(f: &Fixture) -> Result<()> {
    let host = f.host().await?;
    let app = host.router();
    let result = async {
        // Start the current process-static evictor on alpha before alpha is stopped.
        f.snapshot(&app, "/sources/alpha", true).await?;
        f.restart(&host, 2).await?;
        let mut handles = Vec::new();
        let mut shapes = Vec::new();
        for source in SOURCES {
            let prefix = format!("/sources/{source}");
            let handle = f.snapshot(&app, &prefix, false).await?;
            let engine = host.engine_for_test(source).await.context("live source missing")?;
            let graph = engine.graph().await;
            let plain: Vec<_> = graph.shapes.iter().filter(|shape| !shape.is_subquery).collect();
            ensure!(plain.len() == 1, "expected one plain shape on {source}");
            shapes.push((prefix.clone(), plain[0].id.clone()));
            handles.push((prefix, handle));
        }
        await_idle_release(f, &app, &shapes).await?;
        for (prefix, handle) in handles {
            let (code, body) = f.read_handle(&app, &prefix, &handle).await?;
            ensure!(
                code == StatusCode::CONFLICT && body[0]["headers"]["control"] == "must-refetch",
                "idle handle on {prefix} must be evicted, got {code}: {body}"
            );
        }
        Ok(())
    }
    .await;
    stop(&host).await;
    result
}

async fn single_engine_controls(f: &Fixture) -> Result<()> {
    let ds = DsClient::new_for_in_process_test(&f.ds_url);
    let engine = Engine::new_pg_for_in_process_test_with_setup(ds, f.pg_url.clone(), PostgresSetup::ExternallyManaged);
    let tables =
        [TableSelector::parse(&format!("public.{}", f.outer))?, TableSelector::parse(&format!("public.{}", f.inner))?];
    engine.setup_postgres(&tables, &f.slots[0]).await?;
    let app = electric_circuits_engine::http::router_with_introspection(engine.clone(), false);
    let result = async {
        let handle = f.snapshot(&app, "", false).await?;
        let graph = engine.graph().await;
        ensure!(graph.shapes.len() == 1, "single Engine should have one plain shape");
        await_idle_release(f, &app, &[(String::new(), graph.shapes[0].id.clone())]).await?;
        let (code, body) = f.read_handle(&app, "", &handle).await?;
        ensure!(
            code == StatusCode::CONFLICT && body[0]["headers"]["control"] == "must-refetch",
            "single Engine must still evict idle handles: {code} {body}"
        );

        f.snapshot(&app, "", true).await?;
        let graph = engine.graph().await;
        let subquery = graph.shapes.iter().find(|shape| shape.is_subquery).context("subquery missing")?;
        let path = engine.get_shape(&subquery.id).await.context("subquery record missing")?.stream_path;
        let mut changes = f.ds.changed.subscribe();
        engine.force_degraded();
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                changes.borrow_and_update();
                let retired = f.ds.retired.lock().await;
                let close = retired.iter().position(|(method, p)| *method == Method::POST && p.ends_with(&path));
                let delete = retired.iter().position(|(method, p)| *method == Method::DELETE && p.ends_with(&path));
                if let (Some(close), Some(delete)) = (close, delete) {
                    ensure!(close < delete, "degrade reaper must close before deleting");
                    return Ok::<_, anyhow::Error>(());
                }
                drop(retired);
                changes.changed().await?;
            }
        })
        .await
        .context("degrade reaper did not retire its subquery stream")??;
        ensure!(engine.get_shape(&subquery.id).await.is_some(), "degradation must retain the shape record");
        Ok(())
    }
    .await;
    ensure!(
        engine.close(Duration::from_secs(25)).await == electric_circuits_engine::shutdown::ShutdownOutcome::Complete,
        "single Engine did not close cleanly"
    );
    result
}

fn group_body(f: &Fixture, subscription: &str, plain_only: bool) -> serde_json::Value {
    let outer = format!("public.{}", f.outer);
    let inner = format!("public.{}", f.inner);
    let mut members = vec![serde_json::json!({
        "key": "plain",
        "table": outer,
        "columns": ["id", "body"]
    })];
    if plain_only {
        members.push(serde_json::json!({
            "key": "inner",
            "table": inner,
            "columns": ["id"]
        }));
    } else {
        members.push(serde_json::json!({
            "key": "subquery",
            "table": inner,
            "columns": ["id"],
            "where": {
                "col": "id",
                "in": { "table": outer, "project": "id" }
            }
        }));
    }
    serde_json::json!({ "subscription": subscription, "members": members })
}

async fn post_group(app: &Router, body: serde_json::Value) -> Result<(StatusCode, serde_json::Value)> {
    let (status, _, body) = post_group_response(app, body).await?;
    Ok((status, body))
}

async fn post_group_response(
    app: &Router,
    body: serde_json::Value,
) -> Result<(StatusCode, axum::http::HeaderMap, serde_json::Value)> {
    let response = app
        .clone()
        .oneshot(
            axum::http::Request::post("/v1/subsets/load")
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))?,
        )
        .await
        .map_err(|error| anyhow::anyhow!(error.to_string()))?;
    let status = response.status();
    let headers = response.headers().clone();
    let bytes = to_bytes(response.into_body(), 64 * 1024).await?;
    let body = serde_json::from_slice(&bytes)
        .with_context(|| format!("group response {status}: {}", String::from_utf8_lossy(&bytes)))?;
    Ok((status, headers, body))
}

async fn group_engine(f: &Fixture) -> Result<(Engine, Router)> {
    let engine = Engine::new_pg_for_in_process_test_with_setup(
        DsClient::new_for_in_process_test(&f.ds_url),
        f.pg_url.clone(),
        PostgresSetup::ExternallyManaged,
    );
    let tables =
        [TableSelector::parse(&format!("public.{}", f.outer))?, TableSelector::parse(&format!("public.{}", f.inner))?];
    engine.setup_postgres(&tables, &f.slots[0]).await?;
    let app = router_with_introspection(engine.clone(), false);
    Ok((engine, app))
}

async fn stream_events(f: &Fixture, stream_path: &str) -> Vec<serde_json::Value> {
    let streams = f.ds.store.streams.lock().await;
    streams.iter().find(|(path, _)| path.ends_with(stream_path)).map(|(_, events)| events.clone()).unwrap_or_default()
}

async fn wait_group_events(f: &Fixture, stream_path: &str, minimum: usize) -> Result<Vec<serde_json::Value>> {
    let mut changed = f.ds.changed.subscribe();
    let result = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let events = stream_events(f, stream_path).await;
            if events.len() >= minimum {
                return Ok(events);
            }
            changed.changed().await.context("group stream observation channel closed")?;
        }
    })
    .await;
    match result {
        Ok(result) => result,
        Err(_) => {
            Err(anyhow::anyhow!("waiting for group stream events timed out: {:?}", stream_events(f, stream_path).await))
        }
    }
}

async fn wait_group_stream_retired(f: &Fixture, stream_path: &str) -> Result<()> {
    let mut changed = f.ds.changed.subscribe();
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            if f.ds
                .retired
                .lock()
                .await
                .iter()
                .any(|(method, path)| *method == Method::POST && path.ends_with(stream_path))
            {
                return Ok::<_, anyhow::Error>(());
            }
            changed.changed().await.context("group retirement observation channel closed")?;
        }
    })
    .await
    .context("waiting for group stream retirement")??;
    Ok(())
}

async fn wait_group_stream_closed(f: &Fixture, stream_path: &str) -> Result<()> {
    let mut changed = f.ds.changed.subscribe();
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            if f.ds.closed.lock().await.iter().any(|path| path.ends_with(stream_path)) {
                return Ok::<_, anyhow::Error>(());
            }
            changed.changed().await.context("group close observation channel closed")?;
        }
    })
    .await
    .context("waiting for group stream close")??;
    Ok(())
}

async fn pause_group_delete(f: &Fixture, stream_path: &str) -> Arc<Notify> {
    let gate = Arc::new(Notify::new());
    f.ds.delete_gates.lock().await.insert(stream_path.to_string(), gate.clone());
    gate
}

/// Open a delete gate for every DELETE it holds. Each member purge and the group release retire
/// the same group stream, so several DELETEs can be waiting; `notify_one` alone would leave all
/// but one parked. The extra permit covers a DELETE that took the gate but has not yet waited.
async fn release_group_delete(f: &Fixture, stream_path: &str, gate: Arc<Notify>) {
    f.ds.delete_gates.lock().await.remove(stream_path);
    gate.notify_waiters();
    gate.notify_one();
}

async fn install_source_fence(f: &Fixture) -> Result<()> {
    f.client
        .batch_execute(&format!(
            "DROP TABLE IF EXISTS public.circuits_source_fence; \
             CREATE TABLE public.circuits_source_fence (source_commit_id uuid PRIMARY KEY); \
             ALTER PUBLICATION \"{}_pub\" ADD TABLE public.circuits_source_fence",
            f.slots[0]
        ))
        .await?;
    Ok(())
}

async fn remove_source_fence(f: &Fixture) {
    let _ = f
        .client
        .batch_execute(&format!(
            "ALTER PUBLICATION \"{}_pub\" DROP TABLE IF EXISTS public.circuits_source_fence; DROP TABLE IF EXISTS public.circuits_source_fence",
            f.slots[0]
        ))
        .await;
}

async fn wait_source_receipt(engine: &Engine, f: &Fixture, commit: uuid::Uuid) -> Result<()> {
    let mut changed = f.ds.changed.subscribe();
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            if engine.source_drain_receipt(&commit.to_string()).is_some() {
                return Ok::<_, anyhow::Error>(());
            }
            changed.changed().await.context("source receipt observation channel closed")?;
        }
    })
    .await
    .context("waiting for source drain receipt")??;
    Ok(())
}

fn event_key(event: &serde_json::Value) -> Option<String> {
    event
        .get("key")
        .and_then(|key| key.as_str())
        .map(ToOwned::to_owned)
        .or_else(|| event.get("value").and_then(|value| value.get("id")).map(|id| id.to_string()))
}

fn event_operation(event: &serde_json::Value) -> Option<&str> {
    event.get("headers").and_then(|headers| headers.get("operation")).and_then(|op| op.as_str())
}

async fn group_delayed_subquery_flip(f: &Fixture) -> Result<()> {
    install_source_fence(f).await?;
    let (engine, app) = group_engine(f).await?;
    let result = async {
        let (status, body) = post_group(&app, group_body(f, "group-delayed", false)).await?;
        ensure!(status == StatusCode::OK, "group create failed: {status} {body}");
        let stream_path = body["streamPath"].as_str().context("group stream path")?.to_owned();
        let baseline = stream_events(f, &stream_path).await.len();
        let t = uuid::Uuid::new_v4();
        f.client
            .batch_execute(&format!("BEGIN; INSERT INTO public.{} (id) VALUES (2); INSERT INTO public.circuits_source_fence VALUES ('{}'); COMMIT", f.inner, t))
            .await?;
        let t1 = uuid::Uuid::new_v4();
        f.client
            .batch_execute(&format!("BEGIN; UPDATE public.{} SET body = 't+1' WHERE id = 2; INSERT INTO public.circuits_source_fence VALUES ('{}'); COMMIT", f.outer, t1))
            .await?;
        wait_source_receipt(&engine, f, t1).await?;
        let events = wait_group_events(f, &stream_path, baseline + 2).await?;
        let flips: Vec<_> = events
            .iter()
            .skip(baseline)
            .filter(|event| event_key(event).as_deref() == Some("2") && event_operation(event) == Some("upsert"))
            .collect();
        ensure!(flips.len() >= 2, "missing T and T+1 member outputs: {events:?}");
        let first_txid: u64 = flips[0]["headers"]["txid"].as_str().unwrap_or_default().parse().unwrap_or(0);
        let last_txid: u64 = flips.last().and_then(|event| event["headers"]["txid"].as_str()).unwrap_or_default().parse().unwrap_or(0);
        ensure!(first_txid > 0 && first_txid < last_txid, "T+1 output crossed the T fence: {events:?}");
        Ok::<_, anyhow::Error>(())
    }
    .await;
    engine.close(Duration::from_secs(10)).await;
    remove_source_fence(f).await;
    result
}

async fn group_source_drain_receipt(f: &Fixture) -> Result<()> {
    install_source_fence(f).await?;
    let (engine, app) = group_engine(f).await?;
    let result = async {
        let (status, body) = post_group(&app, group_body(f, "group-receipt", false)).await?;
        ensure!(status == StatusCode::OK, "group create failed: {status} {body}");
        let stream_path = body["streamPath"].as_str().context("group stream path")?.to_owned();
        let baseline = stream_events(f, &stream_path).await.len();
        let commit = uuid::Uuid::new_v4();
        f.client
            .batch_execute(&format!(
                "BEGIN; INSERT INTO public.{} (id) VALUES (2); INSERT INTO public.circuits_source_fence VALUES ('{}'); COMMIT",
                f.inner, commit
            ))
            .await?;
        wait_source_receipt(&engine, f, commit).await?;
        let receipt = engine.source_drain_receipt(&commit.to_string()).context("source receipt disappeared")?;
        ensure!(receipt.source_commit_id == commit.to_string(), "wrong source receipt: {receipt:?}");
        let events = wait_group_events(f, &stream_path, baseline + 1).await?;
        ensure!(events.iter().skip(baseline).any(|event| event_key(event).as_deref() == Some("2")), "group lane did not emit before receipt: {events:?}");
        Ok::<_, anyhow::Error>(())
    }
    .await;
    engine.close(Duration::from_secs(10)).await;
    remove_source_fence(f).await;
    result
}

async fn group_equal_key_delete(f: &Fixture) -> Result<()> {
    install_source_fence(f).await?;
    let (engine, app) = group_engine(f).await?;
    let result = async {
        let (status, body) = post_group(&app, group_body(f, "group-delete", true)).await?;
        ensure!(status == StatusCode::OK, "group create failed: {status} {body}");
        let stream_path = body["streamPath"].as_str().context("group stream path")?.to_owned();
        let baseline = stream_events(f, &stream_path).await.len();
        let commit = uuid::Uuid::new_v4();
        f.client
            .batch_execute(&format!(
                "BEGIN; DELETE FROM public.{} WHERE id = 1; DELETE FROM public.{} WHERE id = 1; INSERT INTO public.circuits_source_fence VALUES ('{}'); COMMIT",
                f.outer, f.inner, commit
            ))
            .await?;
        wait_source_receipt(&engine, f, commit).await?;
        let events = wait_group_events(f, &stream_path, baseline + 2).await?;
        let deletes = events
            .iter()
            .skip(baseline)
            .filter(|event| event_key(event).as_deref() == Some("1") && event_operation(event) == Some("delete"))
            .count();
        ensure!(deletes >= 2, "equal-key delete lost a member-table delete: {events:?}");
        let delete_types: std::collections::BTreeSet<_> = events
            .iter()
            .skip(baseline)
            .filter(|event| event_key(event).as_deref() == Some("1") && event_operation(event) == Some("delete"))
            .filter_map(|event| event["type"].as_str())
            .collect();
        ensure!(delete_types.len() >= 2, "equal-key deletes did not retain table identity: {events:?}");
        Ok::<_, anyhow::Error>(())
    }
    .await;
    engine.close(Duration::from_secs(10)).await;
    remove_source_fence(f).await;
    result
}

async fn group_claim_overlap_is_atomic(f: &Fixture) -> Result<()> {
    let (engine, app) = group_engine(f).await?;
    let result = async {
        // Hold the first request immediately after its atomic reservation. The second request
        // must not create either member while that reservation is in flight; after the winner
        // completes, the same request (with different members) is the ordinary 409 conflict.
        let gate = engine.group_load_gate_for_test();
        gate.arm(GroupLoadAwait::AfterReservation).await;
        let first_app = app.clone();
        let first_body = group_body(f, "group-overlap", false);
        let first = tokio::spawn(async move { post_group(&first_app, first_body).await });
        tokio::time::timeout(Duration::from_secs(10), gate.arrived.notified())
            .await
            .context("first group did not reach its reservation gate")?;
        let second_app = app.clone();
        let second_body = group_body(f, "group-overlap", true);
        let loser_body = second_body.clone();
        gate.arm(GroupLoadAwait::BeforeReservation).await;
        let second = tokio::spawn(async move { post_group_response(&second_app, loser_body).await });
        tokio::time::timeout(Duration::from_secs(10), gate.arrived.notified())
            .await
            .context("losing group did not reach its reservation gate")?;
        let winner_id = engine.group_id_for_test("group-overlap").await.context("winner reservation id")?;
        gate.release.notify_last();
        let (status, headers, in_flight_body) = tokio::time::timeout(Duration::from_secs(10), second)
            .await
            .context("losing group did not leave its reservation gate")?
            .context("second group task failed")??;
        ensure!(
            status == StatusCode::SERVICE_UNAVAILABLE,
            "in-flight loser was not retryable: {status} {in_flight_body}"
        );
        ensure!(headers.get("retry-after").is_some(), "in-flight loser omitted Retry-After");
        ensure!(
            engine.group_id_for_test("group-overlap").await.as_deref() == Some(winner_id.as_str())
                && engine.subscription_owner("group-overlap").await.is_none()
                && engine.subscription_owner("group-overlap:member:1").await.is_none(),
            "in-flight loser left a claim or replaced the reservation"
        );
        ensure!(
            !stream_events(f, "meta/catalog")
                .await
                .iter()
                .any(|event| event["subscription"].as_str().is_some_and(|s| s.starts_with("group-overlap"))),
            "in-flight loser wrote a catalog event"
        );
        gate.release.notify_one();
        let (status, first_body) = tokio::time::timeout(Duration::from_secs(15), first)
            .await
            .context("winning group did not finish")?
            .context("first group task failed")??;
        ensure!(status == StatusCode::OK, "first group create failed: {status} {first_body}");
        let mut changed = f.ds.changed.subscribe();
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let events = stream_events(f, "meta/catalog").await;
                let joined = events
                    .iter()
                    .filter(|event| {
                        event["t"] == "joined"
                            && event["subscription"].as_str().is_some_and(|s| s.starts_with("group-overlap"))
                    })
                    .count();
                if joined >= 2 {
                    return Ok::<_, anyhow::Error>(());
                }
                changed.changed().await.context("catalog observation channel closed")?;
            }
        })
        .await
        .context("winner Joined events did not become durable")??;
        let (status, second_body) = post_group(&app, second_body.clone()).await?;
        ensure!(status == StatusCode::CONFLICT, "overlapping group claim was not 409: {status} {second_body}");
        ensure!(!engine.group_reservation_for_test("group-overlap").await, "loser left a group reservation");
        ensure!(engine.group_id_for_test("group-overlap").await.is_some(), "winner group was lost");
        let catalog = stream_events(f, "meta/catalog").await;
        ensure!(
            catalog.iter().all(|event| {
                if event["t"] == "groupCreated" {
                    return event["group"]["subscription"] == "group-overlap"
                        && event["group"]["id"] == first_body["groupId"];
                }
                match event["subscription"].as_str() {
                    Some(subscription) if subscription.starts_with("group-overlap") => match event["t"].as_str() {
                        Some("created") => first_body["results"].as_array().is_some_and(|members| {
                            members.iter().any(|member| member["shapeId"] == event["rec"]["id"])
                        }),
                        Some("joined") | Some("left") => first_body["results"]
                            .as_array()
                            .is_some_and(|members| members.iter().any(|member| member["shapeId"] == event["id"])),
                        _ => false,
                    },
                    _ => true,
                }
            }),
            "losing load wrote an unexpected catalog event: {catalog:?}"
        );
        ensure!(
            catalog
                .iter()
                .filter(|event| event["t"] == "groupCreated" && event["group"]["subscription"] == "group-overlap")
                .count()
                == 1,
            "loser wrote a group catalog event: {catalog:?}"
        );
        Ok::<_, anyhow::Error>(())
    }
    .await;
    engine.close(Duration::from_secs(10)).await;
    result
}

async fn group_uses_one_snapshot_fence(f: &Fixture) -> Result<()> {
    let (engine, app) = group_engine(f).await?;
    let result = async {
        let (status, body) = post_group(&app, group_body(f, "group-fence", false)).await?;
        ensure!(status == StatusCode::OK, "group create failed: {status} {body}");
        ensure!(
            body["lsn"].is_string()
                && body["snapshot"].is_string()
                && body["horizon"].is_string()
                && body["offset"].is_string(),
            "group fence is incomplete: {body}"
        );
        ensure!(
            body["results"].as_array().is_some_and(|results| results.len() == 2),
            "group did not return both member pages: {body}"
        );
        let sql_count: i64 = f.client.query_one(&format!("SELECT count(*) FROM public.{}", f.outer), &[]).await?.get(0);
        let outer = format!("public.{}", f.outer);
        let rows = body["rows"].as_array().context("group answer has no flat rows")?;
        ensure!(rows.iter().all(|row| row["table"].is_string()), "a group row is not tagged with its table: {body}");
        ensure!(
            rows.iter().filter(|row| row["table"] == outer.as_str()).count() as i64 == sql_count,
            "plain page crossed snapshot fence: {body}"
        );
        Ok::<_, anyhow::Error>(())
    }
    .await;
    engine.close(Duration::from_secs(10)).await;
    result
}

async fn group_retirement_recreates_after_close(f: &Fixture) -> Result<()> {
    let (engine, app) = group_engine(f).await?;
    let result = async {
        let (status, body) = post_group(&app, group_body(f, "group-retire", false)).await?;
        ensure!(status == StatusCode::OK, "group create failed: {status} {body}");
        let stream_path = body["streamPath"].as_str().context("group stream path")?.to_owned();
        f.client.batch_execute(&format!("TRUNCATE public.{}", f.inner)).await?;
        let mut changed = f.ds.changed.subscribe();
        tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                if f.ds
                    .retired
                    .lock()
                    .await
                    .iter()
                    .any(|(method, path)| *method == Method::POST && path.ends_with(&stream_path))
                {
                    return Ok::<_, anyhow::Error>(());
                }
                changed.changed().await.context("retirement observation channel closed")?;
            }
        })
        .await
        .context("waiting for group stream retirement")??;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
        let (status, fresh) = loop {
            let (status, body) = post_group(&app, group_body(f, "group-retire", false)).await?;
            if status == StatusCode::OK {
                break (status, body);
            }
            ensure!(
                status == StatusCode::INTERNAL_SERVER_ERROR
                    && body["error"].as_str().is_some_and(|message| message.contains("being resolved")),
                "a fresh group must reload after retirement: {status} {body}"
            );
            ensure!(tokio::time::Instant::now() < deadline, "schema resolution did not settle before reload");
            tokio::time::sleep(Duration::from_millis(100)).await;
        };
        ensure!(status == StatusCode::OK, "a fresh group must reload after retirement: {status} {fresh}");
        ensure!(
            fresh["streamPath"].as_str().is_some_and(|path| path != stream_path),
            "a fresh group must not reuse retired stream path: {fresh}"
        );
        Ok::<_, anyhow::Error>(())
    }
    .await;
    engine.close(Duration::from_secs(10)).await;
    result
}

async fn group_restore_drops_expired_group_before_first_sweep(f: &Fixture) -> Result<()> {
    let (engine, app) = group_engine(f).await?;
    let result = async {
        let (status, first) = post_group(&app, group_body(f, "group-restore-expired", true)).await?;
        ensure!(status == StatusCode::OK, "initial group create failed: {status} {first}");
        let old_path = first["streamPath"].as_str().context("expired group stream path")?.to_owned();
        engine.close(Duration::from_secs(10)).await;

        // The lease is five seconds in this isolated contract. The replacement starts after the
        // persisted claim is definitely stale, before its first retention sweep can run.
        tokio::time::sleep(Duration::from_secs(6)).await;
        let (restarted, restarted_app) = group_engine(f).await?;
        let (status, fresh) = post_group(&restarted_app, group_body(f, "group-restore-expired", true)).await?;
        ensure!(status == StatusCode::OK, "expired group was not recreated on restart: {status} {fresh}");
        ensure!(fresh["streamPath"].as_str() != Some(old_path.as_str()), "expired group reused its old stream");
        restarted.close(Duration::from_secs(10)).await;
        Ok::<_, anyhow::Error>(())
    }
    .await;
    result
}

async fn group_closed_reload_waits_for_release(f: &Fixture) -> Result<()> {
    let (engine, app) = group_engine(f).await?;
    let result = async {
        let (status, first) = post_group(&app, group_body(f, "group-closed-reload", true)).await?;
        ensure!(status == StatusCode::OK, "initial group create failed: {status} {first}");
        let old_group_id = first["groupId"].as_str().context("closed group id")?.to_owned();
        let old_path = first["streamPath"].as_str().context("closed group stream path")?.to_owned();
        let first_shape = first["results"][0]["shapeId"].as_str().context("first group member")?.to_owned();
        let gate = pause_group_delete(f, &old_path).await;
        let release_app = app.clone();
        let release_task = tokio::spawn(async move {
            json(
                &release_app,
                Method::DELETE,
                &format!("/v1/shapes/{first_shape}?subscription=group-closed-reload"),
                Body::empty(),
            )
            .await
        });
        wait_group_stream_closed(f, &old_path).await?;

        *f.ds.hold_group_drop_id.lock().await = Some(old_group_id);
        let reload_app = app.clone();
        let reload_body = group_body(f, "group-closed-reload", true);
        let mut reload_task = tokio::spawn(async move { post_group(&reload_app, reload_body).await });
        release_group_delete(f, &old_path, gate).await;
        tokio::time::timeout(Duration::from_secs(10), f.ds.group_catalog_arrived.notified())
            .await
            .context("group release did not reach GroupDropped durability")?;
        let public_body = serde_json::json!({
            "table": format!("public.{}", f.outer),
            "changesOnly": true,
            "subscription": "group-closed-reload",
        });
        let (status, conflict) = json(&app, Method::POST, "/v1/shapes", Body::from(public_body.to_string())).await?;
        ensure!(
            status == StatusCode::CONFLICT,
            "plain create claimed a group subscription during release: {status} {conflict}"
        );
        let aggregate_body = serde_json::json!({
            "table": format!("public.{}", f.outer),
            "fn": "count",
            "subscription": "group-closed-reload",
        });
        let (status, conflict) =
            json(&app, Method::POST, "/v1/aggregates", Body::from(aggregate_body.to_string())).await?;
        ensure!(
            status == StatusCode::CONFLICT,
            "aggregate claimed a group subscription during release: {status} {conflict}"
        );
        ensure!(
            tokio::time::timeout(Duration::from_millis(250), &mut reload_task).await.is_err(),
            "same-members load renewed a group while GroupDropped was still in flight"
        );
        f.ds.group_catalog_release.notify_one();
        let (release_status, release_body) = tokio::time::timeout(Duration::from_secs(15), release_task)
            .await
            .context("group release did not finish after stream retirement was released")?
            .context("group release task failed")??;
        ensure!(release_status == StatusCode::OK, "group release failed: {release_status} {release_body}");
        let (reload_status, fresh) = tokio::time::timeout(Duration::from_secs(15), reload_task)
            .await
            .context("same-members reload did not finish after release")?
            .context("same-members reload task failed")??;
        ensure!(reload_status == StatusCode::OK, "same-members reload failed: {reload_status} {fresh}");
        let new_path = fresh["streamPath"].as_str().context("fresh group stream path")?;
        ensure!(new_path != old_path, "closed group reload reused its old stream");
        f.client
            .batch_execute(&format!("INSERT INTO public.{} (id, body) VALUES (6, 'closed-reload')", f.outer))
            .await?;
        let events = wait_group_events(f, new_path, 1).await?;
        ensure!(
            events.iter().any(|event| event_key(event).as_deref() == Some("6")),
            "reloaded group missed live output: {events:?}"
        );
        Ok::<_, anyhow::Error>(())
    }
    .await;
    engine.close(Duration::from_secs(10)).await;
    result
}

async fn group_release_reloads_same_subscription(f: &Fixture) -> Result<()> {
    let (engine, app) = group_engine(f).await?;
    let result = async {
        let (status, first) = post_group(&app, group_body(f, "group-release-reload", true)).await?;
        ensure!(status == StatusCode::OK, "group create failed: {status} {first}");
        let old_path = first["streamPath"].as_str().context("old group stream path")?.to_owned();
        let old_shapes: Vec<String> = first["results"]
            .as_array()
            .context("group results")?
            .iter()
            .map(|member| member["shapeId"].as_str().map(ToOwned::to_owned).context("member shape id"))
            .collect::<Result<_>>()?;
        let first_shape = old_shapes.first().context("group has no members")?;
        let (release_status, release) = json(
            &app,
            Method::DELETE,
            &format!("/v1/shapes/{first_shape}?subscription=group-release-reload"),
            Body::empty(),
        )
        .await?;
        ensure!(release_status == StatusCode::OK, "group release failed: {release_status} {release}");
        wait_group_stream_retired(f, &old_path).await?;
        for shape_id in &old_shapes {
            let (status, _) = json(&app, Method::GET, &format!("/v1/shapes/{shape_id}"), Body::empty()).await?;
            ensure!(status == StatusCode::NOT_FOUND, "released member {shape_id} still exists: {status}");
        }
        let (reload_status, second) = post_group(&app, group_body(f, "group-release-reload", true)).await?;
        ensure!(reload_status == StatusCode::OK, "same-subscription reload failed: {reload_status} {second}");
        let new_path = second["streamPath"].as_str().context("new group stream path")?;
        ensure!(new_path != old_path, "reload reused released group stream {old_path}");
        let new_shapes: Vec<String> = second["results"]
            .as_array()
            .context("reloaded group results")?
            .iter()
            .map(|member| member["shapeId"].as_str().map(ToOwned::to_owned).context("reloaded member shape id"))
            .collect::<Result<_>>()?;
        ensure!(new_shapes.iter().all(|id| !old_shapes.contains(id)), "reload reused released member shapes");
        let (stale_status, stale_body) = json(
            &app,
            Method::DELETE,
            &format!("/v1/shapes/{}?subscription=group-release-reload", old_shapes[0]),
            Body::empty(),
        )
        .await?;
        ensure!(stale_status == StatusCode::OK, "stale member release was not idempotent: {stale_status} {stale_body}");
        f.client.batch_execute(&format!("INSERT INTO public.{} (id, body) VALUES (3, 'reload')", f.outer)).await?;
        let events = wait_group_events(f, new_path, 1).await?;
        ensure!(
            events.iter().any(|event| event_key(event).as_deref() == Some("3")),
            "new group missed live output: {events:?}"
        );
        Ok::<_, anyhow::Error>(())
    }
    .await;
    engine.close(Duration::from_secs(10)).await;
    result
}

async fn group_lapse_reloads_same_subscription(f: &Fixture) -> Result<()> {
    let (engine, app) = group_engine(f).await?;
    let result = async {
        let (status, first) = post_group(&app, group_body(f, "group-lapse-reload", true)).await?;
        ensure!(status == StatusCode::OK, "group create failed: {status} {first}");
        let old_path = first["streamPath"].as_str().context("old group stream path")?.to_owned();
        tokio::time::sleep(Duration::from_secs(6)).await;
        wait_group_stream_retired(f, &old_path).await?;
        let (reload_status, second) = post_group(&app, group_body(f, "group-lapse-reload", true)).await?;
        ensure!(reload_status == StatusCode::OK, "lapsed group did not reload: {reload_status} {second}");
        let new_path = second["streamPath"].as_str().context("new group stream path")?;
        ensure!(new_path != old_path, "lapsed group reused old stream {old_path}");
        let (renew_status, renewed) = post_group(&app, group_body(f, "group-lapse-reload", true)).await?;
        ensure!(
            renew_status == StatusCode::OK,
            "reloaded lapsed group could not renew before its live wait: {renew_status} {renewed}"
        );
        ensure!(renewed["streamPath"].as_str() == Some(new_path), "renewal changed the reloaded group stream");
        f.client.batch_execute(&format!("INSERT INTO public.{} (id, body) VALUES (4, 'lapse')", f.outer)).await?;
        let events = wait_group_events(f, new_path, 1).await?;
        ensure!(
            events.iter().any(|event| event_key(event).as_deref() == Some("4")),
            "reloaded lapsed group missed live output: {events:?}"
        );
        Ok::<_, anyhow::Error>(())
    }
    .await;
    engine.close(Duration::from_secs(10)).await;
    result
}

async fn group_one_subscription_claims(f: &Fixture) -> Result<()> {
    let (engine, app) = group_engine(f).await?;
    let result = async {
        let plain_body = serde_json::json!({
            "table": format!("public.{}", f.outer),
            "changesOnly": true,
            "subscription": "group-one-sub",
        });
        let (status, plain) = json(&app, Method::POST, "/v1/shapes", Body::from(plain_body.to_string())).await?;
        ensure!(status == StatusCode::OK, "pre-existing shape failed: {status} {plain}");
        let (status, conflict) = post_group(&app, group_body(f, "group-one-sub", true)).await?;
        ensure!(status == StatusCode::CONFLICT, "group did not reject an occupied subscription: {status} {conflict}");
        let plain_id = plain["shapeId"].as_str().context("pre-existing shape id")?;
        let (status, _) =
            json(&app, Method::DELETE, &format!("/v1/shapes/{plain_id}?subscription=group-one-sub"), Body::empty())
                .await?;
        ensure!(status == StatusCode::OK, "pre-existing shape release failed: {status}");

        let (status, group) = post_group(&app, group_body(f, "group-one-sub", true)).await?;
        ensure!(status == StatusCode::OK, "group create failed: {status} {group}");
        let members = group["results"].as_array().context("group results")?;
        let group_id = members[0]["shapeId"].as_str().context("group member id")?;
        let member_id = members[1]["shapeId"].as_str().context("private member id")?;
        for member_shape in [group_id, member_id] {
            let (status, body) =
                json(&app, Method::DELETE, &format!("/v1/shapes/{member_shape}"), Body::empty()).await?;
            ensure!(
                status == StatusCode::CONFLICT,
                "anonymous member release escaped group ownership: {status} {body}"
            );
            let (status, body) =
                json(&app, Method::DELETE, &format!("/v1/shapes/{member_shape}?purge=true"), Body::empty()).await?;
            ensure!(status == StatusCode::CONFLICT, "native purge of a group member was not refused: {status} {body}");
        }
        ensure!(engine.has_group_record("group-one-sub").await, "a refused member purge released the group");
        ensure!(
            engine.subscription_owner("group-one-sub").await.as_deref() == Some(group_id),
            "anonymous release removed the public group claim"
        );
        ensure!(
            engine.subscription_owner("group-one-sub:member:1").await.as_deref() == Some(member_id),
            "anonymous release removed the private member claim"
        );
        let (status, conflict) = json(&app, Method::POST, "/v1/shapes", Body::from(plain_body.to_string())).await?;
        ensure!(status == StatusCode::CONFLICT, "plain create bypassed group claim: {status} {conflict}");
        let private_body = serde_json::json!({
            "table": format!("public.{}", f.inner),
            "changesOnly": true,
            "subscription": "group-one-sub:member:1",
        });
        let (status, conflict) = json(&app, Method::POST, "/v1/shapes", Body::from(private_body.to_string())).await?;
        ensure!(status == StatusCode::CONFLICT, "private create bypassed group claim: {status} {conflict}");
        let (status, conflict) = json(
            &app,
            Method::DELETE,
            &format!("/v1/shapes/{member_id}?subscription=group-one-sub:member:1"),
            Body::empty(),
        )
        .await?;
        ensure!(status == StatusCode::CONFLICT, "private member release was not refused: {status} {conflict}");
        let (status, renewed) = post_group(&app, group_body(f, "group-one-sub", true)).await?;
        ensure!(status == StatusCode::OK, "same group renewal failed: {status} {renewed}");
        ensure!(renewed["streamPath"] == group["streamPath"], "renewal changed the group stream");
        let (status, released) =
            json(&app, Method::DELETE, &format!("/v1/shapes/{group_id}?subscription=group-one-sub"), Body::empty())
                .await?;
        ensure!(status == StatusCode::OK, "group release failed: {status} {released}");
        let (status, after) = json(&app, Method::POST, "/v1/shapes", Body::from(plain_body.to_string())).await?;
        ensure!(status == StatusCode::OK, "public subscription was not freed: {status} {after}");
        let (status, after_private) =
            json(&app, Method::POST, "/v1/shapes", Body::from(private_body.to_string())).await?;
        ensure!(status == StatusCode::OK, "private subscription was not freed: {status} {after_private}");
        Ok::<_, anyhow::Error>(())
    }
    .await;
    engine.close(Duration::from_secs(10)).await;
    result
}

async fn group_retry_renews(f: &Fixture) -> Result<()> {
    let (engine, app) = group_engine(f).await?;
    let result = async {
        *f.ds.group_head_faults.lock().await = vec![Some(503)];
        let (status, headers, failed) = post_group_response(&app, group_body(f, "group-retry-renews", true)).await?;
        ensure!(
            status == StatusCode::SERVICE_UNAVAILABLE,
            "fresh group tail outage was not retryable: {status} {failed}"
        );
        ensure!(headers.get("retry-after").is_some(), "fresh group tail outage omitted Retry-After");
        ensure!(
            engine.subscription_owner("group-retry-renews").await.is_none(),
            "fresh group failure leaked its public claim"
        );
        ensure!(
            engine.subscription_owner("group-retry-renews:member:1").await.is_none(),
            "fresh group failure leaked a private member claim"
        );
        let (status, first) = post_group(&app, group_body(f, "group-retry-renews", true)).await?;
        ensure!(status == StatusCode::OK, "fresh group retry failed: {status} {first}");
        let old_path = first["streamPath"].as_str().context("initial group stream path")?.to_owned();
        let old_id = first["groupId"].as_str().context("initial group id")?.to_owned();
        let old_members = first["results"]
            .as_array()
            .context("initial group members")?
            .iter()
            .map(|member| member["shapeId"].as_str().map(ToOwned::to_owned).context("member shape id"))
            .collect::<Result<Vec<_>>>()?;
        drop(first);
        f.ds.group_head_faults.lock().await.push(Some(503));
        let (status, headers, second) = post_group_response(&app, group_body(f, "group-retry-renews", true)).await?;
        ensure!(status == StatusCode::SERVICE_UNAVAILABLE, "HEAD outage was not retryable: {status} {second}");
        ensure!(headers.get("retry-after").is_some(), "HEAD outage omitted Retry-After: {headers:?}");
        let (status, second) = post_group(&app, group_body(f, "group-retry-renews", true)).await?;
        ensure!(status == StatusCode::OK, "retry did not renew: {status} {second}");
        ensure!(second["groupId"].as_str() == Some(old_id.as_str()), "HEAD outage recreated the group: {second}");
        ensure!(second["streamPath"].as_str() == Some(old_path.as_str()), "retry created a second group stream");
        let new_members = second["results"]
            .as_array()
            .context("renewed group members")?
            .iter()
            .map(|member| member["shapeId"].as_str().unwrap_or_default().to_owned())
            .collect::<Vec<_>>();
        ensure!(new_members == old_members, "HEAD outage lost a member claim: {second}");

        // The two probes pass through; the third HEAD is the final stream-tail read after renewal.
        *f.ds.group_head_faults.lock().await = vec![None, None, Some(503)];
        let (status, headers, body) = post_group_response(&app, group_body(f, "group-retry-renews", true)).await?;
        ensure!(
            status == StatusCode::SERVICE_UNAVAILABLE,
            "post-renew stream-tail outage was not retryable: {status} {body}"
        );
        ensure!(headers.get("retry-after").is_some(), "post-renew stream-tail outage omitted Retry-After");
        let (status, renewed) = post_group(&app, group_body(f, "group-retry-renews", true)).await?;
        ensure!(status == StatusCode::OK, "group did not survive post-renew outage: {status} {renewed}");
        ensure!(renewed["groupId"].as_str() == Some(old_id.as_str()), "post-renew outage recreated the group");
        ensure!(
            renewed["streamPath"].as_str() == Some(old_path.as_str()),
            "post-renew outage changed the group stream"
        );
        let renewed_members = renewed["results"]
            .as_array()
            .context("post-renew members")?
            .iter()
            .map(|member| member["shapeId"].as_str().unwrap_or_default().to_owned())
            .collect::<Vec<_>>();
        ensure!(renewed_members == old_members, "post-renew outage changed member shape ids: {renewed}");
        let retired = f.ds.retired.lock().await;
        ensure!(
            !retired.iter().any(|(method, path)| *method == Method::DELETE && path.ends_with(&old_path)),
            "post-renew HEAD outage deleted the healthy group stream: {retired:?}"
        );
        Ok::<_, anyhow::Error>(())
    }
    .await;
    engine.close(Duration::from_secs(10)).await;
    result
}

/// A retried load renews its group, then a release starts before the renewal answers. The
/// renewal must never answer `200` with the stream that release is retiring (`#g-group-recheck`):
/// it waits for the release and answers a fresh group on a new stream. With `before_close` the
/// release is held before it closes the stream, so the renewal's tail read sees an open stream
/// and only the recheck before answering can notice the release.
async fn group_renewal_rechecks_a_racing_release(f: &Fixture, before_close: bool) -> Result<()> {
    let (engine, app) = group_engine(f).await?;
    let result = async {
        let subscription = "group-renewal-release";
        let (status, first) = post_group(&app, group_body(f, subscription, true)).await?;
        ensure!(status == StatusCode::OK, "group create failed: {status} {first}");
        let old_path = first["streamPath"].as_str().context("group stream path")?.to_owned();
        let first_shape = first["results"][0]["shapeId"].as_str().context("first group member")?.to_owned();

        // A renewal HEADs the group stream three times: the health probe before and after the
        // renewal, then the stream-tail read for its answer. Hold the third, after the renewal.
        *f.ds.hold_group_head_after.lock().await = Some(2);
        let renew_app = app.clone();
        let renew_body = group_body(f, subscription, true);
        let mut renew = tokio::spawn(async move { post_group(&renew_app, renew_body).await });
        tokio::time::timeout(Duration::from_secs(10), f.ds.group_head_arrived.notified())
            .await
            .context("renewal did not reach its stream-tail read")?;

        // Release the group while the renewal is held, and hold its teardown either before the
        // stream closes or after it has closed, before the stream is deleted.
        let close_gate = Arc::new(Notify::new());
        if before_close {
            f.ds.close_gates.lock().await.insert(old_path.clone(), close_gate.clone());
        }
        let gate = pause_group_delete(f, &old_path).await;
        let release_app = app.clone();
        let release_task = tokio::spawn(async move {
            json(
                &release_app,
                Method::DELETE,
                &format!("/v1/shapes/{first_shape}?subscription={subscription}"),
                Body::empty(),
            )
            .await
        });
        if before_close {
            tokio::time::timeout(Duration::from_secs(10), f.ds.close_gate_arrived.notified())
                .await
                .context("group release did not reach its stream close")?;
        } else {
            wait_group_stream_closed(f, &old_path).await?;
        }
        f.ds.group_head_release.notify_one();
        let early = tokio::time::timeout(Duration::from_millis(500), &mut renew).await;
        if before_close {
            f.ds.close_gates.lock().await.remove(&old_path);
            close_gate.notify_waiters();
            close_gate.notify_one();
        }
        release_group_delete(f, &old_path, gate).await;
        if let Ok(answered) = early {
            let (status, body) = answered.context("renewal task failed")??;
            ensure!(
                !(status == StatusCode::OK && body["streamPath"].as_str() == Some(old_path.as_str())),
                "renewal answered 200 with the stream its racing release is retiring: {body}"
            );
            anyhow::bail!("renewal answered before its racing release finished: {status} {body}");
        }

        let (release_status, release_body) = tokio::time::timeout(Duration::from_secs(15), release_task)
            .await
            .context("group release did not finish")?
            .context("group release task failed")??;
        ensure!(release_status == StatusCode::OK, "group release failed: {release_status} {release_body}");
        let (status, fresh) = tokio::time::timeout(Duration::from_secs(15), renew)
            .await
            .context("renewal did not finish after the release")?
            .context("renewal task failed")??;
        ensure!(status == StatusCode::OK, "renewal after the release failed: {status} {fresh}");
        let new_path = fresh["streamPath"].as_str().context("fresh group stream path")?;
        ensure!(new_path != old_path, "renewal answered the released group's stream");
        ensure!(engine.has_group_record(subscription).await, "fresh group has no record");
        f.client
            .batch_execute(&format!("INSERT INTO public.{} (id, body) VALUES (7, 'renewal-release')", f.outer))
            .await?;
        let events = wait_group_events(f, new_path, 1).await?;
        ensure!(
            events.iter().any(|event| event_key(event).as_deref() == Some("7")),
            "fresh group missed live output: {events:?}"
        );
        Ok::<_, anyhow::Error>(())
    }
    .await;
    engine.close(Duration::from_secs(10)).await;
    result
}

/// A retried load whose group is released and replaced by a load with the same members while
/// the retry is still building its answer renews the replacement: it never answers the released
/// stream, and never a `409` against a group with its own members (`#g-group-retry-renews`).
async fn group_renewal_follows_a_replacement(f: &Fixture) -> Result<()> {
    let (engine, app) = group_engine(f).await?;
    let result = async {
        let subscription = "group-renewal-replaced";
        let (status, first) = post_group(&app, group_body(f, subscription, true)).await?;
        ensure!(status == StatusCode::OK, "group create failed: {status} {first}");
        let old_path = first["streamPath"].as_str().context("group stream path")?.to_owned();
        let first_shape = first["results"][0]["shapeId"].as_str().context("first group member")?.to_owned();

        // Hold the retry at its stream-tail read, after it has renewed the first group.
        *f.ds.hold_group_head_after.lock().await = Some(2);
        let renew_app = app.clone();
        let renew_body = group_body(f, subscription, true);
        let renew = tokio::spawn(async move { post_group(&renew_app, renew_body).await });
        tokio::time::timeout(Duration::from_secs(10), f.ds.group_head_arrived.notified())
            .await
            .context("renewal did not reach its stream-tail read")?;

        let (status, body) =
            json(&app, Method::DELETE, &format!("/v1/shapes/{first_shape}?subscription={subscription}"), Body::empty())
                .await?;
        ensure!(status == StatusCode::OK, "group release failed: {status} {body}");
        wait_group_stream_retired(f, &old_path).await?;
        let (status, replacement) = post_group(&app, group_body(f, subscription, true)).await?;
        ensure!(status == StatusCode::OK, "replacement load failed: {status} {replacement}");
        let new_path = replacement["streamPath"].as_str().context("replacement stream path")?.to_owned();
        ensure!(new_path != old_path, "replacement reused the released stream");

        f.ds.group_head_release.notify_one();
        let (status, renewed) = tokio::time::timeout(Duration::from_secs(15), renew)
            .await
            .context("held renewal did not finish")?
            .context("renewal task failed")??;
        ensure!(status == StatusCode::OK, "held renewal did not renew the replacement: {status} {renewed}");
        ensure!(
            renewed["streamPath"].as_str() == Some(new_path.as_str()),
            "held renewal answered another stream than the replacement's {new_path}: {renewed}"
        );
        ensure!(renewed["groupId"] == replacement["groupId"], "held renewal answered another group: {renewed}");
        Ok::<_, anyhow::Error>(())
    }
    .await;
    engine.close(Duration::from_secs(10)).await;
    result
}

async fn group_release_during_assembly(f: &Fixture) -> Result<()> {
    let (engine, app) = group_engine(f).await?;
    let result = async {
        *f.ds.hold_group_catalog.lock().await = true;
        let load_app = app.clone();
        let load_body = group_body(f, "group-assembly-race", true);
        let load = tokio::spawn(async move { post_group(&load_app, load_body).await });
        tokio::time::timeout(Duration::from_secs(10), f.ds.group_catalog_arrived.notified())
            .await
            .context("group load did not reach GroupCreated durability")?;
        let graph = tokio::time::timeout(Duration::from_secs(10), engine.graph())
            .await
            .context("group graph did not settle while assembly was held")?
            .shapes;
        let member0 = graph
            .iter()
            .find(|shape| {
                shape.stream_path.contains("shape/group-") && shape.table.to_string() == format!("public.{}", f.outer)
            })
            .context("plain member shape was not installed before GroupCreated")?
            .id
            .clone();
        let member1 = graph
            .iter()
            .find(|shape| {
                shape.stream_path.contains("shape/group-") && shape.table.to_string() == format!("public.{}", f.inner)
            })
            .context("private member shape was not installed before GroupCreated")?
            .id
            .clone();
        let aggregate_body = serde_json::json!({
            "table": format!("public.{}", f.outer),
            "fn": "count",
            "subscription": "group-assembly-race",
        });
        let (status, body) = json(&app, Method::POST, "/v1/aggregates", Body::from(aggregate_body.to_string())).await?;
        ensure!(status == StatusCode::CONFLICT, "aggregate escaped the group reservation: {status} {body}");
        let (status, body) = json(&app, Method::DELETE, &format!("/v1/shapes/{member0}"), Body::empty()).await?;
        ensure!(status == StatusCode::CONFLICT, "anonymous reservation release escaped the guard: {status} {body}");
        tokio::time::sleep(Duration::from_secs(6)).await;
        ensure!(
            engine.subscription_owner("group-assembly-race").await.as_deref() == Some(member0.as_str()),
            "public reservation lease lapsed while group assembly was held"
        );
        ensure!(
            engine.subscription_owner("group-assembly-race:member:1").await.as_deref() == Some(member1.as_str()),
            "private reservation lease lapsed while group assembly was held"
        );
        let (status, body) = json(
            &app,
            Method::DELETE,
            &format!("/v1/shapes/{member0}?subscription=group-assembly-race"),
            Body::empty(),
        )
        .await?;
        ensure!(status == StatusCode::CONFLICT, "public release escaped the assembly reservation: {status} {body}");
        // A retry after a lost response, while the first load is still assembling, must hear
        // "retry" (#g-group-retry-renews), never a conflict with itself.
        let (status, headers, body) = post_group_response(&app, group_body(f, "group-assembly-race", true)).await?;
        ensure!(status == StatusCode::SERVICE_UNAVAILABLE, "retry during assembly was not retryable: {status} {body}");
        ensure!(headers.get("retry-after").is_some(), "retry during assembly omitted Retry-After");
        f.ds.group_catalog_release.notify_one();
        let (status, body) = tokio::time::timeout(Duration::from_secs(15), load)
            .await
            .context("group load did not complete after catalog release")?
            .context("group load task failed")??;
        ensure!(status == StatusCode::OK, "assembly completed without every claim: {status} {body}");
        tokio::time::sleep(Duration::from_secs(3)).await;
        ensure!(
            engine.has_group_record("group-assembly-race").await,
            "completed group was retired immediately after its assembly lease renewal"
        );
        ensure!(
            engine.subscription_owner("group-assembly-race").await.as_deref() == Some(member0.as_str())
                && engine.subscription_owner("group-assembly-race:member:1").await.as_deref() == Some(member1.as_str()),
            "completed group lost a member claim after assembly"
        );
        let public_body = serde_json::json!({
            "table": format!("public.{}", f.outer),
            "changesOnly": true,
            "subscription": "group-assembly-race",
        });
        let (status, conflict) = json(&app, Method::POST, "/v1/shapes", Body::from(public_body.to_string())).await?;
        ensure!(status == StatusCode::CONFLICT, "completed group lost its public claim: {status} {conflict}");
        let member_owner = engine.subscription_owner("group-assembly-race:member:1").await;
        ensure!(
            member_owner.as_deref() == Some(member1.as_str()),
            "completed group lost its private claim: owner={member_owner:?}, member={member1}"
        );
        Ok::<_, anyhow::Error>(())
    }
    .await;
    engine.close(Duration::from_secs(10)).await;
    result
}

/// An engine retirement (epoch reset, drift, `TRUNCATE`) of member 0 while its group is still
/// assembling must retire the shape, not be refused by the reservation, and the load must then
/// fail and compensate instead of answering `200` for a group with a retired member.
async fn group_retirement_during_assembly(f: &Fixture) -> Result<()> {
    let (engine, app) = group_engine(f).await?;
    let result = async {
        *f.ds.hold_group_catalog.lock().await = true;
        let load_app = app.clone();
        let load_body = group_body(f, "group-assembly-retire", true);
        let load = tokio::spawn(async move { post_group(&load_app, load_body).await });
        tokio::time::timeout(Duration::from_secs(10), f.ds.group_catalog_arrived.notified())
            .await
            .context("group load did not reach GroupCreated durability")?;
        let member0 = engine
            .subscription_owner("group-assembly-retire")
            .await
            .context("member 0 did not claim the group subscription before GroupCreated")?;
        let (status, body) =
            json(&app, Method::DELETE, &format!("/v1/shapes/{member0}?purge=true"), Body::empty()).await?;
        ensure!(status == StatusCode::CONFLICT, "native purge escaped the assembly reservation: {status} {body}");
        ensure!(
            engine.subscription_owner("group-assembly-retire").await.as_deref() == Some(member0.as_str()),
            "refused native purge still released member 0"
        );
        engine.purge_shape(&member0).await.context("engine retirement of member 0 was refused")?;
        ensure!(
            engine.subscription_owner("group-assembly-retire").await.is_none(),
            "engine retirement left member 0 holding the group subscription"
        );
        *f.ds.hold_group_catalog.lock().await = false;
        f.ds.group_catalog_release.notify_one();
        let (status, body) = tokio::time::timeout(Duration::from_secs(15), load)
            .await
            .context("group load did not finish after catalog release")?
            .context("group load task failed")??;
        ensure!(status != StatusCode::OK, "group answered 200 with a retired member: {body}");
        ensure!(!engine.has_group_record("group-assembly-retire").await, "failed load left a group record");
        ensure!(
            engine.subscription_owner("group-assembly-retire:member:1").await.is_none(),
            "failed load left a private member claim"
        );
        // The reload uses the contract's member shape, `{table, where, columns}` with no `key`.
        let outer = format!("public.{}", f.outer);
        let reload = serde_json::json!({
            "subscription": "group-assembly-retire",
            "members": [
                { "table": outer, "columns": ["id", "body"] },
                { "table": format!("public.{}", f.inner), "columns": ["id"] },
            ],
        });
        let (status, body) = post_group(&app, reload).await?;
        ensure!(status == StatusCode::OK, "reload after a retired assembly failed: {status} {body}");
        ensure!(
            body["shapeId"].is_string() && body["shapeId"] == body["groupId"],
            "group answer has no single shapeId: {body}"
        );
        ensure!(
            body["results"][0]["key"] == outer.as_str(),
            "a member without a key is not named by its table: {body}"
        );
        Ok::<_, anyhow::Error>(())
    }
    .await;
    engine.close(Duration::from_secs(10)).await;
    result
}

async fn group_members_max(f: &Fixture) -> Result<()> {
    let (engine, app) = group_engine(f).await?;
    let result = async {
        let members = (0..9)
            .map(|i| {
                serde_json::json!({
                    "key": format!("member-{i}"),
                    "table": if i % 2 == 0 { format!("public.{}", f.outer) } else { format!("public.{}", f.inner) },
                })
            })
            .collect::<Vec<_>>();
        let too_many = serde_json::json!({ "subscription": "group-too-many", "members": members });
        let (status, body) = post_group(&app, too_many).await?;
        ensure!(status == StatusCode::BAD_REQUEST, "ninth member was accepted: {status} {body}");
        let (status, body) = post_group(&app, group_body(f, "group-too-many", true)).await?;
        ensure!(status == StatusCode::OK, "failed ninth-member request left a reservation: {status} {body}");

        let duplicate = serde_json::json!({
            "subscription": "group-duplicate-table",
            "members": [
                { "key": "one", "table": format!("public.{}", f.outer) },
                { "key": "two", "table": format!("public.{}", f.outer) },
            ],
        });
        let (status, body) = post_group(&app, duplicate).await?;
        ensure!(status == StatusCode::BAD_REQUEST, "duplicate table was accepted: {status} {body}");
        let (status, body) = post_group(&app, group_body(f, "group-duplicate-table", true)).await?;
        ensure!(status == StatusCode::OK, "duplicate-table rejection left a reservation: {status} {body}");

        let refused = serde_json::json!({
            "subscription": "group-refused-member",
            "members": [
                { "key": "one", "table": format!("public.{}", f.outer) },
                { "key": "two", "table": "public.no_such_group_table" },
            ],
        });
        let (status, body) = post_group(&app, refused).await?;
        ensure!(status == StatusCode::BAD_REQUEST, "refused member was accepted: {status} {body}");
        let (status, body) = post_group(&app, group_body(f, "group-refused-member", true)).await?;
        ensure!(status == StatusCode::OK, "member validation failure left a reservation: {status} {body}");

        // Make the second member's stream preflight fail after the first member has registered.
        // This reaches the reservation guard's compensation path rather than only validation.
        let before_shapes: HashSet<String> = engine.graph().await.shapes.into_iter().map(|shape| shape.id).collect();
        *f.ds.fail_group_head_after_put.lock().await = true;
        let (status, body) = post_group(&app, group_body(f, "group-partial-rollback", true)).await?;
        ensure!(status.is_server_error(), "partial member failure unexpectedly succeeded: {status} {body}");
        let after_shapes: HashSet<String> = engine.graph().await.shapes.into_iter().map(|shape| shape.id).collect();
        ensure!(
            after_shapes == before_shapes,
            "partial group failure left member shapes behind: before={before_shapes:?}, after={after_shapes:?}"
        );
        let (status, body) = post_group(&app, group_body(f, "group-partial-rollback", true)).await?;
        ensure!(status == StatusCode::OK, "partial rollback left a claim behind: {status} {body}");
        Ok::<_, anyhow::Error>(())
    }
    .await;
    engine.close(Duration::from_secs(10)).await;
    result
}

/// Exercise the public group-load boundary at the post-GroupCreated cuts. Each variant must
/// compensate its durable GroupCreated with GroupDropped and release both the public and private
/// claims before the subscription can be loaded again.
async fn group_completion_cuts(f: &Fixture) -> Result<()> {
    let (engine, app) = group_engine(f).await?;
    let result = async {
        for (subscription, cut) in [
            ("group-cut-cancel-before-reservation", "cancel-before-reservation"),
            ("group-cut-cancel-after-reservation", "cancel-after-reservation"),
            ("group-cut-cancel-member-create", "cancel-member-create-0"),
            ("group-cut-cancel-member-create-1", "cancel-member-create-1"),
            ("group-cut-cancel-seed", "cancel-seed"),
            ("group-cut-cancel-stream-tail", "cancel-stream-tail"),
            ("group-cut-cancel-snapshot", "cancel-snapshot"),
            ("group-cut-cancel", "cancel"),
            ("group-cut-cancel-head", "cancel-head"),
            ("group-cut-replacement", "replacement"),
            ("group-cut-closed", "closed"),
            ("group-cut-truncate", "truncate"),
            ("group-cut-degraded", "degraded"),
        ] {
            let pre_durability_stage = match cut {
                "cancel-before-reservation" => Some(GroupLoadAwait::BeforeReservation),
                "cancel-after-reservation" => Some(GroupLoadAwait::AfterReservation),
                "cancel-member-create-0" => Some(GroupLoadAwait::MemberCreate(0)),
                "cancel-member-create-1" => Some(GroupLoadAwait::MemberCreate(1)),
                "cancel-stream-tail" => Some(GroupLoadAwait::StreamTail),
                "cancel-snapshot" => Some(GroupLoadAwait::Snapshot),
                _ => None,
            };
            let cancel_at_seed = cut == "cancel-seed";
            if let Some(stage) = pre_durability_stage {
                engine.group_load_gate_for_test().arm(stage).await;
            } else if cancel_at_seed {
                engine.group_seed_gate_for_test().arm();
            } else {
                *f.ds.hold_group_catalog.lock().await = true;
            }
            let load_app = app.clone();
            let load_body = group_body(f, subscription, cut != "cancel-seed");
            let mut load = tokio::spawn(async move { post_group(&load_app, load_body).await });
            let mut pre_attempt_id = None;
            let group_path = if pre_durability_stage.is_some() || cancel_at_seed {
                if cancel_at_seed {
                    tokio::time::timeout(Duration::from_secs(10), engine.group_seed_gate_for_test().arrived.notified())
                        .await
                        .context("group cut did not reach its pre-durability gate")?;
                } else {
                    tokio::time::timeout(Duration::from_secs(10), engine.group_load_gate_for_test().arrived.notified())
                        .await
                        .context("group cut did not reach its pre-durability gate")?;
                }
                if cut != "cancel-before-reservation" {
                    let id = engine.group_id_for_test(subscription).await.context("pre-durability reservation id")?;
                    *f.ds.hold_group_drop_id.lock().await = Some(id.clone());
                    pre_attempt_id = Some(id);
                }
                load.abort();
                if cancel_at_seed {
                    engine.group_seed_gate_for_test().release.notify_waiters();
                } else {
                    engine.group_load_gate_for_test().release.notify_waiters();
                }
                None
            } else {
                tokio::time::timeout(Duration::from_secs(10), f.ds.group_catalog_arrived.notified())
                    .await
                    .context("group cut did not reach GroupCreated durability")?;
                let member_id =
                    engine.subscription_owner(subscription).await.context("group cut has no public member")?;
                Some(
                    engine
                        .get_shape(&member_id)
                        .await
                        .map(|shape| shape.stream_path)
                        .context("group cut did not install its member stream")?,
                )
            };
            let group_path = group_path.unwrap_or_default();
            if pre_attempt_id.is_some() {
                tokio::time::timeout(Duration::from_secs(10), f.ds.group_catalog_arrived.notified())
                    .await
                    .context("pre-durability cleanup did not reach GroupDropped durability")?;
                f.ds.group_catalog_release.notify_one();
            }
            if cut == "cancel" || cut == "cancel-head" {
                *f.ds.hold_group_drop_id.lock().await = Some(group_path.clone());
            }
            if cut == "replacement" {
                let abort_gate = engine.group_abort_gate_for_test();
                abort_gate.arm();
                *f.ds.hold_group_drop_id.lock().await = Some(group_path.clone());
                load.abort();
                *f.ds.hold_group_catalog.lock().await = false;
                f.ds.group_catalog_release.notify_one();
                tokio::time::timeout(Duration::from_secs(10), f.ds.group_catalog_arrived.notified())
                    .await
                    .context("old cleanup did not reach GroupDropped durability")?;
                f.ds.group_catalog_release.notify_one();
                tokio::time::timeout(Duration::from_secs(10), abort_gate.arrived.notified())
                    .await
                    .context("old cleanup did not reach its abort gate")?;
                let replacement_gate = engine.group_load_gate_for_test();
                replacement_gate.arm(GroupLoadAwait::AfterReservation).await;
                let replacement_app = app.clone();
                let replacement_body = group_body(f, subscription, true);
                let replacement_task =
                    tokio::spawn(async move { post_group(&replacement_app, replacement_body).await });
                tokio::time::timeout(Duration::from_secs(10), replacement_gate.arrived.notified())
                    .await
                    .context("replacement did not reach its reservation")?;
                let replacement_id =
                    engine.group_id_for_test(subscription).await.context("replacement reservation id")?;
                abort_gate.release.notify_one();
                tokio::time::timeout(Duration::from_secs(10), abort_gate.finished.notified())
                    .await
                    .context("old abort did not finish")?;
                ensure!(
                    engine.group_reservation_for_test(subscription).await
                        && engine.group_id_for_test(subscription).await.as_deref() == Some(replacement_id.as_str()),
                    "old abort removed replacement reservation"
                );
                replacement_gate.release.notify_one();
                let (status, replacement) = replacement_task.await.context("replacement task failed")??;
                ensure!(
                    status == StatusCode::OK,
                    "replacement load did not survive old cleanup: {status} {replacement}"
                );
                ensure!(replacement["groupId"] == replacement_id, "replacement changed its reservation id");
                ensure!(replacement_id != group_path, "replacement reused the old group id");
                ensure!(
                    engine.group_id_for_test(subscription).await.as_deref() == Some(replacement_id.as_str()),
                    "old abort removed the replacement group"
                );
                for member in replacement["results"].as_array().context("replacement members")? {
                    let member_subscription =
                        member["subscription"].as_str().context("replacement member subscription")?;
                    let member_id = member["shapeId"].as_str().context("replacement member id")?;
                    ensure!(
                        engine.subscription_owner(member_subscription).await.as_deref() == Some(member_id),
                        "old abort removed replacement member claim {member_subscription}"
                    );
                }
                let catalog = stream_events(f, "meta/catalog").await;
                ensure!(
                    catalog.iter().any(|event| event["t"] == "groupDropped" && event["group_id"] == group_path),
                    "old cleanup did not drop its own group"
                );
                ensure!(
                    !catalog.iter().any(|event| event["t"] == "groupDropped" && event["group_id"] == replacement_id),
                    "old cleanup dropped replacement group"
                );
                continue;
            }
            if cut == "cancel-head" || cut == "truncate" {
                *f.ds.hold_group_head_after.lock().await = Some(0);
            } else if cut == "closed" {
                f.ds.closed.lock().await.insert(group_path.clone());
            } else if cut == "degraded" {
                engine.force_degraded();
            }
            if cut == "cancel" {
                load.abort();
            }
            // Release only a GroupCreated this cut actually held. `Notify` stores a permit when no
            // request is waiting, so an unmatched release would let the next cut's held catalog
            // write pass straight through and its load answer before the cut is applied.
            if pre_durability_stage.is_none() && !cancel_at_seed {
                *f.ds.hold_group_catalog.lock().await = false;
                f.ds.group_catalog_release.notify_one();
            }
            if cut == "cancel-head" {
                tokio::time::timeout(Duration::from_secs(10), f.ds.group_head_arrived.notified())
                    .await
                    .context("post-durability HEAD did not reach its gate")?;
                load.abort();
                f.ds.group_head_release.notify_one();
            }
            if cut == "truncate" {
                // TRUNCATE at the post-durability HEAD, not inside the GroupCreated hold: retiring
                // the inner table also releases every completed group with a member on it, and
                // each release waits on the catalog writer that a held GroupCreated would block.
                // The final locked recheck after this HEAD must still refuse the group.
                tokio::time::timeout(Duration::from_secs(10), f.ds.group_head_arrived.notified())
                    .await
                    .context("post-durability HEAD did not reach its gate")?;
                f.client.batch_execute(&format!("TRUNCATE public.{}", f.inner)).await?;
                let mut changed = f.ds.changed.subscribe();
                tokio::time::timeout(Duration::from_secs(15), async {
                    loop {
                        if f.ds
                            .retired
                            .lock()
                            .await
                            .iter()
                            .any(|(method, path)| *method == Method::POST && path.ends_with(&group_path))
                            && engine.subscription_owner(&format!("{subscription}:member:1")).await.is_none()
                        {
                            return Ok::<_, anyhow::Error>(());
                        }
                        changed.changed().await.context("truncate retirement observation channel closed")?;
                    }
                })
                .await
                .context("truncate did not retire the group stream")??;
                f.ds.group_head_release.notify_one();
            }
            if cut == "cancel" || cut == "cancel-head" {
                tokio::time::timeout(Duration::from_secs(10), f.ds.group_catalog_arrived.notified())
                    .await
                    .context("cancel cleanup did not reach GroupDropped durability")?;
                f.ds.group_catalog_release.notify_one();
            }
            if cut != "cancel" && cut != "cancel-head" && pre_durability_stage.is_none() && !cancel_at_seed {
                let (status, body) = tokio::time::timeout(Duration::from_secs(15), &mut load)
                    .await
                    .context("group cut load did not finish")?
                    .context("group cut load task failed")??;
                ensure!(status != StatusCode::OK, "group cut answered 200: {status} {body}");
            }
            tokio::time::timeout(Duration::from_secs(15), async {
                loop {
                    if engine.subscription_owner(subscription).await.is_none()
                        && engine.subscription_owner(&format!("{subscription}:member:1")).await.is_none()
                        && !engine.group_reservation_for_test(subscription).await
                        && !engine.has_group_record(subscription).await
                    {
                        return Ok::<_, anyhow::Error>(());
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .context("group cut compensation left claims behind")??;
            let catalog = stream_events(f, "meta/catalog").await;
            if pre_durability_stage.is_some() || cancel_at_seed {
                ensure!(
                    !catalog.iter().any(|event| {
                        event["t"] == "groupCreated" && event["group"]["subscription"] == subscription
                    }),
                    "pre-durability cancellation wrote GroupCreated: {subscription} events={catalog:?}"
                );
                if let Some(attempt_id) = pre_attempt_id {
                    ensure!(
                        catalog.iter().any(|event| {
                            event["t"] == "groupDropped"
                                && event["subscription"] == subscription
                                && event["group_id"] == attempt_id
                        }),
                        "pre-durability cancellation dropped the wrong group: {subscription} events={catalog:?}"
                    );
                } else {
                    ensure!(
                        !catalog
                            .iter()
                            .any(|event| event["t"] == "groupDropped" && event["subscription"] == subscription),
                        "before-reservation cancellation wrote GroupDropped: {subscription} events={catalog:?}"
                    );
                }
            } else {
                ensure!(
                    catalog.iter().any(|event| event["t"] == "groupDropped" && event["subscription"] == subscription),
                    "group cut did not durably compensate GroupCreated: {subscription} events={catalog:?}"
                );
            }
            if cut == "cancel"
                || cut == "cancel-head"
                || cut == "closed"
                || pre_durability_stage.is_some()
                || cancel_at_seed
            {
                let (status, body) = post_group(&app, group_body(f, subscription, true)).await?;
                ensure!(status == StatusCode::OK, "compensated group could not be retried: {status} {body}");
            }
        }
        Ok::<_, anyhow::Error>(())
    }
    .await;
    engine.close(Duration::from_secs(10)).await;
    result
}

/// The release acknowledgement is durable before the engine-owned teardown is allowed to
/// remove the group record. Dropping the HTTP DELETE at that exact point must not cancel the
/// teardown; a retry can then reuse the subscription without a hidden member claim.
async fn group_release_cancellation(f: &Fixture) -> Result<()> {
    let (engine, app) = group_engine(f).await?;
    let result = async {
        let (status, first) = post_group(&app, group_body(f, "group-release-cancel", true)).await?;
        ensure!(status == StatusCode::OK, "initial group create failed: {status} {first}");
        let member_id = first["results"][0]["shapeId"].as_str().context("release cancellation member")?.to_owned();
        let gate = engine.group_release_gate_for_test();
        gate.arm();
        let delete_app = app.clone();
        let delete = tokio::spawn(async move {
            json(
                &delete_app,
                Method::DELETE,
                &format!("/v1/shapes/{member_id}?subscription=group-release-cancel"),
                Body::empty(),
            )
            .await
        });
        tokio::time::timeout(Duration::from_secs(10), gate.arrived.notified())
            .await
            .context("release did not reach its post-durability gate")?;
        delete.abort();
        gate.release.notify_one();
        tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                if engine.subscription_owner("group-release-cancel").await.is_none()
                    && engine.subscription_owner("group-release-cancel:member:1").await.is_none()
                    && !engine.group_reservation_for_test("group-release-cancel").await
                    && !engine.has_group_record("group-release-cancel").await
                {
                    return Ok::<_, anyhow::Error>(());
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .context("cancelled release did not complete")??;
        let (status, replacement) = post_group(&app, group_body(f, "group-release-cancel", true)).await?;
        ensure!(
            status == StatusCode::OK,
            "subscription was not reusable after cancelled release: {status} {replacement}"
        );
        Ok::<_, anyhow::Error>(())
    }
    .await;
    engine.close(Duration::from_secs(10)).await;
    result
}

async fn group_renewal_cancellation(f: &Fixture) -> Result<()> {
    let (engine, app) = group_engine(f).await?;
    let result = async {
        let (status, first) = post_group(&app, group_body(f, "group-renewal-cancel", true)).await?;
        ensure!(status == StatusCode::OK, "initial group create failed: {status} {first}");
        for stage in [GroupLoadAwait::RenewalHealth, GroupLoadAwait::RenewalStreamTail, GroupLoadAwait::RenewalSnapshot]
        {
            let gate = engine.group_load_gate_for_test();
            gate.arm(stage).await;
            let renew_app = app.clone();
            let renew_body = group_body(f, "group-renewal-cancel", true);
            let renew = tokio::spawn(async move { post_group(&renew_app, renew_body).await });
            tokio::time::timeout(Duration::from_secs(10), gate.arrived.notified())
                .await
                .context("renewal did not reach its cancellation gate")?;
            renew.abort();
            gate.release.notify_waiters();
            ensure!(
                tokio::time::timeout(Duration::from_secs(10), renew)
                    .await
                    .context("cancelled renewal did not terminate")
                    .is_ok_and(|result| result.is_err()),
                "renewal cancellation was not observed"
            );
            ensure!(engine.has_group_record("group-renewal-cancel").await, "cancelled renewal lost the group");
            ensure!(
                engine.subscription_owner("group-renewal-cancel").await.is_some()
                    && engine.subscription_owner("group-renewal-cancel:member:1").await.is_some(),
                "cancelled renewal lost a member claim"
            );
            let (status, retried) = post_group(&app, group_body(f, "group-renewal-cancel", true)).await?;
            ensure!(status == StatusCode::OK && retried["groupId"] == first["groupId"], "renewal retry changed group");
        }
        Ok::<_, anyhow::Error>(())
    }
    .await;
    engine.close(Duration::from_secs(10)).await;
    result
}

/// Production gate #v-gate-reconnect-burst: independent scopes sharing the same member tables
/// claim concurrently, then every reconnect retries the same named load as a renewal.
async fn group_reconnect_burst(f: &Fixture) -> Result<()> {
    let (engine, app) = group_engine(f).await?;
    let result = async {
        let mut tasks = Vec::new();
        for i in 0..30 {
            let app = app.clone();
            let body = group_body(f, &format!("group-burst-{i}"), false);
            tasks.push(tokio::spawn(async move { post_group(&app, body).await }));
        }
        let mut first = Vec::new();
        for (i, task) in tasks.into_iter().enumerate() {
            let (status, body) = task.await.context("burst load task failed")??;
            ensure!(status == StatusCode::OK, "burst load {i} was not admitted: {status} {body}");
            first.push((
                body["groupId"].as_str().unwrap_or_default().to_owned(),
                body["streamPath"].as_str().unwrap_or_default().to_owned(),
                body["results"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|member| member["shapeId"].as_str().unwrap_or_default().to_owned())
                    .collect::<Vec<_>>(),
            ));
        }
        for i in 0..30 {
            let (status, body) = post_group(&app, group_body(f, &format!("group-burst-{i}"), false)).await?;
            ensure!(status == StatusCode::OK, "burst renewal {i} was not idempotent: {status} {body}");
            ensure!(
                body["groupId"] == first[i].0 && body["streamPath"] == first[i].1,
                "burst renewal changed group identity: {body}"
            );
            let members = body["results"]
                .as_array()
                .unwrap()
                .iter()
                .map(|member| member["shapeId"].as_str().unwrap_or_default().to_owned())
                .collect::<Vec<_>>();
            ensure!(members == first[i].2, "burst renewal changed member identities: {body}");
        }
        ensure!(
            engine.graph().await.shapes.iter().filter(|shape| shape.stream_path.contains("shape/group-")).count() == 60,
            "burst changed exact shape count"
        );
        Ok::<_, anyhow::Error>(())
    }
    .await;
    engine.close(Duration::from_secs(10)).await;
    result
}

/// Production gate #v-gate-admission: one held group completion must not block another group or
/// a plain shape create from making progress.
async fn group_admission_gate(f: &Fixture) -> Result<()> {
    let (engine, app) = group_engine(f).await?;
    let result = async {
        let seed_gate = engine.group_seed_gate_for_test();
        seed_gate.arm();
        let first_app = app.clone();
        let first_body = group_body(f, "group-admission-held", false);
        let first = tokio::spawn(async move { post_group(&first_app, first_body).await });
        tokio::time::timeout(Duration::from_secs(10), seed_gate.arrived.notified())
            .await
            .context("held group seed did not reach its gate")?;
        let second_app = app.clone();
        let second_body = group_body(f, "group-admission-independent", true);
        let second = tokio::spawn(async move { post_group(&second_app, second_body).await });
        let plain_body = serde_json::json!({
            "table": format!("public.{}", f.outer),
            "changesOnly": true,
            "subscription": "group-admission-plain",
        });
        let plain = tokio::time::timeout(
            Duration::from_secs(2),
            json(&app, Method::POST, "/v1/shapes", Body::from(plain_body.to_string())),
        )
        .await
        .context("plain create waited behind held group admission")??;
        ensure!(
            plain.0 == StatusCode::OK,
            "plain create failed during independent admission: {} {:?}",
            plain.0,
            plain.1
        );
        let (status, body) = tokio::time::timeout(Duration::from_secs(2), second)
            .await
            .context("independent group load waited behind held admission")?
            .context("independent group task failed")??;
        ensure!(status == StatusCode::OK, "independent group load failed: {status} {body}");
        seed_gate.release.notify_one();
        let (status, body) = tokio::time::timeout(Duration::from_secs(15), first)
            .await
            .context("held admission group did not finish")?
            .context("held admission task failed")??;
        ensure!(status == StatusCode::OK, "held admission group failed: {status} {body}");
        Ok::<_, anyhow::Error>(())
    }
    .await;
    engine.close(Duration::from_secs(10)).await;
    result
}

async fn group_restart_restores_plain_and_drops_mixed(f: &Fixture) -> Result<()> {
    let (engine, app) = group_engine(f).await?;
    let result = async {
        let (status, plain) = post_group(&app, group_body(f, "group-plain-restart", true)).await?;
        ensure!(status == StatusCode::OK, "plain group create failed: {status} {plain}");
        let (status, mixed) = post_group(&app, group_body(f, "group-mixed-restart", false)).await?;
        ensure!(status == StatusCode::OK, "mixed group create failed: {status} {mixed}");
        engine.close(Duration::from_secs(10)).await;
        let (restarted, restarted_app) = group_engine(f).await?;
        let (status, duplicate_plain) = post_group(&restarted_app, group_body(f, "group-plain-restart", true)).await?;
        ensure!(status == StatusCode::OK, "plain group did not renew after restart: {status} {duplicate_plain}");
        ensure!(duplicate_plain["streamPath"] == plain["streamPath"], "restored plain group changed stream path");
        f.client.batch_execute(&format!("INSERT INTO public.{} (id, body) VALUES (5, 'restart')", f.outer)).await?;
        let restart_events =
            wait_group_events(f, duplicate_plain["streamPath"].as_str().context("restored plain stream path")?, 1)
                .await?;
        ensure!(
            restart_events.iter().any(|event| event_key(event).as_deref() == Some("5")),
            "restored group missed live output: {restart_events:?}"
        );
        let old_mixed_path = mixed["streamPath"].as_str().context("mixed group stream path")?;
        let (status, recreated_mixed) = post_group(&restarted_app, group_body(f, "group-mixed-restart", false)).await?;
        ensure!(
            status == StatusCode::OK,
            "mixed group was not dropped as a unit on restart: {status} {recreated_mixed}"
        );
        ensure!(recreated_mixed["streamPath"].as_str() != Some(old_mixed_path), "new group reused dropped stream path");
        restarted.close(Duration::from_secs(10)).await;
        Ok::<_, anyhow::Error>(())
    }
    .await;
    // The restart case closes both engines inside the body; this is idempotent cleanup for the
    // failure path before the explicit restart.
    result
}

async fn failed_source_retry(f: &Fixture) -> Result<()> {
    let secret = f.root.join("late-database-url");
    let secret_ref = format!("file:{}", secret.display());
    f.client
        .execute(&format!("UPDATE {} SET database_secret=$1 WHERE source_id='alpha'", f.sources_table), &[&secret_ref])
        .await?;
    let before: String = f
        .client
        .query_one(&format!("SELECT row_to_json(s)::text FROM {} s WHERE source_id='alpha'", f.sources_table), &[])
        .await?
        .get(0);
    let revision: i64 = f.client.query_one(&format!("SELECT revision FROM {}", f.version_table), &[]).await?.get(0);
    let host = SourcesSupervisor::new(f.config.clone())?;
    host.refresh().await?;
    let app = host.router();
    let result = async {
        let (code, failed) = json(&app, Method::GET, "/sources/alpha/status", Body::empty()).await?;
        ensure!(code == StatusCode::OK && failed["ready"] == false, "missing secret must leave alpha not ready: {code} {failed}");
        let beta_handle = f.snapshot(&app, "/sources/beta", false).await?;
        f.snapshot(&app, "/sources/gamma", false).await?;
        let mut completed = host.poll_completed_for_test();
        // Make the existing secret reference resolvable; no row, revision, or refresh changes follow.
        std::fs::write(&secret, &f.pg_url)?;
        host.spawn_poll();
        tokio::time::timeout(Duration::from_secs(6), completed.changed()).await
            .context("the real one-second poll tick did not complete within its boot budget")??;
        for source in ["beta", "gamma"] {
            f.snapshot(&app, &format!("/sources/{source}"), false).await?;
        }
        let (code, body) = f.read_handle(&app, "/sources/beta", &beta_handle).await?;
        ensure!(code == StatusCode::OK, "healthy beta must retain its Engine and handle: {code} {body}");
        let after: String = f.client.query_one(&format!("SELECT row_to_json(s)::text FROM {} s WHERE source_id='alpha'", f.sources_table), &[]).await?.get(0);
        let after_revision: i64 = f.client.query_one(&format!("SELECT revision FROM {}", f.version_table), &[]).await?.get(0);
        ensure!(before == after && revision == after_revision, "retry must not require a source or version-row mutation");
        let (code, recovered) = json(&app, Method::GET, "/sources/alpha/status", Body::empty()).await?;
        ensure!(code == StatusCode::OK && recovered["ready"] == true,
            "a failed source must become ready on the next completed poll after its secret resolves, with unchanged rows and no refresh: {code} {recovered}");
        f.snapshot(&app, "/sources/alpha", false).await?;
        Ok(())
    }.await;
    stop(&host).await;
    result
}

async fn self_stopped_sources_recover_without_starving_rows(f: &Fixture) -> Result<()> {
    let host = f.host().await?;
    let mut completed = host.poll_completed_for_test();
    host.spawn_poll();
    let result = async {
        for revision in 2..=4 {
            let old = host.engine_for_test("alpha").await.context("alpha missing")?;
            let old_alive = old.state_alive_probe_for_test();
            old.shutdown_token().begin();
            drop(old);
            f.client
                .execute(&format!("UPDATE {} SET revision=$1 WHERE source_id='beta'", f.sources_table), &[&revision])
                .await?;
            set_revision(&f.client, &f.version_table, revision).await?;
            tokio::time::timeout(Duration::from_secs(6), completed.changed()).await??;
            f.ready(&host.router(), "beta", revision).await?;
            f.ready(&host.router(), "alpha", 1).await?;
            ensure!(!old_alive(), "an Engine-initiated stop remained in running after the next poll");
            f.snapshot(&host.router(), "/sources/alpha", false).await?;
            f.snapshot(&host.router(), "/sources/gamma", false).await?;
        }
        Ok::<(), anyhow::Error>(())
    }
    .await;
    stop(&host).await;
    // Restore the fixture revisions for the next independent scenario.
    f.client.execute(&format!("UPDATE {} SET revision=1", f.sources_table), &[]).await?;
    set_revision(&f.client, &f.version_table, 1).await?;
    result
}

async fn changed_version_fetch_failure_does_not_retry_cached_rows(f: &Fixture) -> Result<()> {
    let secret = f.root.join("fetch-failure-secret");
    f.client
        .execute(
            &format!("UPDATE {} SET database_secret=$1 WHERE source_id='alpha'", f.sources_table),
            &[&format!("file:{}", secret.display())],
        )
        .await?;
    let host = SourcesSupervisor::new(f.config.clone())?;
    host.refresh().await?;
    let mut completed = host.poll_completed_for_test();
    let unavailable = format!("{}_held", f.sources_table);
    f.client.batch_execute(&format!("ALTER TABLE {} RENAME TO {unavailable}", f.sources_table)).await?;
    set_revision(&f.client, &f.version_table, 2).await?;
    std::fs::write(&secret, &f.pg_url)?;
    host.spawn_poll();
    let result = async {
        tokio::time::timeout(Duration::from_secs(6), completed.changed()).await??;
        ensure!(
            host.engine_for_test("alpha").await.is_none(),
            "changed version with failed fetch started a stale cached row"
        );
        f.snapshot(&host.router(), "/sources/beta", false).await?;
        Ok::<(), anyhow::Error>(())
    }
    .await;
    stop(&host).await;
    f.client.batch_execute(&format!("ALTER TABLE {unavailable} RENAME TO {}", f.sources_table)).await?;
    result
}
