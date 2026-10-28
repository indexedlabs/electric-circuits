//! OTTO-6003 lifecycle contracts. Each ignored test uses a fresh process because the current
//! adapter caches its TTL in process statics. The parent serializes children and owns fixture cleanup.

use super::*;
use std::collections::BTreeSet;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::ensure;
use electric_circuits_engine::ds::DsClient;
use electric_circuits_engine::engine::{Engine, PostgresSetup};
use electric_circuits_engine::http::router_with_introspection;
use electric_circuits_engine::replication::Refused;
use electric_circuits_engine::table_ref::TableSelector;
use tokio::sync::watch;

#[path = "observability.rs"]
mod observability;

const SOURCES: [&str; 3] = ["alpha", "beta", "gamma"];
const RESTARTS: i64 = 50;
// Re-exec the test binary, not Cargo: the parent's herdr-heavy lock covers the entire run.
fn isolated(name: &str, case: Case) -> Result<()> {
    if isolated_contract(name, |command| {
        command
            .env("ELECTRIC_HANDLE_TTL", "1")
            .env("ELECTRIC_CIRCUITS_SUBQ_STORAGE", "1")
            .env_remove("ELECTRIC_CIRCUITS_SUBQ_STORAGE_DIR")
            .env("ELECTRIC_CIRCUITS_SUBSCRIPTION_LEASE_SECS", "0")
            .env("ELECTRIC_CIRCUITS_SHAPE_IDLE_SECS", "0");
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
            Case::GroupRetirement => group_retirement_reloads_and_closed_stream_is_conflict(&fixture).await,
            Case::GroupRestart => group_restart_restores_plain_and_drops_mixed(&fixture).await,
            Case::GroupSnapshotMoveOut => group_snapshot_only_member_moves_out(&fixture).await,
            Case::GroupMemberLsn => group_member_events_have_lsn(&fixture).await,
            Case::GroupReparent => group_companion_reparenting(&fixture).await,
            Case::GroupNeverMember => group_never_member_writes_are_silent(&fixture).await,
            Case::GroupSibling => group_disjoint_sibling_survives_inner_move_out(&fixture).await,
            Case::GroupPageHandoff => page_handoff_tombstone_contract(&fixture).await,
            Case::GroupPageCancellation => page_cancellation_contract(&fixture).await,
            Case::GroupConcurrentPageCancellation => concurrent_page_cancellation_contract(&fixture).await,
            Case::GroupRollbackCancellation => rollback_cancellation_contract(&fixture).await,
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
    GroupSnapshotMoveOut,
    GroupMemberLsn,
    GroupReparent,
    GroupNeverMember,
    GroupSibling,
    GroupPageHandoff,
    GroupPageCancellation,
    GroupConcurrentPageCancellation,
    GroupRollbackCancellation,
}

#[test]
#[ignore = "isolated PostgreSQL 18 required via ELECTRIC_CIRCUITS_TEST_PG_URL"]
fn group_never_member_insert_and_delete_do_not_wake_stream() -> Result<()> {
    isolated("ownership::group_never_member_insert_and_delete_do_not_wake_stream", Case::GroupNeverMember)
}

#[test]
#[ignore = "isolated PostgreSQL 18 required via ELECTRIC_CIRCUITS_TEST_PG_URL"]
fn group_disjoint_same_table_sibling_survives_move_out() -> Result<()> {
    isolated("ownership::group_disjoint_same_table_sibling_survives_move_out", Case::GroupSibling)
}

#[test]
#[ignore = "isolated PostgreSQL 18 required via ELECTRIC_CIRCUITS_TEST_PG_URL"]
fn group_page_handoff_preserves_newer_move_out() -> Result<()> {
    isolated("ownership::group_page_handoff_preserves_newer_move_out", Case::GroupPageHandoff)
}

#[test]
#[ignore = "isolated PostgreSQL 18 required via ELECTRIC_CIRCUITS_TEST_PG_URL"]
fn group_page_cancellation_releases_queryback_window() -> Result<()> {
    isolated("ownership::group_page_cancellation_releases_queryback_window", Case::GroupPageCancellation)
}

#[test]
#[ignore = "isolated PostgreSQL 18 required via ELECTRIC_CIRCUITS_TEST_PG_URL"]
fn concurrent_group_cancellations_preserve_both_durable_drops() -> Result<()> {
    isolated(
        "ownership::concurrent_group_cancellations_preserve_both_durable_drops",
        Case::GroupConcurrentPageCancellation,
    )
}

#[test]
#[ignore = "isolated PostgreSQL 18 required via ELECTRIC_CIRCUITS_TEST_PG_URL"]
fn group_canceled_rollback_retains_unreleased_group_members() -> Result<()> {
    isolated("ownership::group_canceled_rollback_retains_unreleased_group_members", Case::GroupRollbackCancellation)
}

#[test]
#[ignore = "isolated PostgreSQL 18 required via ELECTRIC_CIRCUITS_TEST_PG_URL"]
fn group_companion_reparenting_uses_selected_membership_mode() -> Result<()> {
    isolated("ownership::group_companion_reparenting_uses_selected_membership_mode", Case::GroupReparent)
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
fn group_retirement_reloads_and_closed_stream_is_409() -> Result<()> {
    isolated("ownership::group_retirement_reloads_and_closed_stream_is_409", Case::GroupRetirement)
}

#[test]
#[ignore = "isolated PostgreSQL 18 required via ELECTRIC_CIRCUITS_TEST_PG_URL"]
fn group_restart_restores_plain_and_drops_mixed_groups() -> Result<()> {
    isolated("ownership::group_restart_restores_plain_and_drops_mixed_groups", Case::GroupRestart)
}

#[test]
#[ignore = "isolated PostgreSQL 18 required via ELECTRIC_CIRCUITS_TEST_PG_URL"]
fn group_snapshot_only_member_emits_move_out() -> Result<()> {
    isolated("ownership::group_snapshot_only_member_emits_move_out", Case::GroupSnapshotMoveOut)
}

#[test]
#[ignore = "isolated PostgreSQL 18 required via ELECTRIC_CIRCUITS_TEST_PG_URL"]
fn group_membership_events_carry_commit_lsn() -> Result<()> {
    isolated("ownership::group_membership_events_carry_commit_lsn", Case::GroupMemberLsn)
}

#[derive(Clone)]
struct ObservedDs {
    store: FeedDs,
    changed: watch::Sender<u64>,
    retired: Arc<Mutex<Vec<(Method, String)>>>,
}

async fn observed_ds(State(ds): State<ObservedDs>, request: AxumRequest) -> Response {
    let method = request.method().clone();
    let path = request.uri().path().to_string();
    let closing = method == Method::POST && request.headers().contains_key("stream-closed");
    let response = feed_ds_handler(State(ds.store.clone()), request).await;
    if closing || method == Method::DELETE {
        ds.retired.lock().await.push((method.clone(), path));
    }
    if method == Method::POST || method == Method::DELETE {
        ds.changed.send_modify(|revision| *revision += 1);
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
            "table": outer,
            "columns": ["id", "body"],
            "where": {
                "col": "id",
                "in": { "table": inner, "project": "id" }
            }
        }));
    }
    serde_json::json!({ "subscription": subscription, "members": members })
}

// A Record table plus its companion table: one member per table, so a companion
// move-out is not masked by a sibling member that still includes the same row.
fn membership_group_body(f: &Fixture, subscription: &str) -> serde_json::Value {
    let mut body = group_body(f, subscription, false);
    body["members"][0] = serde_json::json!({
        "key": "records",
        "table": format!("public.{}", f.inner),
        "columns": ["id"]
    });
    body
}

async fn post_group(app: &Router, body: serde_json::Value) -> Result<(StatusCode, serde_json::Value)> {
    let response = app
        .clone()
        .oneshot(
            axum::http::Request::post("/v1/subset-loads")
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))?,
        )
        .await
        .map_err(|error| anyhow::anyhow!(error.to_string()))?;
    let status = response.status();
    let bytes = to_bytes(response.into_body(), 64 * 1024).await?;
    let body = serde_json::from_slice(&bytes)
        .with_context(|| format!("group response {status}: {}", String::from_utf8_lossy(&bytes)))?;
    Ok((status, body))
}

async fn group_engine(f: &Fixture) -> Result<(Engine, Router)> {
    let engine = Engine::new_pg_for_in_process_test_with_setup(
        DsClient::new_for_in_process_test(&f.ds_url),
        f.pg_url.clone(),
        PostgresSetup::ExternallyManaged,
    );
    let tables =
        [TableSelector::parse(&format!("public.{}", f.outer))?, TableSelector::parse(&format!("public.{}", f.inner))?];
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        match engine.setup_postgres(&tables, &f.slots[0]).await {
            Ok(()) => break,
            Err(error)
                if error.downcast_ref::<Refused>().is_some_and(|refused| matches!(refused, Refused::SlotBusy(_)))
                    && tokio::time::Instant::now() < deadline =>
            {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Err(error) => return Err(error),
        }
    }
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

async fn wait_group_event<F>(f: &Fixture, stream_path: &str, mut matches: F) -> Result<Vec<serde_json::Value>>
where
    F: FnMut(&serde_json::Value) -> bool,
{
    let mut changed = f.ds.changed.subscribe();
    let result = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let events = stream_events(f, stream_path).await;
            if events.iter().any(&mut matches) {
                return Ok(events);
            }
            changed.changed().await.context("group stream observation channel closed")?;
        }
    })
    .await;
    match result {
        Ok(result) => result,
        Err(_) => Err(anyhow::anyhow!(
            "waiting for expected group stream event timed out: {:?}",
            stream_events(f, stream_path).await
        )),
    }
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

async fn remove_source_fence(f: &Fixture) -> Result<()> {
    f.client.batch_execute("DROP TABLE IF EXISTS public.circuits_source_fence").await?;
    Ok(())
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
    let fence = remove_source_fence(f).await;
    result.and(fence)
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
    let fence = remove_source_fence(f).await;
    result.and(fence)
}

// The only source mutation removes a row from the inner set. The outer row came from
// the returned group snapshot and has never had a live upsert. After the source receipt,
// the stream must contain the delete needed to remove that snapshot row from a client.
// Do not wait for a delete here: a missing effect is an assertion failure, not a timeout.
async fn group_snapshot_only_member_moves_out(f: &Fixture) -> Result<()> {
    install_source_fence(f).await?;
    let (engine, app) = group_engine(f).await?;
    let result = async {
        let (status, body) = post_group(&app, membership_group_body(f, "group-snapshot-move-out")).await?;
        ensure!(status == StatusCode::OK, "group create failed: {status} {body}");
        let rows = body["results"][1]["rows"].as_array().context("companion snapshot rows")?;
        ensure!(rows.len() == 1, "expected the one snapshot-only companion row: {body}");
        let path = body["streamPath"].as_str().context("group stream path")?;
        let baseline = stream_events(f, path).await.len();
        let commit = uuid::Uuid::new_v4();
        f.client
            .batch_execute(&format!(
                "BEGIN; DELETE FROM public.{} WHERE id = 1; \
                 INSERT INTO public.circuits_source_fence VALUES ('{}'); COMMIT",
                f.inner, commit
            ))
            .await?;
        wait_source_receipt(&engine, f, commit).await?;
        let receipt = engine.source_drain_receipt(&commit.to_string()).context("source receipt disappeared")?;
        let events = stream_events(f, path).await;
        let expected_type = format!("public.{}", f.outer);
        ensure!(
            events.iter().skip(baseline).any(|event| {
                event_key(event).as_deref() == Some("1")
                    && event_operation(event) == Some("delete")
                    && event["type"].as_str() == Some(expected_type.as_str())
            }),
            "snapshot-only companion remained after its source receipt: {events:?}"
        );
        for event in events.iter().skip(baseline) {
            ensure!(
                event["headers"]["lsn"].as_str() == Some(receipt.commit_lsn.as_str()),
                "move-out event LSN differs from source commit {}: {event}",
                receipt.commit_lsn
            );
        }
        Ok::<_, anyhow::Error>(())
    }
    .await;
    engine.close(Duration::from_secs(10)).await;
    let fence = remove_source_fence(f).await;
    result.and(fence)
}

// Hold the real group request twice so a source transaction can be placed on each side of the
// page/feed handoff: the new row is committed after the private changes-only seed but before the
// public page, then its outer row is deleted after the page and before page keys are installed.
async fn page_handoff_tombstone_contract(f: &Fixture) -> Result<()> {
    install_source_fence(f).await?;
    let barrier = f.root.join("group-page-handoff");
    let base = barrier.to_string_lossy().into_owned();
    unsafe { std::env::set_var("ELECTRIC_CIRCUITS_TEST_GROUP_PAGE_BARRIER", &base) };
    let (engine, app) = group_engine(f).await?;
    let result = async {
        // Keep the source reader behind the page so the row committed after the private seed is
        // visible to SQL but still absent from the member's FeedSet when the page is returned.
        engine.pause_reads_for_test().await?;
        let request = membership_group_body(f, "group-page-handoff");
        let task = tokio::spawn({
            let app = app.clone();
            async move { post_group(&app, request).await }
        });
        async fn wait_ready(base: &str, stage: &str) -> Result<()> {
            let base = base.to_owned();
            let stage = stage.to_owned();
            let path = PathBuf::from(format!("{base}.{stage}.ready"));
            tokio::time::timeout(Duration::from_secs(15), async {
                loop {
                    if path.exists() {
                        return Ok::<_, anyhow::Error>(());
                    }
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            })
            .await
            .context(format!("waiting for group {stage} barrier"))??;
            Ok(())
        }
        wait_ready(&base, "private").await?;
        let entered = uuid::Uuid::new_v4();
        // Keep the outer insert before the inner insert deliberately. T's first envelope is
        // processed before the inner key makes the subquery member eligible; reversing these
        // statements would turn this into an ordinary move-in/delete path.
        f.client
            .batch_execute(&format!(
                "BEGIN; INSERT INTO public.{} (id, body) VALUES (3, 'entered between seeds'); \
                 INSERT INTO public.{} (id) VALUES (3); \
                 INSERT INTO public.circuits_source_fence VALUES ('{}'); COMMIT",
                f.outer, f.inner, entered
            ))
            .await?;
        std::fs::write(format!("{base}.private.release"), b"release")?;

        wait_ready(&base, "page").await?;
        let left = uuid::Uuid::new_v4();
        f.client
            .batch_execute(&format!(
                "BEGIN; DELETE FROM public.{} WHERE id = 3; \
                 INSERT INTO public.circuits_source_fence VALUES ('{}'); COMMIT",
                f.outer, left
            ))
            .await?;
        engine.resume_reads_for_test().await?;
        wait_source_receipt(&engine, f, left).await?;
        let receipt = engine.source_drain_receipt(&left.to_string()).context("move-out receipt disappeared")?;
        std::fs::write(format!("{base}.page.release"), b"release")?;

        let (status, body) = task.await.context("group request task panicked")??;
        ensure!(status == StatusCode::OK, "group create failed: {status} {body}");
        ensure!(
            body["results"][1]["rows"].as_array().is_some_and(|rows| rows.iter().any(|row| row["id"] == 3)),
            "page did not observe the row inserted between private seed and page: {body}"
        );
        let path = body["streamPath"].as_str().context("group stream path")?;
        let table = format!("public.{}", f.outer);
        let expected_lsn = receipt.commit_lsn.clone();
        let events = wait_group_event(f, path, |event| {
            event_key(event).as_deref() == Some("3")
                && event_operation(event) == Some("delete")
                && event["type"].as_str() == Some(table.as_str())
                && event["headers"]["lsn"].as_str() == Some(expected_lsn.as_str())
        })
        .await?;
        ensure!(
            events.iter().any(|event| {
                event_key(event).as_deref() == Some("3")
                    && event_operation(event) == Some("delete")
                    && event["type"].as_str() == Some(table.as_str())
                    && event["headers"]["lsn"].as_str() == Some(expected_lsn.as_str())
            }),
            "page-visible row moved out before key handoff without a fenced tombstone: {events:?}"
        );
        Ok::<_, anyhow::Error>(())
    }
    .await;
    unsafe { std::env::remove_var("ELECTRIC_CIRCUITS_TEST_GROUP_PAGE_BARRIER") };
    engine.close(Duration::from_secs(10)).await;
    let fence = remove_source_fence(f).await;
    result.and(fence)
}

async fn page_cancellation_contract(f: &Fixture) -> Result<()> {
    for (index, cancel_stage) in ["member", "private", "page", "complete"].into_iter().enumerate() {
        install_source_fence(f).await?;
        let barrier = f.root.join(format!("group-page-cancel-{index}"));
        let base = barrier.to_string_lossy().into_owned();
        unsafe { std::env::set_var("ELECTRIC_CIRCUITS_TEST_GROUP_PAGE_BARRIER", &base) };
        unsafe { std::env::set_var("ELECTRIC_CIRCUITS_TEST_GROUP_PAGE_BARRIER_STAGE", cancel_stage) };
        let (engine, app) = group_engine(f).await?;
        let subscription = format!("group-page-cancel-{index}");
        let case_result = async {
            let request = if cancel_stage == "complete" {
                group_body(f, &subscription, true)
            } else {
                membership_group_body(f, &subscription)
            };
            let task = tokio::spawn({
                let app = app.clone();
                async move { post_group(&app, request).await }
            });
            async fn wait_ready(base: &str, stage: &str) -> Result<()> {
                let path = PathBuf::from(format!("{base}.{stage}.ready"));
                tokio::time::timeout(Duration::from_secs(15), async {
                    loop {
                        if path.exists() {
                            return Ok::<_, anyhow::Error>(());
                        }
                        tokio::time::sleep(Duration::from_millis(5)).await;
                    }
                })
                .await
                .context(format!("waiting for cancelled group {stage} barrier"))??;
                Ok(())
            }
            let stage_result = match cancel_stage {
                "member" => wait_ready(&base, "member").await,
                "private" => wait_ready(&base, "private").await,
                "page" => wait_ready(&base, "page").await,
                "complete" => wait_ready(&base, "complete").await,
                _ => unreachable!("unknown group cancellation stage"),
            };
            task.abort();
            let _ = task.await;
            tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    if engine.subquery_inflight_querybacks_for_test().await == 0 {
                        return;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .context(format!("cancelled group {cancel_stage} left queryback state armed"))?;
            stage_result?;
            Ok::<_, anyhow::Error>(())
        }
        .await;
        unsafe { std::env::remove_var("ELECTRIC_CIRCUITS_TEST_GROUP_PAGE_BARRIER") };
        unsafe { std::env::remove_var("ELECTRIC_CIRCUITS_TEST_GROUP_PAGE_BARRIER_STAGE") };
        let close_outcome = engine.close(Duration::from_secs(10)).await;
        let fence_result = remove_source_fence(f).await;
        let replacement_result = async {
            let (replacement, replacement_app) = match group_engine(f).await {
                Ok(value) => value,
                Err(error) => return Err(error),
            };
            // Reusing the exact public claim is the observable proof that every private member
            // claim was released. A durable GroupCreated from the completion window must also be
            // canceled before this replacement engine can accept the same group subscription.
            let retry_deadline = tokio::time::Instant::now() + Duration::from_secs(15);
            let mut retry_body = None;
            let retry_result = async {
                loop {
                    let request = if cancel_stage == "complete" {
                        group_body(f, &subscription, true)
                    } else {
                        membership_group_body(f, &subscription)
                    };
                    let attempt = post_group(&replacement_app, request).await?;
                    if attempt.0 == StatusCode::OK {
                        retry_body = Some(attempt);
                        break;
                    }
                    ensure!(
                        attempt.0 == StatusCode::CONFLICT,
                        "retry of canceled group failed: {} {}",
                        attempt.0,
                        attempt.1
                    );
                    if tokio::time::Instant::now() >= retry_deadline {
                        ensure!(false, "retry of canceled group {cancel_stage} failed: {} {}", attempt.0, attempt.1);
                    }
                    tokio::time::sleep(Duration::from_millis(25)).await;
                }
                Ok::<_, anyhow::Error>(())
            }
            .await;
            let replacement_close = replacement.close(Duration::from_secs(10)).await;
            retry_result?;
            let retry_body = retry_body.expect("successful group retry recorded");
            ensure!(
                retry_body.0 == StatusCode::OK,
                "retry of canceled group {cancel_stage} failed: {} {}",
                retry_body.0,
                retry_body.1
            );
            ensure!(
                replacement_close == electric_circuits_engine::shutdown::ShutdownOutcome::Complete,
                "replacement engine after canceled group {cancel_stage} did not close cleanly"
            );
            Ok::<_, anyhow::Error>(())
        }
        .await;
        let mut errors = Vec::new();
        if close_outcome != electric_circuits_engine::shutdown::ShutdownOutcome::Complete {
            errors.push(format!("cancelled group {cancel_stage} did not close cleanly: {close_outcome:?}"));
        }
        if let Err(error) = case_result {
            errors.push(format!("cancelled group {cancel_stage}: {error:#}"));
        }
        if let Err(error) = replacement_result {
            errors.push(format!("retry after cancelled group {cancel_stage}: {error:#}"));
        }
        if let Err(error) = fence_result {
            errors.push(format!("remove source fence after cancelled group {cancel_stage}: {error:#}"));
        }
        if !errors.is_empty() {
            anyhow::bail!("{}", errors.join("\n"));
        }
    }
    Ok(())
}

async fn concurrent_page_cancellation_contract(f: &Fixture) -> Result<()> {
    install_source_fence(f).await?;
    let barrier = f.root.join("group-page-cancel-concurrent");
    let base = barrier.to_string_lossy().into_owned();
    let subscriptions = ["group-page-cancel-concurrent-a".to_string(), "group-page-cancel-concurrent-b".to_string()];
    unsafe { std::env::set_var("ELECTRIC_CIRCUITS_TEST_GROUP_PAGE_BARRIER", &base) };
    unsafe { std::env::set_var("ELECTRIC_CIRCUITS_TEST_GROUP_PAGE_BARRIER_STAGE", "complete") };
    let (engine, app) = group_engine(f).await?;
    let result = async {
        let mut tasks = Vec::new();
        for subscription in &subscriptions {
            let app = app.clone();
            let request = group_body(f, subscription, true);
            tasks.push(tokio::spawn(async move { post_group(&app, request).await }));
        }
        let ready = PathBuf::from(format!("{base}.complete.ready"));
        let ready_result = tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                let count = std::fs::read_to_string(&ready).map_or(0, |contents| contents.lines().count());
                if count >= subscriptions.len() {
                    return Ok::<_, anyhow::Error>(());
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .context("waiting for both cancelled groups to reach complete barrier")?;
        for task in tasks {
            task.abort();
            let _ = task.await;
        }
        ready_result?;
        Ok::<_, anyhow::Error>(())
    }
    .await;
    unsafe { std::env::remove_var("ELECTRIC_CIRCUITS_TEST_GROUP_PAGE_BARRIER") };
    unsafe { std::env::remove_var("ELECTRIC_CIRCUITS_TEST_GROUP_PAGE_BARRIER_STAGE") };
    let close = engine.close(Duration::from_secs(10)).await;
    let fence = remove_source_fence(f).await;
    let replacement = async {
        let (replacement, replacement_app) = group_engine(f).await?;
        let retry = async {
            for subscription in &subscriptions {
                let (status, body) = post_group(&replacement_app, group_body(f, subscription, true)).await?;
                ensure!(status == StatusCode::OK, "cancelled group claim survived restart: {status} {body}");
            }
            Ok::<_, anyhow::Error>(())
        }
        .await;
        let replacement_close = replacement.close(Duration::from_secs(10)).await;
        retry?;
        ensure!(
            replacement_close == electric_circuits_engine::shutdown::ShutdownOutcome::Complete,
            "replacement engine after concurrent cancellation did not close cleanly"
        );
        Ok::<_, anyhow::Error>(())
    }
    .await;
    let mut errors = Vec::new();
    if close != electric_circuits_engine::shutdown::ShutdownOutcome::Complete {
        errors.push(format!("concurrent cancellation engine did not close cleanly: {close:?}"));
    }
    if let Err(error) = fence {
        errors.push(format!("remove source fence after concurrent cancellation: {error:#}"));
    }
    if let Err(error) = result {
        errors.push(format!("concurrent cancellation contract: {error:#}"));
    }
    if let Err(error) = replacement {
        errors.push(format!("retry after concurrent cancellation: {error:#}"));
    }
    if errors.is_empty() {
        Ok(())
    } else {
        anyhow::bail!("{}", errors.join("\n"));
    }
}

async fn rollback_cancellation_contract(f: &Fixture) -> Result<()> {
    install_source_fence(f).await?;
    let barrier = f.root.join("group-rollback-cancel");
    let base = barrier.to_string_lossy().into_owned();
    let subscription = "group-rollback-cancel";
    unsafe { std::env::set_var("ELECTRIC_CIRCUITS_TEST_GROUP_PAGE_BARRIER", &base) };
    unsafe { std::env::set_var("ELECTRIC_CIRCUITS_TEST_GROUP_PAGE_BARRIER_STAGE", "rollback") };
    unsafe { std::env::set_var("ELECTRIC_CIRCUITS_TEST_GROUP_PAGE_FAIL_STAGE", "member_release") };
    let (engine, app) = group_engine(f).await?;
    let result = async {
        let request = group_body(f, subscription, true);
        let task = tokio::spawn({
            let app = app.clone();
            async move { post_group(&app, request).await }
        });
        let ready = PathBuf::from(format!("{base}.rollback.ready"));
        tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                if std::fs::read_to_string(&ready).map_or(0, |contents| contents.lines().count()) >= 1 {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .context("waiting for rollback cancellation barrier")?;
        unsafe { std::env::set_var("ELECTRIC_CIRCUITS_TEST_RELEASE_AFTER_UNSUBSCRIBE", "once") };
        let fired = format!("{base}.rollback.release-fired");
        unsafe { std::env::set_var("ELECTRIC_CIRCUITS_TEST_RELEASE_AFTER_UNSUBSCRIBE_MARKER", &fired) };
        std::fs::write(format!("{base}.rollback.release"), b"1")?;
        tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                if std::fs::read_to_string(&ready).map_or(0, |contents| contents.lines().count()) >= 2 {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .context("waiting for second rollback cancellation barrier")?;
        task.abort();
        ensure!(task.await.is_err(), "rollback request unexpectedly completed before cancellation");
        ensure!(
            std::fs::read_to_string(&fired).is_ok(),
            "durable-release fault injection did not fire"
        );
        Ok::<_, anyhow::Error>(())
    }
    .await;
    unsafe { std::env::remove_var("ELECTRIC_CIRCUITS_TEST_GROUP_PAGE_BARRIER") };
    unsafe { std::env::remove_var("ELECTRIC_CIRCUITS_TEST_GROUP_PAGE_BARRIER_STAGE") };
    unsafe { std::env::remove_var("ELECTRIC_CIRCUITS_TEST_GROUP_PAGE_FAIL_STAGE") };
    unsafe { std::env::remove_var("ELECTRIC_CIRCUITS_TEST_RELEASE_AFTER_UNSUBSCRIBE") };
    unsafe { std::env::remove_var("ELECTRIC_CIRCUITS_TEST_RELEASE_AFTER_UNSUBSCRIBE_MARKER") };
    let close = engine.close(Duration::from_secs(10)).await;
    let fence = remove_source_fence(f).await;
    let replacement = async {
        let (replacement, replacement_app) = group_engine(f).await?;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
        let mut retry_body = None;
        loop {
            let attempt = post_group(&replacement_app, group_body(f, subscription, true)).await?;
            if attempt.0 == StatusCode::OK {
                retry_body = Some(attempt);
                break;
            }
            ensure!(
                attempt.0 == StatusCode::CONFLICT,
                "retry after rollback cancellation failed: {} {}",
                attempt.0,
                attempt.1
            );
            if tokio::time::Instant::now() >= deadline {
                anyhow::bail!(
                    "retry after rollback cancellation did not release the group claim: {} {}",
                    attempt.0,
                    attempt.1
                );
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        let replacement_close = replacement.close(Duration::from_secs(10)).await;
        ensure!(retry_body.is_some(), "successful rollback retry was not recorded");
        ensure!(
            replacement_close == electric_circuits_engine::shutdown::ShutdownOutcome::Complete,
            "replacement engine after rollback cancellation did not close cleanly"
        );
        Ok::<_, anyhow::Error>(())
    }
    .await;
    let mut errors = Vec::new();
    if close != electric_circuits_engine::shutdown::ShutdownOutcome::Complete {
        errors.push(format!("rollback cancellation engine did not close cleanly: {close:?}"));
    }
    if let Err(error) = result {
        errors.push(format!("rollback cancellation: {error:#}"));
    }
    if let Err(error) = replacement {
        errors.push(format!("retry after rollback cancellation: {error:#}"));
    }
    if let Err(error) = fence {
        errors.push(format!("remove source fence after rollback cancellation: {error:#}"));
    }
    if errors.is_empty() {
        Ok(())
    } else {
        anyhow::bail!("{}", errors.join("\n"));
    }
}

async fn group_member_events_have_lsn(f: &Fixture) -> Result<()> {
    install_source_fence(f).await?;
    let (engine, app) = group_engine(f).await?;
    let result = async {
        let (status, body) = post_group(&app, membership_group_body(f, "group-member-lsn")).await?;
        ensure!(status == StatusCode::OK, "group create failed: {status} {body}");
        let path = body["streamPath"].as_str().context("group stream path")?;
        let baseline = stream_events(f, path).await.len();
        let commit = uuid::Uuid::new_v4();
        f.client
            .batch_execute(&format!(
                "BEGIN; INSERT INTO public.{} (id) VALUES (2); \
                 INSERT INTO public.circuits_source_fence VALUES ('{}'); COMMIT",
                f.inner, commit
            ))
            .await?;
        wait_source_receipt(&engine, f, commit).await?;
        let receipt = engine.source_drain_receipt(&commit.to_string()).context("source receipt disappeared")?;
        let events = stream_events(f, path).await;
        let delivered: Vec<_> = events.iter().skip(baseline).collect();
        let expected_type = format!("public.{}", f.outer);
        ensure!(
            delivered.iter().any(|event| {
                event_key(event).as_deref() == Some("2")
                    && event_operation(event) == Some("upsert")
                    && event["type"].as_str() == Some(expected_type.as_str())
            }),
            "move-in produced no companion event after source receipt: {events:?}"
        );
        for event in delivered {
            ensure!(
                event["headers"]["lsn"].as_str() == Some(receipt.commit_lsn.as_str()),
                "move-in event LSN differs from source commit {}: {event}",
                receipt.commit_lsn
            );
        }
        Ok::<_, anyhow::Error>(())
    }
    .await;
    engine.close(Duration::from_secs(10)).await;
    let fence = remove_source_fence(f).await;
    result.and(fence)
}

// A stable companion primary key moves between a Record in the scope and one
// outside it. Observe the public stream after each source receipt, including a
// return to the original Record; an ordinal left in DBSP must not mask a delete.
async fn group_companion_reparenting(f: &Fixture) -> Result<()> {
    f.client
        .batch_execute(&format!(
            "ALTER TABLE public.{} ADD COLUMN parent_id integer; \
             UPDATE public.{} SET parent_id = id",
            f.outer, f.outer
        ))
        .await?;
    install_source_fence(f).await?;
    let (engine, app) = group_engine(f).await?;
    let result = async {
        let mut request = membership_group_body(f, "group-reparent");
        request["members"][1]["where"]["col"] = serde_json::json!("parent_id");
        let (status, body) = post_group(&app, request).await?;
        ensure!(status == StatusCode::OK, "group create failed: {status} {body}");
        let path = body["streamPath"].as_str().context("group stream path")?;
        let table = format!("public.{}", f.outer);
        for (parent, operation) in [(2, "delete"), (1, "upsert"), (2, "delete")] {
            let baseline = stream_events(f, path).await.len();
            let commit = uuid::Uuid::new_v4();
            f.client
                .batch_execute(&format!(
                    "BEGIN; UPDATE public.{} SET parent_id = {} WHERE id = 1; \
                     INSERT INTO public.circuits_source_fence VALUES ('{}'); COMMIT",
                    f.outer, parent, commit
                ))
                .await?;
            wait_source_receipt(&engine, f, commit).await?;
            let events = stream_events(f, path).await;
            let latest = events.iter().skip(baseline).rev().find(|event| {
                event["type"].as_str() == Some(table.as_str()) && event_key(event).as_deref() == Some("1")
            });
            ensure!(
                latest.and_then(event_operation) == Some(operation),
                "reparent to {parent} expected {operation} after receipt: {events:?}"
            );
        }
        // Diagnostic coverage, separate from the public correctness oracle above:
        // neither CI nor the benchmark may silently compare the baseline to itself.
        let profile = engine.dbsp_profile_dump().await;
        let expected = std::env::var("ELECTRIC_CIRCUITS_GROUP_MEMBERSHIP").unwrap_or_else(|_| "subquery".into());
        ensure!(profile["group_membership"]["mode"].as_str() == Some(expected.as_str()), "wrong mode: {profile}");
        if expected == "semijoin" {
            ensure!(
                profile["group_membership"]["selectionBatches"].as_u64().unwrap_or(0) > 0,
                "semijoin did not select any candidates: {profile}"
            );
        }
        Ok::<_, anyhow::Error>(())
    }
    .await;
    engine.close(Duration::from_secs(10)).await;
    let fence = remove_source_fence(f).await;
    result.and(fence)
}

async fn group_never_member_writes_are_silent(f: &Fixture) -> Result<()> {
    install_source_fence(f).await?;
    let (engine, app) = group_engine(f).await?;
    let result = async {
        let (status, body) = post_group(&app, membership_group_body(f, "group-no-wake")).await?;
        ensure!(status == StatusCode::OK, "group create failed: {status} {body}");
        let path = body["streamPath"].as_str().context("group stream path")?;
        let baseline = stream_events(f, path).await;
        for mutation in [
            format!("INSERT INTO public.{} (id, body) VALUES (3, 'never in this scope')", f.outer),
            format!("DELETE FROM public.{} WHERE id = 3", f.outer),
        ] {
            let commit = uuid::Uuid::new_v4();
            f.client
                .batch_execute(&format!(
                    "BEGIN; {mutation}; INSERT INTO public.circuits_source_fence VALUES ('{commit}'); COMMIT"
                ))
                .await?;
            wait_source_receipt(&engine, f, commit).await?;
            let events = stream_events(f, path).await;
            ensure!(events == baseline, "never-member mutation woke group after its receipt: {mutation}: {events:?}");
        }
        Ok::<_, anyhow::Error>(())
    }
    .await;
    engine.close(Duration::from_secs(10)).await;
    let fence = remove_source_fence(f).await;
    result.and(fence)
}

async fn group_disjoint_sibling_survives_inner_move_out(f: &Fixture) -> Result<()> {
    f.client.batch_execute(&format!("ALTER TABLE public.{} ADD COLUMN parent_id integer DEFAULT 1", f.outer)).await?;
    install_source_fence(f).await?;
    let (engine, app) = group_engine(f).await?;
    let result = async {
        let mut request = group_body(f, "group-sibling", false);
        request["members"][0]["where"] = serde_json::json!({"col": "body", "op": "eq", "value": "kept"});
        request["members"][1]["where"] = serde_json::json!({"and": [
            {"col": "body", "op": "eq", "value": "excluded"},
            {"col": "parent_id", "in": {"table": format!("public.{}", f.inner), "project": "id"}}
        ]});
        let (status, body) = post_group(&app, request).await?;
        ensure!(status == StatusCode::OK, "group create failed: {status} {body}");
        ensure!(body["results"][0]["rows"][0]["id"] == 1, "plain snapshot fixture: {body}");
        ensure!(body["results"][1]["rows"][0]["id"] == 2, "companion snapshot fixture: {body}");
        let path = body["streamPath"].as_str().context("group stream path")?;
        let baseline = stream_events(f, path).await.len();
        let commit = uuid::Uuid::new_v4();
        f.client
            .batch_execute(&format!(
                "BEGIN; DELETE FROM public.{} WHERE id = 1; \
                 INSERT INTO public.circuits_source_fence VALUES ('{}'); COMMIT",
                f.inner, commit
            ))
            .await?;
        wait_source_receipt(&engine, f, commit).await?;
        let events = stream_events(f, path).await;
        let table = format!("public.{}", f.outer);
        let delivered: Vec<_> =
            events.iter().skip(baseline).filter(|e| e["type"].as_str() == Some(table.as_str())).collect();
        ensure!(
            delivered.iter().any(|e| event_key(e).as_deref() == Some("2") && event_operation(e) == Some("delete")),
            "subquery sibling did not move out: {events:?}"
        );
        ensure!(
            !delivered.iter().any(|e| event_key(e).as_deref() == Some("1") && event_operation(e) == Some("delete")),
            "unmodified plain sibling was deleted by a disjoint subquery member: {events:?}"
        );
        Ok::<_, anyhow::Error>(())
    }
    .await;
    engine.close(Duration::from_secs(10)).await;
    let fence = remove_source_fence(f).await;
    result.and(fence)
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
    let fence = remove_source_fence(f).await;
    result.and(fence)
}

async fn group_claim_overlap_is_atomic(f: &Fixture) -> Result<()> {
    let (engine, app) = group_engine(f).await?;
    let result = async {
        let (status, first) = post_group(&app, group_body(f, "group-overlap", false)).await?;
        ensure!(status == StatusCode::OK, "first group create failed: {status} {first}");
        let (status, second) = post_group(&app, group_body(f, "group-overlap", true)).await?;
        ensure!(status == StatusCode::CONFLICT, "overlapping group claim was not 409: {status} {second}");
        let (status, third) = post_group(&app, group_body(f, "group-overlap-2", true)).await?;
        ensure!(status == StatusCode::OK, "failed group left reservation behind: {status} {third}");
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
        ensure!(
            body["results"][0]["rows"].as_array().map_or(0, Vec::len) as i64 == sql_count,
            "plain page crossed snapshot fence: {body}"
        );
        Ok::<_, anyhow::Error>(())
    }
    .await;
    engine.close(Duration::from_secs(10)).await;
    result
}

async fn group_retirement_reloads_and_closed_stream_is_conflict(f: &Fixture) -> Result<()> {
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
        let (status, closed) = post_group(&app, group_body(f, "group-retire-closed", false)).await?;
        ensure!(status == StatusCode::OK, "a fresh group must reload after retirement: {status} {closed}");
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
        ensure!(status == StatusCode::CONFLICT, "durable plain group was not restored: {status} {duplicate_plain}");
        let (status, recreated_mixed) = post_group(&restarted_app, group_body(f, "group-mixed-restart", false)).await?;
        ensure!(
            status == StatusCode::OK,
            "mixed group was not dropped as a unit on restart: {status} {recreated_mixed}"
        );
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
