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
        wait_source_receipt(&engine, f, t).await?;
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
