//! Multi-source discovery, reconciliation, and source-scoped HTTP serving.
//!
//! The control database/file is the sole desired-state authority. This module deliberately keeps
//! no durable source registry: the desired rows are fetched again on refresh and the poll only reads
//! the version row when its revision has not changed.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use axum::Router;
use axum::body::{Body, Bytes};
use axum::extract::{Path as AxumPath, Request, State};
use axum::http::{StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, oneshot};
use tower::ServiceExt;

use crate::config::{Config, SourcesConfig, SourcesMode};
use crate::ds::DsClient;
use crate::engine::{Engine, PostgresSetup};
use crate::shutdown::ShutdownToken;
use crate::store_identity::{StoreBound, StreamScope};
use crate::table_ref::TableSelector;

/// One row from the control-plane sources relation or its JSON file equivalent.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct SourceRow {
    pub source_id: String,
    pub plugin: String,
    pub database_secret: String,
    pub slot: String,
    pub publication: String,
    pub tables: Vec<String>,
    pub revision: i64,
    pub updated_at: String,
}

/// The subset of a running source needed by the pure reconciliation law.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RunningSourceState {
    pub revision: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PlanAction {
    Start(SourceRow),
    Stop(String),
    Restart(SourceRow),
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Plan {
    pub actions: Vec<PlanAction>,
}

/// Compute the idempotent source lifecycle plan for one fetched row set.
pub fn reconcile(running: &BTreeMap<String, RunningSourceState>, rows: &[SourceRow]) -> Plan {
    let desired: BTreeMap<&str, &SourceRow> = rows.iter().map(|row| (row.source_id.as_str(), row)).collect();
    let mut actions = Vec::new();
    for (source_id, state) in running {
        match desired.get(source_id.as_str()) {
            None => actions.push(PlanAction::Stop(source_id.clone())),
            Some(row) if row.revision != state.revision => actions.push(PlanAction::Restart((*row).clone())),
            Some(_) => {}
        }
    }
    for (source_id, row) in desired {
        if !running.contains_key(source_id) {
            actions.push(PlanAction::Start(row.clone()));
        }
    }
    Plan { actions }
}

/// Refresh plan: failed desired rows are started again even when their revision is unchanged.
/// Healthy running rows with the same revision remain no-ops.
pub fn reconcile_refresh(
    running: &BTreeMap<String, RunningSourceState>,
    failed: &BTreeMap<String, RunningSourceState>,
    rows: &[SourceRow],
) -> Plan {
    let mut effective_running = running.clone();
    for source_id in failed.keys() {
        effective_running.remove(source_id);
    }
    reconcile(&effective_running, rows)
}

#[derive(Clone)]
pub struct SourcesSupervisor {
    inner: Arc<SupervisorInner>,
}

struct SupervisorInner {
    #[cfg(feature = "test-support")]
    poll_completed: tokio::sync::watch::Sender<()>,
    restart_notify: Arc<tokio::sync::Notify>,
    config: Config,
    sources: SourcesConfig,
    state: Mutex<SupervisorState>,
    reconcile: Mutex<()>,
    shutdown: ShutdownToken,
    poll_task: std::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
}

struct SupervisorState {
    running: BTreeMap<String, SourceRuntime>,
    failed: BTreeMap<String, FailedSource>,
    last_revision: Option<i64>,
    fetched: bool,
}

struct SourceRuntime {
    row: SourceRow,
    engine: Engine,
    router: Router,
    worker: Option<std::thread::JoinHandle<()>>,
}

#[derive(Clone)]
struct FailedSource {
    row: SourceRow,
    error: String,
}

#[derive(Clone, Debug, Serialize)]
pub struct SourceSummary {
    pub source_id: String,
    pub revision: i64,
    pub ready: bool,
    pub error: Option<String>,
}

struct SourceSnapshot {
    revision: i64,
    rows: Vec<SourceRow>,
}

impl SourcesSupervisor {
    pub fn new(config: Config) -> Result<Self> {
        let sources = config.sources.clone().context("sources supervisor requires SOURCES_MODE")?;
        if matches!(sources.mode, SourcesMode::Table) && sources.pg_url.is_none() {
            bail!("sources table mode requires a control database URL");
        }
        Ok(Self {
            inner: Arc::new(SupervisorInner {
                #[cfg(feature = "test-support")]
                poll_completed: tokio::sync::watch::channel(()).0,
                restart_notify: Arc::new(tokio::sync::Notify::new()),
                config,
                sources,
                state: Mutex::new(SupervisorState {
                    running: BTreeMap::new(),
                    failed: BTreeMap::new(),
                    last_revision: None,
                    fetched: false,
                }),
                reconcile: Mutex::new(()),
                shutdown: ShutdownToken::new(),
                poll_task: std::sync::Mutex::new(None),
            }),
        })
    }

    /// Build the host router. The source router is forwarded with a rewritten URI rather than
    /// nested: Axum's nested path captures otherwise leak into the engine's own Path extractors.
    pub fn router(&self) -> Router {
        let state = self.clone();
        Router::new()
            .route("/health", axum::routing::get(health))
            .route("/ready", axum::routing::get(ready))
            .route("/sources", axum::routing::get(list_sources))
            .route("/sources/{source_id}/status", axum::routing::get(source_status))
            .route("/sources/{source_id}", axum::routing::any(proxy_root))
            .route("/sources/{source_id}/{*path}", axum::routing::any(proxy_path))
            .route("/admin/refresh", axum::routing::post(refresh))
            .with_state(state)
    }

    pub fn shutdown_token(&self) -> ShutdownToken {
        self.inner.shutdown.clone()
    }

    /// Borrow a source Engine for lifecycle observations; callers must drop the clone before stop.
    #[cfg(feature = "test-support")]
    #[doc(hidden)]
    pub async fn engine_for_test(&self, source_id: &str) -> Option<Engine> {
        self.inner.state.lock().await.running.get(source_id).map(|runtime| runtime.engine.clone())
    }

    #[cfg(any(test, feature = "test-support"))]
    #[doc(hidden)]
    pub async fn with_test_source(config: Config, row: SourceRow, engine: Engine) -> Result<Self> {
        let supervisor = Self::new(config)?;
        let router = crate::http::router_with_introspection(engine.clone(), false);
        {
            let mut state = supervisor.inner.state.lock().await;
            state.running.insert(row.source_id.clone(), SourceRuntime { row, engine, router, worker: None });
            state.last_revision = state.running.values().map(|runtime| runtime.row.revision).max();
            state.fetched = true;
        }
        Ok(supervisor)
    }

    #[cfg(any(test, feature = "test-support"))]
    #[doc(hidden)]
    pub async fn with_test_failed_source(config: Config, row: SourceRow, error: impl Into<String>) -> Result<Self> {
        let supervisor = Self::new(config)?;
        {
            let mut state = supervisor.inner.state.lock().await;
            state.last_revision = Some(row.revision);
            state.failed.insert(row.source_id.clone(), FailedSource { row, error: error.into() });
            state.fetched = true;
        }
        Ok(supervisor)
    }

    pub async fn initial_fetch_until_ready(&self) -> Result<()> {
        let mut attempt = 0u32;
        loop {
            match self.refresh().await {
                Ok(_) => return Ok(()),
                Err(error) => {
                    attempt = attempt.saturating_add(1);
                    let delay =
                        crate::replication::backoff_base(attempt.saturating_sub(1)).min(Duration::from_secs(30));
                    tracing::warn!(attempt, delay = ?delay, error = %error, "sources discovery unavailable; retrying");
                    tokio::select! {
                        _ = tokio::time::sleep(delay) => {}
                        _ = self.inner.shutdown.wait() => {
                            bail!("sources discovery interrupted by shutdown");
                        }
                    }
                }
            }
        }
    }

    /// Fetch and reconcile unconditionally. The lock covers the fetch and all lifecycle changes,
    /// so concurrent refreshes cannot interleave plans.
    pub async fn refresh(&self) -> Result<i64> {
        if self.inner.shutdown.is_shutting_down() {
            bail!("sources refresh interrupted by shutdown");
        }
        let _guard = self.inner.reconcile.lock().await;
        if self.inner.shutdown.is_shutting_down() {
            bail!("sources refresh interrupted by shutdown");
        }
        let snapshot = self.fetch_snapshot().await?;
        self.apply_snapshot(snapshot).await
    }

    /// Observe completed real poll ticks without requesting a refresh or driving reconciliation.
    #[cfg(feature = "test-support")]
    #[doc(hidden)]
    pub fn poll_completed_for_test(&self) -> tokio::sync::watch::Receiver<()> {
        self.inner.poll_completed.subscribe()
    }

    pub fn spawn_poll(&self) {
        let supervisor = self.clone();
        let handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(supervisor.inner.sources.poll);
            interval.tick().await;
            loop {
                tokio::select! {
                    biased;
                    _ = supervisor.inner.shutdown.wait() => break,
                    _ = supervisor.inner.restart_notify.notified() => {
                        supervisor.restart_failed_engines().await;
                    }
                    _ = interval.tick() => {
                        if supervisor.inner.shutdown.is_shutting_down() {
                            break;
                        }
                        if let Err(error) = supervisor.poll_once().await {
                            if supervisor.inner.shutdown.is_shutting_down() {
                                break;
                            }
                            tracing::warn!(error = %error, "sources poll failed");
                        }
                        #[cfg(feature = "test-support")]
                        supervisor.inner.poll_completed.send_replace(());
                    }
                }
            }
        });
        let previous = self.inner.poll_task.lock().unwrap().replace(handle);
        if let Some(previous) = previous {
            previous.abort();
        }
    }

    async fn restart_failed_engines(&self) {
        let _guard = self.inner.reconcile.lock().await;
        if self.inner.shutdown.is_shutting_down() {
            return;
        }
        let ids: Vec<_> = self
            .inner
            .state
            .lock()
            .await
            .running
            .iter()
            .filter(|(_, runtime)| runtime.engine.shutdown_token().restart_requested())
            .map(|(id, _)| id.clone())
            .collect();
        for id in ids {
            let runtime = self.inner.state.lock().await.running.remove(&id);
            if let Some(mut runtime) = runtime {
                let row = runtime.row.clone();
                self.inner.state.lock().await.failed.insert(
                    id.clone(),
                    FailedSource { row: row.clone(), error: "source restarting after Engine failure".into() },
                );
                if let Err(error) = stop_runtime(&mut runtime).await {
                    tracing::warn!(source_id = %id, %error, "failed Engine worker did not join");
                }
                drop(runtime);
                self.start_or_record(row).await;
            }
        }
    }

    pub async fn shutdown_all(&self) {
        self.inner.shutdown.begin();
        let poll_task = self.inner.poll_task.lock().unwrap().take();
        if let Some(handle) = poll_task {
            let _ = handle.await;
        }
        let _guard = self.inner.reconcile.lock().await;
        let runtimes = {
            let mut state = self.inner.state.lock().await;
            std::mem::take(&mut state.running).into_values().collect::<Vec<_>>()
        };
        for mut runtime in runtimes {
            runtime.engine.shutdown_token().begin();
            if let Some(worker) = runtime.worker.take() {
                if let Err(error) = join_worker(worker).await {
                    tracing::warn!(source_id = %runtime.row.source_id, error = %error, "source worker did not join cleanly");
                }
            }
        }
    }

    async fn poll_once(&self) -> Result<()> {
        if self.inner.shutdown.is_shutting_down() {
            return Ok(());
        }
        // Discovery and retries share the refresh/restart lock: the cached failed rows are still
        // desired by the latest applied snapshot, and a concurrent refresh cannot replace them.
        let _guard = self.inner.reconcile.lock().await;
        let refreshed = async {
            let changed = match self.inner.sources.mode {
                SourcesMode::File => true,
                SourcesMode::Table => {
                    let revision = self.read_control_revision().await?;
                    self.inner.state.lock().await.last_revision != Some(revision)
                }
            };
            if changed {
                let snapshot = self.fetch_snapshot().await?;
                // apply_snapshot already attempts each desired failed row once.
                self.apply_snapshot(snapshot).await?;
            }
            Ok::<_, anyhow::Error>(changed)
        }
        .await;
        if !matches!(refreshed, Ok(true)) {
            let rows: Vec<_> = self.inner.state.lock().await.failed.values().map(|failed| failed.row.clone()).collect();
            for row in rows {
                self.start_or_record(row).await;
            }
        }
        refreshed.map(|_| ())
    }

    async fn run_unless_shutdown<T, E, F>(&self, fut: F) -> Result<T>
    where
        F: std::future::Future<Output = std::result::Result<T, E>>,
        E: Into<anyhow::Error>,
    {
        tokio::select! {
            biased;
            _ = self.inner.shutdown.wait() => bail!("sources operation interrupted by shutdown"),
            result = fut => result.map_err(Into::into),
        }
    }

    async fn fetch_snapshot(&self) -> Result<SourceSnapshot> {
        match self.inner.sources.mode {
            SourcesMode::File => {
                let path = self.inner.sources.file.as_deref().context("file mode has no sources file")?;
                let body = self
                    .run_unless_shutdown(tokio::fs::read_to_string(path))
                    .await
                    .with_context(|| format!("read {}", path.display()))?;
                let rows: Vec<SourceRow> = serde_json::from_str(&body).context("parse sources JSON")?;
                validate_rows(&rows)?;
                let revision = rows.iter().map(|row| row.revision).max().unwrap_or(0);
                Ok(SourceSnapshot { revision, rows })
            }
            SourcesMode::Table => {
                let revision = self.read_control_revision().await?;
                let url = self.inner.sources.pg_url.as_deref().context("table mode has no control URL")?;
                let client = self
                    .run_unless_shutdown(crate::pg::connect(url))
                    .await
                    .context("connect sources control database")?;
                let query = format!(
                    "SELECT source_id, plugin, database_secret, slot, publication, tables, revision, updated_at::text \
                     FROM {} ORDER BY source_id",
                    quote_table_name(&self.inner.sources.table)
                );
                let rows = self.run_unless_shutdown(client.query(&query, &[])).await.context("read sources table")?;
                let rows = rows.into_iter().map(source_row_from_pg).collect::<Result<Vec<_>>>()?;
                validate_rows(&rows)?;
                Ok(SourceSnapshot { revision, rows })
            }
        }
    }

    async fn read_control_revision(&self) -> Result<i64> {
        let url = self.inner.sources.pg_url.as_deref().context("table mode has no control URL")?;
        let client =
            self.run_unless_shutdown(crate::pg::connect(url)).await.context("connect sources version database")?;
        let query = format!("SELECT revision FROM {} LIMIT 1", quote_table_name(&self.inner.sources.version_table));
        let row = self.run_unless_shutdown(client.query_opt(&query, &[])).await.context("read sources version")?;
        row.map(|row| row.get(0)).context("sources version table has no revision row")
    }

    async fn apply_snapshot(&self, snapshot: SourceSnapshot) -> Result<i64> {
        if self.inner.shutdown.is_shutting_down() {
            bail!("sources reconcile interrupted by shutdown");
        }
        let (running, failed) = {
            let state = self.inner.state.lock().await;
            let running = state
                .running
                .iter()
                .map(|(source_id, runtime)| (source_id.clone(), RunningSourceState { revision: runtime.row.revision }))
                .collect::<BTreeMap<_, _>>();
            let failed = state
                .failed
                .iter()
                .map(|(source_id, failed)| (source_id.clone(), RunningSourceState { revision: failed.row.revision }))
                .collect::<BTreeMap<_, _>>();
            (running, failed)
        };
        let plan = reconcile_refresh(&running, &failed, &snapshot.rows);
        for action in plan.actions {
            if self.inner.shutdown.is_shutting_down() {
                bail!("sources reconcile interrupted by shutdown");
            }
            match action {
                PlanAction::Stop(source_id) => {
                    if let Some(mut runtime) = self.inner.state.lock().await.running.remove(&source_id) {
                        if let Err(error) = stop_runtime(&mut runtime).await {
                            tracing::warn!(source_id = %source_id, error = %error, "source stop failed");
                        }
                    }
                    self.inner.state.lock().await.failed.remove(&source_id);
                }
                PlanAction::Restart(row) => {
                    let runtime = {
                        let mut state = self.inner.state.lock().await;
                        state.running.remove(&row.source_id)
                    };
                    if let Some(mut runtime) = runtime {
                        self.inner.state.lock().await.failed.insert(
                            row.source_id.clone(),
                            FailedSource { row: row.clone(), error: "source restarting".to_string() },
                        );
                        if let Err(error) = stop_runtime(&mut runtime).await {
                            tracing::warn!(source_id = %row.source_id, error = %error, "source restart stop failed");
                        }
                    }
                    self.start_or_record(row).await;
                }
                PlanAction::Start(row) => {
                    self.start_or_record(row).await;
                }
            }
        }
        let mut state = self.inner.state.lock().await;
        let desired_ids =
            snapshot.rows.iter().map(|row| row.source_id.as_str()).collect::<std::collections::BTreeSet<_>>();
        state.failed.retain(|source_id, _| desired_ids.contains(source_id.as_str()));
        state.last_revision = Some(snapshot.revision);
        state.fetched = true;
        Ok(snapshot.revision)
    }

    async fn start_or_record(&self, row: SourceRow) {
        if self.inner.shutdown.is_shutting_down() {
            return;
        }
        match self.start_source(row.clone()).await {
            Ok(runtime) => {
                if self.inner.shutdown.is_shutting_down() {
                    let mut runtime = runtime;
                    if let Err(error) = stop_runtime(&mut runtime).await {
                        tracing::warn!(source_id = %row.source_id, error = %error, "source stop during shutdown failed");
                    }
                    return;
                }
                let mut state = self.inner.state.lock().await;
                state.failed.remove(&row.source_id);
                state.running.insert(row.source_id.clone(), runtime);
            }
            Err(error) => {
                if self.inner.shutdown.is_shutting_down() {
                    return;
                }
                let error = public_source_error(&error);
                tracing::warn!(source_id = %row.source_id, revision = row.revision, error = %error, "source is not ready");
                self.inner.state.lock().await.failed.insert(row.source_id.clone(), FailedSource { row, error });
            }
        }
    }

    async fn start_source(&self, row: SourceRow) -> Result<SourceRuntime> {
        if self.inner.shutdown.is_shutting_down() {
            bail!("source start interrupted by shutdown");
        }
        validate_source_row(&row)?;
        let config = self.inner.config.clone();
        let grace = config.shutdown_grace;
        let host_shutdown = self.inner.shutdown.clone();
        let restart_notify = self.inner.restart_notify.clone();
        let source_id = row.source_id.clone();
        let row_for_thread = row.clone();
        let (ready_tx, ready_rx) = oneshot::channel();
        let thread_name = format!("circuits-source-{}", source_id);
        let worker = std::thread::Builder::new()
            .name(thread_name.clone())
            .spawn(move || {
                let runtime = match tokio::runtime::Builder::new_current_thread().enable_all().build() {
                    Ok(runtime) => runtime,
                    Err(error) => {
                        let _ = ready_tx.send(Err(anyhow::anyhow!("build source runtime: {error}")));
                        return;
                    }
                };
                let booted = runtime.block_on(boot_source(config, row_for_thread, host_shutdown, restart_notify));
                match booted {
                    Ok((engine, router)) => {
                        let shutdown = engine.shutdown_token();
                        if ready_tx.send(Ok((engine.clone(), router))).is_err() {
                            shutdown.begin();
                        }
                        runtime.block_on(async move {
                            shutdown.wait().await;
                            let outcome = engine.close(grace).await;
                            if outcome != crate::shutdown::ShutdownOutcome::Complete {
                                tracing::warn!(?outcome, "source close requires replay");
                            }
                        });
                    }
                    Err(error) => {
                        let _ = ready_tx.send(Err(error));
                    }
                }
            })
            .with_context(|| format!("spawn {thread_name}"))?;
        let result = ready_rx.await.with_context(|| format!("{thread_name} exited before readiness"))?;
        match result {
            Ok((engine, router)) => Ok(SourceRuntime { row, engine, router, worker: Some(worker) }),
            Err(error) => {
                join_worker(worker).await?;
                Err(error)
            }
        }
    }
}

pub async fn run(config: Config) -> Result<()> {
    crate::config::set_globals(
        &config.instance_id,
        &config.stack_id,
        config.secret.as_deref(),
        config.control_secret.as_deref(),
    );
    crate::pg::set_pool_size(config.db_pool_size);
    crate::pg::set_backfill_config(config.backfill);
    if let Some(target) = &config.statsd {
        crate::statsd::init(target, &config.instance_id);
    }

    let supervisor = SourcesSupervisor::new(config.clone())?;
    supervisor.initial_fetch_until_ready().await?;
    let listener =
        tokio::net::TcpListener::bind(&config.bind).await.with_context(|| format!("binding {}", config.bind))?;
    let address = listener.local_addr()?;
    println!("ENGINE_BINDING http://{address}");
    println!("ENGINE_LISTENING http://{address}");
    let poll_supervisor = supervisor.clone();
    poll_supervisor.spawn_poll();
    let shutdown_supervisor = supervisor.clone();
    axum::serve(listener, supervisor.router())
        .with_graceful_shutdown(async move {
            let signal = crate::shutdown::first_signal().await;
            tracing::info!(signal, "sources host shutdown requested");
            shutdown_supervisor.shutdown_token().begin();
            shutdown_supervisor.shutdown_all().await;
        })
        .await
        .context("serve sources host")?;
    Ok(())
}

async fn join_worker(worker: std::thread::JoinHandle<()>) -> Result<()> {
    tokio::task::spawn_blocking(move || worker.join().map_err(|_| anyhow::anyhow!("source worker panicked")))
        .await
        .context("join source worker task")??;
    Ok(())
}

async fn stop_runtime(runtime: &mut SourceRuntime) -> Result<()> {
    runtime.engine.shutdown_token().begin();
    if let Some(worker) = runtime.worker.take() {
        join_worker(worker).await?;
    }
    Ok(())
}

async fn boot_source(
    config: Config,
    row: SourceRow,
    host_shutdown: ShutdownToken,
    restart_notify: Arc<tokio::sync::Notify>,
) -> Result<(Engine, Router)> {
    let prepare = async {
        let database_url = resolve_database_secret(&row.database_secret).await?;
        let child = source_engine_config(&config, &row, &database_url)?;
        child.txn.probe().context("probe source transaction spill directory")?;
        let (ds, scope) = source_ds(&config, &row.source_id).await?;
        let admission = Engine::admit_store(ds, StoreBound::coupled_v1(&scope), config.initialize_namespace).await?;
        Ok::<_, anyhow::Error>((database_url, child, admission))
    };
    let (database_url, child, admission) = tokio::select! {
        biased;
        _ = host_shutdown.wait() => bail!("source start interrupted by shutdown"),
        prepared = prepare => prepared?,
    };
    let engine = Engine::new_with_config(admission, Some(database_url), PostgresSetup::ExternallyManaged, &child);
    engine.shutdown_token().supervise_restarts(restart_notify);
    engine.set_dbsp_config(child.dbsp.clone());
    engine.set_txn_config(child.txn.clone());
    let activated = tokio::select! {
        biased;
        _ = host_shutdown.wait() => Err(anyhow::anyhow!("source start interrupted by shutdown")),
        result = engine.setup_postgres(&child.tables, &row.slot) => result.with_context(|| format!("activate source {}", row.source_id)),
    };
    if let Err(error) = activated {
        engine.close(child.shutdown_grace).await;
        return Err(error);
    }
    if engine.readiness_status() != "active" {
        engine.close(child.shutdown_grace).await;
        bail!("source {} did not become active", row.source_id);
    }
    Ok((engine.clone(), crate::http::router_with_introspection(engine, child.trace)))
}

fn source_engine_config(base: &Config, row: &SourceRow, database_url: &str) -> Result<Config> {
    let sources = base.sources.as_ref().context("source config missing")?;
    let source_root = sources.storage_dir.join(&row.source_id);
    let pg_url = database_url.to_string();
    let slot = row.slot.clone();
    let tables = row.tables.join(",");
    let mut child = Config::resolve(|name| match name {
        "ELECTRIC_CIRCUITS_SOURCES_MODE"
        | "ELECTRIC_CIRCUITS_SOURCES_PG_URL"
        | "ELECTRIC_CIRCUITS_SOURCES_TABLE"
        | "ELECTRIC_CIRCUITS_SOURCES_VERSION_TABLE"
        | "ELECTRIC_CIRCUITS_SOURCES_POLL_SECS"
        | "ELECTRIC_CIRCUITS_SOURCES_FILE"
        | "ELECTRIC_CIRCUITS_SOURCES_STORAGE_DIR" => None,
        "ELECTRIC_CIRCUITS_PG_URL" => Some(pg_url.clone()),
        "ELECTRIC_CIRCUITS_PG_SLOT" => Some(slot.clone()),
        "ELECTRIC_CIRCUITS_PG_TABLES" => Some(tables.clone()),
        "ELECTRIC_STORAGE_DIR" => Some(source_root.to_string_lossy().into_owned()),
        "ELECTRIC_CIRCUITS_DBSP_DIR" => Some(source_root.join("dbsp").to_string_lossy().into_owned()),
        "ELECTRIC_CIRCUITS_SUBQ_STORAGE_DIR" => Some(source_root.join("subq").to_string_lossy().into_owned()),
        "ELECTRIC_CIRCUITS_TXN_SPILL_DIR" => Some(source_root.join("txn-spill").to_string_lossy().into_owned()),
        "ELECTRIC_CIRCUITS_DS_URL" => base.ds_url.clone(),
        "ELECTRIC_CIRCUITS_DS_IN_PROCESS_TEST" => base.ds_in_process_test_url.as_ref().map(|_| "1".to_string()),
        "ELECTRIC_CIRCUITS_INITIALIZE_NAMESPACE" => base.initialize_namespace.then_some("1".to_string()),
        _ => std::env::var(name).ok(),
    })?;
    child.sources = None;
    child.dbsp = base.dbsp.clone();
    child.dbsp.dir = source_root.join("dbsp");
    child.txn = base.txn.clone();
    child.txn.spill_dir = source_root.join("txn-spill");
    child.backfill = base.backfill;
    child.db_pool_size = base.db_pool_size;
    child.trace = base.trace;
    child.secret = base.secret.clone();
    child.control_secret = base.control_secret.clone();
    child.shutdown_grace = base.shutdown_grace;
    child.shutdown_ready_drain = base.shutdown_ready_drain;
    child.storage_dir = Some(source_root.to_string_lossy().into_owned());
    child.stack_id = row.source_id.clone();
    Ok(child)
}

async fn source_ds(config: &Config, source_id: &str) -> Result<(DsClient, StreamScope)> {
    if let Some(connection) = config.ds_connection.as_ref() {
        let scope = source_scope(&connection.scope, source_id)?;
        let connection = crate::ds::DsConnectionConfig::new(
            connection.base_url.clone(),
            connection.ca_bundle_path.clone(),
            connection.client_certificate_path.clone(),
            connection.client_key_path.clone(),
            scope.clone(),
        )?;
        return Ok((DsClient::connect(connection).await?, scope));
    }
    if let Some(base_url) = config.ds_in_process_test_url.as_ref() {
        #[cfg(any(test, feature = "test-support"))]
        {
            let scope = source_scope(&StreamScope::in_process_test_scope(), source_id)?;
            return Ok((DsClient::new_for_in_process_test_with_scope(base_url.clone(), scope.clone()), scope));
        }
        #[cfg(not(any(test, feature = "test-support")))]
        {
            let _ = (base_url, source_id);
            bail!("sources mode with an in-process Durable Streams URL requires test-support");
        }
    }
    bail!("sources mode requires a Durable Streams configuration")
}

fn source_scope(base: &StreamScope, source_id: &str) -> Result<StreamScope> {
    use sha2::{Digest, Sha256};
    let digest = format!("{:x}", Sha256::digest(source_id.as_bytes()));
    StreamScope::new(format!("source-{}", &digest[..40]), base.store.clone(), format!("query-{}", &digest[..40]))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SecretResolveError {
    UrlShaped,
    MissingPrefix,
    UnknownPrefix,
    EmptyName,
    EnvMissing,
    EnvEmpty,
    FileRead,
    AwsLookup,
    InvalidUrl,
}

impl std::fmt::Display for SecretResolveError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::UrlShaped => "resolve failed: url-shaped secret",
            Self::MissingPrefix => "resolve failed: missing prefix",
            Self::UnknownPrefix => "resolve failed: unknown prefix",
            Self::EmptyName => "resolve failed: empty name",
            Self::EnvMissing => "resolve failed: env variable missing",
            Self::EnvEmpty => "resolve failed: env variable empty",
            Self::FileRead => "resolve failed: file read error",
            Self::AwsLookup => "resolve failed: aws-sm lookup error",
            Self::InvalidUrl => "resolve failed: invalid postgres url",
        })
    }
}

impl std::error::Error for SecretResolveError {}

fn is_url_shaped_secret(reference: &str) -> bool {
    let trimmed = reference.trim();
    if trimmed.starts_with("env:") || trimmed.starts_with("file:") || trimmed.starts_with("aws-sm:") {
        return false;
    }
    let lower = trimmed.to_ascii_lowercase();
    lower.contains("://") || lower.starts_with("postgres:") || lower.starts_with("postgresql:")
}

fn public_source_error(error: &anyhow::Error) -> String {
    for cause in error.chain() {
        if let Some(classified) = cause.downcast_ref::<SecretResolveError>() {
            return classified.to_string();
        }
    }
    error.to_string()
}

async fn resolve_database_secret(reference: &str) -> Result<String> {
    if is_url_shaped_secret(reference) {
        bail!(SecretResolveError::UrlShaped);
    }
    let Some((kind, name)) = reference.split_once(':') else {
        bail!(SecretResolveError::MissingPrefix);
    };
    if name.trim().is_empty() {
        bail!(SecretResolveError::EmptyName);
    }
    let value = match kind {
        "env" => return resolve_env_secret(reference, |key| std::env::var(key).ok()),
        "file" => tokio::fs::read_to_string(name).await.map_err(|_| SecretResolveError::FileRead)?,
        "aws-sm" => return resolve_aws_secret(name).await,
        _ => bail!(SecretResolveError::UnknownPrefix),
    };
    validate_database_secret_value(&value)
}

async fn resolve_aws_secret(name: &str) -> Result<String> {
    resolve_aws_secret_with(name, |name| async move {
        let shared = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
        let client = aws_sdk_secretsmanager::Client::new(&shared);
        let display_name = name.clone();
        client
            .get_secret_value()
            .secret_id(name)
            .send()
            .await
            .with_context(|| format!("read AWS Secrets Manager secret {display_name}"))?
            .secret_string()
            .context("AWS Secrets Manager secret has no SecretString")
            .map(ToString::to_string)
    })
    .await
}

async fn resolve_aws_secret_with<F, Fut>(name: &str, fetch: F) -> Result<String>
where
    F: FnOnce(String) -> Fut,
    Fut: std::future::Future<Output = Result<String>>,
{
    let value = fetch(name.to_string()).await.map_err(|_| SecretResolveError::AwsLookup)?;
    validate_database_secret_value(&value)
}

fn validate_database_secret_value(value: &str) -> Result<String> {
    let value = value.trim().to_string();
    if value.is_empty() {
        bail!(SecretResolveError::InvalidUrl);
    }
    validate_database_url(&value)?;
    Ok(value)
}

fn resolve_env_secret(reference: &str, get: impl Fn(&str) -> Option<String>) -> Result<String> {
    if is_url_shaped_secret(reference) {
        bail!(SecretResolveError::UrlShaped);
    }
    let Some((kind, name)) = reference.split_once(':') else {
        bail!(SecretResolveError::MissingPrefix);
    };
    if kind != "env" {
        bail!(SecretResolveError::UnknownPrefix);
    }
    if name.trim().is_empty() {
        bail!(SecretResolveError::EmptyName);
    }
    let Some(value) = get(name) else {
        bail!(SecretResolveError::EnvMissing);
    };
    let value = value.trim().to_string();
    if value.is_empty() {
        bail!(SecretResolveError::EnvEmpty);
    }
    validate_database_url(&value)?;
    Ok(value)
}

fn validate_database_url(value: &str) -> Result<()> {
    let ca_bundle = std::env::var("ELECTRIC_CIRCUITS_PG_TLS_CA_BUNDLE").ok();
    let server_name = std::env::var("ELECTRIC_CIRCUITS_PG_TLS_SERVER_NAME").ok();
    if crate::pg::PgConnectionConfig::resolve(value, ca_bundle.as_deref(), server_name.as_deref()).is_err() {
        bail!(SecretResolveError::InvalidUrl);
    }
    Ok(())
}

fn validate_rows(rows: &[SourceRow]) -> Result<()> {
    let mut source_ids = std::collections::BTreeSet::new();
    for row in rows {
        if !source_ids.insert(row.source_id.as_str()) {
            bail!("sources discovery returned duplicate source_id '{}'", row.source_id);
        }
    }
    Ok(())
}

fn is_safe_source_id(source_id: &str) -> bool {
    if source_id.is_empty() || source_id == "." || source_id == ".." {
        return false;
    }
    !source_id
        .chars()
        .any(|c| c.is_control() || matches!(c, '/' | '\\' | '?' | '#' | '%' | ':' | '*' | '"' | '<' | '>' | '|'))
}

fn validate_source_row(row: &SourceRow) -> Result<()> {
    if !is_safe_source_id(&row.source_id) {
        bail!("source_id is not a single safe path component");
    }
    // `plugin` names the consumer's package that owns the source; the engine records it and
    // always decodes with pgoutput. It is never the logical-decoding plugin.
    if row.plugin.trim().is_empty() {
        bail!("source '{}' has an empty plugin", row.source_id);
    }
    if row.slot.trim().is_empty() {
        bail!("source '{}' has an empty slot", row.source_id);
    }
    if row.publication != format!("{}_pub", row.slot) {
        bail!(
            "source '{}' publication '{}' must match the externally managed slot publication '{}_pub'",
            row.source_id,
            row.publication,
            row.slot
        );
    }
    if row.tables.is_empty() {
        bail!("source '{}' has no tables", row.source_id);
    }
    for table in &row.tables {
        if !table.contains('.') || table.contains('*') {
            bail!("source '{}' table '{}' must be schema-qualified", row.source_id, table);
        }
        TableSelector::parse(table).with_context(|| format!("source '{}' table '{}'", row.source_id, table))?;
    }
    Ok(())
}

fn source_row_from_pg(row: tokio_postgres::Row) -> Result<SourceRow> {
    Ok(SourceRow {
        source_id: row.try_get(0).context("sources source_id")?,
        plugin: row.try_get(1).context("sources plugin")?,
        database_secret: row.try_get(2).context("sources database_secret")?,
        slot: row.try_get(3).context("sources slot")?,
        publication: row.try_get(4).context("sources publication")?,
        tables: row.try_get(5).context("sources tables")?,
        revision: row.try_get(6).context("sources revision")?,
        updated_at: row.try_get(7).context("sources updated_at")?,
    })
}

fn quote_table_name(name: &str) -> String {
    name.split('.').map(|part| format!("\"{}\"", part.replace('\"', "\"\""))).collect::<Vec<_>>().join(".")
}

async fn health() -> Response {
    StatusCode::OK.into_response()
}

async fn ready(State(supervisor): State<SourcesSupervisor>) -> Response {
    let fetched = supervisor.inner.state.lock().await.fetched;
    let status = if fetched { "active" } else { "waiting" };
    let code = if fetched { StatusCode::OK } else { StatusCode::SERVICE_UNAVAILABLE };
    (code, axum::Json(serde_json::json!({ "status": status }))).into_response()
}

async fn list_sources(State(supervisor): State<SourcesSupervisor>) -> axum::Json<Vec<SourceSummary>> {
    axum::Json(supervisor.summaries().await)
}

async fn source_status(State(supervisor): State<SourcesSupervisor>, AxumPath(source_id): AxumPath<String>) -> Response {
    let state = supervisor.inner.state.lock().await;
    if let Some(runtime) = state.running.get(&source_id) {
        return axum::Json(runtime_status(&runtime.row, &runtime.engine)).into_response();
    }
    if let Some(failed) = state.failed.get(&source_id) {
        return axum::Json(failed_status(&failed.row, &failed.error)).into_response();
    }
    (StatusCode::NOT_FOUND, "source not found").into_response()
}

async fn refresh(
    State(supervisor): State<SourcesSupervisor>,
    headers: axum::http::HeaderMap,
    body: Bytes,
) -> Result<axum::Json<serde_json::Value>, crate::http::AppError> {
    crate::http::require_private_admin_with_secret(&headers, crate::config::control_secret())?;
    if !body.is_empty() {
        return Err(crate::http::AppError {
            status: StatusCode::BAD_REQUEST,
            msg: "POST /admin/refresh does not accept a request body".to_string(),
            retry_after: false,
        });
    }
    let revision = supervisor.refresh().await?;
    Ok(axum::Json(serde_json::json!({ "revision": revision })))
}

async fn proxy_root(
    State(supervisor): State<SourcesSupervisor>,
    AxumPath(source_id): AxumPath<String>,
    request: Request,
) -> Response {
    proxy(supervisor, source_id, String::new(), request).await
}

async fn proxy_path(
    State(supervisor): State<SourcesSupervisor>,
    AxumPath((source_id, path)): AxumPath<(String, String)>,
    request: Request,
) -> Response {
    proxy(supervisor, source_id, path, request).await
}

/// Paths the source-scoped proxy must not forward. There is no source-specific admin surface;
/// host `POST /admin/refresh` is the only admin route in sources-table mode.
fn source_proxy_denies(path: &str) -> bool {
    let path = path.trim_start_matches('/');
    ["_admin", "epoch/reset"].iter().any(|denied| path == *denied || path.starts_with(&format!("{denied}/")))
}

async fn proxy(supervisor: SourcesSupervisor, source_id: String, path: String, request: Request) -> Response {
    if source_proxy_denies(&path) {
        return (StatusCode::NOT_FOUND, "not found").into_response();
    }
    let router = {
        let state = supervisor.inner.state.lock().await;
        state.running.get(&source_id).map(|runtime| runtime.router.clone())
    };
    let Some(router) = router else {
        return (StatusCode::NOT_FOUND, "source not found").into_response();
    };
    let query = request.uri().query().map(|query| format!("?{query}")).unwrap_or_default();
    let target = format!("/{}{}", path.trim_start_matches('/'), query);
    let Ok(uri) = target.parse::<Uri>() else {
        return (StatusCode::BAD_REQUEST, "invalid source request URI").into_response();
    };
    let (parts, body) = request.into_parts();
    let mut forwarded = axum::http::Request::new(Body::new(body));
    *forwarded.method_mut() = parts.method;
    *forwarded.uri_mut() = uri;
    *forwarded.version_mut() = parts.version;
    *forwarded.headers_mut() = parts.headers;
    router.oneshot(forwarded).await.unwrap_or_else(|never| match never {})
}

impl SourcesSupervisor {
    async fn summaries(&self) -> Vec<SourceSummary> {
        let state = self.inner.state.lock().await;
        let mut result = state
            .running
            .values()
            .map(|runtime| {
                let readiness = runtime.engine.readiness_status();
                SourceSummary {
                    source_id: runtime.row.source_id.clone(),
                    revision: runtime.row.revision,
                    ready: readiness == "active",
                    error: (readiness != "active").then(|| readiness.to_string()),
                }
            })
            .collect::<Vec<_>>();
        result.extend(state.failed.values().map(|failed| SourceSummary {
            source_id: failed.row.source_id.clone(),
            revision: failed.row.revision,
            ready: false,
            error: Some(failed.error.clone()),
        }));
        result.sort_by(|left, right| left.source_id.cmp(&right.source_id));
        result
    }
}

fn runtime_status(row: &SourceRow, engine: &Engine) -> serde_json::Value {
    let position = engine.changes_position();
    let segments = engine.changes_segments();
    let epoch = engine.epoch_json();
    let consumers = engine
        .consumers()
        .into_iter()
        .map(|(id, position, lag_segments)| {
            let pinned_segments =
                segments.keys().copied().filter(|segment| *segment >= position.segment).collect::<Vec<_>>();
            serde_json::json!({
                "id": id,
                "pinned_segments": pinned_segments,
                "position": position,
                "lagSegments": lag_segments,
            })
        })
        .collect::<Vec<_>>();
    let readiness = engine.readiness_status();
    serde_json::json!({
        "changes": {
            "route": format!("/sources/{}/changes", row.source_id),
            "epoch": engine.changes_generation(),
            "position": position,
            "segments": segments,
        },
        "epoch": epoch,
        "position": position,
        "segments": segments,
        "consumers": consumers,
        "readiness": readiness,
        "revision": row.revision,
        "ready": readiness == "active",
        "error": if readiness == "active" { serde_json::Value::Null } else { serde_json::Value::String(readiness.to_string()) },
    })
}

fn failed_status(row: &SourceRow, error: &str) -> serde_json::Value {
    serde_json::json!({
        "changes": { "route": format!("/sources/{}/changes", row.source_id) },
        "epoch": serde_json::Value::Null,
        "position": serde_json::Value::Null,
        "segments": {},
        "consumers": [],
        "readiness": "not_ready",
        "revision": row.revision,
        "ready": false,
        "error": error,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(source_id: &str, revision: i64) -> SourceRow {
        SourceRow {
            source_id: source_id.into(),
            plugin: "pgoutput".into(),
            database_secret: format!("env:{}", source_id),
            slot: format!("{}_slot", source_id),
            publication: format!("{}_pub", source_id),
            tables: vec!["public.thread_messages".into()],
            revision,
            updated_at: "2026-09-07T00:00:00Z".into(),
        }
    }

    fn running(entries: &[(&str, i64)]) -> BTreeMap<String, RunningSourceState> {
        entries
            .iter()
            .map(|(source_id, revision)| (source_id.to_string(), RunningSourceState { revision: *revision }))
            .collect()
    }

    #[test]
    fn source_proxy_denies_admin_and_epoch_reset_only() {
        for path in ["_admin", "/_admin", "_admin/deployment/promote", "epoch/reset", "/epoch/reset", "epoch/reset/"] {
            assert!(source_proxy_denies(path), "{path} must be denied");
        }
        for path in ["v1/shape", "shapes", "health", "ready", "status", "replication/lsn", "epoch"] {
            assert!(!source_proxy_denies(path), "{path} must still be forwarded");
        }
    }

    #[test]
    fn identical_rows_have_an_empty_plan() {
        let rows = vec![row("alpha", 1), row("beta", 4)];
        assert_eq!(reconcile(&running(&[("alpha", 1), ("beta", 4)]), &rows), Plan::default());
    }

    #[test]
    fn reconcile_plans_start_stop_restart_and_mixed_changes() {
        let rows = vec![row("alpha", 2), row("new", 1)];
        let plan = reconcile(&running(&[("alpha", 1), ("gone", 7)]), &rows);
        assert_eq!(
            plan.actions,
            vec![
                PlanAction::Restart(row("alpha", 2)),
                PlanAction::Stop("gone".into()),
                PlanAction::Start(row("new", 1)),
            ]
        );
    }

    #[test]
    fn applying_a_plan_then_reconciling_is_empty() {
        let rows = vec![row("alpha", 2), row("new", 1)];
        let plan = reconcile(&running(&[("alpha", 1), ("gone", 7)]), &rows);
        let mut after = running(&[("alpha", 1), ("gone", 7)]);
        for action in &plan.actions {
            match action {
                PlanAction::Start(row) | PlanAction::Restart(row) => {
                    after.insert(row.source_id.clone(), RunningSourceState { revision: row.revision });
                }
                PlanAction::Stop(source_id) => {
                    after.remove(source_id);
                }
            }
        }
        assert_eq!(reconcile(&after, &rows), Plan::default());
    }

    #[test]
    fn refresh_retries_failed_desired_rows_without_restarting_healthy_ones() {
        let rows = vec![row("alpha", 1), row("broken", 3)];
        let plan = reconcile_refresh(&running(&[("alpha", 1)]), &running(&[("broken", 3)]), &rows);
        assert_eq!(plan.actions, vec![PlanAction::Start(row("broken", 3))]);
    }

    #[test]
    fn plugin_names_the_owning_package_not_the_decoder() {
        let mut owned = valid_row("alpha", 1, "env:ALPHA_URL");
        owned.plugin = "mighty.agents".into();
        validate_source_row(&owned).expect("a package name is a valid plugin value");
        let mut empty = valid_row("alpha", 1, "env:ALPHA_URL");
        empty.plugin = " ".into();
        assert!(validate_source_row(&empty).is_err());
    }

    #[test]
    fn source_id_must_be_a_single_safe_path_component() {
        assert!(is_safe_source_id("alpha-1_src"));
        for source_id in [".", "..", "a/b", r"a\b", "a\nb", "a\0b", "a:b", ""] {
            assert!(!is_safe_source_id(source_id), "source_id {source_id:?} must be rejected");
            let mut invalid = row("alpha", 1);
            invalid.source_id = source_id.to_string();
            assert!(validate_source_row(&invalid).is_err(), "source_id {source_id:?} must be rejected");
        }
    }

    #[test]
    fn environment_secret_resolves_and_missing_or_malformed_values_fail() {
        let url = resolve_env_secret("env:SOURCE_URL", |name| {
            (name == "SOURCE_URL").then(|| " postgres://postgres@127.0.0.1:5432/source ".to_string())
        })
        .expect("environment secret");
        assert_eq!(url, "postgres://postgres@127.0.0.1:5432/source");
        let missing = resolve_env_secret("env:MISSING_WITH_PASSWORD_LIKE_VALUE", |_| None).unwrap_err();
        assert_classified_secret_error(&missing, "resolve failed: env variable missing");
        let broken = resolve_env_secret("env:BROKEN", |_| Some("not a postgres URL".into())).unwrap_err();
        assert_classified_secret_error(&broken, "resolve failed: invalid postgres url");
        let unknown =
            resolve_env_secret("vault:SOURCE_URL", |_| Some("postgres://postgres@127.0.0.1:5432/source".into()))
                .unwrap_err();
        assert_classified_secret_error(&unknown, "resolve failed: unknown prefix");
        let embedded_url = resolve_env_secret("env:postgres://user:s3cret@127.0.0.1:5432/db", |_| None).unwrap_err();
        assert_classified_secret_error(&embedded_url, "resolve failed: env variable missing");
    }

    fn assert_classified_secret_error(error: &anyhow::Error, expected: &str) {
        let message = format!("{error:#}");
        assert!(message.contains(expected), "{message}");
        assert!(!message.contains("s3cret"), "{message}");
        assert!(!message.contains("MISSING_WITH_PASSWORD_LIKE_VALUE"), "{message}");
        assert!(!message.contains("SOURCE_URL"), "{message}");
        assert!(!message.contains("postgres://"), "{message}");
        assert!(!message.contains("user:"), "{message}");
    }

    #[tokio::test]
    async fn file_secret_is_trimmed_and_invalid_or_missing_files_fail() {
        let path = std::env::temp_dir().join(format!("circuits-source-secret-{}", uuid::Uuid::new_v4()));
        std::fs::write(&path, "\npostgres://postgres@127.0.0.1:5432/source\n").unwrap();
        let resolved = resolve_database_secret(&format!("file:{}", path.display())).await.unwrap();
        assert_eq!(resolved, "postgres://postgres@127.0.0.1:5432/source");
        std::fs::write(&path, "not a postgres URL").unwrap();
        let invalid = resolve_database_secret(&format!("file:{}", path.display())).await.unwrap_err();
        assert_classified_secret_error(&invalid, "resolve failed: invalid postgres url");
        std::fs::remove_file(&path).unwrap();
        let missing = resolve_database_secret(&format!("file:{}", path.display())).await.unwrap_err();
        assert_classified_secret_error(&missing, "resolve failed: file read error");
        assert!(!format!("{missing:#}").contains(&path.display().to_string()), "{missing:#}");
    }

    #[tokio::test]
    async fn url_shaped_database_secret_is_rejected_before_resolution() {
        let error = resolve_database_secret("postgres://user:s3cret@127.0.0.1:5432/source").await.unwrap_err();
        assert_classified_secret_error(&error, "resolve failed: url-shaped secret");
    }

    #[tokio::test]
    async fn aws_secret_resolution_uses_a_stub_without_contacting_aws() {
        let value = resolve_aws_secret_with("unit-test-secret", |name| async move {
            assert_eq!(name, "unit-test-secret");
            Ok(" postgres://postgres@127.0.0.1:5432/source ".into())
        })
        .await
        .unwrap();
        assert_eq!(value, "postgres://postgres@127.0.0.1:5432/source");
        let invalid = resolve_aws_secret_with("bad", |_| async { Ok("not-a-url".into()) }).await.unwrap_err();
        assert_classified_secret_error(&invalid, "resolve failed: invalid postgres url");
        let missing = resolve_aws_secret_with("missing", |_| async {
            Err(anyhow::anyhow!("stubbed lookup failed postgres://user:s3cret@host/db"))
        })
        .await
        .unwrap_err();
        assert_classified_secret_error(&missing, "resolve failed: aws-sm lookup error");
    }

    fn file_mode_config(file: &str) -> Config {
        Config::resolve(|name| match name {
            "ELECTRIC_CIRCUITS_SOURCES_MODE" => Some("file".into()),
            "ELECTRIC_CIRCUITS_SOURCES_FILE" => Some(file.into()),
            "ELECTRIC_CIRCUITS_SOURCES_POLL_SECS" => Some("1".into()),
            _ => None,
        })
        .expect("valid sources test config")
    }

    fn valid_row(source_id: &str, revision: i64, secret: &str) -> SourceRow {
        SourceRow {
            source_id: source_id.into(),
            plugin: "pgoutput".into(),
            database_secret: secret.into(),
            slot: format!("{source_id}_slot"),
            publication: format!("{source_id}_slot_pub"),
            tables: vec!["public.thread_messages".into()],
            revision,
            updated_at: "2026-09-07T00:00:00Z".into(),
        }
    }

    #[tokio::test]
    async fn shutdown_short_circuits_refresh_before_starting_sources() {
        let file = std::env::temp_dir().join(format!("circuits-sources-shutdown-{}.json", uuid::Uuid::new_v4()));
        std::fs::write(&file, b"[]").unwrap();
        let supervisor = SourcesSupervisor::new(file_mode_config(file.to_str().unwrap())).unwrap();
        supervisor.refresh().await.unwrap();
        std::fs::write(
            &file,
            serde_json::to_vec(&vec![valid_row("alpha", 1, "env:MISSING_WITH_PASSWORD_LIKE_VALUE")]).unwrap(),
        )
        .unwrap();
        supervisor.shutdown_token().begin();
        let error = supervisor.refresh().await.expect_err("refresh must stop at the shutdown cut");
        assert!(format!("{error:#}").contains("shutdown"), "{error:#}");
        assert!(supervisor.summaries().await.is_empty(), "shutdown must not start a source");
        let _ = std::fs::remove_file(file);
    }

    #[tokio::test]
    async fn shutdown_joins_the_poll_task() {
        let file = std::env::temp_dir().join(format!("circuits-sources-poll-{}.json", uuid::Uuid::new_v4()));
        std::fs::write(&file, b"[]").unwrap();
        let supervisor = SourcesSupervisor::new(file_mode_config(file.to_str().unwrap())).unwrap();
        supervisor.refresh().await.unwrap();
        supervisor.spawn_poll();
        tokio::time::timeout(std::time::Duration::from_secs(2), supervisor.shutdown_all())
            .await
            .expect("shutdown must join the poll task");
        let _ = std::fs::remove_file(file);
    }
}
