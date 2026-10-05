//! Real-PostgreSQL sources-table lifecycle coverage.
//!
//! The control tables, source publication, and replication slot are test fixtures only. The
//! engine is deliberately exercised with externally managed PostgreSQL objects and an in-process
//! Durable Streams HTTP store so this test isolates source discovery/reconciliation from a second
//! service process.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, Result};
use axum::body::{Body, to_bytes};
use axum::extract::Request as AxumRequest;
use axum::http::{Method, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::{Router, extract::State};
use electric_circuits_engine::config::Config;
use electric_circuits_engine::pg;
use electric_circuits_engine::sources::{SourceRow, SourcesSupervisor};
use tokio::sync::{Mutex, oneshot};
use tower::ServiceExt;

#[cfg(feature = "test-support")]
#[path = "sources_table/ownership.rs"]
mod ownership;

// Every contract owns the server's slot budget until its child exits and cleanup finishes.
// This lock is only taken by synchronous parent tests, never by an async worker or child.
static CONTRACT_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
const CHILD_ENV: &str = "SOURCES_TABLE_TEST_CHILD";

#[derive(serde::Serialize, serde::Deserialize)]
struct FixtureObjects {
    slots: Vec<String>,
    publications: Vec<String>,
    tables: Vec<String>,
}

impl FixtureObjects {
    fn record(&self) -> Result<()> {
        // Publish the complete ownership list before the first DDL, including partial setup.
        std::fs::write(std::env::temp_dir().join("fixtures.json"), serde_json::to_vec(self)?)?;
        Ok(())
    }

    async fn cleanup(&self) -> Result<()> {
        let client = pg::connect(&std::env::var("ELECTRIC_CIRCUITS_TEST_PG_URL")?).await?;
        let mut errors = Vec::new();
        for slot in &self.slots {
            let result = async {
                // The child has exited. Wait for any remaining backend on this fixture's slot.
                client.query("SELECT pg_terminate_backend(active_pid, 5000) FROM pg_replication_slots WHERE slot_name = $1 AND active", &[slot]).await?;
                client.query("SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = $1", &[slot]).await?;
                Ok::<(), anyhow::Error>(())
            }.await;
            if let Err(error) = result {
                errors.push(format!("slot {slot}: {error:#}"));
            }
        }
        for (kind, names) in [("PUBLICATION", &self.publications), ("TABLE", &self.tables)] {
            for name in names {
                if let Err(error) = client.batch_execute(&format!("DROP {kind} IF EXISTS {}", quote_ident(name))).await
                {
                    errors.push(format!("{kind} {name}: {error:#}"));
                }
            }
        }
        anyhow::ensure!(errors.is_empty(), "fixture cleanup failed: {}", errors.join("; "));
        Ok(())
    }
}

// Return false only inside the exact child. Parent cleanup also covers errors and panics in setup.
fn isolated_contract(name: &str, configure: impl FnOnce(&mut std::process::Command)) -> Result<bool> {
    if std::env::var(CHILD_ENV).as_deref() == Ok(name) {
        return Ok(false);
    }
    let _guard = CONTRACT_LOCK.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    let run_root = std::env::temp_dir().join(format!("otto-6003-run-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir(&run_root)?;
    let mut command = std::process::Command::new(std::env::current_exe()?);
    command
        .args([name, "--exact", "--ignored", "--nocapture", "--test-threads=1"])
        .env(CHILD_ENV, name)
        .env("TMPDIR", &run_root);
    configure(&mut command);
    let output = command.output();
    let cleanup = (|| -> Result<()> {
        let manifest = run_root.join("fixtures.json");
        if manifest.exists() {
            let objects: FixtureObjects = serde_json::from_slice(&std::fs::read(manifest)?)?;
            tokio::runtime::Builder::new_current_thread().enable_all().build()?.block_on(objects.cleanup())?;
        }
        std::fs::remove_dir_all(&run_root)?;
        Ok(())
    })();
    if let Err(error) = &cleanup {
        eprintln!("contract {name}: {error:#}; fixture files: {}", run_root.display());
    }
    let output = output?;
    print!("{}", String::from_utf8_lossy(&output.stdout));
    eprint!("{}", String::from_utf8_lossy(&output.stderr));
    anyhow::ensure!(output.status.success(), "isolated contract {name} failed: {}", output.status);
    cleanup?;
    Ok(true)
}

#[derive(Clone, Default)]
struct FeedDs {
    streams: Arc<Mutex<HashMap<String, Vec<serde_json::Value>>>>,
}

async fn feed_ds_handler(State(store): State<FeedDs>, request: AxumRequest) -> Response {
    let path = request.uri().path().to_string();
    match *request.method() {
        Method::HEAD => {
            let streams = store.streams.lock().await;
            let next = streams.get(&path).map_or(0, Vec::len);
            ([("stream-next-offset", next.to_string())], StatusCode::OK).into_response()
        }
        Method::PUT => {
            store.streams.lock().await.entry(path).or_default();
            StatusCode::OK.into_response()
        }
        Method::POST => {
            let body = to_bytes(request.into_body(), 64 * 1024 * 1024).await;
            let Ok(body) = body else { return StatusCode::BAD_REQUEST.into_response() };
            let values = if body.is_empty() {
                Vec::new()
            } else {
                match serde_json::from_slice::<Vec<serde_json::Value>>(&body) {
                    Ok(values) => values,
                    Err(_) => return StatusCode::BAD_REQUEST.into_response(),
                }
            };
            let mut streams = store.streams.lock().await;
            let stream = streams.entry(path).or_default();
            stream.extend(values);
            ([("stream-next-offset", stream.len().to_string())], StatusCode::OK).into_response()
        }
        Method::GET => {
            let offset = request
                .uri()
                .query()
                .and_then(|query| query.split('&').find_map(|pair| pair.strip_prefix("offset=")))
                .and_then(|value| value.parse::<usize>().ok())
                .unwrap_or(0);
            let streams = store.streams.lock().await;
            let values = streams.get(&path).cloned().unwrap_or_default();
            let page = values.get(offset..).unwrap_or_default();
            let mut response =
                ([("stream-next-offset", values.len().to_string())], serde_json::to_string(page).unwrap())
                    .into_response();
            if offset >= values.len() {
                response.headers_mut().insert("stream-up-to-date", "1".parse().unwrap());
            }
            response
        }
        Method::DELETE => {
            store.streams.lock().await.remove(&path);
            StatusCode::OK.into_response()
        }
        _ => StatusCode::METHOD_NOT_ALLOWED.into_response(),
    }
}

async fn feed_store() -> (String, oneshot::Sender<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (stop, wait) = oneshot::channel();
    tokio::spawn(async move {
        let server = axum::serve(listener, Router::new().fallback(feed_ds_handler).with_state(FeedDs::default()));
        tokio::select! { _ = server => {}, _ = wait => {} }
    });
    (format!("http://{address}"), stop)
}

fn quote_ident(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

fn source_config(
    control_url: &str,
    table: &str,
    version_table: &str,
    ds_url: &str,
    storage_dir: &str,
    control_secret: &str,
) -> Config {
    Config::resolve(|name| match name {
        "ELECTRIC_CIRCUITS_SOURCES_MODE" => Some("table".into()),
        "ELECTRIC_CIRCUITS_SOURCES_PG_URL" => Some(control_url.into()),
        "ELECTRIC_CIRCUITS_SOURCES_TABLE" => Some(table.into()),
        "ELECTRIC_CIRCUITS_SOURCES_VERSION_TABLE" => Some(version_table.into()),
        "ELECTRIC_CIRCUITS_SOURCES_POLL_SECS" => Some("1".into()),
        "ELECTRIC_CIRCUITS_SOURCES_STORAGE_DIR" => Some(storage_dir.into()),
        "ELECTRIC_CIRCUITS_DS_URL" => Some(ds_url.into()),
        "ELECTRIC_CIRCUITS_DS_IN_PROCESS_TEST" => Some("1".into()),
        "ELECTRIC_CIRCUITS_INITIALIZE_NAMESPACE" => Some("1".into()),
        "ELECTRIC_CIRCUITS_CONTROL_SECRET" => Some(control_secret.into()),
        "ELECTRIC_CIRCUITS_TRACE" => Some("0".into()),
        "ELECTRIC_CIRCUITS_BIND" => Some("127.0.0.1:0".into()),
        _ => None,
    })
    .expect("valid sources table test config")
}

async fn json(app: &Router, method: Method, uri: &str, body: Body) -> anyhow::Result<(StatusCode, serde_json::Value)> {
    let method_name = method.to_string();
    let response = app
        .clone()
        .oneshot(axum::http::Request::builder().method(method).uri(uri).body(body)?)
        .await
        .map_err(|error| anyhow::anyhow!(error.to_string()))?;
    let status = response.status();
    let body = to_bytes(response.into_body(), 64 * 1024).await?;
    let value = serde_json::from_slice(&body).with_context(|| {
        format!("parse {method_name} {uri} response ({status}): {}", String::from_utf8_lossy(&body))
    })?;
    Ok((status, value))
}

async fn wait_for<F, Fut>(label: &str, mut check: F) -> anyhow::Result<()>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<bool>>,
{
    tokio::time::timeout(std::time::Duration::from_secs(15), async {
        loop {
            if check().await? {
                return Ok(());
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    })
    .await
    .with_context(|| format!("waiting for {label}"))?
}

async fn set_revision(client: &tokio_postgres::Client, version_table: &str, revision: i64) -> anyhow::Result<()> {
    client.execute(&format!("UPDATE {} SET revision = $1", quote_ident(version_table)), &[&revision]).await?;
    Ok(())
}

#[test]
#[ignore = "requires an isolated real PostgreSQL instance via ELECTRIC_CIRCUITS_TEST_PG_URL"]
fn sources_table_discovers_restarts_stops_degrades_and_refreshes() -> Result<()> {
    if isolated_contract("sources_table_discovers_restarts_stops_degrades_and_refreshes", |_| {})? {
        return Ok(());
    }
    tokio::runtime::Builder::new_current_thread().enable_all().build()?.block_on(sources_lifecycle())
}

async fn sources_lifecycle() -> Result<()> {
    let control_url = std::env::var("ELECTRIC_CIRCUITS_TEST_PG_URL").context(
        "ELECTRIC_CIRCUITS_TEST_PG_URL is required for this ignored lifecycle test; a missing value must fail, not skip",
    )?;
    let client = pg::connect(&control_url).await?;
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let source_table = format!("circuits_thread_messages_{suffix}");
    let sources_table = format!("circuits_sources_{suffix}");
    let version_table = format!("circuits_sources_version_{suffix}");
    let slot = format!("circuits_slot_{suffix}");
    let publication = format!("{slot}_pub");
    let source_table_sql = quote_ident(&source_table);
    let sources_table_sql = quote_ident(&sources_table);
    let version_table_sql = quote_ident(&version_table);
    let publication_sql = quote_ident(&publication);

    FixtureObjects {
        slots: vec![slot.clone()],
        publications: vec![publication.clone()],
        tables: vec![sources_table.clone(), version_table.clone(), source_table.clone()],
    }
    .record()?;
    client
        .batch_execute(&format!(
            "CREATE TABLE public.{sources_table_sql} (
               source_id text PRIMARY KEY, plugin text NOT NULL, database_secret text NOT NULL,
               slot text NOT NULL, publication text NOT NULL, tables text[] NOT NULL,
               revision bigint NOT NULL, updated_at timestamptz NOT NULL);
             CREATE TABLE public.{version_table_sql} (revision bigint NOT NULL);
             INSERT INTO public.{version_table_sql} VALUES (1);
             CREATE TABLE public.{source_table_sql} (id bigint PRIMARY KEY, body text NOT NULL);
             ALTER TABLE public.{source_table_sql} REPLICA IDENTITY FULL;
             CREATE PUBLICATION {publication_sql} FOR TABLE public.{source_table_sql};",
        ))
        .await?;
    client
        .query_one("SELECT pg_create_logical_replication_slot($1, 'pgoutput')", &[&slot])
        .await
        .context("create source logical replication slot")?;

    let secret_file = std::env::temp_dir().join(format!("circuits-source-url-{suffix}"));
    std::fs::write(&secret_file, &control_url)?;
    let storage_dir = std::env::temp_dir().join(format!("circuits-sources-storage-{suffix}"));
    let (ds_url, ds_stop) = feed_store().await;
    let config = source_config(
        &control_url,
        &sources_table,
        &version_table,
        &ds_url,
        &storage_dir.to_string_lossy(),
        "sources-table-test-admin",
    );
    electric_circuits_engine::config::set_globals(
        &config.instance_id,
        &config.stack_id,
        config.secret.as_deref(),
        config.control_secret.as_deref(),
    );
    let row = SourceRow {
        source_id: "alpha".into(),
        plugin: "pgoutput".into(),
        database_secret: format!("file:{}", secret_file.display()),
        slot: slot.clone(),
        publication: publication.clone(),
        tables: vec![format!("public.{source_table}")],
        revision: 1,
        updated_at: "2026-09-07T00:00:00Z".into(),
    };
    client
        .execute(
            &format!(
                "INSERT INTO public.{sources_table_sql}
                 (source_id, plugin, database_secret, slot, publication, tables, revision, updated_at)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, now())"
            ),
            &[
                &row.source_id,
                &row.plugin,
                &row.database_secret,
                &row.slot,
                &row.publication,
                &row.tables,
                &row.revision,
            ],
        )
        .await?;

    let result = async {
        let supervisor = SourcesSupervisor::new(config.clone())?;
        supervisor.refresh().await?;
        let app = supervisor.router();
        wait_for("initial source readiness", || async {
            let (_, status) = json(&app, Method::GET, "/sources/alpha/status", Body::empty()).await?;
            Ok(status["ready"] == true)
        })
        .await?;
        let (_, status) = json(&app, Method::GET, "/sources/alpha/status", Body::empty()).await?;
        assert_eq!(status["revision"], 1);
        let shape = app
            .clone()
            .oneshot(
                axum::http::Request::get(format!("/sources/alpha/v1/shape?table=public.{source_table}&offset=-1"))
                    .body(Body::empty())?,
            )
            .await
            .map_err(|error| anyhow::anyhow!(error.to_string()))?;
        assert_eq!(shape.status(), StatusCode::OK, "a ready source must serve an Electric shape");
        let (_, health) = json(&app, Method::GET, "/sources/alpha/v1/health", Body::empty()).await?;
        assert_eq!(health["status"], "active");

        client
            .execute(
                &format!("UPDATE public.{sources_table_sql} SET revision = 2 WHERE source_id = 'alpha'"),
                &[],
            )
            .await?;
        set_revision(&client, &version_table, 2).await?;
        supervisor.spawn_poll();
        wait_for("source revision restart", || async {
            let (_, status) = json(&app, Method::GET, "/sources/alpha/status", Body::empty()).await?;
            Ok(status["revision"] == 2 && status["ready"] == true)
        })
        .await?;

        client
            .execute(&format!("DELETE FROM public.{sources_table_sql} WHERE source_id = 'alpha'"), &[])
            .await?;
        set_revision(&client, &version_table, 3).await?;
        wait_for("source removal", || async {
            let (_, sources) = json(&app, Method::GET, "/sources", Body::empty()).await?;
            Ok(sources.as_array().is_some_and(Vec::is_empty))
        })
        .await?;
        let missing = app
            .clone()
            .oneshot(axum::http::Request::get("/sources/alpha/status").body(Body::empty())?)
            .await
            .map_err(|error| anyhow::anyhow!(error.to_string()))?;
        assert_eq!(missing.status(), StatusCode::NOT_FOUND);

        client
            .execute(
                &format!(
                    "INSERT INTO public.{sources_table_sql}
                     (source_id, plugin, database_secret, slot, publication, tables, revision, updated_at)
                     VALUES ('alpha', 'pgoutput', $1, $2, $3, $4, 4, now()),
                            ('broken', 'pgoutput', 'file:/definitely/missing/circuits-secret', $2, $3, $4, 4, now())"
                ),
                &[&row.database_secret, &row.slot, &row.publication, &row.tables],
            )
            .await?;
        set_revision(&client, &version_table, 4).await?;
        wait_for("healthy and degraded sources", || async {
            let (_, sources) = json(&app, Method::GET, "/sources", Body::empty()).await?;
            Ok(sources.as_array().is_some_and(|sources| {
                sources.iter().any(|source| source["source_id"] == "alpha" && source["ready"] == true)
                    && sources.iter().any(|source| source["source_id"] == "broken" && source["ready"] == false)
            }))
        })
        .await?;

        client
            .execute(
                &format!(
                    "INSERT INTO public.{sources_table_sql}
                     (source_id, plugin, database_secret, slot, publication, tables, revision, updated_at)
                     VALUES ('refresh-only', 'pgoutput', 'file:/definitely/missing/circuits-secret', $1, $2, $3, 5, now())"
                ),
                &[&row.slot, &row.publication, &row.tables],
            )
            .await?;
        let refresh_response = app
            .clone()
            .oneshot(
                axum::http::Request::post("/admin/refresh")
                    .header("authorization", "Bearer sources-table-test-admin")
                    .body(Body::empty())?,
            )
            .await
            .map_err(|error| anyhow::anyhow!(error.to_string()))?;
        let refresh_status = refresh_response.status();
        let refresh: serde_json::Value = serde_json::from_slice(
            &to_bytes(refresh_response.into_body(), 64 * 1024).await?,
        )?;
        assert_eq!(refresh_status, StatusCode::OK);
        assert_eq!(refresh["revision"], 4);
        let (_, sources) = json(&app, Method::GET, "/sources", Body::empty()).await?;
        assert!(sources.as_array().unwrap().iter().any(|source| source["source_id"] == "refresh-only"));

        supervisor.shutdown_token().begin();
        supervisor.shutdown_all().await;
        Ok::<(), anyhow::Error>(())
    }
    .await;

    let _ = ds_stop.send(());
    // The parent removes PostgreSQL objects and files after this child exits.
    result
}
