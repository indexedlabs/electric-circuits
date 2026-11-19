//! PostgreSQL publication-admission regressions.
//!
//! These tests use a real PostgreSQL instance selected by `ELECTRIC_CIRCUITS_TEST_PG_URL`. They
//! are ignored in the ordinary engine-only suite because that suite does not own a database.

use axum::Router;
use axum::extract::{Request, State};
use axum::http::{Method, StatusCode};
use axum::response::{IntoResponse, Response};
use electric_circuits_engine::ds::DsClient;
use electric_circuits_engine::engine::{Engine, PostgresSetup};
use electric_circuits_engine::{
    pg,
    table_ref::{TableRef, TableSelector},
};
use std::sync::{Arc, Mutex};

#[derive(Clone, Default)]
struct DsProbe(Arc<Mutex<Vec<String>>>);

async fn empty_ds(State(probe): State<DsProbe>, req: Request) -> Response {
    match *req.method() {
        Method::GET => ([("stream-next-offset", "0"), ("stream-up-to-date", "1")], "[]").into_response(),
        Method::HEAD => ([("stream-next-offset", "0")], "").into_response(),
        Method::PUT | Method::DELETE => StatusCode::OK.into_response(),
        Method::POST => {
            probe.0.lock().unwrap().push(req.uri().path().to_string());
            StatusCode::OK.into_response()
        }
        _ => StatusCode::METHOD_NOT_ALLOWED.into_response(),
    }
}

#[tokio::test]
#[ignore = "requires a real PostgreSQL instance via ELECTRIC_CIRCUITS_TEST_PG_URL"]
async fn rls_enabled_tracked_table_is_rejected_before_shapes_can_start() -> anyhow::Result<()> {
    let url = std::env::var("ELECTRIC_CIRCUITS_TEST_PG_URL")?;
    let client = pg::connect(&url).await?;
    let table_name = format!("circuits_rls_{}", std::process::id());
    client
        .batch_execute(&format!(
            "drop table if exists public.{table_name}; \
             create table public.{table_name} (id integer primary key); \
             alter table public.{table_name} enable row level security"
        ))
        .await?;
    let table = TableRef::new("public", &table_name)?;
    let result = pg::reject_rls_tables(&client, std::slice::from_ref(&table)).await;
    client.batch_execute(&format!("drop table if exists public.{table_name}")).await?;

    let error = result.expect_err("RLS-enabled tracked table must fail closed at admission");
    let message = format!("{error:#}");
    assert!(message.contains("row-level security"), "{message}");
    assert!(message.contains(&table_name), "{message}");
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real PostgreSQL instance via ELECTRIC_CIRCUITS_TEST_PG_URL"]
async fn unaffected_table_without_rls_remains_admissible() -> anyhow::Result<()> {
    let url = std::env::var("ELECTRIC_CIRCUITS_TEST_PG_URL")?;
    let client = pg::connect(&url).await?;
    let table_name = format!("circuits_plain_{}", std::process::id());
    client
        .batch_execute(&format!(
            "drop table if exists public.{table_name}; \
             create table public.{table_name} (id integer primary key)"
        ))
        .await?;
    let table = TableRef::new("public", &table_name)?;
    let result = pg::reject_rls_tables(&client, std::slice::from_ref(&table)).await;
    client.batch_execute(&format!("drop table if exists public.{table_name}")).await?;
    result?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real PostgreSQL instance via ELECTRIC_CIRCUITS_TEST_PG_URL"]
async fn setup_refuses_rls_before_creating_publication() -> anyhow::Result<()> {
    let url = std::env::var("ELECTRIC_CIRCUITS_TEST_PG_URL")?;
    let client = pg::connect(&url).await?;
    let table_name = format!("circuits_setup_rls_{}", std::process::id());
    let slot = format!("circuits_setup_rls_{}", std::process::id());
    let publication = format!("{slot}_pub");
    client
        .batch_execute(&format!(
            "drop publication if exists {publication}; \
             select pg_drop_replication_slot(slot_name) from pg_replication_slots where slot_name = '{slot}'; \
             drop table if exists public.{table_name}; \
             create table public.{table_name} (id integer primary key); \
             alter table public.{table_name} enable row level security"
        ))
        .await?;

    let probe = DsProbe::default();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let ds_url = format!("http://{}", listener.local_addr()?);
    let app = Router::new().fallback(empty_ds).with_state(probe.clone());
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    let engine = Engine::new_pg_for_in_process_test(DsClient::new_for_in_process_test(&ds_url), url.clone());
    let table = TableRef::new("public", &table_name)?;
    let result = engine.setup_postgres(&[TableSelector::One(table)], &slot).await;
    let publication_count: i64 =
        client.query_one("select count(*) from pg_publication where pubname = $1", &[&publication]).await?.get(0);
    let slot_count: i64 =
        client.query_one("select count(*) from pg_replication_slots where slot_name = $1", &[&slot]).await?.get(0);
    assert!(result.is_err(), "RLS admission must fail closed");
    assert_eq!(publication_count, 0, "RLS refusal must precede publication creation");
    assert_eq!(slot_count, 0, "RLS refusal must precede slot creation");
    assert!(probe.0.lock().unwrap().is_empty(), "RLS refusal must not append a SlotBound/catalog event");

    client
        .batch_execute(&format!(
            "drop table if exists public.{table_name}; \
             select pg_drop_replication_slot(slot_name) from pg_replication_slots where slot_name = '{slot}';"
        ))
        .await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real PostgreSQL instance via ELECTRIC_CIRCUITS_TEST_PG_URL"]
async fn externally_managed_setup_validates_without_mutating_replica_identity() -> anyhow::Result<()> {
    let url = std::env::var("ELECTRIC_CIRCUITS_TEST_PG_URL")?;
    let client = pg::connect(&url).await?;
    let table_name = format!("circuits_managed_{}", std::process::id());
    let slot = format!("circuits_managed_{}", std::process::id());
    let publication = format!("{slot}_pub");
    client
        .batch_execute(&format!(
            "drop publication if exists {publication}; \
             select pg_drop_replication_slot(slot_name) from pg_replication_slots where slot_name = '{slot}'; \
             drop table if exists public.{table_name}; \
             create table public.{table_name} (id integer primary key); \
             create publication {publication} for table public.{table_name}"
        ))
        .await?;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let ds_url = format!("http://{}", listener.local_addr()?);
    let app = Router::new().fallback(empty_ds).with_state(DsProbe::default());
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    let engine = Engine::new_pg_for_in_process_test_with_setup(
        DsClient::new_for_in_process_test(&ds_url),
        url.clone(),
        PostgresSetup::ExternallyManaged,
    );
    let table = TableRef::new("public", &table_name)?;
    let error = engine
        .setup_postgres(&[TableSelector::One(table)], &slot)
        .await
        .expect_err("the host must provision FULL identity before activation");
    let replident: String = client
        .query_one(
            // `$1::text` first: a bare `$1::regclass` makes the parameter itself a `regclass`, which
            // the driver cannot bind a Rust string to.
            "select relreplident::text from pg_class where oid = $1::text::regclass",
            &[&format!("public.{table_name}")],
        )
        .await?
        .get(0);
    assert_eq!(replident, "d", "Engine must not acquire DDL ownership");
    assert!(format!("{error:#}").contains("must already use REPLICA IDENTITY FULL"));

    client
        .batch_execute(&format!(
            "select pg_drop_replication_slot(slot_name) from pg_replication_slots where slot_name = '{slot}'; \
             drop publication if exists {publication}; \
             drop table if exists public.{table_name}"
        ))
        .await?;
    Ok(())
}
