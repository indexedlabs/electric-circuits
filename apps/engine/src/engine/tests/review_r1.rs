use super::*;
use anyhow::ensure;
use std::time::Duration;

#[tokio::test]
async fn explicit_membership_directory_survives_engine_close() -> Result<()> {
    let dir = std::env::temp_dir().join(format!("otto-r1-explicit-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir(&dir)?;
    let marker = dir.join("operator-data");
    std::fs::write(&marker, "keep me")?;
    let config = crate::config::Config::resolve(|name| {
        (name == "ELECTRIC_CIRCUITS_SUBQ_STORAGE_DIR").then(|| dir.to_string_lossy().into_owned())
    })?;
    let server = catalog::testing::FakeDs::start().await;
    let engine = Engine::new_inner(
        DsClient::new_for_in_process_test(server.url()),
        None,
        None,
        PostgresSetup::EngineManaged,
        Some(&config),
        None,
    );
    engine.close(Duration::from_secs(5)).await;
    let survived = std::fs::read_to_string(&marker).ok();
    let _ = std::fs::remove_dir_all(&dir);
    ensure!(survived.as_deref() == Some("keep me"), "Engine close deleted the operator's explicit directory or file");
    Ok(())
}

#[tokio::test]
#[ignore = "requires PostgreSQL via ELECTRIC_CIRCUITS_TEST_PG_URL"]
async fn counts_seed_error_unpublishes_and_falls_back_to_postgres() -> Result<()> {
    let url = std::env::var("ELECTRIC_CIRCUITS_TEST_PG_URL")?;
    let client = crate::pg::connect(&url).await?;
    let table: TableRef = format!("otto_r1_counts_{}", uuid::Uuid::new_v4().simple()).parse()?;
    client
        .batch_execute(&format!(
            "CREATE TABLE {table} (id bigint PRIMARY KEY, body text); INSERT INTO {table} VALUES (1, 'a'), (2, 'b')"
        ))
        .await?;
    let server = catalog::testing::FakeDs::start().await;
    let engine = Engine::new_pg_for_in_process_test(DsClient::new_for_in_process_test(server.url()), url);
    let result = async {
        let def = crate::pg::introspect(&client, &table, false).await?;
        let ts = TableSchema::from_def(&table, &def)?;
        let mut config = crate::config::Config::resolve(|_| None)?.dbsp;
        config.counts = vec![(table.clone(), vec!["body".into()])];
        engine.set_dbsp_config(config);
        let schemas = HashMap::from([(table.clone(), ts.clone())]);
        // Preserve the introspected schema, but make its seed query fail at the real PG boundary.
        client.batch_execute(&format!("ALTER TABLE {table} RENAME COLUMN body TO unavailable")).await?;
        let seeded = engine.maybe_start_arrangements(&schemas).await;
        client.batch_execute(&format!("ALTER TABLE {table} RENAME COLUMN unavailable TO body")).await?;
        ensure!(seeded.is_err(), "seed must fail against the removed column");
        ensure!(engine.arrangements.lock().unwrap().is_none(), "a failed seed left an arrangement published");
        ensure!(engine.arr_gates.read().unwrap().is_empty(), "failed seed published gates");
        *engine.tables_shared.write().unwrap() = schemas.clone();
        engine.subqueries.lock().await.set_schemas(Arc::new(schemas.clone()));
        engine.state.lock().await.tables = schemas;
        engine.health.store(HEALTH_ACTIVE, std::sync::atomic::Ordering::Relaxed);
        let shape = engine.create_aggregate(&table, None, AggFn::Count, None).await?;
        let fold = engine.dump_node(&format!("shape:{}", shape.id)).await.context("COUNT fold missing")?;
        let expected: i64 = client.query_one(&format!("SELECT count(*) FROM {table}"), &[]).await?.get(0);
        ensure!(fold["value"] == expected, "Postgres COUNT fallback differs: {fold}, expected {expected}");
        ensure!(engine.graph().await.arrangements.is_none(), "failed counts layer must not serve shapes");
        Ok::<(), anyhow::Error>(())
    }
    .await;
    engine.close(Duration::from_secs(5)).await;
    client.batch_execute(&format!("DROP TABLE {table}")).await?;
    result
}
