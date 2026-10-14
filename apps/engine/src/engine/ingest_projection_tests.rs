//! Projection restart/admission regressions. Storage is scripted; the real catalog writer,
//! retirement path, schema compiler and HTTP handlers run in-process (not PG18 qualification).
use super::*;
use crate::schema::{FingerprintColumn, SchemaFingerprint, TableDef};
use serde_json::{Value as Json, json};
use std::time::Duration;

const DEADLINE: Duration = Duration::from_secs(5);

fn definition() -> TableDef {
    let mut def: TableDef = serde_json::from_value(json!({
        "columns":{"id":{"type":"int"},"name":{"type":"text"},"payload":{"type":"text"}},
        "primaryKey":"id"
    }))
    .unwrap();
    def.fingerprint = Some(SchemaFingerprint {
        columns: ["id", "name", "payload"]
            .into_iter()
            .map(|name| FingerprintColumn {
                name: name.into(),
                type_oid: if name == "id" { 23 } else { 25 },
                typmod: -1,
            })
            .collect(),
        replident: b'f',
        pk: Some(vec!["id".into()]),
    });
    def
}

fn event(mut value: Json, eid: usize) -> Json {
    value["eid"] = json!(format!("fixture-{eid}"));
    value
}

fn projection(columns: &[&str]) -> Json {
    let names: Vec<String> = columns.iter().map(|s| (*s).into()).collect();
    json!({"t":"ingestColumns", "table":"public.users", "columns":names,
        "hash":ingest_columns_hash(&names).unwrap()})
}

fn catalog(previous: Option<&[&str]>) -> Vec<Json> {
    let rec = json!({"id":"s1", "table":"public.users", "stream_path":"shape/s1",
        "changes_only":false, "where_json":null, "columns":null, "family_key":null,
        "is_subquery":false, "aggregate":null, "fingerprint":definition().fingerprint});
    let mut events = vec![];
    if let Some(columns) = previous {
        events.push(projection(columns));
    }
    events.push(json!({"t":"created", "rec":rec, "sig":null, "subscription":"sub-a", "at":100}));
    events.push(
        serde_json::to_value(CatalogEvent::Dormant {
            id: "s1".into(),
            resume: LogPosition::start(),
            gate: crate::pg::SnapshotGate::passthrough(),
        })
        .unwrap(),
    );
    events.into_iter().enumerate().map(|(i, e)| event(e, i)).collect()
}

fn fixture(
    setting: Option<&str>,
    events: &[Json],
) -> (Engine, Arc<crate::ds::ScriptedStore>, HashMap<TableRef, TableSchema>) {
    let config = crate::config::Config::resolve(|key| match key {
        "ELECTRIC_CIRCUITS_PG_COLUMNS" => setting.map(str::to_owned),
        // Exercise every catalog read under an oversized Slice 5 permit too.
        "ELECTRIC_CIRCUITS_MEMORY_BUDGET_BYTES" => Some("1024".into()),
        _ => None,
    })
    .unwrap();
    let store = Arc::new(crate::ds::ScriptedStore {
        read_pages: std::sync::Mutex::new(vec![("tail".into(), true, serde_json::to_string(events).unwrap())]),
        stall_live_reads: true,
        ..Default::default()
    });
    let engine = Engine::new_inner(
        DsClient::with_test_store("scripted://projection".into(), store.clone()),
        None,
        None,
        PostgresSetup::EngineManaged,
        Some(&config),
        None,
    );
    let table = TableRef::parse("public.users").unwrap();
    let ts = engine.compile_ingest_table(&table, &definition()).unwrap();
    (engine, store, HashMap::from([(table, ts)]))
}

fn appended(store: &crate::ds::ScriptedStore) -> Vec<Json> {
    store
        .appended
        .lock()
        .unwrap()
        .iter()
        .filter(|(p, _, _)| p.ends_with(CATALOG_STREAM))
        .flat_map(|(_, _, body)| serde_json::from_slice::<Vec<Json>>(body).unwrap())
        .collect()
}

async fn restore(engine: &Engine, compiled: &HashMap<TableRef, TableSchema>) {
    tokio::time::timeout(DEADLINE, async {
        let fold = engine.fold_catalog().await.unwrap();
        engine.apply_catalog(fold, compiled, RestoreMode::Resume).await.unwrap();
    })
    .await
    .expect("projection restore must complete");
}

#[tokio::test]
async fn ingest_projection_legacy_compares_full_compiled_columns() {
    let mut results = Vec::new();
    for (setting, retire) in [("public.users:id,name,payload", false), ("public.users:id,name", true)] {
        // An unpublished stored generated column is in the compiled schema/backfill, but
        // absent from the physical pgoutput fingerprint.
        let mut def = definition();
        def.fingerprint.as_mut().unwrap().columns.retain(|c| c.name != "payload");
        let mut events = catalog(None);
        events[0]["rec"]["fingerprint"] = json!(def.fingerprint);
        let (engine, _, _) = fixture(Some(setting), &events);
        let table = TableRef::parse("public.users").unwrap();
        let compiled = HashMap::from([(table.clone(), engine.compile_ingest_table(&table, &def).unwrap())]);
        restore(&engine, &compiled).await;
        results.push(engine.state.lock().await.shapes.contains_key("s1") != retire);
        engine.close(DEADLINE).await;
    }
    assert_eq!(results, [true, true], "legacy narrowing is relative to compiled columns, including generated columns");
}

#[tokio::test]
async fn ingest_projection_dropped_listed_column_uses_witness_but_typo_refuses() {
    let mut after = definition();
    after.columns.remove("payload");
    after.fingerprint.as_mut().unwrap().columns.retain(|c| c.name != "payload");
    let table = TableRef::parse("public.users").unwrap();
    let setting = Some("public.users:id,name,payload");
    let mut history = catalog(Some(&["id", "name", "payload"]));
    for boot in 0..2 {
        let (engine, store, _) = fixture(setting, &history);
        let mut fold = engine.fold_catalog().await.unwrap();
        let result = engine.compile_ingest_table(&table, &after);
        assert!(result.is_ok(), "a previously witnessed column dropped by DDL must not abort boot {boot}: {result:?}");
        let compiled = HashMap::from([(table.clone(), result.unwrap())]);
        engine.reconcile_ingest_columns(&mut fold, &compiled).await.unwrap();
        if boot == 0 {
            assert!(fold.recs.is_empty(), "the changed projection must retire old shapes");
            assert_eq!(
                appended(&store).iter().map(|e| e["t"].as_str().unwrap()).collect::<Vec<_>>(),
                ["dropped", "retired", "ingestColumns"]
            );
        } else {
            assert!(fold.recs.contains_key("s2"), "post-DDL shape survives the next restart");
            assert!(appended(&store).is_empty());
        }
        history.extend(appended(&store));
        if boot == 0 {
            let mut created = catalog(None).remove(0);
            created["rec"]["id"] = json!("s2");
            created["rec"]["stream_path"] = json!("shape/s2");
            created["rec"]["fingerprint"] = json!(after.fingerprint);
            history.push(event(created, 900));
        }
        engine.close(DEADLINE).await;
    }
    // The exact same absent name, without evidence that it ever existed, remains a typo.
    let (engine, store, _) = fixture(setting, &catalog(Some(&["id", "name"])));
    engine.fold_catalog().await.unwrap();
    let error = engine.compile_ingest_table(&table, &after).unwrap_err();
    assert!(format!("{error:#}").contains("payload"));
    assert!(appended(&store).is_empty());
    engine.close(DEADLINE).await;
}

#[tokio::test]
async fn ingest_projection_legacy_all_columns_records_witness_for_add_column_restart() {
    let setting = Some("public.users:id,name,payload");
    let (engine, store, compiled) = fixture(setting, &catalog(None));
    restore(&engine, &compiled).await;
    let writes = appended(&store);
    assert_eq!(
        writes.iter().map(|e| e["t"].as_str().unwrap()).collect::<Vec<_>>(),
        ["ingestColumns"],
        "legacy explicit all must record provenance without retiring shapes"
    );
    assert!(engine.state.lock().await.shapes.contains_key("s1"));
    engine.close(DEADLINE).await;

    // Runtime ADD COLUMN: normal schema drift has retired the old shape; a newly created
    // shape carries the post-DDL fingerprint and exactly the same projected set.
    let mut after = definition();
    after.columns.insert("added".into(), after.columns["name"].clone());
    after.fingerprint.as_mut().unwrap().columns.push(FingerprintColumn {
        name: "added".into(),
        type_oid: 25,
        typmod: -1,
    });
    let mut events = writes;
    let mut post_ddl = catalog(None);
    post_ddl[0]["rec"]["fingerprint"] = json!(after.fingerprint);
    events.extend(post_ddl);
    let (engine, store, _) = fixture(setting, &events);
    let table = TableRef::parse("public.users").unwrap();
    let compiled = HashMap::from([(table.clone(), engine.compile_ingest_table(&table, &after).unwrap())]);
    restore(&engine, &compiled).await;
    assert!(engine.state.lock().await.shapes.contains_key("s1"), "post-DDL shape must survive restart");
    assert!(appended(&store).is_empty(), "the unchanged projection has no second transition");
    engine.close(DEADLINE).await;
}

#[tokio::test]
async fn ingest_projection_restart_matrix() {
    for (label, previous, setting, retire) in [
        ("unchanged", Some(&["id", "name"][..]), Some("public.users:name,id"), false),
        ("widen", Some(&["id"][..]), Some("public.users:id,name"), true),
        ("narrow", Some(&["id", "name"][..]), Some("public.users:id"), true),
        ("removed", Some(&["id", "name"][..]), None, true),
        ("legacy", None, None, false),
        ("legacy first projection", None, Some("public.users:id"), true),
        ("legacy explicit all", None, Some("public.users:payload,id,name"), false),
    ] {
        let (engine, store, compiled) = fixture(setting, &catalog(previous));
        restore(&engine, &compiled).await;
        assert_eq!(engine.state.lock().await.shapes.contains_key("s1"), !retire, "{label}");
        assert_eq!(engine.metrics.memory_budget.held.load(Ordering::Relaxed), 0, "catalog permits released");
        let events = appended(&store);
        let kinds: Vec<_> = events.iter().filter_map(|e| e["t"].as_str()).collect();
        if retire {
            assert_eq!(kinds, ["dropped", "retired", "ingestColumns"], "{label}");
            let physical = compiled.values().next().unwrap().fingerprint.as_ref().unwrap();
            assert_eq!(physical, definition().fingerprint.as_ref().unwrap(), "physical fingerprint untouched");
            assert_eq!(
                events.last().unwrap()["columns"],
                json!(
                    compiled.values().next().unwrap().columns.iter().map(|(name, _)| name.clone()).collect::<Vec<_>>()
                )
            );
            assert!(
                store.operations.lock().unwrap().iter().any(|op| op.starts_with("delete:") && op.ends_with("shape/s1"))
            );
        } else if label == "legacy explicit all" {
            assert_eq!(kinds, ["ingestColumns"], "explicit settings always establish a witness");
        } else {
            assert!(kinds.is_empty(), "unchanged columns must not append events: {label}");
        }
        engine.close(DEADLINE).await;
    }
}

#[tokio::test]
async fn ingest_projection_inner_dependency_retires_before_witness() {
    let mut events = catalog(Some(&["id", "name"]));
    let rec = &mut events[1]["rec"];
    rec["table"] = json!("public.outer_table");
    rec["is_subquery"] = json!(true);
    rec["where_json"] = json!({"col":"id", "in":{"table":"public.users", "project":"id"}});
    let (engine, store, compiled) = fixture(Some("public.users:id"), &events);
    restore(&engine, &compiled).await;
    let events = appended(&store);
    assert_eq!(
        events.iter().map(|e| e["t"].as_str().unwrap()).collect::<Vec<_>>(),
        ["dropped", "retired", "ingestColumns"]
    );
    engine.close(DEADLINE).await;
}

#[tokio::test]
async fn ingest_projection_electric_refuses_output_and_predicate_columns() {
    use tower::ServiceExt;
    let (engine, _, compiled) = fixture(Some("public.users:id"), &[]);
    engine.state.lock().await.tables = compiled;
    for query in ["columns=payload", "where=payload%3D%27x%27"] {
        let response = crate::http::router(engine.clone())
            .oneshot(
                axum::http::Request::get(format!("/v1/shape?table=public.users&offset=-1&{query}"))
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = response.status();
        let bytes = axum::body::to_bytes(response.into_body(), 16384).await.unwrap();
        let body = String::from_utf8(bytes.to_vec()).unwrap();
        assert_eq!(status, axum::http::StatusCode::BAD_REQUEST, "{body}");
        assert!(body.contains("public.users") && body.contains("payload"), "{body}");
    }
    engine.close(DEADLINE).await;
}

#[tokio::test]
async fn ingest_projection_crash_cuts_replay_retirement_before_witness() {
    // Every durable prefix: before intent, after intent, and after completion but before witness.
    // Existing completed retirements need no second delete; unfinished ones are retried.
    for cut in 0..=2 {
        let mut events = catalog(Some(&["id"]));
        if cut >= 1 {
            events.push(event(json!({"t":"dropped", "id":"s1"}), 10));
        }
        if cut >= 2 {
            events.push(event(json!({"t":"retired", "id":"s1"}), 11));
        }
        let (engine, store, compiled) = fixture(Some("public.users:id,name"), &events);
        restore(&engine, &compiled).await;
        let writes = appended(&store);
        let kinds: Vec<_> = writes.iter().map(|e| e["t"].as_str().unwrap()).collect();
        let expected: &[&str] = match cut {
            0 => &["dropped", "retired", "ingestColumns"],
            1 => &["retired", "ingestColumns"],
            _ => &["ingestColumns"],
        };
        assert_eq!(kinds, expected, "crash cut {cut}");
        assert!(!engine.state.lock().await.shapes.contains_key("s1"));
        let deletes = store.operations.lock().unwrap().iter().filter(|op| op.starts_with("delete:")).count();
        assert_eq!(deletes, usize::from(cut < 2));
        // Re-fold precisely the durable prefix plus successful writes: the next boot does nothing.
        events.extend(writes);
        engine.close(DEADLINE).await;
        let (engine, store, compiled) = fixture(Some("public.users:id,name"), &events);
        restore(&engine, &compiled).await;
        assert!(appended(&store).is_empty());
        engine.close(DEADLINE).await;
    }
}

#[tokio::test]
async fn ingest_projection_new_witness_waits_for_durable_retirement() {
    use std::future::Future;
    let events = catalog(Some(&["id"]));
    let (engine, store, compiled) = fixture(Some("public.users:id,name"), &events);
    let gate = Arc::new(crate::ds::BodyGate::default());
    *store.catalog_event_gate.lock().unwrap() = Some(("retired".into(), gate.clone()));
    let mut boot = Box::pin(restore(&engine, &compiled));
    tokio::time::timeout(
        DEADLINE,
        std::future::poll_fn(|cx| {
            assert!(boot.as_mut().poll(cx).is_pending(), "boot cannot finish before Retired lands");
            if gate.arrived.load(Ordering::SeqCst) > 0 {
                std::task::Poll::Ready(())
            } else {
                cx.waker().wake_by_ref();
                std::task::Poll::Pending
            }
        }),
    )
    .await
    .unwrap();
    assert_eq!(appended(&store).iter().map(|e| e["t"].as_str().unwrap()).collect::<Vec<_>>(), ["dropped"]);
    assert!(engine.state.lock().await.shapes.is_empty());
    gate.released.notify_one();
    tokio::time::timeout(DEADLINE, boot).await.unwrap();
    assert_eq!(appended(&store).last().unwrap()["t"], "ingestColumns");
    engine.close(DEADLINE).await;
}

#[tokio::test]
async fn ingest_projection_catalog_refuses_malformed_provenance() {
    for invalid in [
        json!({"table":"users"}),
        json!({"hash":"wrong"}),
        json!({"columns":["id","id"]}),
        json!({"columns":["name","id"]}),
        json!({"columns":[]}),
        json!({"columns":null}),
    ] {
        let mut record = projection(&["id"]);
        record.as_object_mut().unwrap().extend(invalid.as_object().unwrap().clone());
        let (engine, _, _) = fixture(None, &[event(record, 0)]);
        assert!(engine.fold_catalog().await.is_err(), "malformed provenance must refuse boot: {invalid}");
        engine.close(DEADLINE).await;
    }
}

#[tokio::test]
async fn ingest_projection_validates_boot_schema_and_native_http_columns() {
    use tower::ServiceExt;
    let (engine, _, mut compiled) = fixture(Some("public.users:name,id;public.inner:id"), &[]);
    let users = TableRef::parse("public.users").unwrap();
    let inner = TableRef::parse("public.inner").unwrap();
    assert!(
        engine.validate_ingest_tables(std::slice::from_ref(&users)).unwrap_err().to_string().contains("public.inner")
    );
    let mut no_pk = definition();
    no_pk.primary_key = vec!["id".into(), "payload".into()];
    assert!(
        format!("{:#}", engine.compile_ingest_table(&users, &no_pk).unwrap_err())
            .contains("primary-key column 'payload'")
    );
    let mut missing = definition();
    missing.columns.remove("name");
    assert!(
        format!("{:#}", engine.compile_ingest_table(&users, &missing).unwrap_err()).contains("unknown column 'name'")
    );
    compiled.insert(inner.clone(), engine.compile_ingest_table(&inner, &definition()).unwrap());
    engine.state.lock().await.tables = compiled.clone();
    *engine.tables_shared.write().unwrap() = compiled.clone();
    engine.subqueries.lock().await.set_schemas(Arc::new(compiled));
    for extra in [
        json!({"columns":["payload"]}),
        json!({"where":{"col":"payload","op":"eq","value":"x"}}),
        json!({"where":{"col":"id","in":{"table":"public.inner","project":"payload"}}}),
        json!({"where":{"col":"id","in":{"table":"public.inner","project":"id",
            "where":{"col":"payload","isNull":true}}}}),
    ] {
        let expected_table = if extra.to_string().contains("public.inner") { "public.inner" } else { "public.users" };
        let mut request = json!({"table":"public.users", "changesOnly":true});
        request.as_object_mut().unwrap().extend(extra.as_object().unwrap().clone());
        let response = tokio::time::timeout(
            DEADLINE,
            crate::http::router(engine.clone()).oneshot(
                axum::http::Request::post("/v1/shapes")
                    .header("content-type", "application/json")
                    .body(axum::body::Body::from(request.to_string()))
                    .unwrap(),
            ),
        )
        .await
        .unwrap()
        .unwrap();
        let status = response.status();
        let body = axum::body::to_bytes(response.into_body(), 16384).await.unwrap();
        let body = String::from_utf8(body.to_vec()).unwrap();
        assert_eq!(status, axum::http::StatusCode::BAD_REQUEST, "{body}");
        assert!(body.contains(expected_table) && body.contains("payload"), "{body}");
    }
    engine.close(DEADLINE).await;
}

/// Real Postgres boot + fresh snapshot after widening, with scripted stream storage. This owns
/// its table/slot/publication, and checks public shape output against independently written SQL.
#[tokio::test]
#[ignore = "requires PostgreSQL via ELECTRIC_CIRCUITS_TEST_PG_URL; stream storage is scripted"]
async fn ingest_projection_real_postgres_widening_backfills_new_column() -> Result<()> {
    let url = std::env::var("ELECTRIC_CIRCUITS_TEST_PG_URL")?;
    let client = crate::pg::connect(&url).await?;
    let name = format!("projection_{}", uuid::Uuid::new_v4().simple());
    let table = TableRef::new("public", &name)?;
    let slot = format!("{name}_slot");
    client
        .batch_execute(&format!(
            "create table {} (id integer primary key, name text, payload text); \
         insert into {} values (7, 'newly included', 'unlisted payload')",
            table.quote_qualified(),
            table.quote_qualified(),
        ))
        .await?;
    let mut events = Vec::new();
    let mut old_id = None;
    let mut failure = Ok(());
    for columns in ["id", "id,name"] {
        let config = crate::config::Config::resolve(|key| match key {
            "ELECTRIC_CIRCUITS_PG_COLUMNS" => Some(format!("{table}:{columns}")),
            "ELECTRIC_CIRCUITS_MEMORY_BUDGET_BYTES" => Some("67108864".into()),
            _ => None,
        })?;
        let store = Arc::new(crate::ds::ScriptedStore {
            read_pages: std::sync::Mutex::new(vec![("tail".into(), true, serde_json::to_string(&events)?)]),
            stall_live_reads: true,
            ..Default::default()
        });
        let engine = Engine::new_inner(
            DsClient::with_test_store("scripted://pg-projection".into(), store.clone()),
            Some(url.clone()),
            None,
            PostgresSetup::EngineManaged,
            Some(&config),
            None,
        );
        let run = tokio::time::timeout(Duration::from_secs(30), async {
            engine.setup_postgres(&[TableSelector::One(table.clone())], &slot).await?;
            if let Some(id) = &old_id {
                anyhow::ensure!(!engine.state.lock().await.shapes.contains_key(id), "old shape survived widening");
                anyhow::ensure!(
                    store
                        .operations
                        .lock()
                        .unwrap()
                        .iter()
                        .any(|op| op.starts_with("delete:") && op.ends_with(&format!("shape/{id}"))),
                    "old stream was not deleted"
                );
            }
            let rec = engine.create_shape(&table, None, None, false, false).await?;
            let body: Vec<Envelope> = store
                .appended
                .lock()
                .unwrap()
                .iter()
                .filter(|(p, _, _)| p.ends_with(&rec.stream_path))
                .flat_map(|(_, _, b)| serde_json::from_slice::<Vec<Envelope>>(b).unwrap())
                .collect();
            let oracle: Json = client
                .query_one(
                    &format!("select to_jsonb(s) from (select {columns} from {}) s", table.quote_qualified()),
                    &[],
                )
                .await?
                .get(0);
            anyhow::ensure!(
                body.iter().any(|e| e.value.as_ref() == Some(&oracle)),
                "fresh shape must backfill exactly the configured columns: expected {oracle}, got {body:?}"
            );
            anyhow::ensure!(
                body.iter().all(|e| e.value.as_ref().is_none_or(|v| v.get("payload").is_none())),
                "backfill read an unlisted payload"
            );
            old_id = Some(rec.id);
            Ok::<(), anyhow::Error>(())
        })
        .await;
        engine.close(Duration::from_secs(10)).await;
        events.extend(appended(&store));
        failure = run.unwrap_or_else(|error| Err(anyhow::anyhow!("real Postgres projection deadline: {error}")));
        if failure.is_err() {
            break;
        }
    }
    client
        .batch_execute(&format!(
            "select pg_drop_replication_slot(slot_name) from pg_replication_slots where slot_name = '{slot}'; \
         drop publication if exists {slot}_pub; drop table {}",
            table.quote_qualified(),
        ))
        .await?;
    failure
}

#[tokio::test]
async fn ingest_projection_keeps_other_tables_and_deduplicates_witnesses() {
    let mut events = catalog(Some(&["id"]));
    // A later set wins; a replayed older eid must not revert it.
    let old = events[0].clone();
    events.push(event(projection(&["id", "name"]), 20));
    events.push(old);
    let mut other = events[1].clone();
    other["eid"] = json!("other-created");
    other["rec"]["id"] = json!("s2");
    other["rec"]["table"] = json!("public.other");
    other["rec"]["stream_path"] = json!("shape/s2");
    events.push(other);
    let mut dormant = events[2].clone();
    dormant["eid"] = json!("other-dormant");
    dormant["id"] = json!("s2");
    events.push(dormant);
    let (engine, store, mut compiled) = fixture(Some("public.users:id"), &events);
    let other = TableRef::parse("public.other").unwrap();
    compiled.insert(other.clone(), engine.compile_ingest_table(&other, &definition()).unwrap());
    restore(&engine, &compiled).await;
    let state = engine.state.lock().await;
    assert!(!state.shapes.contains_key("s1"), "latest witness was wider and must retire");
    assert!(state.shapes.contains_key("s2"), "one table's change must preserve another table's shape");
    drop(state);
    assert_eq!(
        appended(&store).iter().filter(|e| e["t"] == "dropped").map(|e| e["id"].clone()).collect::<Vec<_>>(),
        [json!("s1")]
    );
    engine.close(DEADLINE).await;
}

fn source_receipt_matches(event: &Json, commit: &str) -> bool {
    event["t"] == "sourceDrained" && event["sourceCommitId"] == commit
}

#[test]
fn ingest_projection_receipt_observer_matches_the_catalog_wire_contract() {
    let commit = "018f5f4d-70c2-7d70-a4d5-5f7355078f85";
    let receipt = json!({"t":"sourceDrained", "sourceCommitId":commit, "commitLsn":"0/16B6C50"});
    assert!(
        source_receipt_matches(&receipt, commit),
        "the live-test fence must recognize the catalog's camelCase receipt"
    );
    assert!(!source_receipt_matches(&receipt, "another-commit"));
    assert!(!source_receipt_matches(&json!({"t":"other", "sourceCommitId":commit}), commit));
}

/// Inherited real-PG regression: real pgoutput, snapshot/engine and public shape reads; the
/// append/read log is an in-memory fixture, so this is not file-backed release qualification.
#[tokio::test]
#[ignore = "requires PostgreSQL via ELECTRIC_CIRCUITS_TEST_PG_URL; stream storage is scripted"]
async fn ingest_projection_real_postgres_live_changes_match_sql() -> Result<()> {
    let url = std::env::var("ELECTRIC_CIRCUITS_TEST_PG_URL")?;
    let mut client = crate::pg::connect(&url).await?;
    let name = format!("projection_{}", uuid::Uuid::new_v4().simple());
    let table = TableRef::new("public", &name)?;
    let slot = format!("{name}_slot");
    client
        .batch_execute(&format!(
            "create table {} (id integer primary key, name text, payload text); \
         create table if not exists public.circuits_source_fence (source_commit_id uuid primary key)",
            table.quote_qualified(),
        ))
        .await?;
    let config = crate::config::Config::resolve(|key| match key {
        "ELECTRIC_CIRCUITS_PG_COLUMNS" => Some(format!("{table}:id,name")),
        "ELECTRIC_CIRCUITS_MEMORY_BUDGET_BYTES" => Some("134217728".into()),
        _ => None,
    })?;
    let store = Arc::new(crate::ds::ScriptedStore { read_appended: true, ..Default::default() });
    let engine = Engine::new_inner(
        DsClient::with_test_store("scripted://projection-live".into(), store.clone()),
        Some(url),
        None,
        PostgresSetup::EngineManaged,
        Some(&config),
        None,
    );
    let result = tokio::time::timeout(Duration::from_secs(45), async {
        engine.setup_postgres(&[TableSelector::One(table.clone())], &slot).await?;
        let shape = engine.create_shape(&table, None, None, false, false).await?;
        let mut rows = std::collections::BTreeMap::new();
        let mut offset = "-1".to_owned();
        for sql in [
            format!("insert into {} values (1, 'first', repeat('x', 16384))", table.quote_qualified()),
            format!(
                "update {} set name = 'second', payload = repeat('y', 32768) where id = 1",
                table.quote_qualified()
            ),
            format!("update {} set payload = 'unlisted-only change' where id = 1", table.quote_qualified()),
            format!("delete from {} where id = 1", table.quote_qualified()),
        ] {
            let commit = uuid::Uuid::new_v4();
            let tx = client.transaction().await?;
            tx.batch_execute(&sql).await?;
            // Independent oracle at exactly this source transaction, before its final marker.
            let oracle: Vec<Json> = tx
                .query(
                    &format!(
                        "select jsonb_build_object('id', id, 'name', name) from {} order by id",
                        table.quote_qualified()
                    ),
                    &[],
                )
                .await?
                .iter()
                .map(|row| row.get(0))
                .collect();
            tx.execute("insert into public.circuits_source_fence (source_commit_id) values ($1)", &[&commit]).await?;
            tx.commit().await?;
            // Event-driven causal fence: the sequencer writes SourceDrained only after all
            // preceding direct/deferred outputs. No sleeps, unrelated SQL polling or sentinels.
            loop {
                let changed = store.appended_changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                if appended(&store).iter().any(|ev| source_receipt_matches(ev, &commit.to_string())) {
                    break;
                }
                changed.await;
            }
            loop {
                let page = engine.read_shape_stream(&shape.stream_path, &offset, false).await?;
                for env in page.envelopes {
                    for image in [&env.value, &env.old].into_iter().flatten() {
                        anyhow::ensure!(image.get("payload").is_none(), "unlisted column leaked: {image}");
                    }
                    if env.headers.operation == "delete" {
                        rows.remove(&env.key);
                    } else if let Some(value) = env.value {
                        rows.insert(env.key, value);
                    }
                }
                let next = page.next_offset.unwrap_or_else(|| offset.clone());
                let done = page.up_to_date || next == offset;
                offset = next;
                if done {
                    break;
                }
            }
            anyhow::ensure!(
                rows.values().cloned().collect::<Vec<_>>() == oracle,
                "shape differs from SQL after {sql}: {rows:?} vs {oracle:?}"
            );
            client.execute("delete from public.circuits_source_fence where source_commit_id = $1", &[&commit]).await?;
        }
        Ok::<(), anyhow::Error>(())
    })
    .await
    .unwrap_or_else(|e| Err(anyhow::anyhow!("live projection deadline: {e}")));
    engine.close(Duration::from_secs(10)).await;
    client.batch_execute(&format!("select pg_drop_replication_slot(slot_name) from pg_replication_slots where slot_name = '{slot}'; drop publication if exists {slot}_pub; drop table {}", table.quote_qualified())).await?;
    result
}
