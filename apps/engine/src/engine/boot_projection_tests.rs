//! Boot-order regression through the real PostgreSQL driver over a socket-free catalog fixture.
//! The fixture supplies schema rows; the engine decides admission and when to enter the epoch.
use super::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};

async fn message(io: &mut DuplexStream, tag: u8, body: &[u8]) {
    io.write_u8(tag).await.unwrap();
    io.write_u32(body.len() as u32 + 4).await.unwrap();
    io.write_all(body).await.unwrap();
}

struct Reply {
    params: Vec<u32>,
    types: Vec<u32>,
    rows: Vec<Vec<Vec<u8>>>,
}

fn reply(query: &str) -> Reply {
    let strings = |rows: &[&[&str]]| rows.iter().map(|r| r.iter().map(|v| v.as_bytes().to_vec()).collect()).collect();
    if query == "show wal_level" {
        Reply { params: vec![], types: vec![25], rows: strings(&[&["logical"]]) }
    } else if query.contains("where c.relrowsecurity") {
        Reply { params: vec![1009, 1009], types: vec![25, 25], rows: vec![] }
    } else if query.contains("a.atttypid::int8") {
        let rows = [("id", 23i64), ("name", 25i64)]
            .into_iter()
            .map(|(col, oid)| {
                vec![
                    b"public".to_vec(),
                    b"users".to_vec(),
                    b"d".to_vec(),
                    col.as_bytes().to_vec(),
                    oid.to_be_bytes().to_vec(),
                    (-1i32).to_be_bytes().to_vec(),
                ]
            })
            .collect();
        Reply { params: vec![1009, 1009, 16], types: vec![25, 25, 25, 25, 20, 23], rows }
    } else if query.starts_with("select n.nspname, c.relname, a.attname") {
        Reply { params: vec![1009, 1009], types: vec![25, 25, 25], rows: strings(&[&["public", "users", "id"]]) }
    } else if query.contains("from information_schema.columns") {
        let mut rows = strings(&[&["id", "integer", "int4"], &["name", "text", "text"]]);
        for row in &mut rows {
            row.push(vec![0]);
        }
        Reply { params: vec![25, 25], types: vec![25, 25, 25, 16], rows }
    } else if query.starts_with("select a.attname from pg_index") {
        Reply { params: vec![25], types: vec![25], rows: strings(&[&["id"]]) }
    } else {
        panic!("unexpected preflight query: {query}");
    }
}

async fn serve(mut io: DuplexStream) {
    let size = io.read_u32().await.unwrap() as usize;
    let mut startup = vec![0; size - 4];
    io.read_exact(&mut startup).await.unwrap();
    message(&mut io, b'R', &0u32.to_be_bytes()).await;
    message(&mut io, b'Z', b"I").await;
    let mut current = reply("show wal_level");
    while let Ok(tag) = io.read_u8().await {
        let size = io.read_u32().await.unwrap() as usize;
        let mut body = vec![0; size - 4];
        io.read_exact(&mut body).await.unwrap();
        match tag {
            b'P' => {
                let query = body.split(|b| *b == 0).nth(1).unwrap();
                current = reply(std::str::from_utf8(query).unwrap());
                message(&mut io, b'1', b"").await;
            }
            b'D' => {
                let mut params = (current.params.len() as i16).to_be_bytes().to_vec();
                for oid in &current.params {
                    params.extend(oid.to_be_bytes());
                }
                message(&mut io, b't', &params).await;
                let mut columns = (current.types.len() as i16).to_be_bytes().to_vec();
                for oid in &current.types {
                    columns.extend(b"value\0");
                    columns.extend(0u32.to_be_bytes());
                    columns.extend(0i16.to_be_bytes());
                    columns.extend(oid.to_be_bytes());
                    columns.extend((-1i16).to_be_bytes());
                    columns.extend((-1i32).to_be_bytes());
                    columns.extend(1i16.to_be_bytes());
                }
                message(&mut io, b'T', &columns).await;
            }
            b'B' => message(&mut io, b'2', b"").await,
            b'E' => {
                for row in &current.rows {
                    let mut bytes = (row.len() as i16).to_be_bytes().to_vec();
                    for value in row {
                        bytes.extend((value.len() as i32).to_be_bytes());
                        bytes.extend(value);
                    }
                    message(&mut io, b'D', &bytes).await;
                }
                message(&mut io, b'C', b"SELECT 1\0").await;
            }
            b'S' => message(&mut io, b'Z', b"I").await,
            b'C' => message(&mut io, b'3', b"").await,
            b'X' => return,
            _ => panic!("unexpected PostgreSQL frontend tag {tag}; preflight must be read-only"),
        }
    }
}

#[tokio::test]
async fn ingest_projection_invalid_columns_refuse_before_entering_the_epoch() {
    let mut reads = Vec::new();
    for columns in ["id,typo", "name"] {
        let config = crate::config::Config::resolve(|key| match key {
            "ELECTRIC_CIRCUITS_PG_COLUMNS" => Some(format!("public.users:{columns}")),
            "ELECTRIC_CIRCUITS_MEMORY_BUDGET_BYTES" => Some("1024".into()),
            _ => None,
        })
        .unwrap();
        // The epoch's catalog read is a named cut: if reached, fail there before any writes.
        let store = Arc::new(crate::ds::ScriptedStore { fail_read_body: true, ..Default::default() });
        let engine = Engine::new_inner(
            DsClient::with_test_store("scripted://preflight".into(), store.clone()),
            Some("postgres://unused".into()),
            None,
            PostgresSetup::EngineManaged,
            Some(&config),
            None,
        );
        let (io, peer) = tokio::io::duplex(8192);
        let server = tokio::spawn(serve(peer));
        let (client, connection) =
            tokio_postgres::Config::new().user("test").connect_raw(io, tokio_postgres::NoTls).await.unwrap();
        let driver = tokio::spawn(async move { connection.await.unwrap() });
        let error = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            engine.setup_postgres_connected(client, &[TableSelector::One("public.users".into())], "projection_test"),
        )
        .await
        .unwrap()
        .unwrap_err();
        reads.push(store.read_count.load(Ordering::Relaxed));
        if reads.last() == Some(&0) {
            assert!(format!("{error:#}").contains("ELECTRIC_CIRCUITS_PG_COLUMNS"), "{error:#}");
        }
        assert!(store.appended.lock().unwrap().is_empty());
        engine.close(std::time::Duration::from_secs(5)).await;
        driver.abort();
        server.abort();
    }
    assert_eq!(reads, [0, 0], "invalid column names and omitted primary keys must refuse before epoch/catalog work");
}
