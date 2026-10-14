//! Socket-free protocol fixture for the real backfill reader's byte-admission tests.
//! It returns fixed snapshot/row data, never an admission decision or a budget oracle.
use super::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};

pub(crate) struct BackfillFixture {
    pub client: PooledClient,
    pub table: TableSchema,
    pub release: Arc<tokio::sync::Notify>,
    pub arrived: Arc<tokio::sync::Notify>,
    server: tokio::task::JoinHandle<()>,
    driver: tokio::task::JoinHandle<()>,
}

impl Drop for BackfillFixture {
    fn drop(&mut self) {
        self.server.abort();
        self.driver.abort();
    }
}

impl BackfillFixture {
    pub async fn new(metrics: Arc<crate::metrics::Metrics>) -> Self {
        Self::with_rows(metrics, vec![r#"{"id":7}"#.into()]).await
    }

    async fn with_rows(metrics: Arc<crate::metrics::Metrics>, rows: Vec<String>) -> Self {
        let (io, peer) = tokio::io::duplex(4096);
        let release = Arc::new(tokio::sync::Notify::new());
        let arrived = Arc::new(tokio::sync::Notify::new());
        let server = tokio::spawn(serve(peer, release.clone(), arrived.clone(), rows));
        let (client, connection) =
            tokio_postgres::Config::new().user("test").connect_raw(io, tokio_postgres::NoTls).await.unwrap();
        let driver = tokio::spawn(async move { connection.await.unwrap() });
        let shutdown = crate::shutdown::ShutdownToken::with_metrics(None, metrics);
        let pool = Pool::owned("postgres://localhost/unused".into(), 1, shutdown);
        let client = PooledClient {
            client: Some(client),
            inner: pool.inner.clone(),
            permit: Some(pool.inner.sem.clone().acquire_owned().await.unwrap()),
            transaction_open: AtomicBool::new(false),
        };
        let table_ref = "public.budget_test".into();
        let def =
            serde_json::from_value(serde_json::json!({"columns":{"id":{"type":"int"}},"primaryKey":"id"})).unwrap();
        let table = TableSchema::from_def(&table_ref, &def).unwrap();
        Self { client, table, release, arrived, server, driver }
    }
}

async fn message(io: &mut DuplexStream, tag: u8, bytes: &[u8]) {
    io.write_u8(tag).await.unwrap();
    io.write_u32(bytes.len() as u32 + 4).await.unwrap();
    io.write_all(bytes).await.unwrap();
}

async fn describe(io: &mut DuplexStream, snapshot: bool) {
    let mut bytes = Vec::new();
    let types: &[u32] = if snapshot { &[25, 25, 25] } else { &[3802] };
    bytes.extend_from_slice(&(types.len() as i16).to_be_bytes());
    for oid in types {
        bytes.extend_from_slice(b"value\0");
        bytes.extend_from_slice(&0u32.to_be_bytes()); // table oid
        bytes.extend_from_slice(&0i16.to_be_bytes()); // attribute
        bytes.extend_from_slice(&oid.to_be_bytes());
        bytes.extend_from_slice(&(-1i16).to_be_bytes());
        bytes.extend_from_slice(&(-1i32).to_be_bytes());
        bytes.extend_from_slice(&1i16.to_be_bytes()); // binary
    }
    message(io, b'T', &bytes).await;
}

async fn data(io: &mut DuplexStream, values: &[&[u8]]) {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&(values.len() as i16).to_be_bytes());
    for value in values {
        bytes.extend_from_slice(&(value.len() as i32).to_be_bytes());
        bytes.extend_from_slice(value);
    }
    message(io, b'D', &bytes).await;
}

async fn serve(
    mut io: DuplexStream,
    release: Arc<tokio::sync::Notify>,
    arrived: Arc<tokio::sync::Notify>,
    rows: Vec<String>,
) {
    let size = io.read_u32().await.unwrap() as usize;
    let mut startup = vec![0; size - 4];
    io.read_exact(&mut startup).await.unwrap();
    message(&mut io, b'R', &0u32.to_be_bytes()).await;
    message(&mut io, b'Z', b"I").await;
    let mut snapshot = false;
    while let Ok(tag) = io.read_u8().await {
        let size = io.read_u32().await.unwrap() as usize;
        let mut bytes = vec![0; size - 4];
        io.read_exact(&mut bytes).await.unwrap();
        match tag {
            b'Q' => {
                message(&mut io, b'C', b"OK\0").await;
                message(&mut io, b'Z', b"T").await;
            }
            b'P' => {
                snapshot = String::from_utf8_lossy(&bytes).contains("pg_current_snapshot");
                message(&mut io, b'1', b"").await;
            }
            b'D' => {
                message(&mut io, b't', &0i16.to_be_bytes()).await;
                describe(&mut io, snapshot).await;
            }
            b'B' => message(&mut io, b'2', b"").await,
            b'E' => {
                if snapshot {
                    data(&mut io, &[b"0/10", b"1:1:", b"0/10"]).await;
                } else {
                    for row in &rows {
                        arrived.notify_one();
                        release.notified().await;
                        let mut frame = vec![1];
                        frame.extend_from_slice(row.as_bytes());
                        data(&mut io, &[&frame]).await;
                    }
                }
                message(&mut io, b'C', b"SELECT 1\0").await;
            }
            b'S' => message(&mut io, b'Z', b"T").await,
            b'C' => message(&mut io, b'3', b"").await,
            b'X' => return,
            _ => panic!("unexpected PostgreSQL frontend tag {tag}"),
        }
    }
}

#[tokio::test]
async fn memory_budget_disabled_backfill_keeps_reading_after_shutdown() {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        let metrics = Arc::new(crate::metrics::Metrics::default());
        assert!(!metrics.memory_budget.enabled());
        let rows = (0..64).map(|id| serde_json::json!({"id":id}).to_string()).collect();
        let mut pg = BackfillFixture::with_rows(metrics, rows).await;
        let shutdown = pg.client.inner.shutdown.clone();
        let scope = SettleScope::internal(&pg.table.table);
        let mut reader = backfill_reader(&mut pg.client, &pg.table, None, &scope).await.unwrap();
        reader.budget = 1; // also exercises admission for an indivisible larger frame
        pg.arrived.notified().await;
        shutdown.begin();
        for id in 0..64 {
            pg.release.notify_one();
            let row = reader.next_row().await;
            assert!(row.is_ok(), "disabled admission must not interrupt the in-flight cursor at row {id}: {row:?}");
            assert_eq!(pg.table.key_string(&row.unwrap().unwrap().0).unwrap(), id.to_string());
            if id < 63 {
                pg.arrived.notified().await;
            }
        }
        assert!(reader.next_row().await.unwrap().is_none());
        reader.finish().await;
    })
    .await
    .expect("backfill fixture deadline");
}

#[tokio::test]
async fn memory_budget_backfill_fixture_reads_real_driver_frames() {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        let mut pg = BackfillFixture::new(Arc::new(crate::metrics::Metrics::default())).await;
        pg.release.notify_one();
        let rows = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            let scope = SettleScope::internal(&pg.table.table);
            backfill_reader(&mut pg.client, &pg.table, None, &scope).await.unwrap().collect().await.unwrap().0
        })
        .await
        .expect("scripted PostgreSQL must complete the backfill");
        assert_eq!(rows.len(), 1);
        assert_eq!(pg.table.key_string(&rows[0]).unwrap(), "7");
    })
    .await
    .expect("backfill fixture reached its diagnostic deadline");
}

#[tokio::test]
async fn memory_budget_backfill_releases_each_frame_and_cancels_queued_admission() {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        use std::{future::Future, task::Poll};
        let mut metrics = crate::metrics::Metrics::default();
        metrics.memory_budget = crate::memory_budget::MemoryBudget::new(Some(100));
        let metrics = Arc::new(metrics);
        let memory = metrics.memory_budget.clone();
        let mut pg = BackfillFixture::with_rows(metrics, vec![r#"{"id":7}"#.into(), r#"{"id":8}"#.into()]).await;
        let scope = SettleScope::internal(&pg.table.table);
        let mut reader = backfill_reader(&mut pg.client, &pg.table, None, &scope).await.unwrap();
        reader.budget = 40;
        let peer = memory.acquire(60).await;
        pg.release.notify_one();
        let first = reader.next_row().await.unwrap().unwrap();
        assert_eq!(pg.table.key_string(&first.0).unwrap(), "7");
        assert_eq!(memory.held.load(Ordering::Relaxed), 60, "decoded row must release frame credit");
        let more = memory.acquire(40).await;
        let mut second = Box::pin(reader.next_row());
        std::future::poll_fn(|cx| {
            assert!(second.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        assert_eq!(memory.held.load(Ordering::Relaxed), 100, "queued frame must own no permit");
        drop(second); // cancellation must remove the queue entry without advancing the cursor
        drop(more);
        pg.release.notify_one();
        let second = reader.next_row().await.unwrap().unwrap();
        assert_eq!(pg.table.key_string(&second.0).unwrap(), "8");
        assert_eq!(memory.held.load(Ordering::Relaxed), 60);
        reader.finish().await;
        drop(peer);
        assert_eq!(memory.held.load(Ordering::Relaxed), 0);
    })
    .await
    .expect("backfill fixture reached its diagnostic deadline");
}

#[tokio::test]
async fn memory_budget_backfill_oversized_frame_releases_read_credit_before_readmission() {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        use std::{future::Future, task::Poll};
        let mut metrics = crate::metrics::Metrics::default();
        metrics.memory_budget = crate::memory_budget::MemoryBudget::new(Some(100));
        let metrics = Arc::new(metrics);
        let memory = metrics.memory_budget.clone();
        let row = serde_json::json!({"id":7, "payload":"x".repeat(200)}).to_string();
        let mut pg = BackfillFixture::with_rows(metrics, vec![row]).await;
        let scope = SettleScope::internal(&pg.table.table);
        let mut reader = backfill_reader(&mut pg.client, &pg.table, None, &scope).await.unwrap();
        reader.budget = 40;
        let peer = memory.acquire(60).await;
        pg.release.notify_one();
        let mut next = Box::pin(reader.next_row());
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            std::future::poll_fn(|cx| {
                assert!(next.as_mut().poll(cx).is_pending());
                if memory.waits.load(Ordering::Relaxed) > 0 { Poll::Ready(()) } else { Poll::Pending }
            }),
        )
        .await
        .unwrap();
        assert_eq!(memory.held.load(Ordering::Relaxed), 60, "oversized admission must relinquish its smaller permit");
        drop(peer);
        let row = tokio::time::timeout(std::time::Duration::from_secs(5), next).await.unwrap().unwrap().unwrap();
        assert_eq!(pg.table.key_string(&row.0).unwrap(), "7");
        assert_eq!(memory.held.load(Ordering::Relaxed), 0);
        reader.finish().await;
    })
    .await
    .expect("backfill fixture reached its diagnostic deadline");
}

#[tokio::test]
async fn memory_budget_ingest_schema_wait_does_not_pin_backfill_frame_admission() {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        use std::{future::Future, task::Poll};
        let mut metrics = crate::metrics::Metrics::default();
        metrics.memory_budget = crate::memory_budget::MemoryBudget::new(Some(100));
        let metrics = Arc::new(metrics);
        let memory = metrics.memory_budget.clone();
        let mut pg = BackfillFixture::new(metrics.clone()).await;
        let pool = Pool { inner: pg.client.inner.clone() };
        // The ingestor owns all byte capacity; the backfill owns the only pool connection.
        let cfg = crate::txn_buffer::TxnBufferConfig { memory_bytes: 60, append_bytes: 40, ..Default::default() };
        let mut txn = crate::txn_buffer::TxnBuffer::admitted(1, cfg, metrics).await.unwrap();
        txn.reserve_input(100).await.unwrap();
        let mut schema_connection = Box::pin(pool.get());
        let mut rows = Box::pin(async {
            let scope = SettleScope::internal(&pg.table.table);
            let mut reader = backfill_reader(&mut pg.client, &pg.table, None, &scope).await.unwrap();
            reader.budget = 40;
            let rows = reader.collect().await.unwrap().0;
            pg.client.release();
            rows
        });
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            std::future::poll_fn(|cx| {
                assert!(rows.as_mut().poll(cx).is_pending());
                assert!(schema_connection.as_mut().poll(cx).is_pending());
                if memory.waits.load(Ordering::Relaxed) > 0 { Poll::Ready(()) } else { Poll::Pending }
            }),
        )
        .await
        .unwrap();
        txn.release_for_schema().unwrap(); // the same handoff used before relation/truncate callbacks
        assert_eq!(memory.held.load(Ordering::Relaxed), 0);
        pg.release.notify_one();
        let (rows, connection) =
            tokio::time::timeout(std::time::Duration::from_secs(5), async { tokio::join!(rows, schema_connection) })
                .await
                .expect("backfill and schema work must both make progress");
        assert_eq!(rows.len(), 1);
        drop(connection.unwrap());
        assert_eq!(memory.held.load(Ordering::Relaxed), 0);
    })
    .await
    .expect("backfill fixture reached its diagnostic deadline");
}
