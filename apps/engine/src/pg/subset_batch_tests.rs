//! A batch's members against real PostgreSQL. A member can be held inside the server on an advisory
//! lock the test owns, which names the moments these tests order: "this member is reading" comes from
//! `pg_locks`, never from a sleep.
use std::time::Duration;

use super::*;

const DEADLINE: Duration = Duration::from_secs(15);

struct Fixture {
    url: String,
    client: Client,
    tables: Vec<TableSchema>,
    gate_key: i64,
}

impl Fixture {
    /// `rows.len()` new tables, each holding its count of rows, so a page's length names its table.
    async fn new(rows: &[i64]) -> Result<Self> {
        let url = std::env::var("ELECTRIC_CIRCUITS_TEST_PG_URL")?;
        let client = connect(&url).await?;
        let mut tables = Vec::new();
        for count in rows {
            let table: TableRef = format!("subset_batch_{}", uuid::Uuid::new_v4().simple()).parse()?;
            client
                .batch_execute(&format!(
                    "CREATE TABLE {table} (id bigint PRIMARY KEY, body text);
                     INSERT INTO {table} SELECT n, 'row' FROM generate_series(1, {count}) n"
                ))
                .await?;
            let def = introspect(&client, &table, false).await?;
            tables.push(TableSchema::from_def(&table, &def)?);
        }
        let gate_key = i64::from(uuid::Uuid::new_v4().as_fields().0 >> 1);
        Ok(Fixture { url, client, tables, gate_key })
    }

    /// Hold every read gated on this fixture's lock.
    async fn close_gate(&self) -> Result<()> {
        self.client.execute("select pg_advisory_lock($1)", &[&self.gate_key]).await?;
        Ok(())
    }

    async fn open_gate(&self) -> Result<()> {
        self.client.execute("select pg_advisory_unlock($1)", &[&self.gate_key]).await?;
        Ok(())
    }

    /// A member's read of table `index`.
    fn plain(&self, index: usize) -> Member {
        Member { ts: self.tables[index].clone(), where_sql: None }
    }

    /// A member's read of table `index`, held at its first row while the gate is closed.
    fn gated(&self, index: usize) -> Member {
        let gate = format!("pg_advisory_xact_lock_shared({})::text is not null", self.gate_key);
        Member { ts: self.tables[index].clone(), where_sql: Some((gate, Vec::new())) }
    }

    /// A member's read of table `index`, failing with `division_by_zero` at its first row.
    fn failing(&self, index: usize) -> Member {
        Member { ts: self.tables[index].clone(), where_sql: Some(("1 / (id - id) = 1".into(), Vec::new())) }
    }

    /// Reads held on the gate right now.
    async fn held_reads(&self) -> Result<i64> {
        let row = self
            .client
            .query_one(
                "select count(*) from pg_locks where locktype = 'advisory' and not granted and objid::bigint = $1",
                &[&self.gate_key],
            )
            .await?;
        Ok(row.get(0))
    }

    /// Wait until `count` reads are held on the gate at once.
    async fn wait_held(&self, count: i64) -> Result<()> {
        let deadline = std::time::Instant::now() + DEADLINE;
        loop {
            let held = self.held_reads().await?;
            if held == count {
                return Ok(());
            }
            if std::time::Instant::now() > deadline {
                bail!("expected {count} members reading at once, found {held}");
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// Commit a new row to every table, outside any pool.
    async fn write_everywhere(&self) -> Result<()> {
        for ts in &self.tables {
            self.client.batch_execute(&format!("INSERT INTO {} VALUES (1000, 'late')", ts.table)).await?;
        }
        Ok(())
    }

    async fn count(&self, index: usize) -> Result<i64> {
        let row = self.client.query_one(&format!("select count(*) from {}", self.tables[index].table), &[]).await?;
        Ok(row.get(0))
    }

    /// Fixture connections and pooled ones left in a transaction that touched these tables.
    async fn stranded_transactions(&self) -> Result<i64> {
        let mut stranded = 0;
        for ts in &self.tables {
            let row = self
                .client
                .query_one(
                    "select count(*) from pg_stat_activity
                     where state like 'idle in transaction%' and position($1 in query) > 0",
                    &[&ts.table.name()],
                )
                .await?;
            stranded += row.get::<_, i64>(0);
        }
        Ok(stranded)
    }

    async fn cleanup(&self) -> Result<()> {
        for ts in &self.tables {
            self.client.batch_execute(&format!("DROP TABLE IF EXISTS {}", ts.table)).await?;
        }
        Ok(())
    }
}

struct Member {
    ts: TableSchema,
    where_sql: Option<(String, Vec<String>)>,
}

struct Loaded {
    pages: Vec<usize>,
    lsn: String,
    snapshot: String,
    horizon: String,
    timing: SubsetBatchTiming,
}

/// The batch as `Engine::query_subset_batch` runs it, on a pooled connection.
async fn load(pool: Pool, members: Vec<Member>) -> Result<Loaded> {
    let mut client = pool.get().await?;
    let mut scope = SettleScope::request(&members[0].ts.table);
    for member in &members[1..] {
        scope = scope.with([&member.ts.table]);
    }
    let reads = members
        .iter()
        .map(|m| SubsetRead { ts: &m.ts, where_sql: m.where_sql.clone(), order: None, limit: None, offset: None })
        .collect();
    let mut timing = SubsetBatchTiming::default();
    let batch = query_subset_batch_where(&mut client, reads, &scope, &mut timing).await?;
    Ok(Loaded {
        pages: batch.pages.iter().map(Vec::len).collect(),
        lsn: batch.lsn,
        snapshot: batch.snapshot,
        horizon: batch.horizon,
        timing,
    })
}

fn start(pool: &Pool, members: Vec<Member>) -> tokio::task::JoinHandle<Result<Loaded>> {
    tokio::spawn(load(pool.clone(), members))
}

async fn wait_in_use(pool: &Pool, count: usize) -> Result<()> {
    let deadline = std::time::Instant::now() + DEADLINE;
    while pool.in_use() != count {
        if std::time::Instant::now() > deadline {
            bail!("expected {count} checkouts, found {}", pool.in_use());
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    Ok(())
}

async fn finished(load: tokio::task::JoinHandle<Result<Loaded>>) -> Result<Loaded> {
    tokio::time::timeout(DEADLINE, load).await.context("the batch did not finish")??
}

/// Members read concurrently, up to the cap, and a commit that lands while they read is invisible to
/// every one of them, including the members that begin after it.
#[tokio::test]
#[ignore = "requires PostgreSQL via ELECTRIC_CIRCUITS_TEST_PG_URL"]
async fn members_read_at_once_up_to_the_cap_and_ignore_a_later_commit() -> Result<()> {
    let f = Fixture::new(&[1, 2, 3, 4, 5]).await?;
    let pool = Pool::new(f.url.clone(), 8);
    f.close_gate().await?;
    let loading = start(&pool, (0..5).map(|i| f.gated(i)).collect());
    f.wait_held(3).await?;
    f.write_everywhere().await?;
    // No further connection joins while three members are held and two wait for a worker.
    for _ in 0..20 {
        tokio::time::sleep(Duration::from_millis(10)).await;
        anyhow::ensure!(pool.in_use() == 3, "a batch took {} connections", pool.in_use());
    }
    f.open_gate().await?;
    let loaded = finished(loading).await?;
    assert_eq!(loaded.pages, [1, 2, 3, 4, 5], "pages come back in request order, without the commit");
    assert_eq!(f.count(4).await?, 6, "the commit is real");
    assert!(!loaded.lsn.is_empty() && !loaded.snapshot.is_empty() && !loaded.horizon.is_empty());
    let reads = loaded.timing.reads.lock().unwrap();
    assert_eq!(reads.connections, 3);
    assert_eq!(reads.members.len(), 5);
    drop(reads);
    pool.close();
    f.cleanup().await
}

/// A helper that connects only after a commit still reads the snapshot taken before it, because it
/// imports the first connection's snapshot rather than taking its own.
#[tokio::test]
#[ignore = "requires PostgreSQL via ELECTRIC_CIRCUITS_TEST_PG_URL"]
async fn a_helper_connecting_after_a_commit_reads_the_earlier_snapshot() -> Result<()> {
    let f = Fixture::new(&[1, 2, 3]).await?;
    let pool = Pool::new(f.url.clone(), 3);
    let held_slots = [pool.hold_slot_for_test().await, pool.hold_slot_for_test().await];
    f.close_gate().await?;
    let loading = start(&pool, vec![f.gated(0), f.plain(1), f.plain(2)]);
    f.wait_held(1).await?;
    f.write_everywhere().await?;
    drop(held_slots);
    // The helpers got their slots after the commit and have read and gone; the first connection is
    // still held, so its exporting transaction is what they imported from.
    wait_in_use(&pool, 1).await?;
    f.open_gate().await?;
    let loaded = finished(loading).await?;
    assert_eq!(loaded.pages, [1, 2, 3], "a member read the commit");
    assert_eq!(f.count(2).await?, 4, "the commit is real");
    let reads = loaded.timing.reads.lock().unwrap();
    // The first helper can read both members before the second is scheduled.
    assert!(reads.connections >= 2, "a helper must have read, or this proves nothing: {}", reads.connections);
    drop(reads);
    pool.close();
    f.cleanup().await
}

/// One member's failure fails the load, even with others mid-read, and every connection comes back
/// clean and usable.
#[tokio::test]
#[ignore = "requires PostgreSQL via ELECTRIC_CIRCUITS_TEST_PG_URL"]
async fn a_member_failure_fails_the_load_and_leaves_the_pool_clean() -> Result<()> {
    let f = Fixture::new(&[1, 2, 3]).await?;
    let pool = Pool::new(f.url.clone(), 5);
    f.close_gate().await?;
    let loading = start(&pool, vec![f.gated(0), f.failing(1), f.plain(2)]);
    f.wait_held(1).await?;
    f.open_gate().await?;
    let failure = finished(loading).await.err().context("a failing member must fail the load")?;
    let sqlstate = failure.chain().find_map(|cause| cause.downcast_ref::<tokio_postgres::Error>()?.code().cloned());
    assert_eq!(sqlstate, Some(tokio_postgres::error::SqlState::DIVISION_BY_ZERO), "{failure:#}");
    wait_in_use(&pool, 0).await?;
    assert_eq!(f.stranded_transactions().await?, 0);
    let again = finished(start(&pool, vec![f.plain(0), f.plain(2)])).await?;
    assert_eq!(again.pages, [1, 3], "the pool must still serve after a failure");
    pool.close();
    f.cleanup().await
}

/// A load dropped mid-read, as a client disconnect drops it, leaves no connection or transaction.
#[tokio::test]
#[ignore = "requires PostgreSQL via ELECTRIC_CIRCUITS_TEST_PG_URL"]
async fn a_dropped_load_gives_every_connection_back() -> Result<()> {
    let f = Fixture::new(&[1, 2, 3]).await?;
    let pool = Pool::new(f.url.clone(), 5);
    f.close_gate().await?;
    let loading = start(&pool, (0..3).map(|i| f.gated(i)).collect());
    f.wait_held(3).await?;
    loading.abort();
    assert!(loading.await.is_err_and(|error| error.is_cancelled()));
    f.open_gate().await?;
    wait_in_use(&pool, 0).await?;
    assert_eq!(f.stranded_transactions().await?, 0);
    let again = finished(start(&pool, vec![f.plain(0), f.plain(1)])).await?;
    assert_eq!(again.pages, [1, 2]);
    pool.close();
    f.cleanup().await
}

/// A single member stays on its one connection.
#[tokio::test]
#[ignore = "requires PostgreSQL via ELECTRIC_CIRCUITS_TEST_PG_URL"]
async fn a_single_member_stays_on_one_connection() -> Result<()> {
    let f = Fixture::new(&[2]).await?;
    let pool = Pool::new(f.url.clone(), 5);
    f.close_gate().await?;
    let loading = start(&pool, vec![f.gated(0)]);
    f.wait_held(1).await?;
    assert_eq!(pool.in_use(), 1);
    f.open_gate().await?;
    let loaded = finished(loading).await?;
    assert_eq!(loaded.pages, [2]);
    let reads = loaded.timing.reads.lock().unwrap();
    assert_eq!(reads.connections, 1);
    drop(reads);
    pool.close();
    f.cleanup().await
}

/// Two members take one helper, never the cap's two.
#[tokio::test]
#[ignore = "requires PostgreSQL via ELECTRIC_CIRCUITS_TEST_PG_URL"]
async fn two_members_take_two_connections() -> Result<()> {
    let f = Fixture::new(&[1, 2]).await?;
    let pool = Pool::new(f.url.clone(), 5);
    f.close_gate().await?;
    let loading = start(&pool, vec![f.gated(0), f.gated(1)]);
    f.wait_held(2).await?;
    assert_eq!(pool.in_use(), 2);
    f.open_gate().await?;
    assert_eq!(finished(loading).await?.pages, [1, 2]);
    pool.close();
    f.cleanup().await
}

/// A full pool never holds a load up for good: its first connection reads every member itself, so
/// loads holding each other's slots still all finish.
#[tokio::test]
#[ignore = "requires PostgreSQL via ELECTRIC_CIRCUITS_TEST_PG_URL"]
async fn loads_filling_the_pool_all_finish() -> Result<()> {
    let f = Fixture::new(&[1, 2, 3, 4, 5]).await?;
    let lone = Pool::new(f.url.clone(), 1);
    let loaded = finished(start(&lone, (0..5).map(|i| f.plain(i)).collect())).await?;
    assert_eq!(loaded.pages, [1, 2, 3, 4, 5]);
    assert_eq!(loaded.timing.reads.lock().unwrap().connections, 1);
    lone.close();

    let pool = Pool::new(f.url.clone(), 4);
    let loads: Vec<_> = (0..12).map(|_| start(&pool, (0..5).map(|i| f.plain(i)).collect())).collect();
    for loading in loads {
        assert_eq!(finished(loading).await?.pages, [1, 2, 3, 4, 5]);
    }
    wait_in_use(&pool, 0).await?;
    pool.close();
    f.cleanup().await
}

/// Postgres refusing a helper's connection is not a member failure: the load finishes on the
/// connection it has, as it did before members read concurrently.
#[tokio::test]
#[ignore = "requires PostgreSQL via ELECTRIC_CIRCUITS_TEST_PG_URL"]
async fn a_refused_helper_connection_leaves_the_load_on_its_first() -> Result<()> {
    let f = Fixture::new(&[1, 2, 3]).await?;
    let role = format!("subset_batch_{}", uuid::Uuid::new_v4().simple());
    f.client.batch_execute(&format!("CREATE ROLE {role} LOGIN CONNECTION LIMIT 1")).await?;
    for ts in &f.tables {
        f.client.batch_execute(&format!("GRANT SELECT ON {} TO {role}", ts.table)).await?;
    }
    let (scheme, rest) = f.url.split_once("://").context("a URL")?;
    let host = rest.rsplit_once('@').map_or(rest, |(_, host)| host);
    let pool = Pool::new(format!("{scheme}://{role}@{host}"), 5);
    let loaded = finished(start(&pool, (0..3).map(|i| f.plain(i)).collect())).await;
    pool.close();
    f.cleanup().await?;
    f.client.batch_execute(&format!("DROP ROLE {role}")).await?;
    let loaded = loaded?;
    assert_eq!(loaded.pages, [1, 2, 3]);
    assert_eq!(loaded.timing.reads.lock().unwrap().connections, 1);
    Ok(())
}
