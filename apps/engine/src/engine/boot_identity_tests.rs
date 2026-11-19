//! Boot's `REPLICA IDENTITY FULL` step against real PostgreSQL, with a reader holding its table.
//!
//! An open transaction that has read a table holds that table's `ACCESS SHARE` lock until it ends.
//! `ALTER TABLE … REPLICA IDENTITY FULL` needs `ACCESS EXCLUSIVE`, and while it waits Postgres
//! queues every later reader of the table behind it. A boot that waits without a bound therefore
//! stalls every reader of a production table for as long as any one transaction stays open on it.
//! These tests hold that reader's transaction open and require boot never to park behind it:
//! a table that is already FULL needs no lock at all, and one that is not gives up the wait and
//! leaves the retry to the boot loop.
//!
//! Stream storage is scripted; this is a real-PG boot regression, not release qualification.
use super::*;
use std::time::Duration;

/// Far longer than a boot against an idle local Postgres takes, and far shorter than "forever",
/// which is what the unbounded wait made of it.
const DEADLINE: Duration = Duration::from_secs(30);

struct Fixture {
    url: String,
    client: tokio_postgres::Client,
    /// Holds an open transaction that has read the table, so it holds the table's `ACCESS SHARE`.
    reader: tokio_postgres::Client,
    table: TableRef,
    slot: String,
}

impl Fixture {
    async fn new(identity: &str) -> Result<Self> {
        let url = std::env::var("ELECTRIC_CIRCUITS_TEST_PG_URL")?;
        let client = crate::pg::connect(&url).await?;
        // `create table if not exists` is not safe against a concurrent identical create (the
        // tests run in parallel), so the shared fence table is created under a lock.
        client
            .batch_execute(
                "begin; select pg_advisory_xact_lock(hashtext('boot_identity_tests_fence')); \
                 create table if not exists public.circuits_source_fence (source_commit_id uuid primary key); \
                 commit",
            )
            .await?;
        let name = format!("boot_identity_{}", uuid::Uuid::new_v4().simple());
        let table = TableRef::new("public", &name)?;
        let slot = format!("{name}_slot");
        client
            .batch_execute(&format!(
                "create table {t} (id integer primary key, name text); \
                 alter table {t} replica identity {identity}; \
                 insert into {t} values (1, 'one')",
                t = table.quote_qualified(),
            ))
            .await?;
        let reader = crate::pg::connect(&url).await?;
        reader.batch_execute(&format!("begin; select count(*) from {}", table.quote_qualified())).await?;
        Ok(Fixture { url, client, reader, table, slot })
    }

    fn engine(&self) -> Engine {
        let store = Arc::new(crate::ds::ScriptedStore { read_appended: true, ..Default::default() });
        Engine::new_inner(
            DsClient::with_test_store("scripted://boot-identity".into(), store),
            Some(self.url.clone()),
            None,
            PostgresSetup::EngineManaged,
            None,
            None,
        )
    }

    async fn boot(&self, engine: &Engine) -> Result<Result<()>, tokio::time::error::Elapsed> {
        tokio::time::timeout(DEADLINE, engine.setup_postgres(&[TableSelector::One(self.table.clone())], &self.slot))
            .await
    }

    /// The `relreplident` of the schema the engine compiled for the table: what drift is judged
    /// against, and so what must be FULL, whatever the catalog says afterwards.
    fn compiled_replident(&self, engine: &Engine) -> Option<u8> {
        engine
            .tables_shared
            .read()
            .unwrap()
            .get(&self.table)?
            .fingerprint
            .as_ref()
            .map(|fingerprint| fingerprint.replident)
    }

    async fn replident(&self) -> Result<String> {
        let row = self
            .client
            .query_one(
                "select relreplident::text from pg_class where oid = $1::text::regclass",
                &[&self.table.quote_qualified()],
            )
            .await?;
        Ok(row.get(0))
    }

    async fn cleanup(self) -> Result<()> {
        self.reader.batch_execute("rollback").await.ok();
        drop(self.reader);
        self.client
            .batch_execute(&format!(
                "select pg_drop_replication_slot(slot_name) from pg_replication_slots where slot_name = '{slot}'; \
                 drop publication if exists {slot}_pub; drop table {t}",
                slot = self.slot,
                t = self.table.quote_qualified(),
            ))
            .await?;
        Ok(())
    }
}

/// A table that already has `REPLICA IDENTITY FULL` is left alone: boot completes while a reader
/// holds the table, because it requests no lock that conflicts with one.
#[tokio::test]
#[ignore = "requires PostgreSQL via ELECTRIC_CIRCUITS_TEST_PG_URL; stream storage is scripted"]
async fn boot_requests_no_exclusive_lock_on_a_table_that_is_already_full() -> Result<()> {
    let fixture = Fixture::new("full").await?;
    let engine = fixture.engine();
    let booted = fixture.boot(&engine).await;
    let compiled = fixture.compiled_replident(&engine);
    engine.close(Duration::from_secs(10)).await;
    let identity = fixture.replident().await;
    fixture.cleanup().await?;
    match booted {
        Ok(result) => result?,
        Err(_) => anyhow::bail!("boot did not finish within {DEADLINE:?} while a reader held an already-FULL table"),
    }
    assert_eq!(identity?, "f");
    assert_eq!(compiled, Some(b'f'), "the engine compiled a non-FULL identity");
    Ok(())
}

/// A table that still needs `REPLICA IDENTITY FULL` while a reader holds it: boot gives up its lock
/// wait with a retryable failure instead of queueing every later reader behind it, readers proceed,
/// and once the reader is gone the SAME engine's next attempt sets the identity, as the boot loop's
/// retry does in production. A fresh engine then boots too, against the now-FULL table.
#[tokio::test]
#[ignore = "requires PostgreSQL via ELECTRIC_CIRCUITS_TEST_PG_URL; stream storage is scripted"]
async fn boot_gives_up_a_held_identity_lock_and_retries_once_it_is_free() -> Result<()> {
    let fixture = Fixture::new("default").await?;

    let engine = fixture.engine();
    let attempted = fixture.boot(&engine).await;

    // While the reader is still open, another reader is not queued behind a parked boot.
    let other = crate::pg::connect(&fixture.url).await?;
    let unblocked = tokio::time::timeout(
        Duration::from_secs(5),
        other.query_one(&format!("select count(*) from {}", fixture.table.quote_qualified()), &[]),
    )
    .await;
    drop(other);

    // The boot loop retries `setup_postgres` on the engine whose attempt failed, with whatever state
    // that attempt left (epoch binding, change log, queued catalog events); so does this.
    fixture.reader.batch_execute("commit").await?;
    let retried = fixture.boot(&engine).await;
    let compiled_after_retry = fixture.compiled_replident(&engine);
    engine.close(Duration::from_secs(10)).await;
    let identity = fixture.replident().await;

    let fresh = fixture.engine();
    let restarted = fixture.boot(&fresh).await;
    let compiled_after_restart = fixture.compiled_replident(&fresh);
    fresh.close(Duration::from_secs(10)).await;
    fixture.cleanup().await?;

    let err = match attempted {
        Ok(Ok(())) => anyhow::bail!("boot set the identity while a reader held the table"),
        Ok(Err(e)) => e,
        Err(_) => anyhow::bail!("boot waited past {DEADLINE:?} for the identity lock instead of giving up"),
    };
    assert_eq!(crate::pg::boot_disposition(&err), crate::pg::BootFailure::Retryable, "{err:#}");
    assert!(unblocked.is_ok_and(|r| r.is_ok()), "a later reader stayed queued behind the boot");
    match retried {
        Ok(result) => result.context("the same engine's retry failed once the reader was gone")?,
        Err(_) => anyhow::bail!("the same engine's retry did not finish within {DEADLINE:?} once the reader was gone"),
    }
    assert_eq!(identity?, "f");
    assert_eq!(compiled_after_retry, Some(b'f'), "the retry compiled the identity it read before setting FULL");
    match restarted {
        Ok(result) => result.context("a fresh engine failed to boot against the now-FULL table")?,
        Err(_) => anyhow::bail!("a fresh engine did not boot within {DEADLINE:?} against the now-FULL table"),
    }
    assert_eq!(compiled_after_restart, Some(b'f'), "the fresh engine compiled a non-FULL identity");
    Ok(())
}
