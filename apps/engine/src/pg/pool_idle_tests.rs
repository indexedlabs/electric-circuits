//! The pool's idle limit and how a query connection reports its end, against real PostgreSQL.
//!
//! Prod Postgres ends a session idle for 2 hours (`idle_session_timeout`), and the engine used to
//! log each of those ends as `postgres TLS connection error: db error`: a burst of ERROR lines two
//! hours after each burst of pool use. The pool now retires a connection idle past its limit before
//! the server does, and a server-side close names its SQLSTATE, at INFO when it is an expected one.
use std::sync::Mutex;
use std::time::Duration;

use super::*;

const DEADLINE: Duration = Duration::from_secs(5);

fn test_url() -> Result<String> {
    std::env::var("ELECTRIC_CIRCUITS_TEST_PG_URL").context("ELECTRIC_CIRCUITS_TEST_PG_URL")
}

async fn backend_pid(client: &Client) -> Result<i32> {
    Ok(client.query_one("select pg_backend_pid()", &[]).await?.get(0))
}

/// Everything logged at INFO and above while the guard lives, on this thread (a `#[tokio::test]`
/// runs its spawned tasks, the pool's connection tasks among them, on the test's own thread).
#[derive(Clone, Default)]
struct Logs(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for Logs {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl Logs {
    fn capture(&self) -> tracing::subscriber::DefaultGuard {
        let writer = self.clone();
        let subscriber = tracing_subscriber::fmt()
            .without_time()
            .with_ansi(false)
            .with_max_level(tracing::Level::INFO)
            .with_writer(move || writer.clone())
            .finish();
        tracing::subscriber::set_default(subscriber)
    }

    fn text(&self) -> String {
        String::from_utf8(self.0.lock().unwrap().clone()).unwrap()
    }
}

/// A checkout after the idle limit dials a new connection instead of reusing the idle one, and the
/// retired connection is closed by the pool (its server backend goes away) without an ERROR line.
#[tokio::test]
#[ignore = "requires PostgreSQL via ELECTRIC_CIRCUITS_TEST_PG_URL"]
async fn a_connection_idle_past_the_limit_is_redialed_not_reused() -> Result<()> {
    let logs = Logs::default();
    let _logs = logs.capture();
    let limit = Duration::from_millis(500);
    let pool = Pool::owned(test_url()?, 1, limit, crate::shutdown::ShutdownToken::new());

    let mut timing = CheckoutTiming::default();
    let first = pool.get_timed(&mut timing).await?;
    assert!(timing.connect.is_some(), "an empty pool dials: {timing:?}");
    let first_pid = backend_pid(&first).await?;
    drop(first);

    // Within the limit the idle connection is reused.
    let mut timing = CheckoutTiming::default();
    let again = pool.get_timed(&mut timing).await?;
    assert!(timing.connect.is_none(), "a connection idle under the limit is reused: {timing:?}");
    assert_eq!(backend_pid(&again).await?, first_pid);
    drop(again);

    tokio::time::sleep(limit + Duration::from_millis(250)).await;
    let mut timing = CheckoutTiming::default();
    let late = pool.get_timed(&mut timing).await?;
    assert!(timing.connect.is_some(), "a connection idle past the limit is redialed: {timing:?}");
    assert_ne!(backend_pid(&late).await?, first_pid, "the idle connection was not reused");

    // The pool closed the retired connection itself, so its backend is gone.
    let started = std::time::Instant::now();
    loop {
        let open: i64 =
            late.query_one("select count(*) from pg_stat_activity where pid = $1", &[&first_pid]).await?.get(0);
        if open == 0 {
            break;
        }
        assert!(started.elapsed() < DEADLINE, "backend {first_pid} still open after the pool retired it");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    drop(late);
    pool.close();
    let logs = logs.text();
    assert!(!logs.contains("ERROR"), "retiring an idle connection logs no error:\n{logs}");
    Ok(())
}

/// A query connection on `url`, with its connection future handed back unspawned so the test sees
/// how it ends.
async fn raw_connection(
    url: &str,
) -> Result<(Client, impl std::future::Future<Output = Result<(), tokio_postgres::Error>> + use<>)> {
    Ok(parse_pg_url(url)?.connect(NoTls).await?)
}

async fn ended(
    connection: impl std::future::Future<Output = Result<(), tokio_postgres::Error>>,
) -> Result<tokio_postgres::Error> {
    match tokio::time::timeout(DEADLINE, connection).await? {
        Ok(()) => bail!("the connection ended cleanly; the server was expected to end it"),
        Err(error) => Ok(error),
    }
}

/// The server ending an idle session (57P05) or terminating a backend (57P01) is an expected
/// close: INFO, naming the SQLSTATE and the server's message instead of a bare `db error`. Any
/// other server-side end, such as an idle-in-transaction timeout (25P03), stays ERROR.
#[tokio::test]
#[ignore = "requires PostgreSQL via ELECTRIC_CIRCUITS_TEST_PG_URL"]
async fn a_server_side_close_names_its_sqlstate_at_the_right_level() -> Result<()> {
    let url = test_url()?;

    let (idle, connection) = raw_connection(&url).await?;
    let connection = tokio::spawn(connection);
    idle.batch_execute("set idle_session_timeout = 100").await?;
    let error = ended(async { connection.await.expect("connection task") }).await?;
    let (level, detail) = connection_end(&error);
    assert_eq!(error.to_string(), "db error", "what the engine used to log");
    assert_eq!(level, tracing::Level::INFO, "{detail}");
    assert!(detail.contains("57P05") && detail.contains("idle-session timeout"), "{detail}");
    drop(idle);

    let (admin, admin_connection) = raw_connection(&url).await?;
    let admin_connection = tokio::spawn(admin_connection);
    let (victim, connection) = raw_connection(&url).await?;
    let connection = tokio::spawn(connection);
    let pid = backend_pid(&victim).await?;
    admin.execute("select pg_terminate_backend($1)", &[&pid]).await?;
    let error = ended(async { connection.await.expect("connection task") }).await?;
    let (level, detail) = connection_end(&error);
    assert_eq!(level, tracing::Level::INFO, "{detail}");
    assert!(detail.contains("57P01"), "{detail}");
    drop(victim);

    let (stuck, connection) = raw_connection(&url).await?;
    let connection = tokio::spawn(connection);
    stuck.batch_execute("set idle_in_transaction_session_timeout = 100; begin").await?;
    let error = ended(async { connection.await.expect("connection task") }).await?;
    let (level, detail) = connection_end(&error);
    assert_eq!(level, tracing::Level::ERROR, "{detail}");
    assert!(detail.contains("25P03"), "{detail}");
    drop(stuck);

    drop(admin);
    admin_connection.await??;
    Ok(())
}

/// A connection ending without a server error (an I/O failure, a protocol error) stays ERROR.
#[test]
fn a_close_without_a_server_error_stays_an_error() {
    let error = "port=notaport".parse::<tokio_postgres::Config>().expect_err("not a port");
    let (level, detail) = connection_end(&error);
    assert_eq!(level, tracing::Level::ERROR);
    assert_eq!(detail, error.to_string());
}
