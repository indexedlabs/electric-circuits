//! Postgres access for the Postgres-backed mode: connection, schema introspection, replication-slot
//! setup, and consistent backfill snapshots. This replaces the engine's in-memory `table_state` —
//! current data lives in Postgres and is read back on demand (shape backfill), while ongoing changes
//! arrive via logical replication (see `replication.rs`).

use std::collections::{BTreeMap, HashMap};
use std::fs::File;
use std::io::BufReader;
use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};

use anyhow::{Context, Result, bail};
use pgwire_replication::{ReplicationConfig, TlsConfig};
use rustls::{ClientConfig, RootCertStore};
use tokio_postgres::{Client, NoTls};
use tokio_postgres_rustls::MakeRustlsConnect;
use tokio_stream::StreamExt;
use url::Url;

use crate::heap_size::HeapSize;
use crate::predicate::CompiledPredicate;
use crate::schema::{ColumnDef, ColumnType, FingerprintColumn, SchemaFingerprint, TableDef, TableSchema};
use crate::table_ref::TableRef;
use crate::value::Row;

/// How long one connection attempt may hang before it is a failure.
///
/// Without this a **non-routable** address (a firewalled host, a stale Service IP) hangs on the
/// kernel's SYN retry schedule — minutes — with nothing above able to tell "connecting" from
/// "wedged". That is a boot that never reports, and a `SIGTERM` that has nothing to interrupt.
/// tokio-postgres maps the expiry to an ordinary error, which the boot taxonomy already calls
/// retryable, so the effect is to turn an invisible hang into a visible, backed-off retry.
/// A `connect_timeout` in the URL wins — an operator who set one meant it.
pub const CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

const PG_TLS_CA_BUNDLE_ENV: &str = "ELECTRIC_CIRCUITS_PG_TLS_CA_BUNDLE";
const PG_TLS_SERVER_NAME_ENV: &str = "ELECTRIC_CIRCUITS_PG_TLS_SERVER_NAME";

#[derive(Clone, Debug, PartialEq, Eq)]
enum PgTlsPolicy {
    Disabled,
    VerifyFull { ca_bundle: PathBuf, server_name: Option<String> },
}

/// One validated Postgres connection policy shared by ordinary queries and logical replication.
///
/// Production/nonlocal connections are accepted only with `sslmode=verify-full` plus an explicit
/// CA bundle. Plaintext is limited to loopback, `.local`, and single-label development service
/// names. The query connector translates verified TLS to tokio-postgres's transport-level
/// `require` mode while rustls performs chain and hostname verification; the replication connector
/// receives the equivalent pgwire `VerifyFull` policy.
#[derive(Clone)]
pub struct PgConnectionConfig {
    query: tokio_postgres::Config,
    url: Url,
    connect_host: String,
    tls: PgTlsPolicy,
}

impl PgConnectionConfig {
    pub fn resolve(url: &str, ca_bundle: Option<&str>, server_name: Option<&str>) -> Result<Self> {
        let mut parsed = Url::parse(url).map_err(|error| {
            anyhow::anyhow!(
                "unusable Postgres URL '{}': {error}. Expected a postgres:// or postgresql:// URL",
                redact_pg_url(url)
            )
        })?;
        if !matches!(parsed.scheme(), "postgres" | "postgresql") || parsed.host_str().is_none() {
            bail!(
                "unusable Postgres URL '{}': expected a postgres:// or postgresql:// URL with one host",
                redact_pg_url(url)
            );
        }

        let ssl_modes: Vec<String> =
            parsed.query_pairs().filter(|(name, _)| name == "sslmode").map(|(_, value)| value.into_owned()).collect();
        if ssl_modes.len() > 1 {
            bail!("Postgres URL must contain at most one sslmode parameter");
        }
        let host = parsed.host_str().expect("host checked above").to_string();
        let configured_mode = ssl_modes.first().map(String::as_str);
        let tls = match configured_mode {
            Some("verify-full") => {
                let ca_bundle = ca_bundle
                    .filter(|value| !value.trim().is_empty())
                    .map(PathBuf::from)
                    .context("sslmode=verify-full requires ELECTRIC_CIRCUITS_PG_TLS_CA_BUNDLE")?;
                if !ca_bundle.is_absolute() {
                    bail!("ELECTRIC_CIRCUITS_PG_TLS_CA_BUNDLE must be an absolute path");
                }
                let server_name = server_name.filter(|value| !value.trim().is_empty()).map(str::to_string);
                if let Some(name) = server_name.as_deref() {
                    validate_server_name(name)?;
                    if name != host {
                        let address = host.parse::<IpAddr>().with_context(|| {
                            format!(
                                "{PG_TLS_SERVER_NAME_ENV} may override only an IP-address Postgres host; DNS host '{host}' already supplies its verification name"
                            )
                        })?;
                        parsed
                            .set_host(Some(name))
                            .map_err(|_| anyhow::anyhow!("{PG_TLS_SERVER_NAME_ENV} is not a valid URL host"))?;
                        let mut pairs: Vec<(String, String)> = parsed
                            .query_pairs()
                            .filter(|(key, _)| key != "hostaddr")
                            .map(|(key, value)| (key.into_owned(), value.into_owned()))
                            .collect();
                        pairs.push(("hostaddr".into(), address.to_string()));
                        replace_query_pairs(&mut parsed, pairs);
                    }
                }
                PgTlsPolicy::VerifyFull { ca_bundle, server_name }
            }
            None | Some("disable") if is_local_development_host(&host) => {
                if ca_bundle.is_some() || server_name.is_some() {
                    bail!("{PG_TLS_CA_BUNDLE_ENV} and {PG_TLS_SERVER_NAME_ENV} require sslmode=verify-full");
                }
                PgTlsPolicy::Disabled
            }
            None | Some("disable") => {
                bail!("nonlocal Postgres host '{host}' requires sslmode=verify-full and {PG_TLS_CA_BUNDLE_ENV}")
            }
            Some("prefer" | "require" | "verify-ca") => bail!(
                "Postgres sslmode={} is not accepted; use sslmode=verify-full with {PG_TLS_CA_BUNDLE_ENV}, or sslmode=disable only for local development",
                configured_mode.expect("matched mode")
            ),
            Some(other) => bail!("unsupported Postgres sslmode={other}; use verify-full for nonlocal connections"),
        };

        let query_mode = match tls {
            PgTlsPolicy::Disabled => "disable",
            PgTlsPolicy::VerifyFull { .. } => "require",
        };
        let pairs = parsed
            .query_pairs()
            .filter(|(name, _)| name != "sslmode")
            .map(|(name, value)| (name.into_owned(), value.into_owned()))
            .chain(std::iter::once(("sslmode".to_string(), query_mode.to_string())))
            .collect();
        replace_query_pairs(&mut parsed, pairs);

        let mut query = parsed.as_str().parse::<tokio_postgres::Config>().map_err(|error| {
            anyhow::anyhow!(
                "unusable Postgres URL '{}': {error}. Expected a postgres:// or postgresql:// URL",
                redact_pg_url(url)
            )
        })?;
        if query.get_connect_timeout().is_none() {
            query.connect_timeout(CONNECT_TIMEOUT);
        }
        Ok(Self { query, url: parsed, connect_host: host, tls })
    }

    pub fn from_process_env(url: &str) -> Result<Self> {
        let ca_bundle = std::env::var(PG_TLS_CA_BUNDLE_ENV).ok();
        let server_name = std::env::var(PG_TLS_SERVER_NAME_ENV).ok();
        Self::resolve(url, ca_bundle.as_deref(), server_name.as_deref())
    }

    async fn connect(&self, owner: Option<&crate::shutdown::ShutdownToken>) -> Result<Client> {
        match &self.tls {
            PgTlsPolicy::Disabled => {
                let (client, connection) = self.query.connect(NoTls).await.context("connect postgres")?;
                spawn_connection(owner, async move {
                    if let Err(error) = connection.await {
                        tracing::error!("postgres connection error: {error}");
                    }
                });
                Ok(client)
            }
            PgTlsPolicy::VerifyFull { ca_bundle, .. } => {
                let connector = MakeRustlsConnect::new(rustls_client_config(ca_bundle)?);
                let (client, connection) =
                    self.query.connect(connector).await.context("connect postgres with verified TLS")?;
                spawn_connection(owner, async move {
                    if let Err(error) = connection.await {
                        tracing::error!("postgres TLS connection error: {error}");
                    }
                });
                Ok(client)
            }
        }
    }

    pub(crate) fn replication_config(&self, slot: &str, publication: &str) -> Result<ReplicationConfig> {
        let user = match self.url.username() {
            "" => "postgres".to_string(),
            value => percent_decode(value),
        };
        let database = match self.url.path().trim_start_matches('/') {
            "" => user.clone(),
            value => percent_decode(value),
        };
        let tls = match &self.tls {
            PgTlsPolicy::Disabled => TlsConfig::disabled(),
            PgTlsPolicy::VerifyFull { ca_bundle, server_name } => {
                let mut tls = TlsConfig::verify_full(Some(ca_bundle.clone()));
                if let Some(name) = server_name {
                    tls = tls.with_sni_hostname(name.clone());
                }
                tls
            }
        };
        Ok(ReplicationConfig {
            host: self.connect_host.clone(),
            port: self.url.port().unwrap_or(5432),
            user,
            password: self.url.password().map(percent_decode).unwrap_or_default(),
            database,
            tls,
            slot: slot.to_string(),
            publication: publication.to_string(),
            start_lsn: pgwire_replication::Lsn::ZERO,
            stop_at_lsn: None,
            status_interval: std::time::Duration::from_secs(1),
            idle_wakeup_interval: std::time::Duration::from_secs(10),
            buffer_events: 8192,
        })
    }
}

fn replace_query_pairs(parsed: &mut Url, pairs: Vec<(String, String)>) {
    parsed.set_query(None);
    parsed.query_pairs_mut().extend_pairs(pairs);
}

fn is_local_development_host(host: &str) -> bool {
    host.eq_ignore_ascii_case("localhost")
        || host.ends_with(".localhost")
        || host.ends_with(".local")
        || !host.contains('.')
        || host.parse::<IpAddr>().is_ok_and(|address| address.is_loopback())
}

fn validate_server_name(value: &str) -> Result<()> {
    if value.parse::<IpAddr>().is_ok()
        || value.is_empty()
        || value.len() > 253
        || value.split('.').any(|label| {
            label.is_empty()
                || label.len() > 63
                || label.starts_with('-')
                || label.ends_with('-')
                || !label.bytes().all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        })
    {
        bail!("{PG_TLS_SERVER_NAME_ENV} must be a valid DNS name");
    }
    Ok(())
}

fn rustls_client_config(ca_bundle: &Path) -> Result<ClientConfig> {
    let file = File::open(ca_bundle).with_context(|| format!("open Postgres TLS CA bundle {}", ca_bundle.display()))?;
    let certificates = rustls_pemfile::certs(&mut BufReader::new(file))
        .collect::<std::io::Result<Vec<_>>>()
        .with_context(|| format!("parse Postgres TLS CA bundle {}", ca_bundle.display()))?;
    if certificates.is_empty() {
        bail!("Postgres TLS CA bundle {} contains no certificates", ca_bundle.display());
    }
    let mut roots = RootCertStore::empty();
    for certificate in certificates {
        roots
            .add(certificate)
            .with_context(|| format!("load certificate from Postgres TLS CA bundle {}", ca_bundle.display()))?;
    }
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    Ok(ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .context("configure Postgres TLS protocol versions")?
        .with_root_certificates(roots)
        .with_no_client_auth())
}

fn percent_decode(value: &str) -> String {
    let bytes = value.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' && index + 2 < bytes.len() {
            if let (Some(high), Some(low)) =
                ((bytes[index + 1] as char).to_digit(16), (bytes[index + 2] as char).to_digit(16))
            {
                decoded.push((high * 16 + low) as u8);
                index += 3;
                continue;
            }
        }
        decoded.push(bytes[index]);
        index += 1;
    }
    String::from_utf8_lossy(&decoded).into_owned()
}

/// Connect and drive the connection on a background task. Returns the query `Client`.
/// For per-request work (backfills, query-backs, subset queries) prefer an Engine-owned [`Pool`] — a fresh
/// TCP+auth handshake per shape creation is the fleet benchmark's p99 driver, and thousands of
/// concurrent creations exhaust ephemeral ports.
///
/// The pool dials through here too, so the boot connection and every pooled one share one config
/// path — including the connect timeout.
pub async fn connect(url: &str) -> Result<Client> {
    PgConnectionConfig::from_process_env(url)?.connect(None).await
}

// ---- boot-time error taxonomy (issue #13) ------------------------------------------------------

/// `sysexits.h` `EX_CONFIG`. The engine exits with this when a boot step failed for a reason
/// retrying cannot fix — a misconfiguration, a missing privilege, the wrong database. Distinct from
/// the circuit-rebuild `75` and from the forced-shutdown code so `kubectl describe` names the class
/// of failure without anyone reading the log.
pub const EXIT_CONFIG: i32 = 78;

/// What the boot should do about a failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BootFailure {
    /// Nothing the engine can do will change the answer: exit [`EXIT_CONFIG`] with a named message.
    Fatal,
    /// The condition may clear on its own (Postgres not up yet, DNS, a refused connection, a
    /// restart in progress): back off and try again, forever. Kubernetes gates traffic on
    /// `GET /ready`, which reports `waiting` throughout, so a restart buys nothing.
    Retryable,
}

/// The parts of a Postgres failure the classifier looks at. Split out so the decision table is a
/// pure function with unit tests — `tokio_postgres::Error` has no public constructor, so the
/// alternative would be no test at all.
#[derive(Debug, Clone, Copy, Default)]
pub struct PgFailure<'a> {
    /// The SQLSTATE the server sent, if the failure got that far.
    pub sqlstate: Option<&'a str>,
    /// The failure was a transport error (TCP, DNS, TLS, timeout) rather than a server answer.
    pub io: bool,
}

/// The decision table. Documented as a table because it IS the contract an operator reads:
///
/// | SQLSTATE | meaning | verdict |
/// |---|---|---|
/// | `28000`, `28P01` | authentication / `pg_hba` refusal | **fatal** |
/// | `42501` | insufficient privilege (`CREATE PUBLICATION`, slot, `REPLICA IDENTITY FULL`, `pg_control_system`) | **fatal** |
/// | `3D000` | unknown database | **fatal** |
/// | class `08` | connection exception | retryable |
/// | class `40` | serialization / deadlock | retryable |
/// | class `53` | insufficient resources (`53300` too many connections, disk full, OOM) | retryable |
/// | class `55` | object not in prerequisite state (`55006` the slot is in use) | retryable |
/// | class `57` | operator intervention — incl. `57P03` "the database system is starting up" | retryable |
/// | any other SQLSTATE | an answer from the server that retrying will repeat | **fatal** |
/// | *none* (transport: connection refused, DNS, timeout, TLS, a dropped connection) | the server is not there **yet** | retryable |
///
/// The default for an unrecognised SQLSTATE is deliberately **fatal**: a server error at boot that
/// is not in the transient classes above is a statement the engine issued being refused, and
/// retrying it forever would hide the message in a log nobody reads. The transient classes are
/// enumerated rather than guessed at, so the fatal surface stays small and named.
///
/// The default for **no** SQLSTATE is the opposite — retryable — and just as deliberate. The server
/// never answered, and by far the commonest reason for that is that it is not up yet; making it
/// fatal would crash-loop a pod for an ordinary transport blip. The one no-SQLSTATE condition that
/// genuinely is a misconfiguration — an unparseable connection string — never reaches here at all:
/// `Config::resolve` refuses it at boot, before the retry loop exists ([`parse_pg_url`]). What
/// remains is reported (see [`failure_name`]) but not exited on: `GET /ready` says `waiting`, every
/// attempt is logged, and an operator reading either can see it.
pub fn classify(f: PgFailure<'_>) -> BootFailure {
    let Some(code) = f.sqlstate else {
        // No SQLSTATE at all: the connection never produced a server answer.
        return BootFailure::Retryable;
    };
    match code {
        "28000" | "28P01" | "42501" | "3D000" => BootFailure::Fatal,
        _ => match &code[..2.min(code.len())] {
            "08" | "40" | "53" | "55" | "57" => BootFailure::Retryable,
            _ => BootFailure::Fatal,
        },
    }
}

/// A short operator-facing name for a classified failure — what the boot's log line leads with,
/// before the full error chain.
///
/// The two no-SQLSTATE names are distinct on purpose. Both retry (see [`classify`]), but they say
/// different things: `io` means the engine could not reach the server at all (check the address,
/// the network, whether Postgres is up), while its absence means the driver produced an error that
/// carries no SQLSTATE — a connect timeout, a TLS handshake, a protocol surprise, or a credential
/// the server demanded and the URL does not carry. The last of those will not fix itself, and the
/// engine keeps retrying it anyway; the name is deliberately descriptive rather than diagnostic,
/// because from here the driver does not say which it was.
pub fn failure_name(f: PgFailure<'_>) -> &'static str {
    match f.sqlstate {
        Some("28000") | Some("28P01") => "authentication failed",
        Some("42501") => "insufficient privilege",
        Some("3D000") => "unknown database",
        Some("57P03") => "the database system is starting up",
        Some(_) => "Postgres refused a boot statement",
        None if f.io => "Postgres is unreachable",
        None => "Postgres returned an error without a SQLSTATE",
    }
}

/// Extract [`PgFailure`] from a `tokio_postgres::Error`.
pub fn failure_of(e: &tokio_postgres::Error) -> PgFailure<'_> {
    PgFailure {
        sqlstate: e.code().map(|c| c.code()),
        io: {
            let mut src: Option<&(dyn std::error::Error + 'static)> = std::error::Error::source(e);
            let mut io = false;
            while let Some(s) = src {
                if s.downcast_ref::<std::io::Error>().is_some() {
                    io = true;
                    break;
                }
                src = s.source();
            }
            io
        },
    }
}

/// What the boot should do about an `anyhow` error from the Postgres setup path.
///
/// Anything that is **not** a Postgres failure — an unusable `ELECTRIC_CIRCUITS_PG_TABLES` entry, a
/// publication with a column list, a `wal_level` that is not `logical`, a durable catalog the engine
/// could not read — is fatal: those are the strictness refusals the engine already refused to boot
/// past, and none of them changes by waiting.
pub fn boot_disposition(e: &anyhow::Error) -> BootFailure {
    // A managed standby is a successful observation, not a bad configuration. It remains alive
    // and polls the ownership row so that a later exact promotion can take effect.
    if e.chain().any(|cause| cause.downcast_ref::<crate::engine::DeploymentNotReady>().is_some()) {
        return BootFailure::Retryable;
    }
    if let Some(pg) = e.chain().find_map(|c| c.downcast_ref::<tokio_postgres::Error>()) {
        return classify(failure_of(pg));
    }
    if e.chain().any(|cause| cause.downcast_ref::<crate::replication::Refused>().is_some()) {
        return BootFailure::Retryable;
    }
    // The boot also talks to durable-streams (the catalog fold, the change log). A storage server
    // that is not up yet is the same kind of "not yet" as a database that is not up yet — and in a
    // compose/Kubernetes start it is the NORMAL one — so it backs off rather than exiting 78. Only
    // the transport is forgiven: a malformed catalog, a change-log segment that is gone, an
    // unusable ELECTRIC_CIRCUITS_DS_URL stay fatal (see `ds::is_unavailable`). A shape stream that
    // is gone is not a boot failure at all any more — restore retires the record (ADR-0011).
    if crate::ds::is_unavailable(e) {
        return BootFailure::Retryable;
    }
    BootFailure::Fatal
}

/// The name for the fatal log line (see [`failure_name`]). A failure with no `tokio_postgres::Error`
/// in its chain is one of the engine's own strictness refusals — an unusable `PG_TABLES`, a
/// publication with a column list, a `wal_level` that is not `logical`, an unreadable durable
/// catalog — and none of them is transient.
pub fn boot_failure_name(e: &anyhow::Error) -> &'static str {
    if let Some(pg) = e.chain().find_map(|c| c.downcast_ref::<tokio_postgres::Error>()) {
        return failure_name(failure_of(pg));
    }
    if crate::ds::is_unavailable(e) {
        return "durable-streams is unreachable";
    }
    "not a transient Postgres condition"
}

/// Parse a Postgres URL into a `tokio_postgres::Config`, **at boot**, so an unusable one is a named
/// refusal instead of a connect that fails identically forever.
///
/// This is the one no-SQLSTATE failure that must never enter the retry loop: `postgres://…:notaport/db`
/// produces a `tokio_postgres::Error` with no SQLSTATE and no server answer, indistinguishable from
/// "the database is not up yet" to [`classify`] — so the engine would back off and re-parse the same
/// broken string every 30 s forever. Parsing is pure and has no I/O, so it belongs in
/// `Config::resolve` where every other unusable setting is refused.
pub fn parse_pg_url(url: &str) -> Result<tokio_postgres::Config> {
    PgConnectionConfig::from_process_env(url).map(|config| config.query)
}

/// A Postgres URL with its password replaced, for a message an operator will paste into a ticket.
fn redact_pg_url(url: &str) -> String {
    // Purely textual (the URL may not even parse — that is why we are here): blank out anything
    // between the first `:` after the scheme's `//` and the `@` that ends the userinfo. The LAST
    // `@` ends it — a password may legally contain one (`p@ss`) and a host name may not — so
    // splitting at the first would print the tail of the password.
    let Some((scheme, rest)) = url.split_once("://") else { return url.to_string() };
    let Some((userinfo, host)) = rest.rsplit_once('@') else { return url.to_string() };
    match userinfo.split_once(':') {
        Some((user, _)) => format!("{scheme}://{user}:***@{host}"),
        None => url.to_string(),
    }
}

/// Refuse to boot against a cluster that cannot produce logical replication at all.
///
/// Checked **explicitly**, right after connecting, rather than left to surface as a slot creation
/// failure: `wal_level` is a `postgresql.conf` setting that needs a **restart** to change, so
/// naming it plainly (and exiting [`EXIT_CONFIG`]) is the difference between a five-minute fix and
/// reading a decoding error's stack.
pub async fn check_wal_level(client: &Client) -> Result<()> {
    let level: String = client.query_one("show wal_level", &[]).await.context("reading wal_level")?.get(0);
    if !level.eq_ignore_ascii_case("logical") {
        bail!(
            "wal_level is '{level}', but logical replication needs 'logical'. Set `wal_level = logical` \
             in postgresql.conf (or your provider's parameter group) and RESTART Postgres; the engine \
             cannot create or read a logical replication slot until then."
        );
    }
    Ok(())
}

/// Maximum connections per [`Pool`], set once at boot from `ELECTRIC_DB_POOL_SIZE` (default 20).
static POOL_SIZE: OnceLock<usize> = OnceLock::new();

// Non-owning inventory preserves the existing process-wide settle statistics until PR B.

/// Host-wide capacity; each Engine gets its own pool at this capacity.
pub fn set_pool_size(size: usize) {
    let _ = POOL_SIZE.set(size.max(1));
}

pub(crate) fn configured_pool_size() -> usize {
    *POOL_SIZE.get_or_init(|| 20)
}

fn spawn_connection(
    owner: Option<&crate::shutdown::ShutdownToken>,
    future: impl std::future::Future<Output = ()> + Send + 'static,
) {
    if let Some(owner) = owner {
        owner.spawn(future);
    } else {
        tokio::spawn(future);
    }
}

pub(crate) async fn connect_owned(url: &str, owner: &crate::shutdown::ShutdownToken) -> Result<Client> {
    PgConnectionConfig::from_process_env(url)?.connect(Some(owner)).await
}

/// A small connection pool: at most `size` concurrent checkouts, idle connections reused.
/// Backfills/query-backs mark their explicit transaction bracket, so check-in only rolls back a
/// checkout that may actually have left a transaction open or aborted.
#[derive(Clone)]
pub struct Pool {
    inner: Arc<PoolInner>,
}

struct PoolInner {
    shutdown: crate::shutdown::ShutdownToken,
    url: String,
    idle: std::sync::Mutex<Vec<Client>>,
    sem: Arc<tokio::sync::Semaphore>,
    /// What the sequencer reading this database's changes has fanned out without a snapshot having
    /// been seen to contain it — the settle record every snapshot on this pool is checked against,
    /// with its own visibility poller (a dedicated connection, outside this pool's `size`).
    sequenced: Arc<SequencedXids>,
}

impl Pool {
    pub fn new(url: String, size: usize) -> Pool {
        Self::owned(url, size, crate::shutdown::ShutdownToken::new())
    }

    pub(crate) fn owned(url: String, size: usize, shutdown: crate::shutdown::ShutdownToken) -> Pool {
        let pool = Pool {
            inner: Arc::new(PoolInner {
                sequenced: Arc::new(SequencedXids::owned(
                    Some(url.clone()),
                    backfill_config().settle,
                    size.max(1),
                    shutdown.clone(),
                )),
                shutdown,
                url,
                idle: std::sync::Mutex::new(Vec::new()),
                sem: Arc::new(tokio::sync::Semaphore::new(size.max(1))),
            }),
        };
        pool
    }

    pub(crate) fn close(&self) {
        self.inner.shutdown.begin();
        self.inner.sem.close();
        self.inner.idle.lock().unwrap().clear();
    }

    /// The settle set for snapshots taken on this pool (see [`SequencedXids`]). The sequencer that
    /// consumes this database's change log notes into it.
    pub fn sequenced(&self) -> Arc<SequencedXids> {
        self.inner.sequenced.clone()
    }

    /// Check out a connection; waits if all `size` are in use. The checkout is returned to the
    /// pool (or discarded, if broken) when the guard drops.
    pub async fn get(&self) -> Result<PooledClient> {
        let (client, permit) = self.inner.checkout().await?;
        Ok(PooledClient {
            client: Some(client),
            inner: self.inner.clone(),
            permit: Some(permit),
            transaction_open: AtomicBool::new(false),
        })
    }
}

impl Drop for PoolInner {
    fn drop(&mut self) {
        self.shutdown.begin();
    }
}

impl PoolInner {
    /// A permit plus a healthy connection: an idle one if there is one, otherwise a new dial.
    async fn checkout(&self) -> Result<(Client, tokio::sync::OwnedSemaphorePermit)> {
        let permit = self.sem.clone().acquire_owned().await.context("pg pool closed")?;
        let reused = self.idle.lock().unwrap().pop().filter(|c| !c.is_closed());
        let client = match reused {
            Some(c) => c,
            None => connect_owned(&self.url, &self.shutdown).await?,
        };
        Ok((client, permit))
    }
}

/// A pooled connection checkout. Derefs to `tokio_postgres::Client`.
///
/// Explicit transactions on a checkout must use the transaction-tracked helpers in this module.
/// Issuing a raw `BEGIN` through [`std::ops::Deref`] bypasses [`PooledClient::transaction_started`]
/// and can return an open or aborted transaction to the idle pool.
pub struct PooledClient {
    client: Option<Client>,
    inner: Arc<PoolInner>,
    permit: Option<tokio::sync::OwnedSemaphorePermit>,
    transaction_open: AtomicBool,
}

impl std::ops::Deref for PooledClient {
    type Target = Client;
    fn deref(&self) -> &Client {
        self.client.as_ref().expect("client present until drop")
    }
}

impl PooledClient {
    fn transaction_started(&self) {
        self.transaction_open.store(true, Ordering::SeqCst);
    }

    fn transaction_finished(&self) {
        self.transaction_open.store(false, Ordering::SeqCst);
    }

    /// Give the connection (and its pool slot) back while keeping this checkout, so a caller that
    /// must wait on something other than Postgres — a snapshot settle — pins nothing. No transaction
    /// may be open. [`Self::reacquire`] takes a connection again; until then the checkout must not be
    /// used (its `Deref` panics).
    fn release(&mut self) {
        debug_assert!(!self.transaction_open.load(Ordering::SeqCst), "released with a transaction open");
        if let Some(client) = self.client.take()
            && !client.is_closed()
            && !self.inner.sem.is_closed()
        {
            self.inner.idle.lock().unwrap().push(client);
        }
        self.permit = None;
    }

    /// Take a connection again after [`Self::release`] — waiting for a pool slot like
    /// [`Pool::get`].
    async fn reacquire(&mut self) -> Result<()> {
        let (client, permit) = self.inner.checkout().await?;
        self.client = Some(client);
        self.permit = Some(permit);
        self.transaction_open.store(false, Ordering::SeqCst);
        Ok(())
    }
}

impl Drop for PooledClient {
    fn drop(&mut self) {
        let Some(client) = self.client.take() else { return };
        let permit = self.permit.take();
        if client.is_closed() || self.inner.sem.is_closed() {
            return; // permit drops here, freeing the slot
        }
        if !self.transaction_open.load(Ordering::SeqCst) {
            self.inner.idle.lock().unwrap().push(client);
            return;
        }
        let inner = self.inner.clone();
        // A cancelled or failed explicit transaction may be open or aborted. Clean it before reuse;
        // clean read-only checkouts skip this path, avoiding both a round trip and PostgreSQL's
        // `there is no transaction in progress` warning. A failed BEGIN or a COMMIT whose response
        // was lost can conservatively reach this path after PostgreSQL already ended the transaction,
        // so a rare warning is preferable to reusing a connection whose state is uncertain.
        self.inner.shutdown.spawn(async move {
            if client.batch_execute("ROLLBACK").await.is_ok() && !inner.sem.is_closed() {
                inner.idle.lock().unwrap().push(client);
            }
            drop(permit);
        });
    }
}

/// Double-quote a Postgres identifier.
pub fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// Map a Postgres `data_type` (from information_schema) to our column type.
fn map_pg_type(data_type: &str) -> ColumnType {
    match data_type {
        "integer" | "bigint" | "smallint" => ColumnType::Int,
        "boolean" => ColumnType::Bool,
        "real" | "double precision" | "numeric" => ColumnType::Float,
        _ => ColumnType::Text, // text, varchar, char, uuid, timestamptz, ... -> treated as text
    }
}

/// List all base tables in `schema` that have a primary key, skipping the engine's own bookkeeping
/// table — which is `public.__el_sync` **specifically**: a user table that happens to be called
/// `__el_sync` in another schema is ordinary data and is replicated like any other. Used by "introspect all" mode (`ELECTRIC_CIRCUITS_PG_TABLES=*` → `public`,
/// `schema.*` → that schema), where the set of tables isn't known up front (e.g. driving Electric's
/// integration tests over varied schemas). The schema is a bound parameter, and `to_regclass` gets
/// the properly quoted qualified name, so an odd schema name can neither inject nor mis-resolve.
pub async fn list_tables(client: &Client, schema: &str) -> Result<Vec<TableRef>> {
    let rows = client
        .query(
            "select t.table_name from information_schema.tables t \
             where t.table_schema = $1 and t.table_type = 'BASE TABLE' \
               and not (t.table_schema = 'public' and t.table_name in ('__el_sync', 'circuits_source_fence', 'native_sync_authority_fence')) \
               and exists (select 1 from pg_index i \
                           where i.indrelid = to_regclass(quote_ident(t.table_schema)||'.'||quote_ident(t.table_name)) \
                             and i.indisprimary) \
             order by t.table_name",
            &[&schema],
        )
        .await
        .with_context(|| format!("list tables in schema '{schema}'"))?;
    // A relation whose name cannot be spelled as a canonical `schema.name` (a quoted identifier
    // containing a dot or a quote) has no unambiguous identity under ADR-0002, so discovery skips it
    // — loudly. It is then simply untracked: a shape on it is refused with "unknown table" at the
    // API boundary, never served wrong. An EXPLICIT list entry for such a table cannot exist at all
    // (the config parse rejects it), so this is discovery's case only.
    Ok(rows
        .iter()
        .filter_map(|r| {
            let name: String = r.get(0);
            TableRef::new(schema, &name)
                .map_err(|e| tracing::warn!("introspect-all: skipping relation '{schema}'.'{name}': {e:#}"))
                .ok()
        })
        .collect())
}

/// Read the live **schema fingerprint** of every given table in ONE round trip: `pg_class` ⨝
/// `pg_attribute`, live columns only (`attnum > 0 and not attisdropped`) in `attnum` order, plus
/// `pg_class.relreplident` (ADR-0005).
///
/// A table the query finds no row for is simply **absent** from the result — that is exactly how a
/// DROPped table is detected, by the reconciler and by the drift handler alike. The wanted set is
/// two bound `text[]` params joined through `unnest`, so neither an odd schema name nor a large
/// table set turns into interpolated SQL.
pub async fn fingerprints(
    client: &Client,
    tables: &[TableRef],
    with_generated: bool,
) -> Result<HashMap<TableRef, SchemaFingerprint>> {
    if tables.is_empty() {
        return Ok(HashMap::new());
    }
    let schemas: Vec<String> = tables.iter().map(|t| t.schema().to_string()).collect();
    let names: Vec<String> = tables.iter().map(|t| t.name().to_string()).collect();
    // Stored generated columns are included **iff the publication publishes them** ($3): pgoutput
    // omits them unless `pubgencols = 's'`, and a fingerprint that disagrees with the wire on this
    // would report drift on every single `Relation` message, forever (ADR-0005).
    let rows = client
        .query(
            "select n.nspname, c.relname, c.relreplident::text, \
                    a.attname, a.atttypid::int8, a.atttypmod \
             from pg_class c \
             join pg_namespace n on n.oid = c.relnamespace \
             join unnest($1::text[], $2::text[]) as w(s, t) on w.s = n.nspname and w.t = c.relname \
             left join pg_attribute a \
               on a.attrelid = c.oid and a.attnum > 0 and not a.attisdropped \
                  and (a.attgenerated = '' or $3::bool) \
             order by n.nspname, c.relname, a.attnum",
            &[&schemas, &names, &with_generated],
        )
        .await
        .context("read schema fingerprints")?;
    let mut out: HashMap<TableRef, SchemaFingerprint> = HashMap::new();
    for r in &rows {
        let schema: String = r.get(0);
        let name: String = r.get(1);
        let Ok(table) = TableRef::new(&schema, &name) else { continue };
        let replident: String = r.get(2);
        let entry = out.entry(table).or_insert_with(|| SchemaFingerprint {
            columns: Vec::new(),
            replident: replident.as_bytes().first().copied().unwrap_or(b'?'),
            // Filled by the primary-key pass below; a table without one keeps `Some(vec![])`, which
            // is still a KNOWN key (and different from any real one), not "unknown".
            pk: Some(Vec::new()),
        });
        // `CREATE TABLE t ()` is legal: the LEFT JOIN then yields one all-NULL column row.
        let Some(attname) = r.get::<_, Option<String>>(3) else { continue };
        let type_oid: i64 = r.get(4);
        let typmod: i32 = r.get(5);
        entry.columns.push(FingerprintColumn { name: attname, type_oid: type_oid as u32, typmod });
    }
    // Second pass: the primary key, in `indkey` order. A separate query rather than another join —
    // joining it into the column query would multiply every column row by the key width.
    let pk_rows = client
        .query(
            "select n.nspname, c.relname, a.attname \
             from pg_class c \
             join pg_namespace n on n.oid = c.relnamespace \
             join unnest($1::text[], $2::text[]) as w(s, t) on w.s = n.nspname and w.t = c.relname \
             join pg_index i on i.indrelid = c.oid and i.indisprimary \
             join pg_attribute a on a.attrelid = c.oid and a.attnum = any(i.indkey) \
             order by n.nspname, c.relname, array_position(i.indkey, a.attnum)",
            &[&schemas, &names],
        )
        .await
        .context("read primary keys")?;
    for r in &pk_rows {
        let schema: String = r.get(0);
        let name: String = r.get(1);
        let Ok(table) = TableRef::new(&schema, &name) else { continue };
        if let Some(fp) = out.get_mut(&table) {
            fp.pk.get_or_insert_with(Vec::new).push(r.get(2));
        }
    }
    Ok(out)
}

/// Introspect a table's columns (+ types), its primary key and its [`SchemaFingerprint`] from the
/// catalog. Both the column lookup and the primary-key lookup are qualified by `(table_schema,
/// table_name)` / the quoted qualified `to_regclass`, so same-named tables in different schemas
/// never cross. Errors when the table does not exist — see [`introspect_opt`] for the caller that
/// treats "gone" as an outcome rather than a failure.
pub async fn introspect(client: &Client, table: &TableRef, with_generated: bool) -> Result<TableDef> {
    introspect_opt(client, table, with_generated)
        .await?
        .with_context(|| format!("table '{table}' not found in postgres"))
}

/// [`introspect`], but a table Postgres no longer has yields `Ok(None)` instead of an error: the
/// schema-drift handler must tell "this table was ALTERed" from "this table was DROPped", and
/// re-introspecting is where it finds out (ADR-0005).
pub async fn introspect_opt(client: &Client, table: &TableRef, with_generated: bool) -> Result<Option<TableDef>> {
    let (schema, name) = (table.schema(), table.name());
    let Some(fingerprint) = fingerprints(client, std::slice::from_ref(table), with_generated).await?.remove(table)
    else {
        return Ok(None);
    };
    let col_rows = client
        .query(
            "select column_name, data_type, udt_name, \
                    (is_identity = 'YES' or column_default is not null) as has_default \
             from information_schema.columns \
             where table_schema = $1 and table_name = $2 order by ordinal_position",
            &[&schema, &name],
        )
        .await
        .context("introspect columns")?;
    if col_rows.is_empty() {
        // `pg_class` had the relation but `information_schema` has no columns for it: a
        // zero-column table, or the table was dropped between the two queries. Either way there is
        // nothing to compile — same answer as "not found".
        return Ok(None);
    }
    let mut columns = BTreeMap::new();
    for r in &col_rows {
        let name: String = r.get(0);
        let dt: String = r.get(1);
        // udt_name (pg_type.typname, e.g. `uuid`, `int4`, `timestamptz`) is the canonical, always-castable
        // type name — used to cast bound text params to the native type in backfill SQL.
        let udt: String = r.get(2);
        // Auto-defaulted (IDENTITY or DEFAULT) → the add-row form can treat it as optional.
        let has_default: bool = r.get(3);
        columns.insert(name, ColumnDef { ty: map_pg_type(&dt), pg_type: Some(udt), has_default });
    }

    // Composite primary keys are supported (e.g. Electric's `*_tags` tables); columns are ordered by
    // their position in the index key so the synthesized row key is deterministic.
    let qualified = table.quote_qualified();
    let pk_rows = client
        .query(
            "select a.attname from pg_index i \
             join pg_attribute a on a.attrelid = i.indrelid and a.attnum = any(i.indkey) \
             where i.indrelid = to_regclass($1) and i.indisprimary \
             order by array_position(i.indkey, a.attnum)",
            &[&qualified],
        )
        .await
        .context("introspect primary key")?;
    if pk_rows.is_empty() {
        bail!("table '{table}' must have a primary key");
    }
    let primary_key: Vec<String> = pk_rows.iter().map(|r| r.get(0)).collect();
    Ok(Some(TableDef { columns, primary_key, fingerprint: Some(fingerprint) }))
}

/// `ALTER TABLE … REPLICA IDENTITY FULL` so logical decoding carries the full old row. Used at
/// boot, where waiting for the lock is the right thing to do.
pub async fn ensure_replica_identity_full(client: &Client, table: &TableRef) -> Result<()> {
    client
        .batch_execute(&format!("ALTER TABLE {} REPLICA IDENTITY FULL", table.quote_qualified()))
        .await
        .with_context(|| format!("set REPLICA IDENTITY FULL on {table}"))
}

/// [`ensure_replica_identity_full`] with a bounded wait for the `ACCESS EXCLUSIVE` lock.
///
/// The drift handler runs INLINE in the ingestor: an unbounded lock wait there would stall the
/// whole replication stream — every table's — behind one long-running reader of this one. With
/// `lock_timeout` the statement gives up instead, the caller marks the table unresolved, and its
/// retry task tries again later while ingest keeps flowing.
pub async fn ensure_replica_identity_full_bounded(
    client: &PooledClient,
    table: &TableRef,
    lock_timeout: std::time::Duration,
) -> Result<()> {
    client.transaction_started();
    let result = client
        .batch_execute(&format!(
            "BEGIN; SET LOCAL lock_timeout = '{}ms'; ALTER TABLE {} REPLICA IDENTITY FULL; COMMIT;",
            lock_timeout.as_millis().max(1),
            table.quote_qualified()
        ))
        .await
        .with_context(|| format!("set REPLICA IDENTITY FULL on {table} (lock_timeout {lock_timeout:?})"));
    if result.is_ok() {
        client.transaction_finished();
    }
    result
}

/// The only output plugin the engine speaks (`replication.rs` decodes pgoutput frames).
pub const PGOUTPUT: &str = "pgoutput";

/// Create the logical replication slot (`pgoutput`) if it does not exist. A leftover slot with a
/// different output plugin (e.g. `test_decoding` from an earlier engine version) is dropped and
/// recreated — the plugin cannot be changed in place.
pub async fn ensure_slot(client: &Client, slot: &str) -> Result<()> {
    let existing = client
        .query("select plugin from pg_replication_slots where slot_name = $1", &[&slot])
        .await
        .context("check slot")?;
    if let Some(row) = existing.first() {
        let plugin: String = row.get(0);
        if plugin == PGOUTPUT {
            return Ok(());
        }
        tracing::warn!("slot '{slot}' uses plugin '{plugin}'; dropping and recreating with pgoutput");
        client.execute("select pg_drop_replication_slot($1)", &[&slot]).await.context("drop stale slot")?;
    }
    create_slot(client, slot).await
}

async fn create_slot(client: &Client, slot: &str) -> Result<()> {
    client
        .execute("select pg_create_logical_replication_slot($1, 'pgoutput')", &[&slot])
        .await
        .context("create slot")?;
    Ok(())
}

/// Drop the slot (if it is still there) and create a fresh one — the Postgres half of an **epoch
/// reset** (ADR-0004). Unlike [`ensure_slot`] this never adopts what it finds: a broken epoch means
/// the slot on the server is not the one the engine bound to, whatever it looks like.
///
/// Only ever called with no walsender of the engine's own attached (boot before the ingestor spawns,
/// or the ingestor's own pre-connect check), so the drop is not fighting our own connection.
pub async fn recreate_slot(client: &Client, slot: &str) -> Result<()> {
    let existing = client
        .query("select 1 from pg_replication_slots where slot_name = $1", &[&slot])
        .await
        .context("check slot")?;
    if !existing.is_empty() {
        client.execute("select pg_drop_replication_slot($1)", &[&slot]).await.context("drop the old epoch's slot")?;
    }
    create_slot(client, slot).await
}

/// The cluster the engine is bound to, plus its clock — the identity half of a [`SlotObservation`].
///
/// `system_identifier` is generated by `initdb` and never changes, so it is what distinguishes "the
/// same database restored from a backup" (or a different cluster reachable at the same URL) from
/// the cluster whose WAL the engine's shapes were built from. `timeline_id` moves on a
/// promotion/PITR; it is **recorded, not acted on** (single primary — ADR-0004).
pub struct ClusterIdentity {
    pub system_identifier: String,
    pub timeline_id: u32,
    /// The server's clock as ISO-8601 UTC — stamped into the binding so the durable record says
    /// *when* the epoch started, on the same clock as everything else in Postgres.
    pub now_iso: String,
}

/// Read the cluster identity. `pg_control_system()` / `pg_control_checkpoint()` are `EXECUTE`-to-
/// `PUBLIC` by default (verified on PG18: `pg_proc.proacl` is NULL and an unprivileged role can call
/// them), so the epoch check needs no privilege beyond what the engine already has.
pub async fn cluster_identity(client: &Client) -> Result<ClusterIdentity> {
    // `system_identifier` is a uint64 stored as int8, so it is read as text rather than risking the
    // sign flip; the timestamp is formatted server-side (no date crate in the engine's tree).
    let row = client
        .query_one(
            "select (select system_identifier::text from pg_control_system()), \
                    (select timeline_id from pg_control_checkpoint()), \
                    to_char(now() at time zone 'utc', 'YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"')",
            &[],
        )
        .await
        .context("read the postgres cluster identity (pg_control_system/pg_control_checkpoint)")?;
    let system_identifier: String = row.get(0);
    let timeline_id: i32 = row.get(1);
    let now_iso: String = row.get(2);
    Ok(ClusterIdentity { system_identifier, timeline_id: timeline_id as u32, now_iso })
}

/// One `pg_replication_slots` row, as far as the epoch verdict cares (ADR-0004).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SlotRow {
    pub active: bool,
    /// The walsender holding the slot, if any. Postgres allows exactly one.
    pub active_pid: Option<i32>,
    /// `reserved` | `extended` | `unreserved` | `lost` (PG13+; absent on older servers).
    pub wal_status: Option<String>,
    pub confirmed_flush_lsn: Option<String>,
    pub plugin: Option<String>,
}

/// Everything the epoch verdict is computed from: the slot as Postgres reports it right now (absent
/// = the slot is gone) and the cluster it would be read from.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SlotObservation {
    pub system_identifier: String,
    pub timeline_id: u32,
    pub slot: Option<SlotRow>,
}

/// Observe the slot + the cluster identity, for the epoch check at boot and before every ingestor
/// (re)connect.
pub async fn observe_slot(client: &Client, slot: &str) -> Result<SlotObservation> {
    let id = cluster_identity(client).await?;
    // `to_jsonb(s)` rather than a column list: `wal_status` exists only on PG13+, and asking jsonb
    // for a missing key is simply `None` (same trick as `inspect_publication`).
    let row = client
        .query_opt("select to_jsonb(s) from pg_replication_slots s where s.slot_name = $1", &[&slot])
        .await
        .context("read the replication slot")?;
    let slot = row.map(|row| {
        let j: serde_json::Value = row.get(0);
        SlotRow {
            active: j.get("active").and_then(serde_json::Value::as_bool).unwrap_or(false),
            active_pid: j.get("active_pid").and_then(serde_json::Value::as_i64).map(|v| v as i32),
            wal_status: j.get("wal_status").and_then(serde_json::Value::as_str).map(str::to_string),
            confirmed_flush_lsn: j.get("confirmed_flush_lsn").and_then(serde_json::Value::as_str).map(str::to_string),
            plugin: j.get("plugin").and_then(serde_json::Value::as_str).map(str::to_string),
        }
    });
    Ok(SlotObservation { system_identifier: id.system_identifier, timeline_id: id.timeline_id, slot })
}

/// Create the publication the pgoutput stream filters on, if it does not exist. `FOR ALL TABLES`
/// (requires superuser): the ingestor drops changes for untracked relations itself, and the set of
/// tracked tables can grow via introspect-all restarts without publication surgery.
pub async fn ensure_publication(client: &Client, publication: &str) -> Result<()> {
    let exists = client
        .query("select 1 from pg_publication where pubname = $1", &[&publication])
        .await
        .context("check publication")?;
    if exists.is_empty() {
        client
            .execute(&format!("create publication {} for all tables", quote_ident(publication)), &[])
            .await
            .context("create publication")?;
    }
    Ok(())
}

/// The two things about the publication that decide whether the wire can ever agree with the
/// catalog (ADR-0005).
///
/// A `Relation` message describes what the publication will DELIVER. If that is not the whole row,
/// no re-introspection can make the engine's compiled schema match it, and the engine would report
/// drift on every message forever — so a column list is refused at boot rather than discovered at
/// runtime, and generated-column publishing is folded into the fingerprint instead.
pub struct PublicationInfo {
    /// `pg_publication.pubgencols = 's'` (PG18+; absent ⇒ false).
    pub publish_generated: bool,
}

/// Check that the publication can deliver whole rows for every tracked table, and learn whether it
/// publishes stored generated columns. Errors — fatally, at boot — on a per-table column list.
pub async fn inspect_publication(client: &Client, publication: &str, tables: &[TableRef]) -> Result<PublicationInfo> {
    // One row-to-jsonb read rather than a version-guarded column list: `pubgencols` exists only on
    // PG18+, and asking jsonb for a missing key is simply `None`.
    let row = client
        .query_opt("select to_jsonb(p) from pg_publication p where p.pubname = $1", &[&publication])
        .await
        .context("read publication")?
        .with_context(|| format!("publication '{publication}' does not exist"))?;
    let pubrow: serde_json::Value = row.get(0);
    let all_tables = pubrow.get("puballtables").and_then(serde_json::Value::as_bool).unwrap_or(false);
    let publish_generated = pubrow.get("pubgencols").and_then(serde_json::Value::as_str) == Some("s");

    // A `FOR ALL TABLES` publication cannot carry a column list at all, so only a hand-made one is
    // worth the second query.
    if !all_tables {
        let schemas: Vec<String> = tables.iter().map(|t| t.schema().to_string()).collect();
        let names: Vec<String> = tables.iter().map(|t| t.name().to_string()).collect();
        let listed = client
            .query(
                "select n.nspname, c.relname from pg_publication_rel pr \
                 join pg_publication p on p.oid = pr.prpubid \
                 join pg_class c on c.oid = pr.prrelid \
                 join pg_namespace n on n.oid = c.relnamespace \
                 join unnest($2::text[], $3::text[]) as w(s, t) on w.s = n.nspname and w.t = c.relname \
                 where p.pubname = $1 and pr.prattrs is not null \
                 order by n.nspname, c.relname",
                &[&publication, &schemas, &names],
            )
            .await
            .context("check publication column lists")?;
        if !listed.is_empty() {
            let which: Vec<String> =
                listed.iter().map(|r| format!("{}.{}", r.get::<_, String>(0), r.get::<_, String>(1))).collect();
            bail!(
                "publication '{publication}' has a column list on {}; the engine requires whole rows \
                 (a partial row can never be reconciled with the table's schema). Drop the column \
                 list, or stop syncing that table.",
                which.join(", ")
            );
        }
    }
    Ok(PublicationInfo { publish_generated })
}

/// Reject tracked tables whose row-level security could make logical replication silently
/// selective. The first production profile does not use table RLS as a tenant boundary; an RLS
/// enabled table is therefore a boot-time configuration error rather than a table that can be
/// served with an uncertain visibility contract.
pub async fn reject_rls_tables(client: &Client, tables: &[TableRef]) -> Result<()> {
    // Keep this query separate from the publication query: relrowsecurity is relation metadata,
    // not part of the publication definition, and checking it for the exact tracked set avoids
    // rejecting unrelated application tables in a FOR ALL TABLES publication.
    if tables.is_empty() {
        return Ok(());
    }
    let schemas: Vec<String> = tables.iter().map(|t| t.schema().to_string()).collect();
    let names: Vec<String> = tables.iter().map(|t| t.name().to_string()).collect();
    let rows = client
        .query(
            "select n.nspname, c.relname \
             from pg_class c \
             join pg_namespace n on n.oid = c.relnamespace \
             join unnest($1::text[], $2::text[]) as w(s, t) \
               on w.s = n.nspname and w.t = c.relname \
             where c.relrowsecurity or c.relforcerowsecurity \
             order by n.nspname, c.relname",
            &[&schemas, &names],
        )
        .await
        .context("check tracked-table row security")?;
    if rows.is_empty() {
        return Ok(());
    }
    let which: Vec<String> =
        rows.iter().map(|r| format!("{}.{}", r.get::<_, String>(0), r.get::<_, String>(1))).collect();
    bail!(
        "tracked table(s) {} have row-level security enabled; RLS is not supported for synced tables \
         because it can filter logical replication. Disable RLS before starting the engine.",
        which.join(", ")
    );
}

/// The fences a backfill snapshot captures, in the same statement that establishes the snapshot.
///
/// Handed back when the snapshot has been fully read (`BackfillReader::finish`), not before: a
/// consumer needs them only at activation, and returning them first would invite treating the
/// backfill as complete while rows are still arriving.
pub struct BackfillFences {
    /// `pg_current_wal_lsn()` of the snapshot. A transaction visible to this REPEATABLE READ snapshot
    /// committed strictly before it, so its commit LSN is `< seed_lsn` and its changes are already in
    /// the rows; a transaction committing at/after the snapshot has commit LSN `>= seed_lsn`.
    pub seed_lsn: String,
    /// The snapshot's transaction-visibility gate — the *sound* backfill↔replication fence. See
    /// [`SnapshotGate`] for why LSN comparison alone is not.
    pub gate: SnapshotGate,
}

/// The backfill snapshot's visibility fence for replicated changes.
///
/// **Why not LSN alone:** `pg_current_wal_lsn()` is a WAL *write* position, but snapshot visibility is
/// decided at `ProcArrayEndTransaction` — which happens *after* the commit record is written and
/// fsynced. A transaction whose commit record is already in the WAL (commit LSN `< seed_lsn`) can
/// still be **invisible** to a snapshot taken during that window; skipping its replicated change by
/// LSN would drop the row from both the backfill and the live stream, permanently. Conversely a
/// visible commit can sit exactly *at* the boundary (`end_lsn == seed_lsn`) and be replayed as a
/// duplicate. Transaction-id visibility (`pg_current_snapshot()`) decides both cases exactly:
/// **skip a replicated change iff its xid was visible to the backfill snapshot.** (Every xid seen on
/// the slot is committed, so visibility reduces to: `xid < xmin`, or `xmin <= xid < xmax` and not
/// in-progress at snapshot time.)
///
/// The stored xids are `pg_current_snapshot()`'s xid8 values masked to 32 bits, because the
/// replication stream (pgoutput `Begin`) carries 32-bit xids. They are compared the way PostgreSQL
/// compares `TransactionId`s — modulo 2^32, relative to each other (`xid_precedes`) — so a snapshot
/// taken just before a 2^32 epoch boundary still orders a transaction from just after it as later.
/// That is sound while every compared xid lies within 2^31 of the snapshot, which PostgreSQL's own
/// wraparound protection guarantees for any transaction that can still be committing.
///
/// `horizon` is `pg_current_wal_insert_lsn()` in the same statement. A transaction visible to the
/// snapshot had inserted its commit record before the snapshot was taken, so its commit LSN is below
/// the horizon; a change at or past the horizon is never skipped, whatever its (possibly wrapped)
/// 32-bit xid claims. A gate from before this field existed deserializes with `horizon = 0`, which
/// disables that guard and keeps the old xid-only rule.
///
/// When a change carries no parseable xid (library mode / non-PG sources), the gate falls back to
/// the LSN comparison, and with neither it never skips.
#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct SnapshotGate {
    /// `pg_current_wal_lsn()` at the snapshot (numeric) — the fallback fence + subset positioning.
    pub lsn: u64,
    xmin: u64,
    xmax: u64,
    xip: std::collections::HashSet<u64>,
    /// `pg_current_wal_insert_lsn()` at the snapshot (numeric); `0` = unknown (see the type docs).
    #[serde(default)]
    horizon: u64,
}

impl HeapSize for SnapshotGate {
    /// Only `xip` (the in-progress xid set) owns heap; `lsn`/`xmin`/`xmax` are inline `u64`s.
    fn heap_bytes(&self) -> usize {
        self.xip.heap_bytes()
    }
}

/// Does 32-bit transaction id `a` logically precede `b`? PostgreSQL's `TransactionIdPrecedes` for
/// normal xids: the difference taken modulo 2^32 and read as signed.
fn xid_precedes(a: u64, b: u64) -> bool {
    ((a as u32).wrapping_sub(b as u32) as i32) < 0
}

impl SnapshotGate {
    /// A gate that never skips — for `changes_only` feeds (no backfill ⇒ forward everything) and
    /// library mode (no Postgres snapshot exists).
    pub fn passthrough() -> Self {
        SnapshotGate::default()
    }

    /// Build from `pg_current_snapshot()::text` ("xmin:xmax:xip1,xip2,…") + the snapshot LSN.
    pub fn parse(snapshot: &str, lsn: &str) -> Self {
        let mask = |v: u64| v & 0xFFFF_FFFF;
        let mut parts = snapshot.split(':');
        let xmin = parts.next().and_then(|s| s.trim().parse::<u64>().ok()).map(mask).unwrap_or(0);
        let xmax = parts.next().and_then(|s| s.trim().parse::<u64>().ok()).map(mask).unwrap_or(0);
        let xip = parts
            .next()
            .map(|s| s.split(',').filter_map(|x| x.trim().parse::<u64>().ok()).map(mask).collect())
            .unwrap_or_default();
        SnapshotGate { lsn: lsn_to_u64(lsn), xmin, xmax, xip, horizon: 0 }
    }

    /// [`parse`](Self::parse), plus the `pg_current_wal_insert_lsn()` horizon read in the same
    /// statement.
    pub fn parse_with_horizon(snapshot: &str, lsn: &str, horizon: &str) -> Self {
        SnapshotGate { horizon: lsn_to_u64(horizon), ..Self::parse(snapshot, lsn) }
    }

    /// Is this a real snapshot (not [`passthrough`](Self::passthrough))?
    fn is_snapshot(&self) -> bool {
        // xmax is never 0 for a real snapshot: the first normal xid is 3, and a masked xmax of 0
        // would need an xmax of exactly k·2^32, which PostgreSQL never assigns (it skips the
        // special xids 0..2 at every wrap). So 0 stays the passthrough marker.
        self.xmax != 0
    }

    /// Was committed transaction `xid` visible to this snapshot (i.e. already reflected in the
    /// backfill rows)?
    pub fn visible(&self, xid: u64) -> bool {
        if !self.is_snapshot() {
            return false; // passthrough gate: nothing is "already seeded"
        }
        let xid = xid & 0xFFFF_FFFF;
        if xid_precedes(xid, self.xmin) {
            return true;
        }
        if !xid_precedes(xid, self.xmax) {
            return false;
        }
        !self.xip.contains(&xid)
    }

    /// Should a replicated change (commit LSN + optional xid) be skipped because the backfill
    /// snapshot already reflects it?
    pub fn should_skip(&self, commit_lsn: u64, xid: Option<u64>) -> bool {
        match xid {
            // At or past the horizon the commit record did not exist yet when the snapshot was
            // taken, so the transaction cannot be in it — whatever its 32-bit xid compares as.
            Some(_) if self.horizon != 0 && commit_lsn != 0 && commit_lsn >= self.horizon => false,
            Some(x) => self.visible(x),
            None => commit_lsn != 0 && self.lsn != 0 && commit_lsn < self.lsn,
        }
    }
}

// ---- settled snapshots ---------------------------------------------------------------------------

// The record of what the sequencer has fanned out, its visibility poller, and the bounded settle
// wait every backfill/query-back snapshot goes through (see the module docs).
mod settle;

use settle::begin_settled_snapshot;
pub use settle::{
    DEFAULT_SETTLE_MAX_XIDS, MAX_SETTLE_MAX_XIDS, MIN_SETTLE_MAX_XIDS, SequencedXids, SettleConfig, SettleScope,
    SnapshotUnsettled, UnsettledCause,
};

/// The fences of a settled snapshot (see [`begin_settled_snapshot`]).
pub struct SnapshotFences {
    /// `pg_current_wal_lsn()` in the snapshot's first statement.
    pub seed_lsn: String,
    /// `pg_current_wal_insert_lsn()` in the same statement.
    pub horizon_lsn: String,
    /// `pg_current_snapshot()::text` — full xid8 values, as PostgreSQL prints them.
    pub snapshot: String,
    pub gate: SnapshotGate,
}

/// `BEGIN … REPEATABLE READ READ ONLY`, the optional statement timeout, and the fence statement that
/// establishes the snapshot.
async fn open_snapshot(client: &PooledClient, statement_timeout_ms: u64, what: &str) -> Result<SnapshotFences> {
    client
        .batch_execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
        .await
        .with_context(|| format!("begin {what} snapshot"))?;
    // Slow-backfill guard, off by default. `LOCAL` scopes it to this transaction, so the pooled
    // connection carries nothing back to the next borrower. A timeout fails THIS create with a
    // clear, retryable error; nothing is retired and nothing is purged.
    if statement_timeout_ms > 0 {
        client
            .batch_execute(&format!("SET LOCAL statement_timeout = {statement_timeout_ms}"))
            .await
            .context("setting the backfill statement timeout")?;
    }
    // One statement establishes the snapshot AND captures every fence atomically with it.
    let fence = client
        .query_one(
            "select pg_current_wal_lsn()::text, pg_current_snapshot()::text, pg_current_wal_insert_lsn()::text",
            &[],
        )
        .await
        .with_context(|| format!("{what} snapshot fences"))?;
    let seed_lsn: String = fence.get(0);
    let snapshot: String = fence.get(1);
    let horizon_lsn: String = fence.get(2);
    let gate = SnapshotGate::parse_with_horizon(&snapshot, &seed_lsn, &horizon_lsn);
    Ok(SnapshotFences { seed_lsn, horizon_lsn, snapshot, gate })
}

// ---- streamed backfill (issue #13) -------------------------------------------------------------

/// Byte budget for one backfill append, and the optional slow-backfill guard. Resolved once at boot
/// (`ELECTRIC_CIRCUITS_BACKFILL_APPEND_BYTES`, `ELECTRIC_CIRCUITS_BACKFILL_STATEMENT_TIMEOUT_MS`)
/// and published process-wide, like the pool size — every backfill site is deep inside the engine
/// and none of them has a config handle to thread.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BackfillConfig {
    /// Largest request body one backfill append may build. Bounds the engine's memory per backfill
    /// to one chunk.
    pub append_bytes: u64,
    /// `SET LOCAL statement_timeout` inside the backfill transaction, in milliseconds. `0` = off
    /// (the default): a backfill takes as long as it takes.
    pub statement_timeout_ms: u64,
    /// How backfill and subset snapshots are settled against what the sequencer already fanned out
    /// (`ELECTRIC_CIRCUITS_SNAPSHOT_SETTLE_*`, see [`SequencedXids`]).
    pub settle: SettleConfig,
}

impl Default for BackfillConfig {
    fn default() -> Self {
        BackfillConfig { append_bytes: 16 * 1024 * 1024, statement_timeout_ms: 0, settle: SettleConfig::default() }
    }
}

static BACKFILL_CFG: OnceLock<BackfillConfig> = OnceLock::new();

/// Publish the backfill settings. Call once at boot, before the first backfill.
pub fn set_backfill_config(cfg: BackfillConfig) {
    let _ = BACKFILL_CFG.set(cfg);
}

/// The backfill settings (defaults when nothing set them — library users and unit tests).
pub fn backfill_config() -> BackfillConfig {
    *BACKFILL_CFG.get_or_init(BackfillConfig::default)
}

/// Fixed bytes an `upsert` envelope adds around one backfill row's JSON value:
/// `{"type":"","key":"","value":,"headers":{"operation":"upsert"}}` is 62 bytes; 96 leaves headroom
/// for `DsClient`'s framing. The table name and the row's key are measured exactly on top of this,
/// so the per-row estimate is an upper bound on what the envelope actually serializes to — and
/// over-counting only makes chunks smaller.
const BACKFILL_ENVELOPE_FRAMING: u64 = 96;

/// Would adding a row costing `row_bytes` overflow a chunk that already holds `chunk_rows` rows and
/// `chunk_bytes` bytes?
///
/// A chunk always takes at least one row: a single row bigger than the whole budget still has to go
/// somewhere, and splitting a row would produce two invalid JSON items. Pure, so the packing rule is
/// a unit test rather than an inference from a stack trace (mirrors `TxnDrain::next_chunk`).
pub fn chunk_is_full(chunk_rows: usize, chunk_bytes: u64, row_bytes: u64, budget: u64) -> bool {
    chunk_rows > 0 && chunk_bytes.saturating_add(row_bytes) > budget
}

/// A backfill snapshot being read, one chunk at a time.
///
/// The `REPEATABLE READ` transaction is open and its fences are already captured; rows arrive over a
/// `query_raw` cursor with tokio-postgres's own backpressure, so the engine never holds more than
/// **one chunk**, whatever the table's size. Shape creation is two-phase (a pending buffer, then a
/// gated activation), so appending the snapshot chunk by chunk needs no protocol change: the shape
/// is not live until `ActivateShape` lands either way.
pub struct BackfillReader<'a> {
    client: &'a PooledClient,
    ts: &'a TableSchema,
    /// `None` once the cursor is exhausted.
    stream: Option<std::pin::Pin<Box<tokio_postgres::RowStream>>>,
    /// The row that did not fit the chunk just closed, carried into the next one.
    pending: Option<(Row, u64)>,
    budget: u64,
    /// Per-row envelope framing (the fixed part plus this table's qualified name).
    frame: u64,
    fences: BackfillFences,
    rows: u64,
    chunks: u64,
    /// Approximate serialized row/envelope bytes read from Postgres. This is
    /// deliberately an estimate, but it lets us correlate a memory jump with
    /// the amount of data a backfill actually traversed without logging rows.
    estimated_bytes: u64,
    where_present: bool,
}

impl<'a> BackfillReader<'a> {
    /// The snapshot's fences. Available from the start (they were captured before the first row),
    /// but consumers normally take them from [`Self::finish`].
    pub fn fences(&self) -> &BackfillFences {
        &self.fences
    }

    /// Rows delivered so far.
    pub fn rows_read(&self) -> u64 {
        self.rows
    }

    /// Chunks delivered so far.
    pub fn chunks_read(&self) -> u64 {
        self.chunks
    }

    /// Estimated serialized bytes traversed by this snapshot.
    pub fn estimated_bytes_read(&self) -> u64 {
        self.estimated_bytes
    }

    /// The next chunk of rows, in snapshot order, or `None` when the snapshot is exhausted.
    pub async fn next_chunk(&mut self) -> Result<Option<Vec<Row>>> {
        if self.stream.is_none() && self.pending.is_none() {
            return Ok(None);
        }
        let mut chunk: Vec<Row> = Vec::new();
        // `DsClient` posts the chunk as a JSON array: two brackets, plus a comma between items.
        let mut body = 2u64;
        loop {
            let next = match self.pending.take() {
                Some(p) => Some(p),
                None => self.next_row().await?,
            };
            let Some((row, cost)) = next else { break };
            let cost = cost + u64::from(!chunk.is_empty());
            if chunk_is_full(chunk.len(), body, cost, self.budget) {
                self.pending = Some((row, cost));
                break;
            }
            body += cost;
            chunk.push(row);
        }
        if chunk.is_empty() {
            return Ok(None);
        }
        self.rows += chunk.len() as u64;
        self.chunks += 1;
        if self.chunks == 1 || self.chunks % 8 == 0 {
            tracing::info!(
                target: "electric_circuits_engine::backfill",
                table = %self.ts.table,
                where_present = self.where_present,
                chunk_rows = chunk.len() as u64,
                chunk_estimated_bytes = body,
                rows_read = self.rows,
                estimated_bytes = self.estimated_bytes,
                chunks_read = self.chunks,
                "backfill progress"
            );
        }
        Ok(Some(chunk))
    }

    /// One row off the cursor, with the bytes it will cost in an append body.
    async fn next_row(&mut self) -> Result<Option<(Row, u64)>> {
        let Some(stream) = self.stream.as_mut() else { return Ok(None) };
        let Some(row) = stream.next().await else {
            self.stream = None;
            return Ok(None);
        };
        let row = row.with_context(|| format!("backfill select {}", self.ts.table))?;
        let j: serde_json::Value = row.get(0);
        // Measured on the jsonb the SELECT returned — already in hand, so no extra allocation, and
        // an upper bound on the emitted value (a projection emits fewer columns, never more).
        let value_bytes = crate::txn_buffer::serialized_json_len(&j)?;
        let obj = j.as_object().context("backfill row expr did not return an object")?;
        let r = self.ts.row_from_json(obj)?;
        let key_bytes = self.ts.key_string(&r).map(|k| k.len() as u64).unwrap_or(0);
        let estimated_bytes = value_bytes + key_bytes + self.frame;
        self.estimated_bytes = self.estimated_bytes.saturating_add(estimated_bytes);
        Ok(Some((r, estimated_bytes)))
    }

    /// Read the whole snapshot into memory.
    ///
    /// For the few consumers whose RESULT is an in-memory set — a subquery inner-set node's seed, a
    /// membership query-back's candidate rows — where the set is the engine state being built and
    /// there is nothing to stream it to. Everything that writes to a stream must use
    /// [`Self::next_chunk`] instead.
    pub async fn collect(mut self) -> Result<(Vec<Row>, BackfillFences)> {
        let mut all = Vec::new();
        while let Some(mut chunk) = self.next_chunk().await? {
            all.append(&mut chunk);
        }
        tracing::info!(
            target: "electric_circuits_engine::backfill",
            table = %self.ts.table,
            where_present = self.where_present,
            rows = all.len() as u64,
            chunks = self.chunks,
            estimated_bytes = self.estimated_bytes,
            "backfill materialized in memory"
        );
        Ok((all, self.finish().await))
    }

    /// Close the snapshot transaction and hand back its fences.
    pub async fn finish(mut self) -> BackfillFences {
        tracing::info!(
            target: "electric_circuits_engine::backfill",
            table = %self.ts.table,
            where_present = self.where_present,
            rows = self.rows,
            chunks = self.chunks,
            estimated_bytes = self.estimated_bytes,
            "backfill finished"
        );
        // Drop the cursor before COMMIT: tokio-postgres discards whatever is left of an abandoned
        // portal, and the transaction is READ ONLY, so nothing is lost either way.
        self.stream = None;
        if self.client.batch_execute("COMMIT").await.is_ok() {
            self.client.transaction_finished();
        }
        self.fences
    }
}

/// Open a streamed backfill with the shape's compiled predicate pushed into the `SELECT`.
///
/// Text literals are bound parameters; numeric/bool/null are inlined (see [`crate::sql`]). The
/// engine still applies `matches()` afterwards, so the SQL only has to be a sound superset filter.
pub async fn backfill_reader<'a>(
    client: &'a mut PooledClient,
    ts: &'a TableSchema,
    filter: Option<&CompiledPredicate>,
    scope: &SettleScope,
) -> Result<BackfillReader<'a>> {
    let where_sql = filter.and_then(|p| crate::sql::predicate_to_sql(p, ts));
    backfill_where_reader(client, ts, where_sql, scope).await
}

/// Open a streamed backfill with a **prebuilt** `WHERE` fragment + params (from the JSON SQL
/// emitter) — used for subquery shapes/nodes, whose `IN (SELECT …)` SQL the compiled-predicate
/// emitter cannot reconstruct. `where_sql = None` reads the whole table.
///
/// The `REPEATABLE READ READ ONLY` bracket and the fence capture are byte-for-byte what the
/// materialising version did; only the row transport changed (`query` → `query_raw`).
///
/// `scope` names every table the read depends on (`ts`, plus the inner tables of a subquery in
/// `where_sql`) and whether its settle wait is admission-capped (see [`SettleScope`]). The
/// checkout's connection is released while a settle waits, so it is `&mut`.
pub async fn backfill_where_reader<'a>(
    client: &'a mut PooledClient,
    ts: &'a TableSchema,
    where_sql: Option<(String, Vec<String>)>,
    scope: &SettleScope,
) -> Result<BackfillReader<'a>> {
    let cfg = backfill_config();
    let fences = begin_settled_snapshot(client, scope, cfg.statement_timeout_ms, "backfill").await?;
    let client: &'a PooledClient = client;
    match backfill_open_in_txn(client, ts, where_sql, fences).await {
        Ok(reader) => Ok(reader),
        Err(error) => {
            if client.batch_execute("ROLLBACK").await.is_ok() {
                client.transaction_finished();
            }
            Err(error)
        }
    }
}

async fn backfill_open_in_txn<'a>(
    client: &'a PooledClient,
    ts: &'a TableSchema,
    where_sql: Option<(String, Vec<String>)>,
    fences: SnapshotFences,
) -> Result<BackfillReader<'a>> {
    let cfg = backfill_config();
    let SnapshotFences { seed_lsn, gate, .. } = fences;
    let where_present = where_sql.is_some();
    let (where_clause, params) = match where_sql {
        Some((w, ps)) => (format!(" where {w}"), ps),
        None => (String::new(), Vec::new()),
    };
    let q = format!("select {} from {} t{}", row_json_expr(ts), ts.table.quote_qualified(), where_clause);
    let stream = client
        .query_raw(&q, params.iter().map(|s| s as &(dyn tokio_postgres::types::ToSql + Sync)))
        .await
        .with_context(|| format!("backfill select {}", ts.table))?;
    Ok(BackfillReader {
        client,
        ts,
        stream: Some(Box::pin(stream)),
        pending: None,
        budget: cfg.append_bytes,
        frame: BACKFILL_ENVELOPE_FRAMING + ts.table.as_str().len() as u64,
        fences: BackfillFences { seed_lsn, gate },
        rows: 0,
        chunks: 0,
        estimated_bytes: 0,
        where_present,
    })
}

/// Group-count seed for a counts pipeline: `SELECT <group cols>, count(*) … GROUP BY` under a
/// `REPEATABLE READ` snapshot — O(distinct groups) rather than O(rows) — with the same
/// visibility fences as a row backfill. Returned rows are full-width with only the group
/// columns populated (the counts pipeline projects exactly those positions); text-mapped
/// columns are cast `::text` for live-path byte identity.
pub async fn backfill_group_counts(
    client: &mut PooledClient,
    ts: &TableSchema,
    group_cols: &[usize],
) -> Result<(Vec<(Row, i64)>, SnapshotGate)> {
    let fences = begin_settled_snapshot(client, &SettleScope::internal(&ts.table), 0, "counts seed").await?;
    let result = group_counts_in_txn(client, ts, group_cols).await.map(|rows| (rows, fences.gate));
    if client.batch_execute("COMMIT").await.is_ok() {
        client.transaction_finished();
    }
    result
}

async fn group_counts_in_txn(client: &Client, ts: &TableSchema, group_cols: &[usize]) -> Result<Vec<(Row, i64)>> {
    let mut args = Vec::new();
    let mut by = Vec::new();
    for &i in group_cols {
        let (name, ty) = ts.columns.get(i).with_context(|| format!("group col {i} out of range"))?;
        let lit = format!("'{}'", name.replace('\'', "''"));
        let qi = quote_ident(name);
        match ty {
            ColumnType::Text => args.push(format!("{lit}, t.{qi}::text")),
            _ => args.push(format!("{lit}, to_jsonb(t.{qi})")),
        }
        by.push(format!("t.{qi}"));
    }
    let q = format!(
        "select jsonb_build_object({}), count(*)::bigint from {} t group by {}",
        args.join(", "),
        ts.table.quote_qualified(),
        by.join(", ")
    );
    let rows = client.query(&q, &[]).await.with_context(|| format!("counts seed select {}", ts.table))?;
    let mut out = Vec::with_capacity(rows.len());
    for r in &rows {
        let j: serde_json::Value = r.get(0);
        let obj = j.as_object().context("counts seed expr did not return an object")?;
        // Missing (non-group) columns default to Null — the pipeline projects only group cols.
        out.push((ts.row_from_json(obj)?, r.get::<_, i64>(1)));
    }
    Ok(out)
}

/// The per-row JSON projection used by backfill and subset query-backs. Text-mapped columns are cast
/// with `::text` so the value is Postgres's *text output* — the same representation `test_decoding`
/// prints on the live path. `to_jsonb(t)` would instead give e.g. ISO-8601 `T`-form timestamps and
/// raw jsonb objects, so the same cell would compare unequal between a backfilled row and its first
/// replicated update (breaking retractions, equality routing, and MIN/MAX multisets). Int/Float/Bool
/// columns keep `to_jsonb` (native JSON scalars, matching the live parser). Chunked into multiple
/// `jsonb_build_object` calls `||`-concatenated to stay under the 100-argument limit.
fn row_json_expr(ts: &TableSchema) -> String {
    let mut objs: Vec<String> = Vec::new();
    for chunk in ts.columns.chunks(40) {
        let args: Vec<String> = chunk
            .iter()
            .map(|(name, ty)| {
                let lit = format!("'{}'", name.replace('\'', "''"));
                let qi = quote_ident(name);
                match ty {
                    ColumnType::Text => format!("{lit}, t.{qi}::text"),
                    _ => format!("{lit}, to_jsonb(t.{qi})"),
                }
            })
            .collect();
        objs.push(format!("jsonb_build_object({})", args.join(", ")));
    }
    objs.join(" || ")
}

/// Result of a one-shot subset query: the page rows + the fences of the settled snapshot they were
/// read in.
pub struct SubsetQuery {
    pub rows: Vec<Row>,
    /// `pg_current_wal_lsn()` at the snapshot — the page's LSN positioning point (older clients).
    pub lsn: String,
    /// `pg_current_snapshot()::text` of the page's snapshot, full xid8 values: a client skips a live
    /// change only if its transaction was visible to this snapshot.
    pub snapshot: String,
    /// `pg_current_wal_insert_lsn()` at the snapshot: no change at or past it can be in the page, so
    /// a client may drop the snapshot once its tail passes this LSN.
    pub horizon: String,
}

/// Run a **non-materialized** subset query: a single `SELECT … WHERE … ORDER BY … LIMIT … OFFSET …`
/// against Postgres in a `REPEATABLE READ` snapshot, returning the page rows and the snapshot LSN.
/// Unlike [`backfill_reader`], this creates no shape and no durable stream — it is the ephemeral query-back a
/// subset/pagination view uses (the live tail is followed separately). `order` is `(column index,
/// descending?)`; the pk is appended as a tiebreaker so the window is total/stable.
pub async fn query_subset(
    client: &mut PooledClient,
    ts: &TableSchema,
    filter: Option<&CompiledPredicate>,
    order: Option<(usize, bool)>,
    limit: Option<i64>,
    offset: Option<i64>,
) -> Result<SubsetQuery> {
    let where_sql = filter.and_then(|p| crate::sql::predicate_to_sql(p, ts));
    query_subset_where(client, ts, where_sql, &SettleScope::request(&ts.table), order, limit, offset).await
}

/// Like [`query_subset`], but with a **prebuilt** `WHERE` fragment + params — used when the predicate
/// contains an `IN (SELECT …)` subquery (the JSON SQL emitter builds it; Postgres evaluates it natively,
/// so paginated subquery lists work without engine-side subquery state).
///
/// `scope` must name the inner tables of any subquery in `where_sql` (see [`SettleScope`]).
pub async fn query_subset_where(
    client: &mut PooledClient,
    ts: &TableSchema,
    where_sql: Option<(String, Vec<String>)>,
    scope: &SettleScope,
    order: Option<(usize, bool)>,
    limit: Option<i64>,
    offset: Option<i64>,
) -> Result<SubsetQuery> {
    let fences = begin_settled_snapshot(client, scope, 0, "subset").await?;
    let result = query_subset_in_txn(client, ts, where_sql, order, limit, offset).await;
    if client.batch_execute("COMMIT").await.is_ok() {
        client.transaction_finished();
    }
    let rows = result?;
    Ok(SubsetQuery { rows, lsn: fences.seed_lsn, snapshot: fences.snapshot, horizon: fences.horizon_lsn })
}

/// One `ORDER BY` term for a subset page.
///
/// A text column is ordered `COLLATE "C"` — **code-point order** — because the subset client
/// classifies live rows against the loaded page's boundary itself, and it can only compare the
/// strings it received. Ordering by the database's default collation (`en_US.UTF-8` sorts
/// case-insensitively-ish, `C.UTF-8` by bytes) would put the page in an order the client cannot
/// reproduce, and a row on the wrong side of the boundary silently enters or leaves the window.
/// `COLLATE "C"` is also the order the engine's own predicate evaluation uses, so all three agree.
/// Documented in `docs/ARCHITECTURE.md` §7 and `packages/client/README.md`.
fn order_term(ts: &TableSchema, col: usize, dir: &str) -> String {
    let ident = quote_ident(&ts.columns[col].0);
    if ts.is_collatable_text(col) { format!("{ident} collate \"C\" {dir}") } else { format!("{ident} {dir}") }
}

async fn query_subset_in_txn(
    client: &Client,
    ts: &TableSchema,
    where_sql: Option<(String, Vec<String>)>,
    order: Option<(usize, bool)>,
    limit: Option<i64>,
    offset: Option<i64>,
) -> Result<Vec<Row>> {
    let (where_clause, params) = match where_sql {
        Some((w, ps)) => (format!(" where {w}"), ps),
        None => (String::new(), Vec::new()),
    };
    let param_refs: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> =
        params.iter().map(|s| s as &(dyn tokio_postgres::types::ToSql + Sync)).collect();
    // ORDER BY <col> <dir>, <pk> <dir> for a total order; a limit/offset without an explicit order
    // falls back to pk order so the page is deterministic. Idents are quoted; limit/offset are
    // non-negative integer literals — no injection surface. Text terms carry an explicit
    // `COLLATE "C"` — see `order_term`.
    let order_sql = match order {
        Some((col, desc)) => {
            let d = if desc { "desc" } else { "asc" };
            format!(" order by {}, {}", order_term(ts, col, d), order_term(ts, ts.pk_index, d))
        }
        None if limit.is_some() || offset.is_some() => {
            format!(" order by {}", order_term(ts, ts.pk_index, "asc"))
        }
        None => String::new(),
    };
    let limit_sql = limit.map(|n| format!(" limit {}", n.max(0))).unwrap_or_default();
    let offset_sql = offset.map(|n| format!(" offset {}", n.max(0))).unwrap_or_default();
    let q = format!(
        "select {} from {} t{}{}{}{}",
        row_json_expr(ts),
        ts.table.quote_qualified(),
        where_clause,
        order_sql,
        limit_sql,
        offset_sql
    );
    let rows = client.query(&q, &param_refs).await.with_context(|| format!("subset select {}", ts.table))?;
    let mut out = Vec::with_capacity(rows.len());
    for r in &rows {
        let j: serde_json::Value = r.get(0);
        let obj = j.as_object().context("subset row expr did not return an object")?;
        out.push(ts.row_from_json(obj)?);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The snapshot gate must skip exactly the transactions the backfill snapshot could see:
    /// committed-before (`xid < xmin`) → skip; in-progress at snapshot (`xip`) → process; started
    /// after (`xid >= xmax`) → process — regardless of WAL position. This is the fence that closes
    /// the "commit record written but not yet visible" window an LSN comparison cannot express.
    #[test]
    fn snapshot_gate_visibility() {
        // snapshot: xmin 100, xmax 110, in-progress {103, 107}; snapshot LSN 0/100.
        let g = SnapshotGate::parse("100:110:103,107", "0/100");
        // committed before the snapshot -> already in the backfill -> skip
        assert!(g.should_skip(0, Some(99)));
        // between xmin and xmax, not in-progress -> visible -> skip (even if its commit LSN were AT
        // or ABOVE the snapshot LSN, the boundary-duplicate case)
        assert!(g.should_skip(0x200, Some(105)));
        // in-progress at snapshot time -> INVISIBLE to the backfill -> must be processed, even though
        // its commit LSN may be below the snapshot LSN (the dropped-row race the LSN rule had)
        assert!(!g.should_skip(0x50, Some(103)));
        assert!(!g.should_skip(0x50, Some(107)));
        // started after the snapshot -> process
        assert!(!g.should_skip(0x200, Some(110)));
        assert!(!g.should_skip(0x200, Some(200)));
        // no xid -> LSN fallback (strict <)
        assert!(g.should_skip(0x50, None));
        assert!(!g.should_skip(0x100, None));
        // passthrough gate never skips
        let p = SnapshotGate::passthrough();
        assert!(!p.should_skip(0x50, Some(99)));
        assert!(!p.should_skip(0x50, None));
    }

    /// The gate compares 32-bit xids the way PostgreSQL does (modulo 2^32), so a snapshot taken just
    /// before an xid epoch boundary still treats a transaction from just after it as LATER. Masking
    /// and comparing with plain `<` made such a transaction (masked xid 5 < masked xmin 0xFFFF_FFFA)
    /// look committed-before-the-snapshot, and every one of its changes was skipped.
    #[test]
    fn snapshot_gate_is_wraparound_safe() {
        // xid8 values straddling 2^32: xmin 2^32-6, xmax 2^32+4, in progress {2^32-1, 2^32+3}.
        let g = SnapshotGate::parse("4294967290:4294967300:4294967295,4294967299", "0/100");
        assert!(g.should_skip(0, Some(4_294_967_288)), "before xmin: in the snapshot");
        assert!(g.should_skip(0, Some(4_294_967_294)), "between xmin and xmax, not in progress: in the snapshot");
        assert!(g.should_skip(0, Some(2)), "2^32+2 masked: between xmin and xmax across the boundary");
        assert!(!g.should_skip(0, Some(4_294_967_295)), "in progress before the boundary: not in the snapshot");
        assert!(!g.should_skip(0, Some(3)), "in progress after the boundary: not in the snapshot");
        assert!(!g.should_skip(0, Some(4)), "at xmax: not in the snapshot");
        assert!(!g.should_skip(0, Some(5)), "the next epoch's xid 5 is after the snapshot, not before xmin");
    }

    /// A change whose commit LSN is at or past the snapshot's insert horizon cannot be in the
    /// snapshot, whatever its xid compares as; a gate persisted before the horizon existed keeps the
    /// xid-only rule.
    #[test]
    fn snapshot_gate_horizon_bounds_the_skip() {
        let g = SnapshotGate::parse_with_horizon("100:110:103", "0/100", "0/180");
        assert!(g.should_skip(0x170, Some(99)), "visible and below the horizon: skip");
        assert!(!g.should_skip(0x180, Some(99)), "at the horizon: newer than the snapshot");
        assert!(!g.should_skip(0x170, Some(103)), "in progress: never skipped");
        assert!(g.should_skip(0, Some(99)), "unknown commit LSN: the xid decides");
        let legacy: SnapshotGate =
            serde_json::from_str(r#"{"lsn":256,"xmin":100,"xmax":110,"xip":[103]}"#).expect("old catalog gate");
        assert!(legacy.should_skip(0x200, Some(99)), "no horizon recorded: xid-only, as before");
    }

    /// The boot taxonomy IS the contract: a fatal class must never be retried into an invisible
    /// loop, and a transient one must never crash-loop the pod. One assertion per row of
    /// `classify`'s table.
    #[test]
    fn boot_error_classification() {
        let verdict = |code: &str| classify(PgFailure { sqlstate: Some(code), io: false });

        // Fatal: nothing changes by waiting.
        assert_eq!(verdict("28000"), BootFailure::Fatal, "pg_hba refusal");
        assert_eq!(verdict("28P01"), BootFailure::Fatal, "bad password");
        assert_eq!(verdict("42501"), BootFailure::Fatal, "insufficient privilege");
        assert_eq!(verdict("3D000"), BootFailure::Fatal, "unknown database");
        assert_eq!(verdict("42601"), BootFailure::Fatal, "syntax error: the engine's own statement");
        assert_eq!(verdict("42P01"), BootFailure::Fatal, "undefined table");

        // Retryable: the server is not ready, not wrong.
        assert_eq!(verdict("57P03"), BootFailure::Retryable, "the database system is starting up");
        assert_eq!(verdict("57P01"), BootFailure::Retryable, "admin shutdown");
        assert_eq!(verdict("08006"), BootFailure::Retryable, "connection failure");
        assert_eq!(verdict("08001"), BootFailure::Retryable, "cannot connect");
        assert_eq!(verdict("53300"), BootFailure::Retryable, "too many connections");
        assert_eq!(verdict("55006"), BootFailure::Retryable, "object in use (slot held)");
        assert_eq!(verdict("40001"), BootFailure::Retryable, "serialization failure");

        // No SQLSTATE at all: DNS, connection refused, timeout, TLS — the server is not there yet.
        assert_eq!(classify(PgFailure { sqlstate: None, io: true }), BootFailure::Retryable);
        assert_eq!(classify(PgFailure::default()), BootFailure::Retryable);
    }

    #[test]
    fn managed_standby_is_a_retryable_boot_observation() {
        let error = anyhow::Error::new(crate::engine::DeploymentNotReady);
        assert_eq!(boot_disposition(&error), BootFailure::Retryable);
    }

    #[test]
    fn fatal_failures_are_named_for_operators() {
        let named = |code: &str| failure_name(PgFailure { sqlstate: Some(code), io: false });
        assert_eq!(named("28P01"), "authentication failed");
        assert_eq!(named("42501"), "insufficient privilege");
        assert_eq!(named("3D000"), "unknown database");
        assert_eq!(named("57P03"), "the database system is starting up");
        // The two no-SQLSTATE cases are told apart, because they send an operator to different
        // places even though both retry.
        assert_eq!(failure_name(PgFailure { sqlstate: None, io: true }), "Postgres is unreachable");
        assert_eq!(failure_name(PgFailure::default()), "Postgres returned an error without a SQLSTATE");
    }

    /// An unparseable connection string must be refused at boot, not retried: to [`classify`] it is
    /// indistinguishable from "the database is not up yet" (no SQLSTATE, no server answer), so
    /// without this check a typo in `ELECTRIC_CIRCUITS_PG_URL` would back off and re-parse the same
    /// broken string forever.
    #[test]
    fn an_unusable_pg_url_is_refused_with_its_password_redacted() {
        assert!(parse_pg_url("postgres://u:p@127.0.0.1:5432/app").is_ok());
        let e = parse_pg_url("postgres://u:hunter2@host:notaport/db").unwrap_err();
        let msg = format!("{e:#}");
        assert!(msg.contains("unusable Postgres URL"), "{msg}");
        assert!(msg.contains("u:***@host:notaport/db"), "the password must not be logged: {msg}");
        assert!(!msg.contains("hunter2"), "{msg}");
        // ...and it is NOT a `tokio_postgres::Error` in the chain, so `boot_disposition` — which
        // only ever sees it if this check were removed — would call it fatal too.
        assert_eq!(boot_disposition(&e), BootFailure::Fatal);
    }

    #[test]
    fn query_pool_and_replication_share_the_same_verified_tls_policy() {
        let config = PgConnectionConfig::resolve(
            "postgresql://repl:p%40ss@example.cluster.us-east-1.rds.amazonaws.com/app?sslmode=verify-full",
            Some("/run/secrets/postgres/rds-ca.pem"),
            None,
        )
        .unwrap();

        assert_eq!(
            config.query.get_ssl_mode(),
            tokio_postgres::config::SslMode::Require,
            "tokio-postgres must negotiate TLS; rustls owns full verification"
        );
        let replication = config.replication_config("circuits", "circuits_pub").unwrap();
        assert_eq!(replication.host, "example.cluster.us-east-1.rds.amazonaws.com");
        assert_eq!(replication.user, "repl");
        assert_eq!(replication.password, "p@ss");
        assert_eq!(replication.database, "app");
        assert_eq!(replication.tls.mode, pgwire_replication::SslMode::VerifyFull);
        assert_eq!(replication.tls.ca_pem_path.as_deref(), Some(Path::new("/run/secrets/postgres/rds-ca.pem")));
    }

    #[test]
    fn local_development_uses_explicit_plaintext_on_both_paths() {
        let config =
            PgConnectionConfig::resolve("postgresql://postgres:password@postgres:5432/app", None, None).unwrap();
        assert_eq!(config.query.get_ssl_mode(), tokio_postgres::config::SslMode::Disable);
        assert_eq!(
            config.replication_config("circuits", "circuits_pub").unwrap().tls.mode,
            pgwire_replication::SslMode::Disable
        );
    }

    /// A boot failure that is not a Postgres failure at all — an unusable `PG_TABLES` entry, an
    /// unreadable durable catalog, a `wal_level` that is not `logical` — is fatal, because none of
    /// them changes by waiting.
    #[test]
    fn non_postgres_boot_failures_are_fatal() {
        let e = anyhow::anyhow!("ELECTRIC_CIRCUITS_PG_TABLES has 1 unusable entry");
        assert_eq!(boot_disposition(&e), BootFailure::Fatal);
        assert_eq!(boot_failure_name(&e), "not a transient Postgres condition");
    }

    /// Chunk packing: a chunk fills up to the budget and never past it, but a single row bigger
    /// than the whole budget still becomes a chunk of its own (splitting a row would emit invalid
    /// JSON, and refusing it would cost the shape its stream for being wide).
    #[test]
    fn chunk_packing_respects_the_budget_but_never_drops_a_row() {
        // Empty chunk: never "full", whatever the row costs.
        assert!(!chunk_is_full(0, 2, 10, 100));
        assert!(!chunk_is_full(0, 2, 10_000, 100), "an over-budget row is still a chunk of its own");
        // Fits exactly.
        assert!(!chunk_is_full(1, 90, 10, 100));
        // One byte over.
        assert!(chunk_is_full(1, 91, 10, 100));
        // Saturating: no overflow panic near u64::MAX.
        assert!(chunk_is_full(1, u64::MAX, 10, 100));
    }

    /// The packing INVARIANT, exercised over a synthetic run of the same predicate
    /// `BackfillReader::next_chunk` drives: every chunk body stays within the budget (unless it is
    /// one over-budget row on its own), no row is dropped, and snapshot order is preserved across
    /// the chunk boundaries. Those are the three properties a streamed backfill's correctness rests
    /// on — a body over the budget is an append durable-streams can refuse, a dropped row is a
    /// missing row in the shape, and a reordered one breaks the adapter's deterministic snapshot.
    #[test]
    fn packing_a_run_of_rows_respects_the_budget_and_preserves_order() {
        const BUDGET: u64 = 100;
        // A mix of ordinary rows and one that alone exceeds the whole budget.
        let costs: Vec<u64> = vec![30, 30, 30, 30, 250, 10, 10, 10, 10, 10, 10, 10, 10, 90];
        let mut chunks: Vec<(Vec<usize>, u64)> = Vec::new();
        let mut chunk: Vec<usize> = Vec::new();
        let mut body = 2u64; // the JSON array's brackets, as in `next_chunk`
        for (i, &cost) in costs.iter().enumerate() {
            let cost = cost + u64::from(!chunk.is_empty()); // the comma between items
            if chunk_is_full(chunk.len(), body, cost, BUDGET) {
                chunks.push((std::mem::take(&mut chunk), body));
                body = 2 + cost;
                chunk.push(i);
            } else {
                body += cost;
                chunk.push(i);
            }
        }
        chunks.push((chunk, body));

        assert!(chunks.len() > 1, "this run must actually chunk, or it proves nothing");
        for (rows, body) in &chunks {
            assert!(!rows.is_empty(), "an empty chunk is never emitted");
            assert!(
                *body <= BUDGET || rows.len() == 1,
                "a chunk may only exceed the budget when it is a single over-budget row: {rows:?} = {body}"
            );
        }
        // Every row, exactly once, in snapshot order.
        let flat: Vec<usize> = chunks.iter().flat_map(|(r, _)| r.iter().copied()).collect();
        assert_eq!(flat, (0..costs.len()).collect::<Vec<_>>());
    }

    /// The packer's byte measure is the real serializer's, not an estimate of it: the same counting
    /// writer the ingestor's chunking uses.
    #[test]
    fn row_payload_is_measured_with_the_real_serializer() {
        let j = serde_json::json!({"id": 1, "label": "abc"});
        let measured = crate::txn_buffer::serialized_json_len(&j).unwrap();
        assert_eq!(measured as usize, serde_json::to_vec(&j).unwrap().len());
    }

    /// The default budget must be appendable: a value above the durable-streams body cap could
    /// never land, and 16 MiB leaves three orders of magnitude of headroom.
    #[test]
    fn default_backfill_budget_is_within_the_storage_body_cap() {
        let d = BackfillConfig::default();
        assert_eq!(d.append_bytes, 16 * 1024 * 1024);
        assert!(d.append_bytes <= crate::txn_buffer::DS_MAX_BODY_BYTES);
        assert_eq!(d.statement_timeout_ms, 0, "the slow-backfill guard is off by default");
    }

    #[test]
    fn lsn_parse_roundtrip() {
        assert_eq!(lsn_to_u64("0/1A2B3C"), 0x1A2B3C);
        assert_eq!(lsn_to_u64("2/10"), (2u64 << 32) | 0x10);
        assert_eq!(lsn_to_u64("garbage"), 0);
    }
}

/// Parse a Postgres LSN ("X/Y", hex) into a comparable u64. Returns 0 on parse failure.
pub fn lsn_to_u64(lsn: &str) -> u64 {
    match lsn.split_once('/') {
        Some((hi, lo)) => {
            let hi = u64::from_str_radix(hi.trim(), 16).unwrap_or(0);
            let lo = u64::from_str_radix(lo.trim(), 16).unwrap_or(0);
            (hi << 32) | lo
        }
        None => 0,
    }
}

#[cfg(test)]
mod ownership_tests {
    use super::*;

    #[test]
    fn same_url_pools_have_independent_settle_records() {
        let a = Pool::new("postgres://unused/test".into(), 2);
        let b = Pool::new("postgres://unused/test".into(), 2);
        assert!(!Arc::ptr_eq(&a.inner, &b.inner));
        a.sequenced().note(42, ["public.items"]);
        assert_eq!(a.sequenced().len(), 1);
        assert_eq!(b.sequenced().len(), 0);
        a.close();
        assert!(a.inner.sem.is_closed());
        assert!(!b.inner.sem.is_closed());
        b.close();
    }
}
