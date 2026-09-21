//! Minimal durable-streams HTTP client: PUT-create, POST-append (JSON array), and
//! offset-resumable reads (catch-up + long-poll live). Offsets are opaque tokens; we just
//! persist and replay `Stream-Next-Offset`.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::{fs, path::PathBuf};

use anyhow::{Context, Result, bail};

use crate::heap_size::HeapSize;
use serde::{Deserialize, Serialize};

use crate::store_identity::{StoreIdentityV1, StreamScope};

/// Scoped Durable Streams connection with optional TLS material.
/// HTTP ignores all certificate paths. HTTPS uses system roots unless a CA bundle is supplied,
/// and sends a client identity only when both certificate and key paths are supplied.
#[derive(Clone, Debug)]
pub struct DsConnectionConfig {
    pub base_url: String,
    pub ca_bundle_path: Option<PathBuf>,
    pub client_certificate_path: Option<PathBuf>,
    pub client_key_path: Option<PathBuf>,
    pub scope: StreamScope,
}

impl DsConnectionConfig {
    pub fn new(
        base_url: String,
        ca_bundle_path: Option<PathBuf>,
        client_certificate_path: Option<PathBuf>,
        client_key_path: Option<PathBuf>,
        scope: StreamScope,
    ) -> Result<Self> {
        let url = url::Url::parse(&base_url).context("ELECTRIC_CIRCUITS_DS_URL must be an absolute URL")?;
        if !matches!(url.scheme(), "http" | "https") {
            bail!("ELECTRIC_CIRCUITS_DS_URL must use http or https");
        }
        if url.host_str().is_none()
            || !url.username().is_empty()
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
        {
            bail!("ELECTRIC_CIRCUITS_DS_URL must be an HTTP(S) origin without credentials, query, or fragment");
        }
        if url.path() != "/" && !url.path().is_empty() {
            bail!("ELECTRIC_CIRCUITS_DS_URL must not contain a path prefix");
        }
        let (ca_bundle_path, client_certificate_path, client_key_path) = if url.scheme() == "http" {
            (None, None, None)
        } else {
            if client_certificate_path.is_some() != client_key_path.is_some() {
                bail!("ELECTRIC_CIRCUITS_DS_CLIENT_CERT and ELECTRIC_CIRCUITS_DS_CLIENT_KEY must be set together");
            }
            (ca_bundle_path, client_certificate_path, client_key_path)
        };
        Ok(Self {
            base_url: base_url.trim_end_matches('/').to_string(),
            ca_bundle_path,
            client_certificate_path,
            client_key_path,
            scope,
        })
    }
}

/// Validate the loopback-only HTTP store used by the self-contained conformance image.
///
/// The constructor remains unavailable unless `test-support` is compiled in. Keeping the URL
/// validation unconditional lets configuration fail closed even in a production engine binary.
pub(crate) fn validate_in_process_test_url(base_url: &str) -> Result<()> {
    let url = url::Url::parse(base_url).context("ELECTRIC_CIRCUITS_DS_URL must be an absolute URL")?;
    let is_loopback = match url.host() {
        Some(url::Host::Domain(host)) => host.eq_ignore_ascii_case("localhost"),
        Some(url::Host::Ipv4(address)) => address.is_loopback(),
        Some(url::Host::Ipv6(address)) => address.is_loopback(),
        None => false,
    };
    if url.scheme() != "http"
        || !is_loopback
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || (url.path() != "/" && !url.path().is_empty())
    {
        bail!(
            "ELECTRIC_CIRCUITS_DS_IN_PROCESS_TEST=1 requires an HTTP loopback origin without credentials, path, query, or fragment"
        );
    }
    Ok(())
}

/// Store readiness response decoded before any ordinary Durable Streams or Postgres operation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoreReadinessV1 {
    pub identity: StoreIdentityV1,
    /// Maximum response page advertised by the server. `None`/zero means an uncapped server.
    pub max_chunk_bytes: Option<u64>,
    /// Maximum size of one value on the store. This is diagnostic only and never sizes reads.
    pub max_value_bytes: Option<u64>,
}

#[derive(Debug)]
pub enum StoreReadinessError {
    Response { status: u16 },
    Malformed { detail: String },
    NotReady { status: String },
    IdentityMismatch { expected: StoreIdentityV1, observed: StoreIdentityV1 },
    PageCapability { detail: String },
}

impl std::fmt::Display for StoreReadinessError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Response { status } => write!(f, "storage readiness endpoint refused the boot with HTTP {status}"),
            Self::Malformed { detail } => write!(f, "storage readiness response is malformed: {detail}"),
            Self::NotReady { status } => write!(f, "storage readiness status is '{status}', not 'ready'"),
            Self::IdentityMismatch { expected, observed } => {
                write!(f, "storage identity mismatch: expected {expected:?}, observed {observed:?}")
            }
            Self::PageCapability { detail } => write!(f, "storage page capability is incompatible: {detail}"),
        }
    }
}

impl std::error::Error for StoreReadinessError {}

/// A State-Protocol change event, the JSON item on every table/shape stream.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Envelope {
    #[serde(rename = "type")]
    pub type_: String,
    pub key: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<serde_json::Value>,
    /// The full prior row, carried by replication on UPDATE/DELETE (`REPLICA IDENTITY FULL`). Lets
    /// the engine compute the input delta without an in-memory `table_state`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub old: Option<serde_json::Value>,
    pub headers: EnvelopeHeaders,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnvelopeHeaders {
    pub operation: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub txid: Option<String>,
    // The server stamps an `offset` onto each item; accept it on read, never send it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub offset: Option<String>,
    /// Postgres commit LSN of the change (set by the replication ingestor). Used to skip changes a
    /// shape/family already reflects from its backfill snapshot (`lsn <= seed_lsn`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lsn: Option<String>,
    /// Position of this change within its transaction (set by the ingestor). `(lsn, seq)` uniquely
    /// identifies a change, letting the tailer skip duplicates when the ingestor re-appends a batch
    /// after a partial failure or a crash between append and slot-advance (at-least-once delivery).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub seq: Option<u64>,
    /// **Transaction-end marker**: `Some(true)` on the LAST envelope of a transaction, and only
    /// there (ADR-0003).
    ///
    /// It is what keeps per-transaction atomic emission true on the wire now that a commit too
    /// large for one request body is appended in several chunks. Each append is exposed atomically
    /// by durable-streams, so without the marker the sequencer would see chunk 1 as a complete
    /// `(txid, lsn)` run, fan it out and flush it to shape streams, then do the same for chunks
    /// 2..N — a subscriber would observe a fraction of a transaction. With it, the sequencer HOLDS a
    /// trailing run whose last envelope is unmarked and processes the transaction only once the
    /// marker arrives.
    ///
    /// Every producer sets it: the ingestor on the last envelope of the last chunk (single-chunk
    /// commits included, so the rule is uniform), and library-mode writers on every envelope
    /// (one-envelope transactions). An envelope WITHOUT it that is not followed by one is an
    /// incomplete transaction, by definition — never a transaction that opted out.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last: Option<bool>,
}

/// Envelope framing used by the replay reader. Row bodies stay as raw JSON until the envelope's
/// `type` has been matched to the waking shape's table, avoiding allocation for unrelated tables
/// in the global change log.
#[derive(Debug, Deserialize)]
struct LazyEnvelope {
    #[serde(rename = "type")]
    type_: String,
    key: String,
    #[serde(default)]
    value: Option<Box<serde_json::value::RawValue>>,
    #[serde(default)]
    old: Option<Box<serde_json::value::RawValue>>,
    headers: EnvelopeHeaders,
}

impl LazyEnvelope {
    fn into_envelope(self) -> Result<Envelope> {
        let value =
            self.value.map(|raw| serde_json::from_str(raw.get())).transpose().context("decoding envelope value")?;
        let old =
            self.old.map(|raw| serde_json::from_str(raw.get())).transpose().context("decoding envelope old value")?;
        Ok(Envelope { type_: self.type_, key: self.key, value, old, headers: self.headers })
    }
}

impl crate::heap_size::HeapSize for EnvelopeHeaders {
    fn heap_bytes(&self) -> usize {
        self.operation.heap_bytes() + self.txid.heap_bytes() + self.offset.heap_bytes() + self.lsn.heap_bytes()
    }
}

impl crate::heap_size::HeapSize for Envelope {
    fn heap_bytes(&self) -> usize {
        self.type_.heap_bytes()
            + self.key.heap_bytes()
            + self.value.heap_bytes()
            + self.old.heap_bytes()
            + self.headers.heap_bytes()
    }
}

/// What one envelope costs to hold in memory: its inline representation plus the heap it owns.
///
/// This is the quantity `ELECTRIC_CIRCUITS_TXN_MEMORY_BYTES` is measured in (ADR-0003), so the knob
/// counts what is actually held rather than what the same data would serialize to. It is a lower
/// bound in the same sense as every other [`crate::heap_size::HeapSize`] estimate (allocator
/// overhead and `serde_json::Map` bucket overhead are not modelled).
pub fn envelope_memory_bytes(env: &Envelope) -> u64 {
    (std::mem::size_of::<Envelope>() + env.heap_bytes()) as u64
}

pub struct ReadResult {
    pub envelopes: Vec<Envelope>,
    pub next_offset: Option<String>,
    pub up_to_date: bool,
    /// The server reported `stream-closed`: the stream is **terminal** — it will never grow again.
    /// For a shape stream that means the engine retired the shape (close-then-delete, see
    /// [`DsClient::retire_stream`]) and readers must stop, not re-poll: a closed stream answers a
    /// long-poll instantly, so looping on "empty page, same offset" would spin on the server. For a
    /// `changes/<n>` segment it means the log ROTATED (ADR-0006) and the reader follows the batch's
    /// rotation pointer onto the next segment.
    pub closed: bool,
}

/// What a `HEAD` found: the stream's tail offset and whether it is closed. `None` from
/// [`DsClient::head`] means the stream is not there (404) or soft-deleted (410).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StreamHead {
    pub next_offset: Option<String>,
    pub closed: bool,
}

/// A read hit a stream that is not there: deleted (404) or soft-deleted (410).
///
/// A typed error because callers make very different decisions about it than about a transient read
/// failure. On the change log (ADR-0006) it is never expected — a segment is deleted only once
/// nothing can resume inside it — so the sequencer treats it as an error to log loudly and back off
/// from, the boot treats it as fatal for the position it is about to resume, and a dormant shape's
/// replay treats it as "this shape can never be brought up to date", which evicts it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamGone {
    pub path: String,
    pub status: u16,
}

impl std::fmt::Display for StreamGone {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "stream '{}' is gone ({})", self.path, self.status)
    }
}

impl std::error::Error for StreamGone {}

/// Does this error (or anything it was contextualised from) mean "the stream is not there"?
pub fn is_stream_gone(e: &anyhow::Error) -> bool {
    e.chain().any(|c| c.downcast_ref::<StreamGone>().is_some())
}

/// The durable-streams server answered, but with a 5xx: it is reachable and not serving. Typed so
/// callers (the boot, above all) can tell "storage is having a moment" from "this request is
/// wrong", which a status embedded in a message string cannot express.
#[derive(Debug)]
pub struct DsUnavailable {
    pub op: &'static str,
    pub path: String,
    pub status: u16,
}

impl std::fmt::Display for DsUnavailable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {} -> {} (durable-streams unavailable)", self.op, self.path, self.status)
    }
}

impl std::error::Error for DsUnavailable {}

/// Is this a durable-streams failure that may clear on its own — the server not up yet, a refused
/// connection, a timeout, a connection dropped mid-response, or a 5xx?
///
/// This is the read the **boot** takes: a storage server that comes up after its engine is the
/// normal case in a compose/Kubernetes start, so it must back off rather than exit `EX_CONFIG`.
/// Deliberately narrow — it forgives the TRANSPORT and nothing else:
///
/// * `reqwest::Error::is_builder` (an unusable `ELECTRIC_CIRCUITS_DS_URL`) is **not** forgiven: no
///   amount of waiting reshapes a URL;
/// * `is_decode` (a body that is not what it claims) is **not** forgiven — that is a malformed
///   catalog, which is fatal by design;
/// * a [`StreamGone`] (404/410) is **not** forgiven: a stream that is not there is an answer, and
///   every caller has its own, very different response to it;
/// * a typed catalog strictness refusal carries no `reqwest::Error` at all, so it stays fatal.
pub fn is_unavailable(e: &anyhow::Error) -> bool {
    if e.chain().any(|c| c.downcast_ref::<StreamGone>().is_some()) {
        return false;
    }
    if e.chain().any(|c| c.downcast_ref::<DsUnavailable>().is_some()) {
        return true;
    }
    e.chain().filter_map(|c| c.downcast_ref::<reqwest::Error>()).any(|r| {
        !r.is_builder() && !r.is_decode() && (r.is_connect() || r.is_timeout() || r.is_request() || r.is_body())
    })
}

/// Build the error for a non-2xx durable-streams response: typed when the server said it is
/// unavailable (5xx), an ordinary message otherwise.
fn status_error(op: &'static str, path: &str, status: u16, body: &str) -> anyhow::Error {
    if (500..600).contains(&status) {
        return anyhow::Error::new(DsUnavailable { op, path: path.to_string(), status });
    }
    if body.is_empty() {
        anyhow::anyhow!("{op} {path} -> {status}")
    } else {
        anyhow::anyhow!("{op} {path} -> {status}: {body}")
    }
}

/// The outcome of an append that treats retirement as an answer rather than an error (see
/// [`DsClient::append_checked`]).
pub enum Appended {
    /// Landed; `next_offset` is the stream's tail afterwards (`stream-next-offset`).
    Ok { next_offset: Option<String> },
    /// The stream is retired — deleted (404), soft-deleted (410) or closed (409 + `stream-closed`).
    Retired(u16),
}

/// Why an append failed: the stream is retired — deleted (404), soft-deleted (410) or closed
/// (409 + `stream-closed`), all terminal, discard — or a transient/other error (retry or surface).
/// `Gone` carries the status so the log/error names which of the three it was.
enum AppendError {
    Gone(u16),
    Other(anyhow::Error),
}

/// What the engine decides about a shape stream whose append came back **terminal** (404, 410, or
/// 409 + `stream-closed`) — see [`DsClient::set_gone_reconciler`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GoneVerdict {
    /// The engine does not hold this stream any more (retired, evicted, purged, or never a shape
    /// stream at all): discarding the batch is correct and complete.
    Discard,
    /// The shape is still registered AND storage still has its stream: the terminal answer was
    /// FALSE — a proxy, a router or a failover said 404 about a stream that is right there. Retry
    /// the append; the batch belongs to a live shape and dropping it is permanent divergence.
    Retry,
}

/// Reconcile a terminal-looking append answer against engine state. Installed once by the engine
/// (see `Engine::install_gone_reconciler`); absent in the tests/tools that use a bare `DsClient`,
/// where a terminal answer is taken at face value exactly as before.
pub type GoneReconciler = std::sync::Arc<
    dyn Fn(String) -> std::pin::Pin<Box<dyn std::future::Future<Output = GoneVerdict> + Send>> + Send + Sync,
>;

/// One response from the durable-stream protocol.  This deliberately stays private while the
/// provider boundary is being extracted: DSP-003 will replace the status-oriented compatibility
/// mapping with a closed outcome vocabulary.  Until then it lets the existing facade preserve its
/// exact error/retry behavior while keeping HTTP mechanics below the port.
pub(crate) struct StoreResponse {
    status: u16,
    /// The response body is deliberately retained as an outcome rather than normalized to text.
    /// Successful stream reads must fail if their body cannot be acquired: accepting the advertised
    /// next offset with an empty page could checkpoint past envelopes that never reached Circuits.
    /// Other legacy operations intentionally retain their prior best-effort body handling.
    body: Option<std::result::Result<String, anyhow::Error>>,
    next_offset: Option<String>,
    up_to_date: bool,
    closed: bool,
}

impl StoreResponse {
    /// Preserve legacy best-effort response-body handling for write/control operations.
    fn body_or_default(self) -> String {
        self.body.and_then(std::result::Result::ok).unwrap_or_default()
    }

    /// A successful stream read is not a successful page until its body has been acquired.
    fn required_body(self) -> Result<String> {
        self.body.unwrap_or_else(|| Err(anyhow::anyhow!("provider omitted a successful stream response body")))
    }
}

#[derive(Clone, Copy)]
pub(crate) enum BodyRead {
    Never,
    OnFailure,
    OnData,
    Always,
}

/// The cap against a store that advertises a page: four of the 4 MiB pages the server serves, so a
/// page never reaches it. It is a starting point, not the cap in force: a JSON page cuts only on a
/// value boundary, so one value larger than the page is framed whole, and the cap this process
/// installs is never below its own append budget ([`ds_read_cap_floor`]).
const DEFAULT_DS_READ_MAX_BYTES: u64 = 16 * 1024 * 1024;

/// The floor under every derived cap: the largest single message this engine appends
/// (`ELECTRIC_CIRCUITS_CHANGES_APPEND_BYTES`, one POST body). The store frames a value that large
/// whole on read, so a cap below it would refuse a page this engine's own writes are entitled to
/// produce — and an engine that latched on its own change log would cycle under the health check
/// forever, re-reading the same value from the same checkpoint. An explicit
/// `ELECTRIC_CIRCUITS_DS_READ_MAX_BYTES` below this floor is refused at boot (`config.rs`).
pub(crate) fn ds_read_cap_floor() -> u64 {
    crate::txn_buffer::TxnBufferConfig::from_env()
        .map(|cfg| cfg.append_bytes)
        .unwrap_or_else(|_| crate::txn_buffer::TxnBufferConfig::default().append_bytes)
}

/// The cap against a store that advertises NO page. Such a store answers a read with the whole
/// remainder of the stream, so the client cap is the only thing between a large backlog and a
/// **permanently stalled live loop**: the read fails, the sequencer retries the identical read, and
/// it fails identically forever — no data flows and nothing recovers it. A larger ceiling does not
/// make that store safe; it makes progress the default and leaves the boot WARN (and
/// `ELECTRIC_CIRCUITS_REQUIRE_DS_CHUNK_CAP=1`) as the way to demand a store that pages.
const UNCAPPED_STORE_READ_MAX_BYTES: u64 = 64 * 1024 * 1024;

#[derive(Debug)]
pub(crate) struct ReadCapExceeded {
    pub path: String,
    pub observed: u64,
    pub limit: u64,
    pub max_value_bytes: Option<u64>,
}

impl std::fmt::Display for ReadCapExceeded {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "GET {} response body exceeded {} bytes (observed at least {} bytes); store advertises single messages up to {}; this response was {} bytes against client cap {}",
            self.path,
            self.limit,
            self.observed,
            self.max_value_bytes.map_or_else(|| "unknown".to_string(), |v| v.to_string()),
            self.observed,
            self.limit
        )
    }
}

impl std::error::Error for ReadCapExceeded {}

/// A single Durable Streams read the store accepted and never finished.
///
/// The pinned HTTP store carries reqwest's own connect/read/request deadlines, but those bind that
/// one provider adapter, not the port: any [`DurableStreamStore`] whose `read` future never resolves
/// would otherwise park the caller forever. That caller is the coalesced reactivation worker, which
/// holds a reactivation permit for the whole scan — so an unbounded read leaves the shape
/// `Reactivating`, pins the permit, and queues every later reactivation behind it. This is the
/// engine's own bound on one replay page, and it is typed so callers can tell a dead transport from
/// a missing stream or a malformed page.
#[derive(Debug)]
pub(crate) struct DsReadTimeout {
    pub path: String,
    pub after: std::time::Duration,
}

impl std::fmt::Display for DsReadTimeout {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "GET {} did not complete within {}s (ELECTRIC_CIRCUITS_DS_READ_TIMEOUT_SECS); the store accepted the read and never finished it",
            self.path,
            self.after.as_secs()
        )
    }
}

impl std::error::Error for DsReadTimeout {}

/// Transport deadlines for the pinned HTTP store. A long-poll gets its own read deadline because
/// it is not a bounded request: the store holds it open for its whole long-poll window and answers
/// empty, so a deadline shorter than that window turns every idle read into an error.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct DsTimeouts {
    pub connect: std::time::Duration,
    pub read: std::time::Duration,
    pub live_read: std::time::Duration,
    pub request: std::time::Duration,
}

impl DsTimeouts {
    fn from_env() -> Self {
        let secs = |name: &str, default: u64| {
            std::time::Duration::from_secs(
                std::env::var(name)
                    .ok()
                    .and_then(|v| v.trim().parse::<u64>().ok())
                    .filter(|v| *v > 0)
                    .unwrap_or(default),
            )
        };
        Self {
            connect: secs("ELECTRIC_CIRCUITS_DS_CONNECT_TIMEOUT_SECS", 10),
            read: secs("ELECTRIC_CIRCUITS_DS_READ_TIMEOUT_SECS", DEFAULT_DS_READ_TIMEOUT_SECS),
            live_read: secs("ELECTRIC_CIRCUITS_DS_LIVE_READ_TIMEOUT_SECS", DEFAULT_DS_LIVE_READ_TIMEOUT_SECS),
            request: secs("ELECTRIC_CIRCUITS_DS_REQUEST_TIMEOUT_SECS", 60),
        }
    }

    /// The whole-request deadline for a long-poll. reqwest's request timeout covers the same wait
    /// the read timeout does, so leaving it at the ordinary value would just move the premature
    /// cut-off one layer out; it is never shorter than the long-poll deadline plus room for the
    /// response body.
    fn live_request(&self) -> std::time::Duration {
        self.request.max(self.live_read + std::time::Duration::from_secs(15))
    }
}

/// How long one replay page read may take before it is abandoned. It shares
/// `ELECTRIC_CIRCUITS_DS_READ_TIMEOUT_SECS` with the HTTP store's transport read deadline so an
/// operator has one number to reason about: no single page read outlives it, whichever layer
/// notices first.
pub(crate) const DEFAULT_DS_READ_TIMEOUT_SECS: u64 = 30;

/// How long a LONG-POLL read may take. It must exceed the store's own long-poll window or the
/// client gives up first on every idle log: the deployed durable-streams holds an idle read for
/// 35 s (`long_poll_timeout_ms`), so 30 s produced a read error, a WARN and a fresh connection
/// every 30 s forever, and buried real transport faults in that noise. 45 s leaves 10 s of margin.
pub(crate) const DEFAULT_DS_LIVE_READ_TIMEOUT_SECS: u64 = 45;

fn ds_read_deadline() -> std::time::Duration {
    std::time::Duration::from_secs(
        std::env::var("ELECTRIC_CIRCUITS_DS_READ_TIMEOUT_SECS")
            .ok()
            .and_then(|value| value.trim().parse::<u64>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(DEFAULT_DS_READ_TIMEOUT_SECS),
    )
}

/// The hard ceiling a raised cap may not pass. Above this a single read really is unserviceable and
/// halting is the honest answer; below it, raising and retrying is what keeps an uncapped store's
/// backlog moving.
const DEFAULT_DS_READ_MAX_CEILING_BYTES: u64 = 512 * 1024 * 1024;

fn ds_read_max_ceiling_bytes() -> u64 {
    std::env::var("ELECTRIC_CIRCUITS_DS_READ_MAX_CEILING_BYTES")
        .ok()
        .and_then(|value| value.trim().parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_DS_READ_MAX_CEILING_BYTES)
}

/// Whether the store this process attested advertises a page size. Together with the value bound it
/// advertises beside it ([`ADVERTISED_MAX_VALUE_BYTES`]) it decides what an oversized read MEANS:
/// a store that promised a page AND a value bound the cap already covers has broken its promise,
/// while one that promised no page, or a value bound above the cap, has answered with a large
/// value the protocol entitles it to frame whole.
static STORE_ADVERTISES_PAGE_CAP: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

/// What to do about a read that exceeded the body cap.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CapBreachOutcome {
    /// Retry with this larger cap: below the hard ceiling, with no operator-named cap, against a
    /// store that either advertises no page or advertises a value bound the cap does not cover.
    Raise { limit: u64 },
    /// Stop reading and withdraw readiness: nothing about a retry can make this read fit.
    Latch,
}

/// The policy behind [`raise_read_cap_after_breach`], separated from the process statics so it can
/// be exercised as the decision table it is.
pub(crate) fn cap_after_breach(
    advertises_page: bool,
    advertised_max_value: Option<u64>,
    configured: Option<u64>,
    current: u64,
    ceiling: u64,
) -> CapBreachOutcome {
    // An operator who named a cap has decided what this process may buffer. Raising it silently
    // would be the engine overruling that decision on its own.
    if configured.is_some() {
        return CapBreachOutcome::Latch;
    }
    // A page is a target, not a bound: a JSON page cuts only on a value boundary, so a store that
    // advertises a page is still entitled to answer with one value larger than it, up to the value
    // bound it advertises beside the page. Only a store that promised a value bound the cap already
    // covers, and then exceeded it, is broken in a way a bigger buffer does not fix — hiding that
    // behind a raised cap would lose the only signal that says so. A store that names no value
    // bound, or an unbounded one (`0`), has made no such promise.
    if advertises_page && advertised_max_value.is_some_and(|bound| bound > 0 && bound <= current) {
        return CapBreachOutcome::Latch;
    }
    let raised = current.saturating_mul(2).min(ceiling);
    if raised <= current { CapBreachOutcome::Latch } else { CapBreachOutcome::Raise { limit: raised } }
}

/// Decide what an oversized live read means for THIS process, and install the raised cap when the
/// answer is to retry.
pub(crate) fn raise_read_cap_after_breach() -> CapBreachOutcome {
    let outcome = cap_after_breach(
        STORE_ADVERTISES_PAGE_CAP.load(std::sync::atomic::Ordering::Relaxed),
        advertised_max_value_bytes(),
        configured_ds_read_max_bytes(),
        ds_read_max_bytes(),
        ds_read_max_ceiling_bytes(),
    );
    if let CapBreachOutcome::Raise { limit } = outcome {
        EFFECTIVE_READ_MAX_BYTES.store(limit, std::sync::atomic::Ordering::Relaxed);
    }
    outcome
}

/// The cap in force for this process, chosen at boot from the store's readiness. Zero means boot has
/// not resolved it yet, in which case the configured or default value applies.
static EFFECTIVE_READ_MAX_BYTES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static ADVERTISED_MAX_VALUE_BYTES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

fn configured_ds_read_max_bytes() -> Option<u64> {
    std::env::var("ELECTRIC_CIRCUITS_DS_READ_MAX_BYTES")
        .ok()
        .and_then(|value| value.trim().parse::<u64>().ok())
        .filter(|value| *value > 0)
}

pub(crate) fn ds_read_max_bytes() -> u64 {
    match EFFECTIVE_READ_MAX_BYTES.load(std::sync::atomic::Ordering::Relaxed) {
        0 => configured_ds_read_max_bytes().unwrap_or_else(|| DEFAULT_DS_READ_MAX_BYTES.max(ds_read_cap_floor())),
        installed => installed,
    }
}

/// The largest single message the attested store said it accepts (`max_value_bytes`), or `None`
/// when it advertised none or an unbounded `0`. The store is entitled to frame a value that large
/// whole on read, whatever page it advertises beside it.
fn advertised_max_value_bytes() -> Option<u64> {
    match ADVERTISED_MAX_VALUE_BYTES.load(std::sync::atomic::Ordering::Relaxed) {
        0 => None,
        value => Some(value),
    }
}

#[cfg(test)]
pub(crate) fn store_advertises_page_cap() -> bool {
    STORE_ADVERTISES_PAGE_CAP.load(std::sync::atomic::Ordering::Relaxed)
}

/// An explicit `ELECTRIC_CIRCUITS_DS_READ_MAX_BYTES` is always obeyed: an operator who names a cap
/// has decided what this process may buffer, including against an uncapped store. A derived cap is
/// never below `floor`, the engine's own append budget: the store frames a value the engine wrote
/// whole, so the engine must be able to read back anything it is allowed to write.
pub(crate) fn effective_read_max_bytes(verdict: PageCapVerdict, configured: Option<u64>, floor: u64) -> u64 {
    match (configured, verdict) {
        (Some(explicit), _) => explicit,
        (None, PageCapVerdict::Compatible { .. }) => DEFAULT_DS_READ_MAX_BYTES.max(floor),
        (None, PageCapVerdict::Unknown | PageCapVerdict::Exceeds { .. }) => UNCAPPED_STORE_READ_MAX_BYTES.max(floor),
    }
}

async fn read_body_bounded(mut response: reqwest::Response, limit: u64, path: &str) -> Result<String> {
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(anyhow::Error::new)? {
        let size = body.len() as u64 + chunk.len() as u64;
        if size > limit {
            tracing::error!(path, size, limit, "durable-streams response exceeded client body limit");
            let max_value_bytes = match ADVERTISED_MAX_VALUE_BYTES.load(std::sync::atomic::Ordering::Relaxed) {
                0 => None,
                value => Some(value),
            };
            return Err(anyhow::Error::new(ReadCapExceeded {
                path: path.to_string(),
                observed: size,
                limit,
                max_value_bytes,
            }));
        }
        body.extend_from_slice(&chunk);
    }
    String::from_utf8(body).context("durable-streams response body was not UTF-8")
}

type StoreFuture<'a> = Pin<Box<dyn Future<Output = Result<StoreResponse>> + Send + 'a>>;

/// The engine-owned, provider-neutral single-attempt Durable Streams port.
///
/// The contract intentionally accepts and returns opaque offset strings.  It owns no retry,
/// reconciliation, envelope codec, retirement, or byte-accounting policy: those are Circuits
/// invariants and remain on [`DsClient`].  It is private because no external crate is entitled to
/// rely on this first compatibility-shaped outcome representation.
pub(crate) trait DurableStreamStore: Send + Sync {
    fn ready<'a>(&'a self) -> StoreFuture<'a>;
    fn ensure<'a>(&'a self, path: &'a str, content_type: &'a str) -> StoreFuture<'a>;
    fn append<'a>(
        &'a self,
        path: &'a str,
        content_type: &'a str,
        body: Vec<u8>,
        response_body: BodyRead,
    ) -> StoreFuture<'a>;
    fn read<'a>(&'a self, path: &'a str, offset: &'a str, live: bool) -> StoreFuture<'a>;
    fn head<'a>(&'a self, path: &'a str) -> StoreFuture<'a>;
    fn close<'a>(&'a self, path: &'a str) -> StoreFuture<'a>;
    fn delete<'a>(&'a self, path: &'a str) -> StoreFuture<'a>;
}

/// The currently pinned pgxsinkit/durable-streams-rust wire adapter.  It performs exactly one
/// HTTP request per port call; `DsClient` owns the interpretation and retry policy above it.
struct HttpDurableStreamsStore {
    base: String,
    http: reqwest::Client,
    /// The long-poll client. Identical to `http` except for its deadlines: reqwest's read timeout
    /// is a client-wide setting (there is no per-request form in 0.12), and a long-poll needs one
    /// longer than the store's own long-poll window while every other operation stays bounded by
    /// the short one.
    live_http: reqwest::Client,
}

impl HttpDurableStreamsStore {
    fn new(config: &DsConnectionConfig) -> Result<Self> {
        let tls = url::Url::parse(&config.base_url)?.scheme() == "https";
        // HTTP never reads TLS files, even when a caller supplies stale paths directly.
        let ca = if tls {
            config
                .ca_bundle_path
                .as_ref()
                .map(|path| -> Result<Vec<reqwest::Certificate>> {
                    let pem = fs::read(path)
                        .with_context(|| format!("reading Durable Streams CA bundle {}", path.display()))?;
                    let certificates =
                        reqwest::Certificate::from_pem_bundle(&pem).context("parsing Durable Streams CA bundle")?;
                    if certificates.is_empty() {
                        bail!("Durable Streams CA bundle contains no certificates");
                    }
                    Ok(certificates)
                })
                .transpose()?
        } else {
            None
        };
        let identity = if tls {
            match (&config.client_certificate_path, &config.client_key_path) {
                (Some(certificate_path), Some(key_path)) => {
                    let mut pem = fs::read(certificate_path).with_context(|| {
                        format!("reading Durable Streams client certificate {}", certificate_path.display())
                    })?;
                    let key = fs::read(key_path)
                        .with_context(|| format!("reading Durable Streams client key {}", key_path.display()))?;
                    if !pem.ends_with(b"\n") {
                        pem.push(b'\n');
                    }
                    pem.extend(key);
                    Some(reqwest::Identity::from_pem(&pem).context("parsing Durable Streams client certificate/key")?)
                }
                (None, None) => None,
                _ => bail!("ELECTRIC_CIRCUITS_DS_CLIENT_CERT and ELECTRIC_CIRCUITS_DS_CLIENT_KEY must be set together"),
            }
        } else {
            None
        };
        let timeouts = DsTimeouts::from_env();
        let client = |read: std::time::Duration, request: std::time::Duration| {
            let mut builder = reqwest::Client::builder()
                .https_only(tls)
                // Disable feature-unified bundled roots; only the selected trust source is used.
                .tls_built_in_root_certs(false)
                .tls_built_in_native_certs(tls && ca.is_none())
                .connect_timeout(timeouts.connect)
                .read_timeout(read)
                .timeout(request);
            if !tls {
                builder = builder.http1_only();
            }
            if let Some(certificates) = &ca {
                for certificate in certificates {
                    builder = builder.add_root_certificate(certificate.clone());
                }
            }
            if let Some(identity) = &identity {
                builder = builder.identity(identity.clone());
            }
            builder.build()
        };
        let http = client(timeouts.read, timeouts.request).context("building Durable Streams HTTP client")?;
        let live_http =
            client(timeouts.live_read, timeouts.live_request()).context("building Durable Streams long-poll client")?;
        Ok(Self { base: config.base_url.clone(), http, live_http })
    }

    #[cfg(any(test, feature = "test-support"))]
    fn new_in_process(base: String) -> Self {
        Self { base, http: reqwest::Client::new(), live_http: reqwest::Client::new() }
    }

    /// Test seam: the same client production builds, from explicit deadlines instead of the
    /// environment and without mTLS material.
    #[cfg(test)]
    fn new_in_process_with_timeouts(base: String, timeouts: DsTimeouts) -> Self {
        let client = |read: std::time::Duration, request: std::time::Duration| {
            reqwest::Client::builder()
                .connect_timeout(timeouts.connect)
                .read_timeout(read)
                .timeout(request)
                .build()
                .expect("in-process client")
        };
        Self {
            base,
            http: client(timeouts.read, timeouts.request),
            live_http: client(timeouts.live_read, timeouts.live_request()),
        }
    }

    fn stream_url(&self, path: &str) -> String {
        format!("{}/{}", self.base.trim_end_matches('/'), path.trim_start_matches('/'))
    }

    async fn response(&self, res: reqwest::Response, body_read: BodyRead, path: Option<&str>) -> StoreResponse {
        let status = res.status().as_u16();
        let next_offset = header(&res, "stream-next-offset");
        let up_to_date = res.headers().get("stream-up-to-date").is_some();
        let closed = header(&res, "stream-closed").is_some_and(|v| v.eq_ignore_ascii_case("true"));
        let should_read = match body_read {
            BodyRead::Never => false,
            BodyRead::OnFailure => !(200..300).contains(&status),
            // Existing read paths return before acquiring a 204 body.
            BodyRead::OnData => (200..300).contains(&status) && status != 204,
            BodyRead::Always => true,
        };
        // Keep acquisition fallible for the facade to interpret per operation.  In particular,
        // `read` and `read_json` used `res.text().await?` after a successful GET; turning an
        // interrupted body into `""` would manufacture an empty page at a real next offset.
        // The selected mode otherwise preserves the legacy per-operation best-effort behavior.
        let body = if should_read {
            Some(read_body_bounded(res, ds_read_max_bytes(), path.unwrap_or("<unknown>")).await)
        } else {
            None
        };
        StoreResponse { status, body, next_offset, up_to_date, closed }
    }
}

impl DurableStreamStore for HttpDurableStreamsStore {
    fn ready<'a>(&'a self) -> StoreFuture<'a> {
        Box::pin(async move {
            let res = self.http.get(format!("{}/_admin/ready", self.base)).send().await.context("GET /_admin/ready")?;
            Ok(self.response(res, BodyRead::Always, None).await)
        })
    }

    fn ensure<'a>(&'a self, path: &'a str, content_type: &'a str) -> StoreFuture<'a> {
        Box::pin(async move {
            let res = self
                .http
                .put(self.stream_url(path))
                .header(reqwest::header::CONTENT_TYPE, content_type)
                .send()
                .await
                .with_context(|| format!("PUT {path}"))?;
            Ok(self.response(res, BodyRead::Always, Some(path)).await)
        })
    }

    fn append<'a>(
        &'a self,
        path: &'a str,
        content_type: &'a str,
        body: Vec<u8>,
        response_body: BodyRead,
    ) -> StoreFuture<'a> {
        Box::pin(async move {
            let res = self
                .http
                .post(self.stream_url(path))
                .header(reqwest::header::CONTENT_TYPE, content_type)
                .body(body)
                .send()
                .await
                .with_context(|| format!("POST {path}"))?;
            Ok(self.response(res, response_body, Some(path)).await)
        })
    }

    fn read<'a>(&'a self, path: &'a str, offset: &'a str, live: bool) -> StoreFuture<'a> {
        Box::pin(async move {
            let mut url = format!("{}?offset={}", self.stream_url(path), offset);
            if live {
                url.push_str("&live=long-poll");
            }
            let client = if live { &self.live_http } else { &self.http };
            let res = client.get(url).send().await.with_context(|| format!("GET {path}"))?;
            Ok(self.response(res, BodyRead::OnData, Some(path)).await)
        })
    }

    fn head<'a>(&'a self, path: &'a str) -> StoreFuture<'a> {
        Box::pin(async move {
            let res = self.http.head(self.stream_url(path)).send().await.with_context(|| format!("HEAD {path}"))?;
            Ok(self.response(res, BodyRead::Never, Some(path)).await)
        })
    }

    fn close<'a>(&'a self, path: &'a str) -> StoreFuture<'a> {
        Box::pin(async move {
            let res = self
                .http
                .post(self.stream_url(path))
                .header("stream-closed", "true")
                .send()
                .await
                .with_context(|| format!("POST {path} (close)"))?;
            Ok(self.response(res, BodyRead::Always, Some(path)).await)
        })
    }

    fn delete<'a>(&'a self, path: &'a str) -> StoreFuture<'a> {
        Box::pin(async move {
            let res = self.http.delete(self.stream_url(path)).send().await.with_context(|| format!("DELETE {path}"))?;
            Ok(self.response(res, BodyRead::Always, Some(path)).await)
        })
    }
}

#[derive(Clone)]
pub struct DsClient {
    base: String,
    scope: StreamScope,
    store: Arc<dyn DurableStreamStore>,
    /// Shared across clones (installed after the engine exists, seen by every copy of the client
    /// from then on). See [`Self::set_gone_reconciler`].
    reconcile: std::sync::Arc<std::sync::OnceLock<GoneReconciler>>,
    /// Bytes appended per stream path since this process started (serialized request bodies).
    /// The durable-streams server exposes no per-stream sizes, so this engine-side accounting is
    /// what the retention disk-budget layer works from. It undercounts streams that already
    /// existed before the process started (restart persistence is the catalog work, GH #8).
    appended: std::sync::Arc<std::sync::Mutex<HashMap<String, u64>>>,
}

impl DsClient {
    /// Construct the scoped production client over HTTP or verified HTTPS, optionally with mTLS.
    /// Store readiness and identity verification are required for every transport.
    pub async fn connect(config: DsConnectionConfig) -> Result<Self> {
        let expected = config.scope.store.clone();
        let store = Arc::new(HttpDurableStreamsStore::new(&config)?);
        let client = Self::with_store(config.base_url, config.scope, store);
        // A production client is not constructible until the store has attested the exact identity.
        client.refresh_readiness(&expected).await?;
        Ok(client)
    }

    /// Construct the semantic facade over a supplied single-attempt store.  This is restricted to
    /// the engine crate so application callers cannot acquire a dependency on a provider or HTTP
    /// status behavior; deterministic stores belong in `ds.rs` unit tests.
    fn with_store(base: String, scope: StreamScope, store: Arc<dyn DurableStreamStore>) -> Self {
        DsClient {
            base,
            scope,
            store,
            reconcile: std::sync::Arc::new(std::sync::OnceLock::new()),
            appended: std::sync::Arc::new(std::sync::Mutex::new(HashMap::new())),
        }
    }

    /// An HTTP test double requires an explicit in-process test scope. This constructor is absent
    /// from non-test builds so deployments cannot bypass store identity checks.
    #[cfg(any(test, feature = "test-support"))]
    #[doc(hidden)]
    pub fn new_for_in_process_test(base: impl Into<String>) -> Self {
        Self::new_for_in_process_test_with_scope(base, StreamScope::in_process_test_scope())
    }

    #[cfg(any(test, feature = "test-support"))]
    #[doc(hidden)]
    pub fn new_for_in_process_test_with_scope(base: impl Into<String>, scope: StreamScope) -> Self {
        let base = base.into();
        Self::with_store(base.clone(), scope, Arc::new(HttpDurableStreamsStore::new_in_process(base)))
    }

    #[cfg(test)]
    pub(crate) fn with_test_store(base: String, store: Arc<dyn DurableStreamStore>) -> Self {
        Self::with_store(base, StreamScope::in_process_test_scope(), store)
    }

    /// Install the reconciler [`Self::append_reliable`] consults before believing a terminal
    /// append answer (see [`GoneVerdict`]). Shared by every clone of this client, including the ones
    /// already handed to the sequencer, the emission lanes and the subquery registry — which is why
    /// it can be installed after construction. Idempotent: a second install is ignored.
    pub fn set_gone_reconciler(&self, reconciler: GoneReconciler) {
        let _ = self.reconcile.set(reconciler);
    }

    /// Tracked bytes appended to `path` since process start (0 if never appended).
    pub fn appended_bytes(&self, path: &str) -> u64 {
        self.appended.lock().unwrap().get(path).copied().unwrap_or(0)
    }

    /// Snapshot of tracked appended bytes for every stream path with the given prefix
    /// (e.g. `"shape/"` for the retention disk budget).
    pub fn appended_bytes_with_prefix(&self, prefix: &str) -> HashMap<String, u64> {
        self.appended
            .lock()
            .unwrap()
            .iter()
            .filter(|(p, _)| p.starts_with(prefix))
            .map(|(p, b)| (p.clone(), *b))
            .collect()
    }

    pub fn base(&self) -> &str {
        &self.base
    }

    /// Immutable query-generation namespace of the scoped store this client reads.  A public
    /// positioned cursor includes this value so a reader can refuse to replay a cursor issued by
    /// an older query generation instead of mistaking its path for a newly missing segment.
    pub fn query_generation(&self) -> &str {
        &self.scope.query_generation
    }

    pub fn stream_url(&self, path: &str) -> String {
        match self.scope.qualify(path) {
            Ok(path) => format!("{}/{path}", self.base.trim_end_matches('/')),
            Err(_) => "<invalid-logical-path>".to_string(),
        }
    }

    /// The first and only allowed preflight network operation. Any failure leaves ordinary stream
    /// operations untouched and is therefore safe to run before engine or Postgres construction.
    pub async fn preflight_readiness(&self, expected: &StoreIdentityV1) -> Result<StoreReadinessV1> {
        let response = self.store.ready().await?;
        if response.status != 200 {
            return Err(anyhow::Error::new(StoreReadinessError::Response { status: response.status }));
        }
        let body = response.required_body()?;
        let observed = decode_readiness(&body)?;
        if &observed.identity != expected {
            return Err(anyhow::Error::new(StoreReadinessError::IdentityMismatch {
                expected: expected.clone(),
                observed: observed.identity,
            }));
        }
        Ok(observed)
    }

    /// Re-attest the store after a transport reconnect. Readiness is deliberately not cached:
    /// page capabilities are startup state that can change independently of store identity.
    pub(crate) async fn refresh_readiness(&self, expected: &StoreIdentityV1) -> Result<PageCapVerdict> {
        let readiness = self.preflight_readiness(expected).await?;
        let configured = configured_ds_read_max_bytes();
        let floor = ds_read_cap_floor();
        let verdict = assess_page_cap(&readiness, configured.unwrap_or_else(|| DEFAULT_DS_READ_MAX_BYTES.max(floor)));
        let cap = effective_read_max_bytes(verdict, configured, floor);
        EFFECTIVE_READ_MAX_BYTES.store(cap, std::sync::atomic::Ordering::Relaxed);
        ADVERTISED_MAX_VALUE_BYTES.store(readiness.max_value_bytes.unwrap_or(0), std::sync::atomic::Ordering::Relaxed);
        STORE_ADVERTISES_PAGE_CAP
            .store(matches!(verdict, PageCapVerdict::Compatible { .. }), std::sync::atomic::Ordering::Relaxed);
        // The cap in force is the number an operator needs when a read fails on size, so it is
        // stated at every attestation rather than left to be inferred from the defaults.
        tracing::info!(
            cap,
            floor,
            explicit = configured.is_some(),
            ?verdict,
            max_value_bytes = readiness.max_value_bytes,
            "durable-streams read cap in force"
        );
        if let Some(advisory) = enforce_page_cap(verdict, ds_read_max_bytes(), require_ds_chunk_cap())? {
            warn_page_cap_once(&advisory);
        }
        Ok(verdict)
    }

    /// Re-attest the store this client is bound to, after a transport failure and the reconnect
    /// that follows it. The expected identity is the one this client was scoped with, so a caller
    /// on the reconnect path does not have to carry it: a store that came back as a *different*
    /// store is refused here exactly as it would be at boot.
    pub(crate) async fn reattest_readiness(&self) -> Result<PageCapVerdict> {
        let expected = self.scope.store.clone();
        self.refresh_readiness(&expected).await
    }

    fn physical_path(&self, logical_path: &str) -> Result<String> {
        self.scope.qualify(logical_path)
    }

    /// Idempotently create a JSON stream (PUT). Existing stream with same config -> 200.
    pub async fn ensure_stream(&self, path: &str) -> Result<()> {
        let physical_path = self.physical_path(path)?;
        let res = self.store.ensure(&physical_path, "application/json").await?;
        if (200..300).contains(&res.status) {
            Ok(())
        } else {
            let status = res.status;
            Err(status_error("PUT", path, status, &res.body_or_default()))
        }
    }

    /// Append envelopes as a JSON array (the server flattens one array level into N messages).
    /// Append raw JSON events (non-envelope streams, e.g. the shape catalog).
    pub async fn append_json(&self, path: &str, events: &[serde_json::Value]) -> Result<()> {
        if events.is_empty() {
            return Ok(());
        }
        let body = serde_json::to_vec(events).with_context(|| format!("serializing POST {path}"))?;
        let physical_path = self.physical_path(path)?;
        let res = self.store.append(&physical_path, "application/json", body, BodyRead::OnFailure).await?;
        if !(200..300).contains(&res.status) {
            let status = res.status;
            return Err(status_error("POST", path, status, &res.body_or_default()));
        }
        Ok(())
    }

    /// Read raw JSON events (non-envelope streams). Returns `(events, next_offset, up_to_date)`.
    pub async fn read_json(&self, path: &str, offset: &str) -> Result<(Vec<serde_json::Value>, Option<String>, bool)> {
        let physical_path = self.physical_path(path)?;
        let res = self.store.read(&physical_path, offset, false).await?;
        if res.status == 204 || res.status == 404 {
            return Ok((Vec::new(), res.next_offset, true));
        }
        if !(200..300).contains(&res.status) {
            return Err(status_error("GET", path, res.status, ""));
        }
        let next_offset = res.next_offset.clone();
        let up_to_date = res.up_to_date;
        let body = res.required_body()?;
        let events: Vec<serde_json::Value> = if body.trim().is_empty() {
            Vec::new()
        } else {
            serde_json::from_str(&body).with_context(|| format!("parsing stream body: {body}"))?
        };
        Ok((events, next_offset, up_to_date))
    }

    /// Append envelopes. A retired stream (404/410/closed) is an error here; the live path uses
    /// [`Self::append_reliable`], which discards instead, and the change log uses
    /// [`Self::append_checked`], which routes around it.
    pub async fn append(&self, path: &str, envelopes: &[Envelope]) -> Result<()> {
        match self.append_once(path, envelopes).await {
            Ok(_) => Ok(()),
            Err(AppendError::Gone(status)) => bail!("POST {path} -> {status} (stream retired)"),
            Err(AppendError::Other(e)) => Err(e),
        }
    }

    /// How long [`Self::append_retrying`] keeps trying a transient failure before giving up. Long
    /// enough to ride out a storage restart or a failover, short enough that a boot does not hang
    /// on a dependency that is not coming back.
    pub const RESTORE_APPEND_BUDGET: std::time::Duration = std::time::Duration::from_secs(30);

    /// Append on a path where **failing costs the shape**: activation, the catalog restore's
    /// re-seed, a dormant shape's replay. Transient storage failures (`ds::is_unavailable`:
    /// transport, timeout, 5xx) are retried with capped backoff until `budget` runs out or the
    /// shutdown token fires.
    ///
    /// The plain [`Self::append`] propagates the first error, and these callers turn an error into
    /// a **retirement** — so one 503 during a boot used to permanently delete an acknowledged
    /// subscription and its stream. Permanent removal is for records that are genuinely
    /// unrecoverable: a definite refusal (a 4xx, an unserialisable event), a stream storage confirms
    /// is gone (`HEAD` → 404/410/closed), or an exhausted budget. A service that is merely
    /// unavailable is backpressure, not loss.
    ///
    /// A **terminal** answer (404/410/`stream-closed`) gets the same reconciliation
    /// [`Self::append_reliable`] gives it, and for the same reason: it is what a proxy, a storage
    /// router or a failover says just as readily as a real deletion, and believing one here retires
    /// an acknowledged shape. `HEAD` decides — the stream is there and open ⇒ the status was false,
    /// keep retrying within the budget; storage agrees it is gone (or a `HEAD` that itself fails
    /// cannot say) ⇒ only the first of those is terminal.
    pub async fn append_retrying(
        &self,
        path: &str,
        envelopes: &[Envelope],
        budget: std::time::Duration,
        shutdown: &crate::shutdown::ShutdownToken,
    ) -> Result<()> {
        let deadline = std::time::Instant::now() + budget;
        let mut attempt = 0u32;
        loop {
            let e = match self.append_checked(path, envelopes).await {
                Ok(Appended::Ok { .. }) => return Ok(()),
                Ok(Appended::Retired(status)) => match self.head(path).await {
                    // There and appendable: the terminal status did not come from storage.
                    Ok(Some(head)) if !head.closed => anyhow::anyhow!(
                        "POST {path} -> {status}, contradicted by HEAD (the stream is there): treating it as transient"
                    ),
                    // Storage agrees: this one really is terminal, and the caller retires the record.
                    Ok(_) => bail!("POST {path} -> {status} (stream retired)"),
                    // Cannot tell. Retrying costs a stale append at worst; retiring on a guess costs
                    // an acknowledged subscription.
                    Err(he) => anyhow::anyhow!("POST {path} -> {status}; HEAD could not confirm it ({he:#})"),
                },
                Err(e) => {
                    if !is_unavailable(&e) {
                        return Err(e);
                    }
                    // Storage answering "no such stream" to a HEAD is the one transient-looking case
                    // that is really terminal: stop waiting and let the caller retire the record.
                    if let Ok(None) = self.head(path).await {
                        return Err(e.context(format!("stream '{path}' is gone")));
                    }
                    e
                }
            };
            attempt += 1;
            if std::time::Instant::now() >= deadline {
                return Err(e.context(format!("appending to {path} kept failing for {budget:?} ({attempt} attempts)")));
            }
            let backoff = std::time::Duration::from_millis(100u64.saturating_mul(1 << attempt.min(5)).min(2000));
            tracing::warn!("append to {path} failed (attempt {attempt}), retrying in {backoff:?}: {e:#}");
            tokio::select! {
                _ = tokio::time::sleep(backoff) => {}
                _ = shutdown.wait() => {
                    return Err(e.context(format!("appending to {path} abandoned: shutting down")));
                }
            }
        }
    }

    /// Append, reporting a retired stream as an outcome instead of an error, and handing back the
    /// stream's tail offset on success.
    ///
    /// The change log's writer needs both (ADR-0006): the tail offset IS the segment's size (the
    /// rotation decision), and a closed segment is a routing signal — walk forward to the successor
    /// — not a loss. Transient failures still surface as `Err` so the ingestor can tear its
    /// connection down unacknowledged rather than lose the commit.
    pub async fn append_checked(&self, path: &str, envelopes: &[Envelope]) -> Result<Appended> {
        match self.append_once(path, envelopes).await {
            Ok(next_offset) => Ok(Appended::Ok { next_offset }),
            Err(AppendError::Gone(status)) => Ok(Appended::Retired(status)),
            Err(AppendError::Other(e)) => Err(e),
        }
    }

    async fn append_once(
        &self,
        path: &str,
        envelopes: &[Envelope],
    ) -> std::result::Result<Option<String>, AppendError> {
        if envelopes.is_empty() {
            return Ok(None);
        }
        // Serialize once ourselves (instead of `.json(...)`) so the successful append's byte size
        // can be recorded for the retention disk-budget accounting.
        let payload = serde_json::to_vec(envelopes)
            .map_err(|e| AppendError::Other(anyhow::Error::new(e).context(format!("serializing POST {path}"))))?;
        let payload_len = payload.len() as u64;
        let physical_path = self.physical_path(path).map_err(AppendError::Other)?;
        let res = self
            .store
            .append(&physical_path, "application/json", payload, BodyRead::Always)
            .await
            .map_err(AppendError::Other)?;
        // A retired stream answers 404 (deleted), 410 (soft-deleted) or 409 + `stream-closed: true`
        // (closed, which retirement does before deleting).  The provider parses the header before
        // draining its response; the body text ("stream is closed") is not the contract.
        if (200..300).contains(&res.status) {
            *self.appended.lock().unwrap().entry(path.to_string()).or_insert(0) += payload_len;
            Ok(res.next_offset)
        } else if res.status == 404 || res.status == 410 || (res.status == 409 && res.closed) {
            Err(AppendError::Gone(res.status))
        } else {
            let status = res.status;
            Err(AppendError::Other(status_error("POST", path, status, &res.body_or_default())))
        }
    }

    /// Append with **no silent loss**: retry transient failures with capped backoff until the append
    /// lands. A dropped shape-stream append is a permanent divergence for every subscriber of that
    /// shape, so the only sound behaviors are (a) retry until success — the storage server being down
    /// simply backpressures the tailer, matching the ingestor's read-then-commit stance — or (b) stop
    /// because the stream was retired (the shape was dropped/evicted mid-flush), which is a clean
    /// no-op. Envelopes are absolute per-pk (`upsert`/`delete` by key), so an at-least-once retry
    /// that double-appends after an ambiguous network failure is idempotent for readers.
    /// Returns `false` iff the stream is retired (404, 410, or closed).
    ///
    /// Treating a **closed** stream as terminal is sound only for shape streams: their envelopes are
    /// absolute per-pk and the stream is about to be deleted, so the discarded batch has no reader
    /// left to diverge. The change log must keep using [`Self::append`] (which propagates): a closed
    /// `changes/*` segment is a routing signal, and silently dropping ingest there would lose data.
    ///
    /// **A terminal answer is reconciled, never taken on trust.** "404" is what a proxy, a storage
    /// router or a failover says just as readily as a real deletion, and this method's `false` makes
    /// the caller advance past the batch — leaving a still-registered shape permanently missing a
    /// committed Postgres change, with nothing anywhere that remembers it. So when a reconciler is
    /// installed ([`Self::set_gone_reconciler`]) the engine gets to answer: [`GoneVerdict::Retry`]
    /// (the shape is registered and its stream is right there — the 404 was false) keeps retrying,
    /// and [`GoneVerdict::Discard`] means the engine has confirmed the stream is gone and has retired
    /// the shape, so the batch has no reader left. Either way the shape's batch is never silently
    /// abandoned while the shape stays registered and stale.
    pub async fn append_reliable(&self, path: &str, envelopes: &[Envelope]) -> bool {
        let mut attempt = 0u32;
        let mut false_gone = 0u32;
        loop {
            match self.append_once(path, envelopes).await {
                Ok(_) => return true,
                Err(AppendError::Gone(status)) => {
                    let verdict = match self.reconcile.get() {
                        Some(reconcile) => reconcile(path.to_string()).await,
                        None => GoneVerdict::Discard,
                    };
                    if verdict == GoneVerdict::Discard {
                        tracing::debug!(
                            "append to {path}: stream retired ({status}); discarding {} envelopes",
                            envelopes.len()
                        );
                        return false;
                    }
                    false_gone += 1;
                    if false_gone == 1 {
                        tracing::warn!(
                            "append to {path} answered {status}, but the shape is still registered and its \
                             stream is still there: treating the terminal status as transient and retrying"
                        );
                    }
                    attempt += 1;
                    let backoff =
                        std::time::Duration::from_millis(100u64.saturating_mul(1 << attempt.min(5)).min(2000));
                    tokio::time::sleep(backoff).await;
                }
                Err(AppendError::Other(e)) => {
                    attempt += 1;
                    let backoff =
                        std::time::Duration::from_millis(100u64.saturating_mul(1 << attempt.min(5)).min(2000));
                    if attempt.is_multiple_of(10) {
                        tracing::error!("append to {path} still failing after {attempt} attempts: {e:#}");
                    } else {
                        tracing::warn!("append to {path} failed (attempt {attempt}), retrying in {backoff:?}: {e:#}");
                    }
                    tokio::time::sleep(backoff).await;
                }
            }
        }
    }

    /// Close a stream: `POST` with `Stream-Closed: true` and an empty body. Closing is terminal —
    /// appends are refused with `409` + `stream-closed` afterwards — and it releases every waiting
    /// long-poll reader immediately with `stream-closed: true` instead of leaving it blocked until
    /// the read times out. Idempotent (`204` again for an already-closed stream); an absent (`404`)
    /// or soft-deleted (`410`) stream is a success, there is nothing left to close.
    pub async fn close_stream(&self, path: &str) -> Result<()> {
        let physical_path = self.physical_path(path)?;
        let res = self.store.close(&physical_path).await?;
        if (200..300).contains(&res.status) || res.status == 404 || res.status == 410 {
            Ok(())
        } else {
            let status = res.status;
            bail!("POST {path} (close) -> {status}: {}", res.body_or_default())
        }
    }

    /// Retire a stream: close it, THEN delete it (see `docs/adr/0007-retirement-closes-before-delete.md`).
    /// Every engine-initiated removal of a shape stream goes through here — eviction, purge,
    /// drop-at-restore, the degraded subquery reap, and the future schema-drift / epoch-reset paths —
    /// so a tailing client is released at once with `stream-closed` and "closed" unambiguously means
    /// "the engine retired this shape; re-subscribe". Closing is terminal, so the paths that are NOT
    /// retirement must not use it: deactivation parks a dormant shape whose stream stays appendable
    /// for reactivation, and creation rollback removes a stream no subscriber ever saw.
    /// A failed close is logged and does not block the delete: removing the stream is the must-have,
    /// the close is the courtesy signal. Returns the delete's result.
    pub async fn retire_stream(&self, path: &str) -> Result<()> {
        if let Err(e) = self.close_stream(path).await {
            tracing::warn!("retiring stream {path}: close failed ({e:#}); deleting anyway");
        }
        self.delete_stream(path).await
    }

    /// Delete a stream (DELETE). An already-gone stream — absent (404) or soft-deleted (410) — is a
    /// success: deletion is idempotent, and a retry loop (the degraded reap) must not spin forever
    /// on a stream storage has already retired.
    pub async fn delete_stream(&self, path: &str) -> Result<()> {
        let physical_path = self.physical_path(path)?;
        let res = self.store.delete(&physical_path).await?;
        if (200..300).contains(&res.status) || res.status == 404 || res.status == 410 {
            self.appended.lock().unwrap().remove(path);
            Ok(())
        } else {
            let status = res.status;
            bail!("DELETE {path} -> {status}: {}", res.body_or_default())
        }
    }

    /// `HEAD` a stream: its tail offset and whether it is closed, without reading a byte of it (and
    /// without resetting its TTL). `Ok(None)` = not there (404) or soft-deleted (410).
    ///
    /// The change log's boot walk uses this to step over segments a crashed predecessor closed
    /// (ADR-0006): a closed segment can be a gigabyte, and durable-streams offers no bounded tail
    /// read, so the successor is derived and *verified* rather than read back.
    pub async fn head(&self, path: &str) -> Result<Option<StreamHead>> {
        let physical_path = self.physical_path(path)?;
        let res = self.store.head(&physical_path).await?;
        if res.status == 404 || res.status == 410 {
            return Ok(None);
        }
        if !(200..300).contains(&res.status) {
            return Err(status_error("HEAD", path, res.status, ""));
        }
        Ok(Some(StreamHead { next_offset: res.next_offset, closed: res.closed }))
    }

    /// Read from `offset` (use "-1" for the beginning). `live` enables long-poll tailing.
    pub async fn read(&self, path: &str, offset: &str, live: bool) -> Result<ReadResult> {
        let physical_path = self.physical_path(path)?;
        let res = self.store.read(&physical_path, offset, live).await?;

        // 204 = long-poll timeout / no new data / a close that woke this long-poll.
        if res.status == 204 {
            return Ok(ReadResult {
                envelopes: Vec::new(),
                next_offset: res.next_offset,
                up_to_date: res.up_to_date,
                closed: res.closed,
            });
        }
        // A stream that is not there is a TYPED error (see `StreamGone`), never a generic read
        // failure: on the change log every caller has its own, very different answer to it.
        if res.status == 404 || res.status == 410 {
            return Err(anyhow::Error::new(StreamGone { path: path.to_string(), status: res.status }));
        }
        if !(200..300).contains(&res.status) {
            return Err(status_error("GET", path, res.status, ""));
        }
        let next_offset = res.next_offset.clone();
        let up_to_date = res.up_to_date;
        let closed = res.closed;
        let body = res.required_body()?;
        let envelopes: Vec<Envelope> = if body.trim().is_empty() {
            Vec::new()
        } else {
            serde_json::from_str(&body).with_context(|| format!("parsing stream body: {body}"))?
        };
        Ok(ReadResult { envelopes, next_offset, up_to_date, closed })
    }

    /// Read one page while decoding row bodies only for `table` and change-log control envelopes.
    /// This is the replay path's cheap-page variant: framing and headers are still validated for
    /// every item, but unrelated table rows remain borrowed raw bytes until discarded.
    pub async fn read_for_table(&self, path: &str, offset: &str, live: bool, table: &str) -> Result<ReadResult> {
        let physical_path = self.physical_path(path)?;
        // A replay page is a bounded request, so it gets a deadline of its own; a long-poll is not
        // and keeps the store's own idle semantics.
        let res = if live {
            self.store.read(&physical_path, offset, live).await?
        } else {
            let deadline = ds_read_deadline();
            match tokio::time::timeout(deadline, self.store.read(&physical_path, offset, live)).await {
                Ok(res) => res?,
                Err(_) => {
                    tracing::error!(path, deadline_secs = deadline.as_secs(), "durable-streams read never completed");
                    return Err(anyhow::Error::new(DsReadTimeout { path: path.to_string(), after: deadline }));
                }
            }
        };
        if res.status == 204 {
            return Ok(ReadResult {
                envelopes: Vec::new(),
                next_offset: res.next_offset,
                up_to_date: res.up_to_date,
                closed: res.closed,
            });
        }
        if res.status == 404 || res.status == 410 {
            return Err(anyhow::Error::new(StreamGone { path: path.to_string(), status: res.status }));
        }
        if !(200..300).contains(&res.status) {
            return Err(status_error("GET", path, res.status, ""));
        }
        let next_offset = res.next_offset.clone();
        let up_to_date = res.up_to_date;
        let closed = res.closed;
        let body = res.required_body()?;
        let mut envelopes = Vec::new();
        if !body.trim().is_empty() {
            let raw: Vec<LazyEnvelope> =
                serde_json::from_str(&body).with_context(|| format!("parsing stream body: {body}"))?;
            for item in raw {
                if item.type_ == table || item.type_ == crate::changelog::CONTROL_TYPE {
                    envelopes.push(item.into_envelope()?);
                }
            }
        }
        Ok(ReadResult { envelopes, next_offset, up_to_date, closed })
    }
}

fn header(res: &reqwest::Response, name: &str) -> Option<String> {
    res.headers().get(name).and_then(|v| v.to_str().ok()).map(str::to_string)
}

#[derive(Deserialize)]
struct ReadinessWire {
    contract_version: String,
    status: String,
    artifact_digest: String,
    manifest: ManifestWire,
    recovery: RecoveryWire,
    reserve: ReserveWire,
    #[serde(default)]
    max_chunk_bytes: Option<u64>,
    #[serde(default)]
    max_value_bytes: Option<u64>,
}

#[derive(Deserialize)]
struct ManifestWire {
    store_id: String,
    store_generation: String,
    protocol_version: u32,
    layout_version: u32,
    durability_mode: String,
    wal_shard_count: u32,
    stream_lane_count: u32,
    filesystem_uuid: String,
    creation_time: String,
}

#[derive(Deserialize)]
struct RecoveryWire {
    completed: bool,
    wal_shards: Vec<WalShardWire>,
}

#[derive(Deserialize)]
struct WalShardWire {
    shard: u32,
    durable_lsn: u64,
    checkpoint_lsn: u64,
}

#[derive(Deserialize)]
struct ReserveWire {
    free_bytes: u64,
    free_inodes: u64,
    minimum_free_bytes: u64,
    minimum_free_inodes: u64,
    satisfied: bool,
}

fn decode_readiness(body: &str) -> Result<StoreReadinessV1> {
    let strict: StrictJson = serde_json::from_str(body)
        .map_err(|e| anyhow::Error::new(StoreReadinessError::Malformed { detail: e.to_string() }))?;
    let wire: ReadinessWire = serde_json::from_value(strict.into_value())
        .map_err(|e| anyhow::Error::new(StoreReadinessError::Malformed { detail: e.to_string() }))?;
    if wire.contract_version != "durable-streams-store-ready-v1" {
        return Err(anyhow::Error::new(StoreReadinessError::Malformed {
            detail: format!("unsupported contract_version '{}'", wire.contract_version),
        }));
    }
    if !matches!(wire.status.as_str(), "starting" | "recovering" | "ready" | "stopping") {
        return Err(anyhow::Error::new(StoreReadinessError::Malformed {
            detail: format!("invalid readiness status '{}'", wire.status),
        }));
    }
    if wire.status != "ready" {
        return Err(anyhow::Error::new(StoreReadinessError::NotReady { status: wire.status }));
    }
    if !is_artifact_digest(&wire.artifact_digest) {
        return Err(anyhow::Error::new(StoreReadinessError::Malformed {
            detail: "artifact_digest must be lowercase sha256:<64 hexadecimal digits>".to_string(),
        }));
    }
    let creation_time =
        time::OffsetDateTime::parse(&wire.manifest.creation_time, &time::format_description::well_known::Rfc3339)
            .map_err(|_| {
                anyhow::Error::new(StoreReadinessError::Malformed {
                    detail: "manifest.creation_time must be RFC 3339 UTC using Z".to_string(),
                })
            })?;
    let canonical_seconds = wire.manifest.creation_time.as_bytes();
    if canonical_seconds.len() != 20
        || canonical_seconds[4] != b'-'
        || canonical_seconds[7] != b'-'
        || canonical_seconds[10] != b'T'
        || canonical_seconds[13] != b':'
        || canonical_seconds[16] != b':'
        || canonical_seconds[19] != b'Z'
        || ![0, 1, 2, 3, 5, 6, 8, 9, 11, 12, 14, 15, 17, 18]
            .iter()
            .all(|index| canonical_seconds[*index].is_ascii_digit())
        || !creation_time.offset().is_utc()
    {
        return Err(anyhow::Error::new(StoreReadinessError::Malformed {
            detail: "manifest.creation_time must be canonical whole-second YYYY-MM-DDTHH:MM:SSZ".to_string(),
        }));
    }
    let identity = StoreIdentityV1::new(
        wire.manifest.store_id,
        wire.manifest.store_generation,
        wire.manifest.protocol_version,
        wire.manifest.layout_version,
        wire.manifest.durability_mode,
        wire.manifest.wal_shard_count,
        wire.manifest.stream_lane_count,
        wire.manifest.filesystem_uuid,
    )
    .map_err(|e| anyhow::Error::new(StoreReadinessError::Malformed { detail: e.to_string() }))?;
    if !wire.recovery.completed || !wire.reserve.satisfied {
        return Err(anyhow::Error::new(StoreReadinessError::NotReady {
            status: "ready-with-incomplete-recovery-or-reserve".to_string(),
        }));
    }
    if wire.recovery.wal_shards.len() != identity.wal_shard_count as usize {
        return Err(anyhow::Error::new(StoreReadinessError::Malformed {
            detail: "recovery.wal_shards does not contain one entry for every expected shard".to_string(),
        }));
    }
    let mut seen = std::collections::BTreeSet::new();
    for shard in &wire.recovery.wal_shards {
        if shard.shard >= identity.wal_shard_count || !seen.insert(shard.shard) {
            return Err(anyhow::Error::new(StoreReadinessError::Malformed {
                detail: "recovery.wal_shards has an out-of-range or duplicate shard index".to_string(),
            }));
        }
        let _ = (shard.durable_lsn, shard.checkpoint_lsn);
    }
    let _ = (
        wire.reserve.free_bytes,
        wire.reserve.free_inodes,
        wire.reserve.minimum_free_bytes,
        wire.reserve.minimum_free_inodes,
    );
    Ok(StoreReadinessV1 { identity, max_chunk_bytes: wire.max_chunk_bytes, max_value_bytes: wire.max_value_bytes })
}

/// What the client concluded about the read page the store advertises in its readiness response.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PageCapVerdict {
    /// The store advertises a page that fits inside the client body cap.
    Compatible { observed: u64 },
    /// Readiness carries no cap field, or advertises zero. Every durable-streams build shipped so
    /// far omits the field, so this is the ordinary verdict, not an anomaly.
    Unknown,
    /// The store advertises a page larger than the client body cap; a full page would be refused
    /// by [`read_body_bounded`].
    Exceeds { observed: u64 },
}

pub(crate) fn assess_page_cap(readiness: &StoreReadinessV1, client_cap: u64) -> PageCapVerdict {
    match readiness.max_chunk_bytes {
        Some(0) => PageCapVerdict::Unknown,
        Some(observed) if observed.saturating_add(2) <= client_cap => PageCapVerdict::Compatible { observed },
        Some(observed) => PageCapVerdict::Exceeds { observed },
        None => PageCapVerdict::Unknown,
    }
}

/// `1` makes an unadvertised or oversized store page a boot refusal instead of a warning. Left
/// unset (the default) because no released durable-streams build advertises a cap at all, so a
/// refusal would reject every real store including the deployed one.
fn require_ds_chunk_cap() -> bool {
    std::env::var("ELECTRIC_CIRCUITS_REQUIRE_DS_CHUNK_CAP").is_ok_and(|value| value.trim() == "1")
}

/// Advisory by default: the read path already fails closed on an oversized page
/// ([`read_body_bounded`] returns a typed error naming the path, the observed size and the cap), so
/// an unverifiable server capability is a diagnosable warning rather than a reason to refuse the
/// boot. `Ok(None)` means nothing to say; `Ok(Some(message))` is the warning to log once; `Err` is
/// the strict-mode refusal.
pub(crate) fn enforce_page_cap(verdict: PageCapVerdict, client_cap: u64, strict: bool) -> Result<Option<String>> {
    let detail = match verdict {
        PageCapVerdict::Compatible { .. } => return Ok(None),
        PageCapVerdict::Unknown => format!(
            "durable-streams readiness advertises no max_chunk_bytes (uncapped or older store); \
             client body cap is {client_cap} bytes and a larger page will fail the read"
        ),
        PageCapVerdict::Exceeds { observed } => format!(
            "durable-streams readiness advertises max_chunk_bytes={observed}, larger than the \
             client body cap of {client_cap} bytes; a full page will fail the read"
        ),
    };
    if strict {
        return Err(anyhow::Error::new(StoreReadinessError::PageCapability { detail }));
    }
    Ok(Some(detail))
}

/// The advisory is a property of the store, not of the request, so it is logged once per process
/// however many clients are constructed.
fn warn_page_cap_once(message: &str) {
    static WARNED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
    if !WARNED.swap(true, std::sync::atomic::Ordering::Relaxed) {
        tracing::warn!(
            advisory = message,
            strict_env = "ELECTRIC_CIRCUITS_REQUIRE_DS_CHUNK_CAP",
            "durable-streams read page capability could not be verified"
        );
    }
}

fn is_artifact_digest(value: &str) -> bool {
    let Some(hex) = value.strip_prefix("sha256:") else { return false };
    hex.len() == 64 && hex.bytes().all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

/// A JSON value decoded with duplicate object keys rejected at every nesting level. `serde_json`
/// otherwise follows its normal last-key-wins rule, which is unsafe for a storage identity
/// attestation: two parsers could make different lineage decisions from the same bytes.
#[derive(Debug)]
enum StrictJson {
    Null,
    Bool(bool),
    Number(serde_json::Number),
    String(String),
    Array(Vec<Self>),
    Object(serde_json::Map<String, serde_json::Value>),
}

impl StrictJson {
    fn into_value(self) -> serde_json::Value {
        match self {
            Self::Null => serde_json::Value::Null,
            Self::Bool(value) => serde_json::Value::Bool(value),
            Self::Number(value) => serde_json::Value::Number(value),
            Self::String(value) => serde_json::Value::String(value),
            Self::Array(values) => serde_json::Value::Array(values.into_iter().map(Self::into_value).collect()),
            Self::Object(values) => serde_json::Value::Object(values),
        }
    }
}

impl<'de> Deserialize<'de> for StrictJson {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct Visitor;
        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = StrictJson;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("valid JSON without duplicate object keys")
            }

            fn visit_unit<E>(self) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(StrictJson::Null)
            }

            fn visit_bool<E>(self, value: bool) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(StrictJson::Bool(value))
            }

            fn visit_u64<E>(self, value: u64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(StrictJson::Number(value.into()))
            }

            fn visit_i64<E>(self, value: i64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(StrictJson::Number(value.into()))
            }

            fn visit_f64<E>(self, value: f64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                serde_json::Number::from_f64(value)
                    .map(StrictJson::Number)
                    .ok_or_else(|| E::custom("non-finite JSON number"))
            }

            fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(StrictJson::String(value.to_string()))
            }

            fn visit_string<E>(self, value: String) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(StrictJson::String(value))
            }

            fn visit_seq<A>(self, mut sequence: A) -> std::result::Result<Self::Value, A::Error>
            where
                A: serde::de::SeqAccess<'de>,
            {
                let mut values = Vec::new();
                while let Some(value) = sequence.next_element()? {
                    values.push(value);
                }
                Ok(StrictJson::Array(values))
            }

            fn visit_map<A>(self, mut map: A) -> std::result::Result<Self::Value, A::Error>
            where
                A: serde::de::MapAccess<'de>,
            {
                let mut values = serde_json::Map::new();
                while let Some((key, value)) = map.next_entry::<String, StrictJson>()? {
                    if values.insert(key.clone(), value.into_value()).is_some() {
                        return Err(serde::de::Error::custom(format!("duplicate key '{key}'")));
                    }
                }
                Ok(StrictJson::Object(values))
            }
        }
        deserializer.deserialize_any(Visitor)
    }
}

#[cfg(test)]
pub(crate) use tests::{ScriptedStore, read_cap_test_guard, readiness_json};

#[cfg(test)]
mod tests {
    use super::*;

    /// The effective read cap is process-wide state that `refresh_readiness` installs, so any test
    /// that attests a store moves it under every other test in the same binary. Tests that read or
    /// write it hold this guard: it serialises them and restores the previous values on drop, which
    /// is what makes their assertions independent of scheduling order.
    static READ_CAP_TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    pub(crate) struct ReadCapTestGuard {
        _lock: std::sync::MutexGuard<'static, ()>,
        cap: u64,
        max_value: u64,
        advertises_page: bool,
    }

    impl Drop for ReadCapTestGuard {
        fn drop(&mut self) {
            EFFECTIVE_READ_MAX_BYTES.store(self.cap, std::sync::atomic::Ordering::Relaxed);
            ADVERTISED_MAX_VALUE_BYTES.store(self.max_value, std::sync::atomic::Ordering::Relaxed);
            STORE_ADVERTISES_PAGE_CAP.store(self.advertises_page, std::sync::atomic::Ordering::Relaxed);
        }
    }

    pub(crate) fn read_cap_test_guard() -> ReadCapTestGuard {
        // A panicking test must not make every later one fail on a poisoned lock; the guard's whole
        // job is restoring the statics, which the drop below does either way.
        let lock = READ_CAP_TEST_LOCK.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        ReadCapTestGuard {
            _lock: lock,
            cap: EFFECTIVE_READ_MAX_BYTES.load(std::sync::atomic::Ordering::Relaxed),
            max_value: ADVERTISED_MAX_VALUE_BYTES.load(std::sync::atomic::Ordering::Relaxed),
            advertises_page: STORE_ADVERTISES_PAGE_CAP.load(std::sync::atomic::Ordering::Relaxed),
        }
    }

    #[test]
    fn read_body_cap_defaults_to_the_append_budget() {
        // The default is a property of the sizing rule, not of whatever another test last attested:
        // four pages, lifted to the append budget so the engine can read back what it writes.
        let floor = ds_read_cap_floor();
        assert_eq!(
            effective_read_max_bytes(PageCapVerdict::Compatible { observed: 4 * 1024 * 1024 }, None, floor),
            (16 * 1024 * 1024).max(floor)
        );
        // And with nothing installed, that is what the read path uses.
        let _guard = read_cap_test_guard();
        EFFECTIVE_READ_MAX_BYTES.store(0, std::sync::atomic::Ordering::Relaxed);
        assert_eq!(ds_read_max_bytes(), (16 * 1024 * 1024).max(floor));
    }

    #[derive(Default)]
    pub(crate) struct ScriptedStore {
        pub(crate) appended: std::sync::Mutex<Vec<(String, String, Vec<u8>)>>,
        pub(crate) operations: std::sync::Mutex<Vec<String>>,
        pub(crate) fail_read_body: bool,
        pub(crate) read_pages: std::sync::Mutex<Vec<(String, bool, String)>>,
        /// Pages keyed by the offset the reader asked for. A single ordered log two readers share
        /// (the sequencer's live tail and a replay scan) cannot be modelled by one pop-front queue:
        /// whichever read arrives first consumes the other's page. When this map is non-empty it
        /// takes over from `read_pages`: an offset it knows answers with that page, an offset it
        /// does not know parks a live read (an idle long-poll) and answers a replay read with an
        /// empty up-to-date page (the end of the log).
        pub(crate) pages_by_offset: std::sync::Mutex<HashMap<String, (String, bool, String)>>,
        pub(crate) read_count: std::sync::atomic::AtomicUsize,
        /// Every offset a read was asked for, in order. A coalesced scan's routing is only correct
        /// if the scan STARTS at the earliest parked cursor, which is invisible from the pages.
        pub(crate) read_offsets: std::sync::Mutex<Vec<String>>,
        pub(crate) head_count: std::sync::atomic::AtomicUsize,
        pub(crate) head_offsets: std::sync::Mutex<Vec<String>>,
        pub(crate) fail_append_path: Option<String>,
        /// How many non-live reads the store accepts and then never finishes. It models a provider
        /// that answers a replay `GET` and then stops yielding body bytes: the store call itself
        /// never resolves, exactly as the HTTP store's response future would not.
        pub(crate) stall_nonlive_reads: std::sync::atomic::AtomicUsize,
        /// Park every long-poll forever, which is what an idle tail read does. Only scaffolding: a
        /// spawned sequencer must not spin on a synthetic empty page while a stall is under test.
        pub(crate) stall_live_reads: bool,
        /// How many long-polls are answered with a transport failure before the rest are served
        /// normally (or parked). It models the store going away and the client reconnecting.
        pub(crate) fail_live_reads: std::sync::atomic::AtomicUsize,
        /// How many long-polls come back as a body that breached the client cap, which is what an
        /// uncapped store does when the backlog is bigger than the ceiling.
        pub(crate) cap_breach_live_reads: std::sync::atomic::AtomicUsize,
        /// How long each page takes to come back. Under `start_paused` this is virtual time, which
        /// is how a test can say "the remaining span is ten minutes of pages" without spending any.
        pub(crate) page_delay: Option<std::time::Duration>,
        pub(crate) readiness_status: u16,
        pub(crate) readiness_body: std::sync::Mutex<Option<String>>,
        pub(crate) head_status: u16,
    }

    fn response(status: u16) -> StoreResponse {
        StoreResponse {
            status,
            body: Some(Ok(String::new())),
            next_offset: Some("opaque-provider-token".to_string()),
            up_to_date: true,
            closed: false,
        }
    }

    impl DurableStreamStore for ScriptedStore {
        fn ready<'a>(&'a self) -> StoreFuture<'a> {
            Box::pin(async move {
                self.operations.lock().unwrap().push("ready".to_string());
                let mut response = response(if self.readiness_status == 0 { 200 } else { self.readiness_status });
                response.body = Some(Ok(self.readiness_body.lock().unwrap().clone().unwrap_or_default()));
                Ok(response)
            })
        }

        fn ensure<'a>(&'a self, _path: &'a str, _content_type: &'a str) -> StoreFuture<'a> {
            Box::pin(async { Ok(response(201)) })
        }

        fn append<'a>(
            &'a self,
            path: &'a str,
            content_type: &'a str,
            body: Vec<u8>,
            _response_body: BodyRead,
        ) -> StoreFuture<'a> {
            Box::pin(async move {
                if self.fail_append_path.as_deref().is_some_and(|p| path.ends_with(p)) {
                    return Err(anyhow::anyhow!("scripted append failure"));
                }
                self.operations.lock().unwrap().push("append".to_string());
                self.appended.lock().unwrap().push((path.to_string(), content_type.to_string(), body));
                Ok(response(204))
            })
        }

        fn read<'a>(&'a self, path: &'a str, offset: &'a str, live: bool) -> StoreFuture<'a> {
            Box::pin(async move {
                self.read_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                self.read_offsets.lock().unwrap().push(offset.to_string());
                if live
                    && self
                        .cap_breach_live_reads
                        .fetch_update(std::sync::atomic::Ordering::Relaxed, std::sync::atomic::Ordering::Relaxed, |n| {
                            (n > 0).then(|| n - 1)
                        })
                        .is_ok()
                {
                    let limit = ds_read_max_bytes();
                    let mut res = response(200);
                    res.body = Some(Err(anyhow::Error::new(ReadCapExceeded {
                        path: path.to_string(),
                        observed: limit + 1,
                        limit,
                        max_value_bytes: None,
                    })));
                    return Ok(res);
                }
                if live
                    && self
                        .fail_live_reads
                        .fetch_update(std::sync::atomic::Ordering::Relaxed, std::sync::atomic::Ordering::Relaxed, |n| {
                            (n > 0).then(|| n - 1)
                        })
                        .is_ok()
                {
                    return Ok(response(503));
                }
                let stalled = !live
                    && self
                        .stall_nonlive_reads
                        .fetch_update(std::sync::atomic::Ordering::Relaxed, std::sync::atomic::Ordering::Relaxed, |n| {
                            (n > 0).then(|| n - 1)
                        })
                        .is_ok();
                if stalled || (live && self.stall_live_reads) {
                    std::future::pending::<()>().await;
                }
                if let Some(delay) = self.page_delay {
                    tokio::time::sleep(delay).await;
                }
                let keyed = {
                    let pages = self.pages_by_offset.lock().unwrap();
                    (!pages.is_empty()).then(|| pages.get(offset).cloned())
                };
                if let Some(keyed) = keyed {
                    let mut res = response(200);
                    match keyed {
                        Some((next, up_to_date, body)) => {
                            res.next_offset = Some(next);
                            res.up_to_date = up_to_date;
                            res.body = Some(Ok(body));
                        }
                        None if live => std::future::pending::<()>().await,
                        None => {
                            res.next_offset = Some(offset.to_string());
                            res.up_to_date = true;
                            res.body = Some(Ok("[]".to_string()));
                        }
                    }
                    return Ok(res);
                }
                let mut res = response(200);
                res.next_offset = Some("tempting-next-offset".to_string());
                if self.fail_read_body {
                    res.body = Some(Err(anyhow::anyhow!("scripted stream body failure")));
                } else {
                    let page = {
                        let mut pages = self.read_pages.lock().unwrap();
                        (!pages.is_empty()).then(|| pages.remove(0))
                    };
                    if let Some((next, up_to_date, body)) = page {
                        res.next_offset = Some(next);
                        res.up_to_date = up_to_date;
                        res.body = Some(Ok(body));
                    }
                }
                Ok(res)
            })
        }

        fn head<'a>(&'a self, _path: &'a str) -> StoreFuture<'a> {
            Box::pin(async move {
                self.head_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let mut res = response(if self.head_status == 0 { 200 } else { self.head_status });
                if let Some(offset) = self.head_offsets.lock().unwrap().pop() {
                    res.next_offset = Some(offset);
                }
                Ok(res)
            })
        }

        fn close<'a>(&'a self, _path: &'a str) -> StoreFuture<'a> {
            Box::pin(async { Ok(response(204)) })
        }

        fn delete<'a>(&'a self, _path: &'a str) -> StoreFuture<'a> {
            Box::pin(async { Ok(response(204)) })
        }
    }

    #[tokio::test]
    async fn facade_keeps_envelope_codec_and_byte_accounting_above_the_store_port() {
        let store = Arc::new(ScriptedStore::default());
        let client = DsClient::with_test_store("scripted://provider".to_string(), store.clone());
        let envelope = Envelope {
            type_: "public.items".to_string(),
            key: "item-1".to_string(),
            value: Some(serde_json::json!({ "id": "item-1" })),
            old: None,
            headers: EnvelopeHeaders {
                operation: "upsert".to_string(),
                txid: None,
                offset: None,
                lsn: None,
                seq: Some(7),
                last: Some(true),
            },
        };
        let expected = serde_json::to_vec(&[envelope.clone()]).unwrap();

        let appended = client.append_checked("shape/s1", &[envelope]).await.unwrap();

        assert!(matches!(appended, Appended::Ok { next_offset: Some(ref token) } if token == "opaque-provider-token"));
        assert_eq!(client.appended_bytes("shape/s1"), expected.len() as u64);
        assert_eq!(
            client.stream_url("shape/s1"),
            "scripted://provider/circuits/v1/test-stack/stores/ff8b5fa6-e786-4994-8da0-f14e9e79f318/queries/test-query/shape/s1"
        );
        assert_eq!(
            *store.appended.lock().unwrap(),
            vec![(
                StreamScope::in_process_test_scope().qualify("shape/s1").unwrap(),
                "application/json".to_string(),
                expected
            )]
        );
    }

    #[tokio::test]
    async fn successful_read_body_failure_never_accepts_the_advertised_next_offset() {
        let store = Arc::new(ScriptedStore { fail_read_body: true, ..Default::default() });
        let client = DsClient::with_test_store("scripted://provider".to_string(), store);

        let envelope_err = match client.read("changes/0", "prior-offset", false).await {
            Ok(_) => panic!("a successful GET with an unreadable body must not produce a page"),
            Err(err) => err,
        };
        assert!(
            format!("{envelope_err:#}").contains("scripted stream body failure"),
            "the source body failure must survive the facade boundary"
        );

        let json_err = client
            .read_json("meta/catalog", "prior-offset")
            .await
            .expect_err("a successful GET with an unreadable body must not produce JSON events");
        assert!(
            format!("{json_err:#}").contains("scripted stream body failure"),
            "the source body failure must survive the facade boundary"
        );
    }

    #[tokio::test]
    async fn scripted_reads_preserve_partial_page_metadata_for_callers_to_page() {
        let store = Arc::new(ScriptedStore {
            read_pages: std::sync::Mutex::new(vec![
                (
                    "page-2".to_string(),
                    false,
                    "[{\"type\":\"public.items\",\"key\":\"1\",\"headers\":{\"operation\":\"upsert\"}}]".to_string(),
                ),
                ("tail".to_string(), true, "[]".to_string()),
            ]),
            ..Default::default()
        });
        let client = DsClient::with_test_store("scripted://provider".to_string(), store);

        let first = client.read("changes/0", "page-1", false).await.unwrap();
        assert_eq!(first.next_offset.as_deref(), Some("page-2"));
        assert!(!first.up_to_date, "partial pages must not be treated as drained");
        assert_eq!(first.envelopes.len(), 1);

        let second = client.read("changes/0", "page-2", false).await.unwrap();
        assert_eq!(second.next_offset.as_deref(), Some("tail"));
        assert!(second.up_to_date);
        assert!(second.envelopes.is_empty());
    }

    #[tokio::test]
    async fn table_read_discards_unmatched_rows_before_decoding_their_bodies() {
        let store = Arc::new(ScriptedStore {
            read_pages: std::sync::Mutex::new(vec![(
                "tail".to_string(),
                true,
                "[{\"type\":\"public.other\",\"key\":\"x\",\"value\":{\"large\":true},\"headers\":{\"operation\":\"upsert\"}},{\"type\":\"public.items\",\"key\":\"1\",\"value\":{\"id\":1},\"headers\":{\"operation\":\"upsert\"}}]".to_string(),
            )]),
            ..Default::default()
        });
        let client = DsClient::with_test_store("scripted://provider".to_string(), store);

        let page = client.read_for_table("changes/0", "-1", false, "public.items").await.unwrap();
        assert_eq!(page.envelopes.len(), 1);
        assert_eq!(page.envelopes[0].type_, "public.items");
    }

    /// A store that accepts a replay `GET` and then never finishes the response must not park the
    /// caller. The caller here is the coalesced reactivation worker, which holds a reactivation
    /// permit for the whole scan, so an unbounded read is what leaves a shape `Reactivating` and
    /// queues every later reactivation behind a permit nothing will ever return. Virtual time keeps
    /// the deadline the production default instead of a test-only value.
    #[tokio::test(start_paused = true)]
    async fn a_replay_page_the_store_never_finishes_fails_with_a_typed_read_timeout() {
        let store = Arc::new(ScriptedStore {
            stall_nonlive_reads: std::sync::atomic::AtomicUsize::new(1),
            ..Default::default()
        });
        let client = DsClient::with_test_store("scripted://provider".to_string(), store.clone());

        let started = tokio::time::Instant::now();
        let read = tokio::time::timeout(
            std::time::Duration::from_secs(600),
            client.read_for_table("changes/0", "-1", false, "public.items"),
        )
        .await
        .expect("a replay page the store never finishes must be bounded by the engine, not read forever");

        let Err(error) = read else { panic!("a page whose response never arrived is not a page") };
        let timeout = error
            .downcast_ref::<DsReadTimeout>()
            .expect("a dead transport must be typed, not folded into a generic read failure");
        assert_eq!(timeout.path, "changes/0");
        assert_eq!(timeout.after, std::time::Duration::from_secs(DEFAULT_DS_READ_TIMEOUT_SECS));
        let waited = started.elapsed();
        assert!(waited >= timeout.after, "waited {waited:?}, short of the configured read deadline");
        assert!(waited < std::time::Duration::from_secs(600), "waited {waited:?}, the outer guard, not the deadline");
        assert_eq!(
            store.read_count.load(std::sync::atomic::Ordering::Relaxed),
            1,
            "the abandoned read is not retried inside the port"
        );
    }

    pub(crate) fn readiness_json(identity: &StoreIdentityV1) -> String {
        serde_json::json!({
            "contract_version": "durable-streams-store-ready-v1",
            "status": "ready",
            "artifact_digest": "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            "manifest": {
                "store_id": identity.store_id,
                "store_generation": identity.store_generation,
                "protocol_version": identity.protocol_version,
                "layout_version": identity.layout_version,
                "durability_mode": identity.durability_mode,
                "wal_shard_count": identity.wal_shard_count,
                "stream_lane_count": identity.stream_lane_count,
                "filesystem_uuid": identity.filesystem_uuid,
                "creation_time": "2026-08-27T19:00:00Z"
            },
            "recovery": {
                "completed": true,
                "wal_shards": [
                    { "shard": 0, "durable_lsn": 0, "checkpoint_lsn": 0 },
                    { "shard": 1, "durable_lsn": 0, "checkpoint_lsn": 0 }
                ]
            },
            "reserve": {
                "free_bytes": 85899345920u64,
                "free_inodes": 1000000u64,
                "minimum_free_bytes": 21474836480u64,
                "minimum_free_inodes": 10000u64,
                "satisfied": true
            }
        })
        .to_string()
    }

    /// A store that answers `delay` after the request arrives, on `connections` connections. It is
    /// the shape of a durable-streams long-poll: the response head does not start until the store
    /// has something to say, or its own long-poll window expires.
    async fn slow_store(delay: std::time::Duration, connections: usize) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        tokio::spawn(async move {
            for _ in 0..connections {
                let Ok((mut socket, _)) = listener.accept().await else { return };
                tokio::spawn(async move {
                    use tokio::io::{AsyncReadExt, AsyncWriteExt};
                    let mut request = Vec::new();
                    let mut byte = [0u8; 1];
                    while !request.ends_with(b"\r\n\r\n") {
                        match socket.read(&mut byte).await {
                            Ok(0) | Err(_) => return,
                            Ok(_) => request.push(byte[0]),
                        }
                    }
                    tokio::time::sleep(delay).await;
                    let body = "[]";
                    let response = format!(
                        "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nstream-next-offset: 0000000000000000_0000000000000000\r\nstream-up-to-date: true\r\n\r\n{body}",
                        body.len()
                    );
                    let _ = socket.write_all(response.as_bytes()).await;
                    let _ = socket.flush().await;
                });
            }
        });
        format!("http://{addr}")
    }

    /// The deployed store holds an idle long-poll for 35 s before answering empty
    /// (`long_poll_timeout_ms` in the indexed infra). A read deadline shorter than that window is
    /// not a safety bound on a long-poll — it is a guaranteed error on every idle read — so the
    /// long-poll carries its own, longer deadline while ordinary reads keep the short one.
    #[tokio::test]
    async fn a_long_poll_outlives_the_ordinary_read_deadline() {
        let timeouts = DsTimeouts {
            connect: std::time::Duration::from_secs(5),
            read: std::time::Duration::from_millis(150),
            live_read: std::time::Duration::from_secs(5),
            request: std::time::Duration::from_secs(10),
        };
        // Longer than the ordinary read deadline, well inside the long-poll one: the store is
        // holding the request open exactly as it holds an idle long-poll.
        let base = slow_store(std::time::Duration::from_millis(400), 2).await;
        let store = HttpDurableStreamsStore::new_in_process_with_timeouts(base, timeouts);

        let live = store.read("changes/0", "-1", true).await;
        assert!(
            live.as_ref().is_ok_and(|res| res.status == 200),
            "a long-poll must be bounded by the long-poll deadline, not the ordinary read deadline: {:?}",
            live.err().map(|e| format!("{e:#}"))
        );
        let ordinary = store.read("changes/0", "-1", false).await;
        assert!(ordinary.is_err(), "an ordinary read is a bounded request and keeps the short deadline");
    }

    /// The pair of defaults this depends on, in one place: the long-poll deadline has to clear the
    /// store's window, and it is the ordinary read deadline that stays short.
    #[test]
    fn the_long_poll_deadline_clears_the_stores_long_poll_window() {
        const DEPLOYED_STORE_LONG_POLL_SECS: u64 = 35;
        let timeouts = DsTimeouts::from_env();
        assert!(
            timeouts.live_read.as_secs() > DEPLOYED_STORE_LONG_POLL_SECS,
            "a long-poll deadline of {}s expires before the store's {DEPLOYED_STORE_LONG_POLL_SECS}s window",
            timeouts.live_read.as_secs()
        );
        assert!(timeouts.read < timeouts.live_read, "an ordinary read must stay bounded more tightly than a long-poll");
        assert_eq!(timeouts.read.as_secs(), DEFAULT_DS_READ_TIMEOUT_SECS);
    }

    /// The decision table behind a body-cap breach, in one place: raise only where a bigger buffer
    /// is what the situation actually calls for.
    #[test]
    fn a_cap_breach_raises_unless_an_operator_named_the_cap_or_the_store_broke_its_value_bound() {
        let ceiling = 512 * 1024 * 1024;
        let gib = 1024 * 1024 * 1024;
        assert_eq!(
            cap_after_breach(false, None, None, 64 * 1024 * 1024, ceiling),
            CapBreachOutcome::Raise { limit: 128 * 1024 * 1024 },
            "an uncapped store's backlog is not a fault; make progress on it"
        );
        assert_eq!(
            cap_after_breach(true, Some(gib), None, 64 * 1024 * 1024, ceiling),
            CapBreachOutcome::Raise { limit: 128 * 1024 * 1024 },
            "the deployed store: a 4 MiB page target beside a 1 GiB value bound. A value larger than the \
             page is framed whole by the protocol, so the read is a large value, not a broken store"
        );
        assert_eq!(
            cap_after_breach(true, None, None, 64 * 1024 * 1024, ceiling),
            CapBreachOutcome::Raise { limit: 128 * 1024 * 1024 },
            "a store that advertises a page but no value bound has promised nothing about value size"
        );
        assert_eq!(
            cap_after_breach(true, Some(0), None, 64 * 1024 * 1024, ceiling),
            CapBreachOutcome::Raise { limit: 128 * 1024 * 1024 },
            "a value bound of 0 is unbounded, which is the same absence of a promise"
        );
        assert_eq!(
            cap_after_breach(true, Some(8 * 1024 * 1024), None, 16 * 1024 * 1024, ceiling),
            CapBreachOutcome::Latch,
            "a store whose advertised value bound the cap already covers, and that still answered with \
             more, broke its contract; that must stay visible, not be papered over"
        );
        assert_eq!(
            cap_after_breach(true, Some(16 * 1024 * 1024), None, 16 * 1024 * 1024, ceiling),
            CapBreachOutcome::Latch,
            "a value bound exactly at the cap is covered by it"
        );
        assert_eq!(
            cap_after_breach(false, None, Some(16 * 1024 * 1024), 16 * 1024 * 1024, ceiling),
            CapBreachOutcome::Latch,
            "an operator who named a cap decided what this process may buffer"
        );
        assert_eq!(
            cap_after_breach(true, Some(gib), Some(64 * 1024 * 1024), 64 * 1024 * 1024, ceiling),
            CapBreachOutcome::Latch,
            "and that decision holds against a store whose value bound exceeds it"
        );
        assert_eq!(
            cap_after_breach(false, None, None, ceiling, ceiling),
            CapBreachOutcome::Latch,
            "past the hard ceiling a single read really is unserviceable"
        );
        assert_eq!(
            cap_after_breach(true, Some(gib), None, ceiling, ceiling),
            CapBreachOutcome::Latch,
            "the ceiling binds a large value against a paged store exactly as it binds a backlog"
        );
        assert_eq!(
            cap_after_breach(false, None, None, 400 * 1024 * 1024, ceiling),
            CapBreachOutcome::Raise { limit: ceiling },
            "the last raise stops at the ceiling rather than overshooting it"
        );
    }

    #[tokio::test]
    async fn readiness_mismatch_stops_before_any_normal_store_operation() {
        let expected = StoreIdentityV1::in_process_test_identity();
        let mut observed = expected.clone();
        observed.store_generation = "aa8b5fa6-e786-4994-8da0-f14e9e79f318".to_string();
        let store = Arc::new(ScriptedStore {
            readiness_body: std::sync::Mutex::new(Some(readiness_json(&observed))),
            ..Default::default()
        });
        let client = DsClient::with_test_store("scripted://provider".to_string(), store.clone());

        let error =
            client.preflight_readiness(&expected).await.expect_err("mismatched store identity must refuse boot");
        assert!(error.downcast_ref::<StoreReadinessError>().is_some());
        assert_eq!(*store.operations.lock().unwrap(), vec!["ready".to_string()]);
        assert!(store.appended.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn readiness_is_reread_and_verdict_rederived_after_reconnect() {
        let _guard = read_cap_test_guard();
        let identity = StoreIdentityV1::in_process_test_identity();
        let store = Arc::new(ScriptedStore { readiness_body: std::sync::Mutex::new(None), ..Default::default() });
        // Replace the helper's decoded value with a wire body for the first and second connections.
        *store.readiness_body.lock().unwrap() =
            Some(readiness_json(&identity).replace("\"reserve\":{", "\"max_chunk_bytes\":4194304,\"reserve\":{"));
        let client = DsClient::with_test_store("scripted://provider".to_string(), store.clone());
        assert_eq!(
            client.refresh_readiness(&identity).await.unwrap(),
            PageCapVerdict::Compatible { observed: 4194304 }
        );
        // A page above the floored default (64 MiB, the append budget), so the verdict flips.
        *store.readiness_body.lock().unwrap() =
            Some(readiness_json(&identity).replace("\"reserve\":{", "\"max_chunk_bytes\":134217728,\"reserve\":{"));
        assert_eq!(client.refresh_readiness(&identity).await.unwrap(), PageCapVerdict::Exceeds { observed: 134217728 });
    }

    #[tokio::test]
    async fn malformed_logical_path_never_reaches_the_store() {
        let store = Arc::new(ScriptedStore::default());
        let client = DsClient::with_test_store("scripted://provider".to_string(), store.clone());

        assert!(client.append_json("shape/../s1", &[serde_json::json!({"t": "x"})]).await.is_err());
        assert!(store.operations.lock().unwrap().is_empty());
        assert!(store.appended.lock().unwrap().is_empty());
    }

    #[test]
    fn readiness_rejects_duplicate_keys_and_noncanonical_values() {
        let identity = StoreIdentityV1::in_process_test_identity();
        let duplicate =
            readiness_json(&identity).replacen("\"status\":\"ready\"", "\"status\":\"ready\",\"status\":\"ready\"", 1);
        assert!(decode_readiness(&duplicate).is_err());
        let noncanonical = readiness_json(&identity).replace("2026-08-27T19:00:00Z", "2026-08-27T19:00:00+00:00");
        assert!(decode_readiness(&noncanonical).is_err());
        let fractional = readiness_json(&identity).replace("2026-08-27T19:00:00Z", "2026-08-27T19:00:00.001Z");
        assert!(decode_readiness(&fractional).is_err());
    }

    const TEST_CLIENT_CAP: u64 = 16 * 1024 * 1024;

    fn readiness_with_cap(identity: &StoreIdentityV1, cap: u64) -> StoreReadinessV1 {
        let body =
            readiness_json(identity).replace("\"reserve\":{", &format!("\"max_chunk_bytes\":{cap},\"reserve\":{{"));
        decode_readiness(&body).unwrap()
    }

    #[test]
    fn readiness_preserves_max_value_bytes_separately_from_page_cap() {
        let identity = StoreIdentityV1::in_process_test_identity();
        let body = readiness_json(&identity)
            .replace("\"reserve\":{", "\"max_chunk_bytes\":4194304,\"max_value_bytes\":1073741824,\"reserve\":{");
        let readiness = decode_readiness(&body).unwrap();
        assert!(format!("{readiness:?}").contains("max_value_bytes: Some(1073741824)"));
        assert_eq!(assess_page_cap(&readiness, TEST_CLIENT_CAP), PageCapVerdict::Compatible { observed: 4194304 });
        let message = ReadCapExceeded {
            path: "changes/0".to_string(),
            observed: TEST_CLIENT_CAP + 1,
            limit: TEST_CLIENT_CAP,
            max_value_bytes: readiness.max_value_bytes,
        }
        .to_string();
        assert!(message.contains("single messages up to 1073741824"));
    }

    #[test]
    fn a_store_that_advertises_no_page_cap_is_read_as_unknown() {
        let identity = StoreIdentityV1::in_process_test_identity();
        let missing = decode_readiness(&readiness_json(&identity)).unwrap();
        assert_eq!(assess_page_cap(&missing, TEST_CLIENT_CAP), PageCapVerdict::Unknown);
        // A server that advertises zero means "unlimited", which is the same unknown page size.
        let zero = readiness_with_cap(&identity, 0);
        assert_eq!(assess_page_cap(&zero, TEST_CLIENT_CAP), PageCapVerdict::Unknown);
        let fitting = readiness_with_cap(&identity, 4 * 1024 * 1024);
        assert_eq!(
            assess_page_cap(&fitting, TEST_CLIENT_CAP),
            PageCapVerdict::Compatible { observed: 4 * 1024 * 1024 }
        );
        let oversized = readiness_with_cap(&identity, 32 * 1024 * 1024);
        assert_eq!(
            assess_page_cap(&oversized, TEST_CLIENT_CAP),
            PageCapVerdict::Exceeds { observed: 32 * 1024 * 1024 }
        );
    }

    #[test]
    fn json_page_cap_includes_two_bytes_of_array_framing() {
        let identity = StoreIdentityV1::in_process_test_identity();
        let readiness = readiness_with_cap(&identity, TEST_CLIENT_CAP);
        assert_eq!(assess_page_cap(&readiness, TEST_CLIENT_CAP), PageCapVerdict::Exceeds { observed: TEST_CLIENT_CAP });
    }

    #[test]
    fn json_page_cap_fits_when_observed_plus_two_is_within_client_cap() {
        let identity = StoreIdentityV1::in_process_test_identity();
        let readiness = readiness_with_cap(&identity, TEST_CLIENT_CAP - 2);
        assert_eq!(
            assess_page_cap(&readiness, TEST_CLIENT_CAP),
            PageCapVerdict::Compatible { observed: TEST_CLIENT_CAP - 2 }
        );
    }

    /// No released durable-streams build advertises `max_chunk_bytes`, so refusing an unverifiable
    /// page cap refuses every real store — including the deployed one and the conformance harness.
    /// The default must boot and warn; the read path is what stays fail-closed.
    #[test]
    fn an_unverifiable_page_cap_is_advisory_by_default_and_refused_only_in_strict_mode() {
        for verdict in [PageCapVerdict::Unknown, PageCapVerdict::Exceeds { observed: 32 * 1024 * 1024 }] {
            let advisory = enforce_page_cap(verdict, TEST_CLIENT_CAP, false)
                .expect("an unverifiable page cap must not refuse the boot by default")
                .expect("an unverifiable page cap must produce a warning");
            assert!(advisory.contains("16777216"), "the advisory names the client body cap: {advisory}");

            let refusal = enforce_page_cap(verdict, TEST_CLIENT_CAP, true)
                .expect_err("strict mode must refuse an unverifiable page cap");
            assert!(refusal.to_string().contains("storage page capability is incompatible"), "{refusal}");
        }
        assert!(
            enforce_page_cap(PageCapVerdict::Unknown, TEST_CLIENT_CAP, false)
                .unwrap()
                .unwrap()
                .contains("no max_chunk_bytes")
        );
        assert!(
            enforce_page_cap(PageCapVerdict::Exceeds { observed: 32 * 1024 * 1024 }, TEST_CLIENT_CAP, false)
                .unwrap()
                .unwrap()
                .contains("max_chunk_bytes=33554432")
        );
    }

    /// A store that does not page answers a read with the whole remainder of the stream. A cap
    /// below that backlog does not bound memory — it stops the live loop entirely, because the
    /// sequencer retries the identical read and it fails identically forever. Sized for the store,
    /// not for a number.
    #[test]
    fn an_uncapped_store_raises_the_read_ceiling_unless_an_operator_named_one() {
        // A floor below every default, so the verdicts are what these assertions see.
        let small_floor = 4 * 1024 * 1024;
        assert_eq!(
            effective_read_max_bytes(PageCapVerdict::Compatible { observed: 4 * 1024 * 1024 }, None, small_floor),
            16 * 1024 * 1024,
            "a store that pages needs only defense in depth"
        );
        assert_eq!(
            effective_read_max_bytes(PageCapVerdict::Unknown, None, small_floor),
            64 * 1024 * 1024,
            "a store that does not page must not be able to stall the live loop at 16 MiB"
        );
        assert_eq!(
            effective_read_max_bytes(PageCapVerdict::Exceeds { observed: 32 * 1024 * 1024 }, None, small_floor),
            64 * 1024 * 1024,
            "a store whose page exceeds the client cap is the same hazard"
        );
        for verdict in [
            PageCapVerdict::Unknown,
            PageCapVerdict::Compatible { observed: 4 * 1024 * 1024 },
            PageCapVerdict::Exceeds { observed: 32 * 1024 * 1024 },
        ] {
            assert_eq!(
                effective_read_max_bytes(verdict, Some(8 * 1024 * 1024), small_floor),
                8 * 1024 * 1024,
                "an operator who names a cap has decided what this process may buffer"
            );
        }
    }

    /// The engine writes one POST body of up to its append budget, and the store frames that value
    /// whole on read. A derived cap below the budget would refuse the engine's own change log — the
    /// production incident: a 16 MiB cap against a 64 MiB append budget latched degraded on a value
    /// this engine had itself appended, and the task cycled under the health check on every restart.
    #[test]
    fn a_derived_read_cap_is_never_below_the_append_budget() {
        let budget = 64 * 1024 * 1024;
        assert_eq!(
            effective_read_max_bytes(PageCapVerdict::Compatible { observed: 4 * 1024 * 1024 }, None, budget),
            budget,
            "four pages is not enough to read back one of this engine's own appends"
        );
        assert_eq!(
            effective_read_max_bytes(PageCapVerdict::Unknown, None, 96 * 1024 * 1024),
            96 * 1024 * 1024,
            "a larger append budget lifts the uncapped-store cap with it"
        );
        assert_eq!(
            effective_read_max_bytes(
                PageCapVerdict::Compatible { observed: 4 * 1024 * 1024 },
                Some(8 * 1024 * 1024),
                budget
            ),
            8 * 1024 * 1024,
            "an explicit cap is obeyed here; a contradictory one is refused at boot, not corrected in flight"
        );
        assert_eq!(
            ds_read_cap_floor(),
            crate::txn_buffer::TxnBufferConfig::default().append_bytes,
            "the floor is the append budget, which defaults to 64 MiB"
        );
    }

    #[test]
    fn a_compatible_page_cap_says_nothing_in_either_mode() {
        let verdict = PageCapVerdict::Compatible { observed: 4 * 1024 * 1024 };
        assert!(enforce_page_cap(verdict, TEST_CLIENT_CAP, false).unwrap().is_none());
        assert!(enforce_page_cap(verdict, TEST_CLIENT_CAP, true).unwrap().is_none());
    }

    /// The boot classification of a durable-streams failure. Getting this wrong is expensive in
    /// both directions: forgiving too much hides a malformed catalog behind an infinite retry,
    /// forgiving too little exits `EX_CONFIG` for a storage pod that is merely slower to start than
    /// the engine — which, in a compose or Kubernetes start, is the normal ordering.
    #[tokio::test]
    async fn transport_failures_are_retryable_and_answers_are_not() {
        // A REAL connect refusal (nothing listens on port 1), not a fabricated one: `reqwest::Error`
        // has no public constructor, and a mock would only prove the mock.
        let refused = match DsClient::new_for_in_process_test("http://127.0.0.1:1").read("changes/0", "-1", false).await
        {
            Err(e) => e,
            Ok(_) => panic!("nothing listens on port 1"),
        };
        assert!(is_unavailable(&refused), "a refused connection must retry: {refused:#}");

        // A 5xx: the server is there and not serving.
        let five_oh_three = anyhow::Error::new(DsUnavailable { op: "GET", path: "meta/catalog".into(), status: 503 })
            .context("folding the durable catalog");
        assert!(is_unavailable(&five_oh_three));
        assert_eq!(
            crate::pg::boot_disposition(&five_oh_three),
            crate::pg::BootFailure::Retryable,
            "storage that is not serving yet must not exit EX_CONFIG"
        );
        assert_eq!(crate::pg::boot_failure_name(&five_oh_three), "durable-streams is unreachable");

        // ...but an ANSWER is not a transport failure. A stream that is gone, a malformed catalog
        // and a strictness refusal all stay fatal.
        let gone = anyhow::Error::new(StreamGone { path: "meta/catalog".into(), status: 404 });
        assert!(!is_unavailable(&gone));
        assert_eq!(crate::pg::boot_disposition(&gone), crate::pg::BootFailure::Fatal);

        let malformed = anyhow::Error::new(serde_json::from_str::<serde_json::Value>("{oh no").unwrap_err())
            .context("parsing stream body");
        assert!(!is_unavailable(&malformed));
        assert_eq!(crate::pg::boot_disposition(&malformed), crate::pg::BootFailure::Fatal);

        // A typed catalog strictness refusal carries no `reqwest::Error` at all, so it stays fatal
        // — the same path every non-transport boot failure takes.
        let strictness = anyhow::anyhow!("catalog predates ADR-0006 segmentation");
        assert!(!is_unavailable(&strictness));
        assert_eq!(crate::pg::boot_disposition(&strictness), crate::pg::BootFailure::Fatal);
        assert_eq!(crate::pg::boot_failure_name(&strictness), "not a transient Postgres condition");
    }

    /// A 4xx carries its body (the server's own words); a 5xx becomes the typed, retryable error.
    #[test]
    fn status_errors_are_typed_only_for_5xx() {
        let four = status_error("PUT", "shape/1", reqwest::StatusCode::BAD_REQUEST.as_u16(), "bad config");
        assert!(!is_unavailable(&four));
        assert!(format!("{four:#}").contains("bad config"));
        let five = status_error("PUT", "shape/1", reqwest::StatusCode::BAD_GATEWAY.as_u16(), "");
        assert!(is_unavailable(&five));
        assert!(format!("{five:#}").contains("502"));
    }
}
