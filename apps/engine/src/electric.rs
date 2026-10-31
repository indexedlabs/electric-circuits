//! Electric-protocol adapter: serves `GET /v1/shape` so Electric's official `Electric.Client` (and its
//! oracle test harness) can read shapes from our engine. We translate the engine's materialized-shape
//! durable stream into Electric's change-message log with the headers/control-messages the client
//! requires (`electric-handle`, `electric-offset`, `electric-schema`, `electric-cursor`,
//! `electric-up-to-date`; `up-to-date` / `must-refetch` control messages).
//!
//! Mapping notes:
//! - A `GET` with `offset=-1` is the **snapshot**: parse `where` → predicate, create a materialized
//!   shape, fold its stream to the current row set, emit every row as an `insert`, then an
//!   `up-to-date` control. The handle is the shape id; the offset is the stream's tail. An
//!   `offset > -1` without a handle is a 400 (Electric: "handle required").
//! - A `GET` with `live=true` long-polls the stream from `offset` and emits `insert`/`update`/`delete`.
//!   Our engine emits absolute `upsert`/`delete`, so we reconstruct insert-vs-update from a per-handle
//!   key set (Electric's client rejects insert-of-existing / update-of-missing). Requests on one handle
//!   are serialized by a per-handle mutex; a retry at an older offset rebuilds the key set **as of that
//!   offset** ([`keys_as_of`]) before replaying. Concurrent **live** requests at the *same* (handle, offset) are
//!   identical, so they are **coalesced**: one leader does the read+apply and every waiter receives the
//!   same response (write-fanout: N clients long-poll one handle at one offset — serializing them behind
//!   the mutex would hand each a full long-poll timeout in turn). A live request keeps re-polling the
//!   ds stream until data arrives or `ELECTRIC_LIVE_TIMEOUT_MS` (default 20000, Electric-like ~20s)
//!   elapses, then returns Electric's `200` `up-to-date` control message. Every individual ds poll is bounded by the *remaining* deadline
//!   (`poll_live_until`): the ds server's own long-poll window can exceed ours, and an idle stream
//!   must still produce that response on time.
//! - Identical shape definitions (same table + canonical `where` + columns) share ONE engine shape
//!   (`create_shape` with `share = true`): concurrent clients and returning clients rejoin the same
//!   retained stream instead of re-backfilling from Postgres. Handles stay **per client** (each
//!   snapshot mints a unique handle id over the shared stream), so cursor state is never contended
//!   across clients; each live handle holds one subscription on the shared shape — and every request
//!   on a handle RENEWS that subscription's lease (ADR-0008), in memory, so a long-polling client
//!   keeps its claim without knowing the id exists.
//! - **A handle outlives the registry entry that serves it.** Its text names its shape, the stream
//!   scope it belongs to and its claim, and is unique across boots. A request whose handle the registry
//!   does not know (after a restart or idle eviction) **resumes** when that is safe
//!   ([`resume_handle`]): the definition the client resent must resolve to the handle's shape id — ids
//!   are never re-minted, so that is the stream its offset points into — and the offset must be a
//!   position in that stream. A handle that cannot resume gets the standard `409 must-refetch`, and the
//!   re-snapshot rejoins the retained shape (reactivating it if dormant) rather than rebuilding from
//!   Postgres. A failure that says nothing about the handle stays retryable, as on every other path.
//! - Handles are evicted after sitting idle for `ELECTRIC_HANDLE_TTL` seconds (default 600). This is
//!   **handle-state cleanup only**: the per-handle cursor state is dropped and the shape subscription
//!   released — the underlying engine shape and its durable stream are retained and follow the
//!   retention lifecycle (idle → dormant → evicted; see [`crate::retention`]).
//! - Errors split by who must act: validation failures (bad `where`, unknown table/column, missing
//!   handle, table/handle mismatch) are 400 with an Electric-style `{"message": …}` body — the client
//!   treats 400 as fatal. Everything else (durable-streams hiccups, engine failures) is 500 so the
//!   client retries.
//! - Values are encoded as Postgres **text** (`bool`→`"true"`/`"false"`, text as-is); Electric's default
//!   value-mapper only coerces int/float, leaving these as strings — matching the oracle's stringified
//!   comparison.

use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::hash::Hash;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use axum::extract::{Query, State};
use axum::http::{HeaderMap, HeaderName, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use serde::Deserialize;
use tokio::sync::watch;

use crate::ds::{Envelope, ReadResult};
use crate::engine::Engine;
use crate::heap_size::HeapSize;
use crate::schema::{ColumnType, PK_SEP, TableSchema, unescape_key_component};
use crate::table_ref::TableRef;

#[derive(Debug, Deserialize)]
pub struct ShapeParams {
    /// The requested table. Electric clients send `public.users`; a bare `users` is the same
    /// `public.users` (the ingress sugar). RESOLVED, not stripped: `private.users` is
    /// `private.users`, and if that table is not served the request is a 400 — the prefix-stripping
    /// this replaced answered it with `public.users`' rows.
    table: TableRef,
    #[serde(default)]
    offset: Option<String>,
    #[serde(default)]
    handle: Option<String>,
    #[serde(default, rename = "where")]
    where_: Option<String>,
    #[serde(default)]
    columns: Option<String>,
    #[serde(default)]
    live: Option<String>,
    #[serde(default)]
    cursor: Option<String>,
    #[serde(default)]
    replica: Option<String>,
    /// `ELECTRIC_SECRET` auth: either query param, when the secret is configured, must equal it.
    #[serde(default)]
    secret: Option<String>,
    #[serde(default)]
    api_secret: Option<String>,
}

/// A registered handle. The registry maps handle → `Arc<HandleEntry>`; the mutable cursor state
/// (`keys`/`offset`) sits behind a per-handle async mutex held across read+apply so two concurrent
/// requests on one handle (client abort + retry) cannot interleave and corrupt the key set. The
/// registry lock itself is only ever held briefly (get/insert/remove), never across I/O.
struct HandleEntry {
    stream_path: String,
    /// The underlying (shared) engine shape this handle subscribes to. Handles are per-client —
    /// identical definitions share ONE engine shape/stream but each snapshot mints its own handle,
    /// so per-handle cursor state is never contended across clients — and each live handle holds
    /// exactly one shape subscription, released when the handle is evicted.
    shape_id: String,
    /// The **subscription id** this handle's create/join took (ADR-0008), so the idle evictor
    /// releases exactly this handle's claim rather than "one of them". An Electric client never
    /// sees it: the handle is its name for the same thing.
    subscription: String,
    table: TableRef,
    pk_cols: Vec<String>,
    /// When this handle was last touched by a request — drives idle-TTL eviction.
    last_access: std::sync::Mutex<Instant>,
    state: tokio::sync::Mutex<HandleState>,
    /// In-flight **live** long-polls keyed by request offset. Concurrent live requests at the same
    /// (handle, offset) are identical, so the first arrival (the leader) does the read+apply while
    /// every other one awaits the published [`ReadOutcome`] on the watch channel instead of queueing
    /// behind the state mutex (which would hand each waiter a full long-poll timeout in turn). Only
    /// the leader touches [`HandleState`]. Lock order: this lock is only ever held briefly and never
    /// across I/O or the state mutex.
    live_inflight: std::sync::Mutex<HashMap<String, watch::Receiver<LiveSlot>>>,
}

/// `None` until the leader publishes; then the shared result every coalesced waiter clones.
type LiveSlot = Option<Result<ReadOutcome, ApiError>>;

impl HandleEntry {
    fn touch(&self) {
        *self.last_access.lock().unwrap() = Instant::now();
    }
}

/// Per-handle live state: which keys the client currently holds (to pick insert vs update) and the last
/// offset we served it.
struct HandleState {
    keys: HashSet<String>,
    offset: String,
}

/// Where a resume stands: `Ok(None)` is a definite refusal, `409 must-refetch`, because the handle
/// cannot resume. An `Err` says nothing about the handle: the client keeps it and retries.
type ResumeOutcome<T> = Result<Option<T>, ApiError>;

/// Concurrent callers of one key share one run of its work. A cancelled runner hands over to the next
/// caller, and nothing is kept once the last caller has its answer.
struct Flights<K, V> {
    slots: std::sync::Mutex<HashMap<K, Arc<tokio::sync::OnceCell<V>>>>,
}

impl<K, V> Default for Flights<K, V> {
    fn default() -> Self {
        Self { slots: std::sync::Mutex::default() }
    }
}

impl<K: Eq + Hash + Clone, V: Clone> Flights<K, V> {
    async fn run<F, Fut>(&self, key: K, work: F) -> V
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = V>,
    {
        /// Every clone and drop of a slot happens under the map lock, so a count of 1 means this
        /// was the last caller; a later one starts a new flight rather than reading an old answer.
        struct Leave<'a, K: Eq + Hash, V> {
            flights: &'a Flights<K, V>,
            key: K,
            slot: Option<Arc<tokio::sync::OnceCell<V>>>,
        }
        impl<K: Eq + Hash, V> Drop for Leave<'_, K, V> {
            fn drop(&mut self) {
                let mut slots = self.flights.slots.lock().unwrap();
                drop(self.slot.take());
                if slots.get(&self.key).is_some_and(|slot| Arc::strong_count(slot) == 1) {
                    slots.remove(&self.key);
                }
            }
        }
        let slot = self.slots.lock().unwrap().entry(key.clone()).or_default().clone();
        let leave = Leave { flights: self, key, slot: Some(slot) };
        leave.slot.as_ref().expect("held until drop").get_or_init(work).await.clone()
    }
}

#[derive(Default)]
pub(crate) struct HandleRegistry {
    entries: std::sync::Mutex<HashMap<String, Arc<HandleEntry>>>,
    /// One resume at a time per handle, so concurrent ones end in one entry and one claim renewal.
    resumes: Flights<String, ResumeOutcome<Arc<HandleEntry>>>,
    /// One key-set rebuild at a time per (stream path, offset). The registry belongs to one Engine,
    /// which is what makes the path enough: `shape/s5` repeats across sources.
    rebuilds: Flights<(String, String), ResumeOutcome<Arc<HashSet<String>>>>,
    started: OnceLock<()>,
}

pub(crate) fn close_handles(engine: &Engine) {
    // Durable subscriptions retain their existing lease; shutdown adds no per-handle catalog work.
    drop(std::mem::take(&mut *engine.electric_handles.entries.lock().unwrap()));
}

/// Observe the serving Engine's registry without creating or renewing a handle.
#[cfg(feature = "test-support")]
#[doc(hidden)]
pub async fn registry_usage_for_test(engine: &Engine) -> (usize, usize) {
    let count = engine.electric_handles.entries.lock().unwrap().len();
    (count, ttl_registry_heap_bytes(engine).await)
}

/// Owned-heap estimate of the `/v1/shape` handle registry — the memory probe's
/// `bytes_electric_adapter` term. `HandleEntry`/`HandleState` hold sync/async primitives
/// (`Mutex`, `watch::Receiver`), so this is a hand-rolled walk rather than a `HeapSize` impl:
/// the registry map's own key strings + each entry's owned strings (`stream_path`, `shape_id`,
/// `table`, `pk_cols`) + its cursor state (`keys`/`offset`) + the in-flight live-poll map's key
/// strings (the `watch::Receiver` values are shared channel handles, not uniquely owned, so
/// only their keys are counted).
///
/// Single lock, allocation-free: holds the registry's std `Mutex` for the whole walk (no cloning
/// entries out into a `Vec` first, no second lock for the map's own bucket-overhead estimate) and
/// reads each handle's cursor state with `try_lock` instead of awaiting the async `Mutex` — this
/// is a best-effort byte estimate, not a request path, so a handle mid-request (lock momentarily
/// held) is simply skipped for its `keys`/`offset` term rather than blocked on.
pub(crate) async fn ttl_registry_heap_bytes(engine: &Engine) -> usize {
    let map = engine.electric_handles.entries.lock().unwrap();
    let cap = map.capacity();
    let mut total = (cap * (std::mem::size_of::<(String, Arc<HandleEntry>)>() + 1) * 11) / 10;
    for (id, entry) in map.iter() {
        total += id.heap_bytes()
            + entry.stream_path.heap_bytes()
            + entry.shape_id.heap_bytes()
            + entry.table.heap_bytes()
            + entry.pk_cols.heap_bytes();
        if let Ok(st) = entry.state.try_lock() {
            total += st.keys.heap_bytes() + st.offset.heap_bytes();
        }
        total += entry.live_inflight.lock().unwrap().keys().map(HeapSize::heap_bytes).sum::<usize>();
    }
    total
}

/// Overall deadline for a `live=true` long-poll, decoupled from the ds server's own long-poll timeout:
/// the adapter keeps re-polling the ds stream until data arrives or this elapses, then returns an
/// Electric-compatible `up-to-date` control message.
/// `ELECTRIC_LIVE_TIMEOUT_MS` env var; default 20000 (Electric's ~20s live long-poll).
pub(crate) fn live_timeout() -> Duration {
    static T: OnceLock<Duration> = OnceLock::new();
    *T.get_or_init(|| {
        let ms =
            std::env::var("ELECTRIC_LIVE_TIMEOUT_MS").ok().and_then(|s| s.trim().parse::<u64>().ok()).unwrap_or(20_000);
        Duration::from_millis(ms)
    })
}

/// Idle TTL for `/v1/shape` handles: a handle not touched for this long has its per-handle cursor
/// state evicted and its shape subscription released. The underlying shape + stream are retained
/// (retention lifecycle, see [`crate::retention`]) — this is handle-state cleanup, not shape
/// teardown. `ELECTRIC_HANDLE_TTL` env var, in seconds; default 600 (10 min).
fn handle_ttl() -> Duration {
    static TTL: OnceLock<Duration> = OnceLock::new();
    *TTL.get_or_init(|| {
        let secs = std::env::var("ELECTRIC_HANDLE_TTL").ok().and_then(|s| s.trim().parse::<u64>().ok()).unwrap_or(600);
        Duration::from_secs(secs)
    })
}

/// Start this Engine's shutdown-aware background evictor once that cleans up handles idle longer than [`handle_ttl`]:
/// the per-handle cursor state is dropped and the shape's subscription released (`release_subscription`)
/// — the shape itself is retained and ages through the retention lifecycle. A request arriving
/// with an evicted handle resumes, taking its claim back, or else gets the standard `409 must-refetch`
/// and re-snapshots (rejoining the retained shape).
pub(crate) fn ensure_evictor(engine: &Engine) {
    engine.electric_handles.started.get_or_init(|| {
        let engine = engine.clone();
        engine.shutdown_token().spawn_background(async move {
            let ttl = handle_ttl();
            let mut tick = tokio::time::interval(Duration::from_secs(60));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tick.tick().await;
                let idle: Vec<(String, Arc<HandleEntry>)> = engine
                    .electric_handles
                    .entries
                    .lock()
                    .unwrap()
                    .iter()
                    .filter(|(_, e)| e.last_access.lock().unwrap().elapsed() > ttl)
                    .map(|(id, e)| (id.clone(), e.clone()))
                    .collect();
                for (id, entry) in idle {
                    // Skip a handle mid-request (its state mutex is held). Holding the guard while we
                    // unregister keeps a racing request from mutating state we are tearing down; a
                    // request that snatched the Arc just before removal still reads the retained
                    // stream fine, and its next request resumes or gets 409 must-refetch (registry miss).
                    let Ok(guard) = entry.state.try_lock() else { continue };
                    if entry.last_access.lock().unwrap().elapsed() <= ttl {
                        continue; // touched between the scan and now
                    }
                    let Ok(_control) = engine.admit_control() else {
                        continue; // deployment handoff: keep the handle and its subscription intact
                    };
                    {
                        let mut entries = engine.electric_handles.entries.lock().unwrap();
                        entries.remove(&id);
                        if entries.is_empty() {
                            entries.shrink_to_fit();
                        }
                    }
                    drop(guard);
                    engine.release_subscription(&entry.shape_id, Some(&entry.subscription)).await;
                    tracing::debug!("evicted idle electric handle {id} (shape retained)");
                }
            }
        });
    });
}

fn next_cursor() -> u64 {
    static C: AtomicU64 = AtomicU64::new(1);
    C.fetch_add(1, Ordering::Relaxed)
}

fn col_csv(c: &Option<String>) -> Option<Vec<String>> {
    c.as_ref().map(|s| s.split(',').map(|x| x.trim().to_string()).filter(|x| !x.is_empty()).collect())
}

/// A Postgres type name for the `electric-schema` header. JSONB stays opaque Postgres text inside
/// the engine, but an introspected `jsonb` column must retain its wire type so Electric clients parse
/// the text into a JSON value. Other native types keep the existing coarse compatibility mapping.
fn pg_type(ty: ColumnType, native: Option<&str>) -> &'static str {
    if native == Some("jsonb") {
        return "jsonb";
    }
    match ty {
        ColumnType::Int => "int8",
        ColumnType::Float => "float8",
        ColumnType::Text => "text",
        ColumnType::Bool => "bool",
    }
}

/// Build the `electric-schema` JSON: `{col: {type, pk_index?}}`.
fn schema_json(ts: &TableSchema, columns: &Option<Vec<String>>) -> serde_json::Value {
    let mut map = serde_json::Map::new();
    for (index, (name, ty)) in ts.columns.iter().enumerate() {
        if let Some(cols) = columns {
            if !cols.iter().any(|c| c == name) && name != &ts.pk_name {
                continue;
            }
        }
        let mut entry = serde_json::Map::new();
        let native = ts.pg_types.get(index).and_then(|value| value.as_deref());
        entry.insert("type".into(), serde_json::Value::String(pg_type(*ty, native).into()));
        if name == &ts.pk_name {
            entry.insert("pk_index".into(), serde_json::Value::from(0));
        }
        map.insert(name.clone(), serde_json::Value::Object(entry));
    }
    serde_json::Value::Object(map)
}

/// Re-encode a row JSON value (typed: string/bool/number/null) as Electric text values (`{col: "text"}`).
fn encode_value(v: &serde_json::Value) -> serde_json::Value {
    match v {
        serde_json::Value::Object(m) => {
            serde_json::Value::Object(m.iter().map(|(k, val)| (k.clone(), pg_text(val))).collect())
        }
        other => other.clone(),
    }
}

fn pg_text(v: &serde_json::Value) -> serde_json::Value {
    match v {
        serde_json::Value::Null => serde_json::Value::Null,
        serde_json::Value::Bool(b) => serde_json::Value::String(if *b { "true".into() } else { "false".into() }),
        serde_json::Value::String(s) => serde_json::Value::String(s.clone()),
        serde_json::Value::Number(n) => serde_json::Value::String(n.to_string()),
        other => serde_json::Value::String(other.to_string()),
    }
}

fn change_msg(op: &str, key: &str, value: Option<serde_json::Value>, txid: Option<&str>) -> serde_json::Value {
    let mut m = serde_json::Map::new();
    let mut headers = serde_json::Map::new();
    headers.insert("operation".into(), serde_json::Value::String(op.into()));
    if let Some(txid) = txid {
        headers.insert("txid".into(), serde_json::Value::String(txid.into()));
        if let Ok(txid) = txid.parse::<u64>() {
            headers.insert("txids".into(), serde_json::json!([txid]));
        }
    }
    m.insert("headers".into(), serde_json::Value::Object(headers));
    m.insert("key".into(), serde_json::Value::String(key.into()));
    if let Some(v) = value {
        m.insert("value".into(), v);
    }
    serde_json::Value::Object(m)
}

fn control_msg(control: &str) -> serde_json::Value {
    let mut m = serde_json::Map::new();
    let mut headers = serde_json::Map::new();
    headers.insert("control".into(), serde_json::Value::String(control.into()));
    if control == "up-to-date" {
        headers.insert("global_last_seen_lsn".into(), serde_json::Value::String("0".into()));
    }
    m.insert("headers".into(), serde_json::Value::Object(headers));
    serde_json::Value::Object(m)
}

fn hv(s: &str) -> HeaderValue {
    HeaderValue::from_str(s).unwrap_or_else(|_| HeaderValue::from_static(""))
}

fn respond(messages: Vec<serde_json::Value>, mut headers: HeaderMap, status: StatusCode) -> Response {
    headers.insert(axum::http::header::CONTENT_TYPE, HeaderValue::from_static("application/json"));
    let body = serde_json::to_string(&messages).unwrap_or_else(|_| "[]".into());
    let len = body.len() as u64;
    let mut resp = (status, headers, body).into_response();
    resp.extensions_mut().insert(BodyLen(len));
    resp
}

fn must_refetch() -> Response {
    // No `electric-handle` header: the evicted/unknown handle has no replacement shape yet — the
    // client re-requests with offset=-1 and the snapshot response assigns the fresh handle.
    let mut headers = HeaderMap::new();
    headers.insert(HeaderName::from_static("electric-offset"), hv("-1"));
    respond(vec![control_msg("must-refetch")], headers, StatusCode::CONFLICT)
}

/// Adapter error split by who must act: Electric's client treats **400 as fatal** (it kills the sync
/// loop), so only request-validation failures may use it; anything transient/internal must be a 500 so
/// the client retries. The body mirrors Electric's error shape: `{"message": …}`.
#[derive(Clone, Debug)]
struct ApiError {
    status: StatusCode,
    message: String,
    /// `Retry-After`, in seconds, when the answer is "come back" rather than "you were wrong".
    retry_after: Option<u32>,
    /// The backing shape stream is terminal; Electric must discard this handle and snapshot again.
    must_refetch: bool,
}

impl ApiError {
    fn bad_request(message: impl Into<String>) -> Self {
        ApiError { status: StatusCode::BAD_REQUEST, message: message.into(), retry_after: None, must_refetch: false }
    }

    /// A NEW `live=true` request arriving after the shutdown token flipped.
    ///
    /// Returning an ordinary empty 204 would be technically correct and practically awful: an
    /// Electric client re-polls immediately on a 204, so every live subscriber would spin a tight
    /// poll loop for the whole drain window. A 5xx is what the client backs off on, and
    /// `Retry-After` says for how long — by which time this pod is gone and the client reconnects
    /// to its successor. Polls ALREADY parked are untouched: they return their normal up-to-date
    /// control response with the offset they had (see `poll_live_until`), because they were promised
    /// an answer before any of this started.
    fn draining() -> Self {
        ApiError {
            status: StatusCode::SERVICE_UNAVAILABLE,
            message: "engine is shutting down; retry".to_string(),
            retry_after: Some(1),
            must_refetch: false,
        }
    }

    fn deployment_not_ready() -> Self {
        ApiError {
            status: StatusCode::SERVICE_UNAVAILABLE,
            message: "managed deployment is not ready; retry".to_string(),
            retry_after: Some(1),
            must_refetch: false,
        }
    }

    fn must_refetch() -> Self {
        ApiError {
            status: StatusCode::CONFLICT,
            message: "must-refetch".to_string(),
            retry_after: None,
            must_refetch: true,
        }
    }
}

impl From<anyhow::Error> for ApiError {
    fn from(e: anyhow::Error) -> Self {
        if e.downcast_ref::<crate::engine::ReplayFailed>().is_some() {
            return ApiError {
                status: StatusCode::SERVICE_UNAVAILABLE,
                message: format!("{e:#}"),
                retry_after: None,
                must_refetch: false,
            };
        }
        if e.downcast_ref::<crate::schema::UnknownColumn>().is_some() {
            return ApiError::bad_request(format!("{e:#}"));
        }
        // Matched by TYPE, exactly as the native surface does (`http.rs`): a create that kept losing
        // the same race is not a server fault — the request is valid and the engine is busy retiring
        // things underneath it — so an Electric client is told to come back rather than to treat the
        // shape as broken.
        if e.downcast_ref::<crate::engine::CreateRaced>().is_some()
            || e.downcast_ref::<crate::engine::ControlAdmissionClosed>().is_some()
            || e.downcast_ref::<crate::engine::DeploymentNotReady>().is_some()
            // A deferred reactivation is retryable on this surface too: the shape still exists, so
            // `must-refetch` would throw away a stream that is about to come back.
            || e.downcast_ref::<crate::engine::ReactivationDeferred>().is_some()
        {
            return ApiError {
                status: StatusCode::SERVICE_UNAVAILABLE,
                message: format!("{e:#}"),
                retry_after: Some(1),
                must_refetch: false,
            };
        }
        // A shape retired because its dormant replay exceeded the budget is gone in exactly the
        // sense `must-refetch` describes: the client's stream no longer exists and the answer is a
        // fresh subscription, not a server error.
        if crate::ds::is_stream_gone(&e) || e.downcast_ref::<crate::engine::ReactivationRecreate>().is_some() {
            return ApiError::must_refetch();
        }
        ApiError {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: format!("{e:#}"),
            retry_after: None,
            must_refetch: false,
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        if self.must_refetch {
            return must_refetch();
        }
        let body = serde_json::json!({ "message": self.message }).to_string();
        let len = body.len() as u64;
        let mut headers = HeaderMap::new();
        headers.insert(axum::http::header::CONTENT_TYPE, HeaderValue::from_static("application/json"));
        if let Some(secs) = self.retry_after {
            if let Ok(v) = HeaderValue::from_str(&secs.to_string()) {
                headers.insert(axum::http::header::RETRY_AFTER, v);
            }
        }
        let mut resp = (self.status, headers, body).into_response();
        resp.extensions_mut().insert(BodyLen(len));
        resp
    }
}

/// Page-driven fold of a shape stream into its current key→row map (pure, so unit-testable).
///
/// `done` is set on `up-to-date` or when the next offset stops advancing; an empty page mid-stream
/// does **not** end the fold (that truncated snapshots), but a `closed` page does: a retired stream
/// is terminal, so there is nothing further to wait for.
struct StreamFold {
    /// Key → current row value.
    rows: HashMap<String, serde_json::Value>,
    /// Keys in first-appearance (stream) order. `rows` alone would randomize the snapshot's
    /// row order per request (HashMap iteration), which readers observably depend on — the
    /// engine appends the backfill in a deterministic order and the adapter must not shuffle
    /// it. Deleted keys keep their slot here and are filtered at emission by map membership.
    order: Vec<String>,
    offset: String,
    done: bool,
}

impl StreamFold {
    fn to_tail() -> Self {
        StreamFold { rows: HashMap::new(), order: Vec::new(), offset: "-1".into(), done: false }
    }

    /// The folded rows in stream (first-appearance) order.
    fn ordered_rows(&self) -> impl Iterator<Item = (&String, &serde_json::Value)> {
        self.order.iter().filter_map(|k| self.rows.get_key_value(k))
    }

    fn apply_page(&mut self, r: ReadResult) {
        for env in r.envelopes {
            match env.headers.operation.as_str() {
                "delete" => {
                    self.rows.remove(&env.key);
                }
                _ => {
                    if let Some(v) = env.value {
                        if self.rows.insert(env.key.clone(), v).is_none() {
                            self.order.push(env.key);
                        }
                    }
                }
            }
        }
        let advanced = match r.next_offset {
            Some(n) if n != self.offset => {
                self.offset = n;
                true
            }
            _ => false,
        };
        if r.up_to_date || r.closed || !advanced {
            self.done = true;
        }
    }
}

/// Drive a [`StreamFold`] with catch-up reads until it completes.
async fn drive_fold(engine: &Engine, path: &str, mut fold: StreamFold) -> anyhow::Result<StreamFold> {
    let ds = engine.shape_fold_input(path).await?;
    if !ds.memory_budget().enabled() {
        while !fold.done {
            let page = engine.read_shape_stream(path, &fold.offset, false).await?;
            fold.apply_page(page);
        }
        return Ok(fold);
    }
    let shutdown = engine.shutdown_token();
    while !fold.done {
        // Retention/degradation can change while consuming a page. Reactivation may itself
        // need memory, so perform this check only after releasing the previous page owner.
        engine.shape_fold_input(path).await?;
        let (page, permit) = tokio::select! {
            _ = shutdown.wait() => anyhow::bail!("shape fold interrupted by shutdown"),
            page = ds.read_budgeted(path, &fold.offset, false) => page?,
        };
        fold.apply_page(page);
        // The page has been consumed. Retained map/order/result storage is outside the ledger.
        // Never keep this owner while requesting the next page.
        drop(permit);
    }
    Ok(fold)
}

/// Fold a shape's whole stream (catch-up reads from `-1`) into the current key→row-value map and return
/// it with the stream's tail offset.
async fn materialize(engine: &Engine, path: &str) -> anyhow::Result<(Vec<(String, serde_json::Value)>, String)> {
    let fold = drive_fold(engine, path, StreamFold::to_tail()).await?;
    let rows = fold.ordered_rows().map(|(k, v)| (k.clone(), v.clone())).collect();
    Ok((rows, fold.offset))
}

/// What the key set needs from a stream item. Its row value is skipped as raw text, never parsed: a
/// rebuild over a wide shape holds keys, not a table's worth of JSON.
#[derive(Deserialize)]
struct KeyItem<'a> {
    #[serde(borrow)]
    key: Cow<'a, str>,
    #[serde(borrow, default)]
    value: Option<&'a serde_json::value::RawValue>,
    #[serde(borrow)]
    headers: KeyItemHeaders<'a>,
}

#[derive(Deserialize)]
struct KeyItemHeaders<'a> {
    #[serde(borrow)]
    operation: Cow<'a, str>,
}

/// How far one page took a [`keys_as_of`] fold.
#[derive(Debug, PartialEq, Eq)]
enum KeyFold {
    /// Every item in the page is before the target; the next page starts at this byte.
    Continue(u64),
    /// The target is the boundary after the item just folded.
    Reached,
    /// The target falls inside an item.
    NotAPosition,
    /// The page does not add up to its own next offset, so no count over it can be trusted.
    Misframed,
}

/// Fold one page of a stream into `keys`, stopping exactly at byte `target`.
///
/// Items carry no offset of their own, and a read runs from its offset to the tail, so the only way
/// to stop at a position is to count to it. A stream is stored as its literal wire bytes, `item,` per
/// item, and a page starting at byte `start` is `[` + those bytes without the last comma + `]`. An item
/// that ends `n` bytes into the page's items therefore ends at stream byte `start + n + 1`, which is
/// what an offset names. `next` is the check on that arithmetic, made before any item is folded: a
/// page that does not add up to it is refused rather than trusted, wherever the target falls in it.
fn fold_keys_page(keys: &mut HashSet<String>, body: &str, start: u64, next: u64, target: u64) -> KeyFold {
    let Some(items) = body.strip_prefix('[').and_then(|rest| rest.strip_suffix(']')) else {
        return KeyFold::Misframed;
    };
    let framed_end = if items.is_empty() { start } else { start + items.len() as u64 + 1 };
    if next != framed_end {
        return KeyFold::Misframed;
    }
    let mut position = 0;
    while position < items.len() {
        let mut stream = serde_json::Deserializer::from_str(&items[position..]).into_iter::<KeyItem>();
        let Some(Ok(item)) = stream.next() else { return KeyFold::Misframed };
        position += stream.byte_offset();
        let end = start + position as u64 + 1;
        if end > target {
            return KeyFold::NotAPosition;
        }
        if item.headers.operation == "delete" {
            keys.remove(item.key.as_ref());
        } else if item.value.is_some() {
            keys.insert(item.key.into_owned());
        }
        if end == target {
            return KeyFold::Reached;
        }
        match items.as_bytes().get(position) {
            Some(b',') => position += 1,
            None => break,
            Some(_) => return KeyFold::Misframed,
        }
    }
    KeyFold::Continue(next)
}

/// The key set a client holds when positioned at `offset`, or `None` when the stream has no such
/// position: past its tail, inside an item, not an offset at all, or the stream was retired. A client
/// holding such an offset was not reading this stream, and only a refetch can put that right.
async fn keys_as_of(engine: &Engine, path: &str, offset: &str) -> anyhow::Result<Option<HashSet<String>>> {
    let Some(target) = crate::changelog::offset_bytes(offset) else { return Ok(None) };
    let ds = engine.shape_fold_input(path).await?;
    let shutdown = engine.shutdown_token();
    let mut keys = HashSet::new();
    let mut start = 0;
    let mut from = "-1".to_string();
    while start < target {
        // As in `drive_fold`: retention can change between pages, and a reactivation can need memory,
        // so this runs with the previous page's reservation already released.
        engine.shape_fold_input(path).await?;
        let (page, _permit) = tokio::select! {
            _ = shutdown.wait() => anyhow::bail!("key-set rebuild interrupted by shutdown"),
            page = ds.read_raw(path, &from) => page?,
        };
        let Some(next) = page.next_offset.as_deref().and_then(crate::changelog::offset_bytes) else {
            return Ok(None);
        };
        // `next <= start` is the tail: there is no more stream, and the target is past it.
        if page.closed || next <= start {
            return Ok(None);
        }
        match fold_keys_page(&mut keys, &page.body, start, next, target) {
            KeyFold::Reached => break,
            KeyFold::Continue(byte) => start = byte,
            KeyFold::NotAPosition => return Ok(None),
            KeyFold::Misframed => {
                // Not a client's mistake: on this store no resume can work, and each one refetches.
                tracing::warn!("/v1/shape key-set rebuild of {path}: page at byte {start} is not framed as its offset");
                return Ok(None);
            }
        }
        from = page.next_offset.unwrap_or_default();
    }
    Ok(Some(keys))
}

/// Reconstruct a delete's key columns as Electric text values, matching [`encode_value`].
fn delete_key_value(pk_cols: &[String], key: &str) -> serde_json::Value {
    let mut value = serde_json::Map::new();
    // Escaped components contain no bare PK_SEP, so every separator here is a tuple boundary.
    if pk_cols.len() > 1 && key.split(PK_SEP).count() == pk_cols.len() {
        for (column, part) in pk_cols.iter().zip(key.split(PK_SEP)) {
            value.insert(column.clone(), serde_json::Value::String(unescape_key_component(part)));
        }
    } else if let Some(column) = pk_cols.first() {
        // Single-column keys are never escaped. Malformed composite keys retain the old fallback.
        value.insert(column.clone(), serde_json::Value::String(key.to_string()));
    }
    serde_json::Value::Object(value)
}

/// Classify the engine's absolute `upsert`/`delete` envelopes into Electric `insert`/`update`/`delete`
/// change messages against the client's key set (mutating it as it goes).
fn apply_changes(keys: &mut HashSet<String>, pk_cols: &[String], envelopes: Vec<Envelope>) -> Vec<serde_json::Value> {
    let mut messages = Vec::new();
    for env in envelopes {
        match env.headers.operation.as_str() {
            "delete" => {
                if keys.remove(&env.key) {
                    // Electric's client requires a `value` on every change message (its parser matches
                    // on `"value"`). For a delete we carry the row's old value if present, else all key columns.
                    let value =
                        env.value.as_ref().map(encode_value).unwrap_or_else(|| delete_key_value(pk_cols, &env.key));
                    messages.push(change_msg("delete", &env.key, Some(value), env.headers.txid.as_deref()));
                }
            }
            _ => {
                let value = env.value.as_ref().map(encode_value);
                if keys.contains(&env.key) {
                    messages.push(change_msg("update", &env.key, value, env.headers.txid.as_deref()));
                } else {
                    keys.insert(env.key.clone());
                    messages.push(change_msg("insert", &env.key, value, env.headers.txid.as_deref()));
                }
            }
        }
    }
    messages
}

/// Schema-validation collector: lets [`CompiledPredicate::compile_with`] walk `IN (SELECT …)` leaves
/// (returning their signature) without registering nodes — used to pre-validate a snapshot request's
/// predicate against the table schema so unknown columns / mistyped literals are a 400, not a 500.
struct ValidateOnly;
impl crate::predicate::SubqueryCollector for ValidateOnly {
    fn collect(
        &mut self,
        table: &TableRef,
        project: &str,
        where_: Option<&crate::predicate::PredicateJson>,
    ) -> anyhow::Result<crate::predicate::SubquerySig> {
        Ok(crate::predicate::subquery_sig(table, project, where_))
    }
}

/// The result of one positioned read on a handle, cloneable to every coalesced waiter. `body: None`
/// is reserved for a body-less response; normal live deadline and data responses carry serialized
/// JSON message arrays.
/// `retired` short-circuits both: the shape's stream was closed by the engine (retirement), so the
/// caller answers `409 must-refetch` — the same answer an evicted handle already gets. Coalesced
/// waiters at the same offset all receive it, which is correct: the shape is gone for all of them.
#[derive(Clone)]
struct ReadOutcome {
    offset: String,
    up_to_date: bool,
    cursor: u64,
    body: Option<String>,
    retired: bool,
}

/// Coalesce concurrent live requests at one (handle, offset): the first arrival becomes the leader,
/// runs `work` (the serialized resync+read+apply path) and publishes its result; every concurrent
/// caller at the same offset awaits that result instead of re-running `work`. If the leader vanishes
/// without publishing (client disconnect drops its future mid-poll), the channel closes and waiters
/// re-elect a leader (`work` is re-buildable via `Fn`).
async fn coalesce_live<F, Fut>(entry: &HandleEntry, offset: &str, work: F) -> Result<ReadOutcome, ApiError>
where
    F: Fn() -> Fut,
    Fut: Future<Output = Result<ReadOutcome, ApiError>>,
{
    enum Role {
        Leader(watch::Sender<LiveSlot>),
        Waiter(watch::Receiver<LiveSlot>),
    }
    loop {
        let role = {
            let mut map = entry.live_inflight.lock().unwrap();
            match map.get(offset) {
                Some(rx) => Role::Waiter(rx.clone()),
                None => {
                    let (tx, rx) = watch::channel(None);
                    map.insert(offset.to_string(), rx);
                    Role::Leader(tx)
                }
            }
        };
        match role {
            Role::Leader(tx) => {
                // Unregister on scope exit *and* on cancellation (the future being dropped), so an
                // aborted leader never leaves a dead in-flight slot that waiters would hang on.
                struct Unregister<'a>(&'a HandleEntry, &'a str);
                impl Drop for Unregister<'_> {
                    fn drop(&mut self) {
                        self.0.live_inflight.lock().unwrap().remove(self.1);
                    }
                }
                let _unregister = Unregister(entry, offset);
                let result = work().await;
                // The map still holds a receiver until `_unregister` drops, so send cannot fail; late
                // arrivals that grabbed a receiver before removal see the value immediately.
                let _ = tx.send(Some(result.clone()));
                return result;
            }
            Role::Waiter(mut rx) => {
                loop {
                    if let Some(result) = rx.borrow_and_update().clone() {
                        return result;
                    }
                    if rx.changed().await.is_err() {
                        break; // leader cancelled without publishing — re-elect
                    }
                }
            }
        }
    }
}

/// The live polling loop: repeatedly `read(from)` until a page carries envelopes, the stream is
/// **closed**, or `deadline` elapses. Each poll is bounded by the **remaining** deadline — the ds
/// server holds an idle
/// long-poll far longer than our window, so an unbounded read would blow straight through the
/// deadline (observed: idle `live=true` requests hanging >60s instead of an up-to-date response at ~20s). On expiry
/// the last empty page is returned so a `next_offset` advanced past empty pages is preserved;
/// expiry mid-poll (no page yet) yields an offset-less empty result, leaving the handle offset
/// unchanged. A `closed` page ends the loop immediately: the engine retired the shape, so the stream
/// can never grow — re-polling would hammer the storage server (a closed stream answers a long-poll
/// at once) for the whole live window.
pub(crate) async fn poll_live_until<F, Fut, E>(
    mut from: String,
    deadline: Instant,
    shutdown: &crate::shutdown::ShutdownToken,
    mut read: F,
) -> Result<ReadResult, E>
where
    F: FnMut(String) -> Fut,
    Fut: Future<Output = Result<ReadResult, E>>,
{
    let mut last = ReadResult { envelopes: Vec::new(), next_offset: None, up_to_date: false, closed: false };
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Ok(last);
        }
        // Shutdown ends the poll like an expired deadline: the client gets its up-to-date control response with the offset
        // it had, and re-polls the next process. Without this a parked `live=true` request would
        // hold the whole termination grace hostage for its full ~20 s window — the single loudest
        // symptom of an engine that does not shut down gracefully.
        let page = tokio::select! {
            biased;
            _ = shutdown.wait() => return Ok(last),
            r = tokio::time::timeout(remaining, read(from.clone())) => r,
        };
        match page {
            Err(_elapsed) => return Ok(last),
            Ok(page) => {
                let page = page?;
                if page.closed || !page.envelopes.is_empty() {
                    return Ok(page);
                }
                if let Some(n) = &page.next_offset {
                    from = n.clone();
                }
                last = page;
            }
        }
    }
}

/// The serialized (per-handle state mutex) resync+read+apply path shared by non-live requests and the
/// live leader. Holds the state mutex across the whole read loop so only one request mutates
/// [`HandleState`]; for `live`, keeps re-polling the ds stream via [`poll_live_until`] until data
/// arrives or [`live_timeout`] elapses, every poll bounded by the remaining deadline.
async fn positioned_read(
    engine: &Engine,
    entry: &HandleEntry,
    offset: &str,
    live: bool,
) -> Result<ReadOutcome, ApiError> {
    let mut st = entry.state.lock().await;
    if st.offset != offset {
        // The client resumed at a different (older) offset than we last served: rebuild the key set
        // **as of that offset** (fold -1..=offset), then replay from there. Folding to the tail instead
        // would silently drop a delete of a key absent at tail and emit updates for reinserted keys.
        st.keys = keys_as_of(engine, &entry.stream_path, offset).await?.ok_or_else(ApiError::must_refetch)?;
        st.offset = offset.to_string();
    }
    let from = offset.to_string();
    let r = if live {
        let deadline = Instant::now() + live_timeout();
        let read_engine = engine.clone();
        let path = entry.stream_path.clone();
        let shutdown = engine.shutdown_token();
        poll_live_until(from, deadline, &shutdown, move |f| {
            let engine = read_engine.clone();
            let path = path.clone();
            async move { engine.read_shape_stream(&path, &f, true).await }
        })
        .await?
    } else {
        engine.read_shape_stream(&entry.stream_path, &from, false).await?
    };

    // The engine retired this shape (its stream was closed before being deleted): answer
    // `409 must-refetch` — identical to an evicted handle — instead of serving a dead stream or
    // failing with a 500 once the delete lands. Handle state is left for the TTL to reap; the client
    // re-requests at offset=-1 and gets a fresh shape + handle.
    if r.closed {
        drop(st);
        return Ok(ReadOutcome {
            offset: offset.to_string(),
            up_to_date: false,
            cursor: next_cursor(),
            body: None,
            retired: true,
        });
    }

    if let Some(n) = &r.next_offset {
        st.offset = n.clone();
    }

    // Live deadline reached with no new data: current Electric answers 200 with its up-to-date
    // control message plus the normal cursor headers. A plain 200 `[]` would make the client busy-loop.
    if live && r.envelopes.is_empty() {
        let served_offset = st.offset.clone();
        drop(st);
        entry.touch();
        return Ok(ReadOutcome {
            offset: served_offset,
            up_to_date: true,
            cursor: next_cursor(),
            body: Some(serde_json::to_string(&vec![control_msg("up-to-date")]).unwrap_or_else(|_| "[]".into())),
            retired: false,
        });
    }

    let mut messages = apply_changes(&mut st.keys, &entry.pk_cols, r.envelopes);
    if r.up_to_date {
        messages.push(control_msg("up-to-date"));
    }
    let served_offset = st.offset.clone();
    drop(st);
    entry.touch();
    Ok(ReadOutcome {
        offset: served_offset,
        up_to_date: r.up_to_date,
        cursor: next_cursor(),
        body: Some(serde_json::to_string(&messages).unwrap_or_else(|_| "[]".into())),
        retired: false,
    })
}

/// Response body size in bytes, stamped on every `/v1/shape` response so [`shape`] can emit the byte
/// metrics without re-reading (and consuming) the response body.
#[derive(Clone, Copy)]
struct BodyLen(u64);

/// `401 {"message":"Unauthorized"}` for a request that fails the `ELECTRIC_SECRET` check.
fn unauthorized() -> Response {
    let body = r#"{"message":"Unauthorized"}"#;
    let mut headers = HeaderMap::new();
    headers.insert(axum::http::header::CONTENT_TYPE, HeaderValue::from_static("application/json"));
    let mut resp = (StatusCode::UNAUTHORIZED, headers, body).into_response();
    resp.extensions_mut().insert(BodyLen(body.len() as u64));
    resp
}

/// 503 for a degraded engine (see `Engine::ensure_not_degraded`): serving a shape would hand out
/// membership the engine knows is wrong, or rows from an epoch the engine is no longer bound to
/// (ADR-0004). Carries the control plane's `error` body — this is the engine refusing outright, not
/// a protocol-level shape error, so the reason travels verbatim from the same typed refusal the
/// native surface returns.
fn degraded(why: &anyhow::Error) -> Response {
    let body = serde_json::json!({ "error": format!("{why:#}") }).to_string();
    let len = body.len() as u64;
    let mut headers = HeaderMap::new();
    headers.insert(axum::http::header::CONTENT_TYPE, HeaderValue::from_static("application/json"));
    // Boot/ownership admission is temporary. Terminal replay failure has a distinct type and
    // intentionally gets no Retry-After, just like the conversion from shape_inner.
    if why.downcast_ref::<crate::engine::DeploymentNotReady>().is_some() {
        headers.insert(axum::http::header::RETRY_AFTER, HeaderValue::from_static("1"));
    }
    let mut resp = (StatusCode::SERVICE_UNAVAILABLE, headers, body).into_response();
    resp.extensions_mut().insert(BodyLen(len));
    resp
}

pub async fn shape(
    State(engine): State<Engine>,
    headers: HeaderMap,
    Query(p): Query<ShapeParams>,
    // Raw query pairs (decoded) for `params` — bracket form `params[1]=…` isn't a single serde field.
    Query(raw_pairs): Query<Vec<(String, String)>>,
) -> Response {
    ensure_evictor(&engine);
    let start = Instant::now();
    let live = p.live.as_deref() == Some("true");
    // `root_table` tag: the canonical `schema.name`, computed before shape_inner moves p. Qualified,
    // like every other spelling of a table — two same-named tables in different schemas are two
    // series, not one.
    let root_table = p.table.to_string();

    // Mighty forwards its server-held gateway credential as a bearer, keeping it out of URLs.
    // The controller credential has separate authority and must never authorize this surface.
    let gateway_bearer = crate::config::secret().is_some_and(|expected| {
        headers
            .get(axum::http::header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.strip_prefix("Bearer "))
            .is_some_and(|provided| crate::config::secret_matches(expected, provided))
    });
    let resp = if !gateway_bearer
        && !crate::config::secret_ok(crate::config::secret(), p.secret.as_deref(), p.api_secret.as_deref())
    {
        unauthorized()
    } else if let Err(e) = engine.ensure_not_degraded() {
        degraded(&e)
    } else {
        match shape_inner(engine.clone(), p, &raw_pairs).await {
            Ok(resp) => resp,
            Err(e) => {
                tracing::warn!("/v1/shape error ({}): {}", e.status, e.message);
                e.into_response()
            }
        }
    };

    let status = resp.status().as_u16();
    let bytes = resp.extensions().get::<BodyLen>().map(|b| b.0).unwrap_or(0);
    engine.metrics.emitter().serve_shape(&root_table, live, status, start.elapsed(), bytes);
    resp
}

/// Resolve + validate `params` (Electric-compatible), then substitute `$N` into the where clause
/// BEFORE parsing, so the predicate/subquery machinery — and the shape's identity/signature — are
/// derived from the substituted text (distinct param values never collide onto one shape). These are
/// client errors (400); a failure past this point (stream creation, backfill, reads) is internal (500).
fn resolve_definition(
    p: &ShapeParams,
    ts: &TableSchema,
    columns: &Option<Vec<String>>,
    raw_pairs: &[(String, String)],
) -> Result<Option<crate::predicate::PredicateJson>, ApiError> {
    let params = crate::params::parse_params(raw_pairs).map_err(ApiError::bad_request)?;
    let where_raw = p.where_.as_deref().unwrap_or("");
    let where_sub = crate::params::substitute(where_raw, &params).map_err(ApiError::bad_request)?;
    let pred = crate::where_sql::parse_where_typed(&where_sub, Some(ts))
        .map_err(|e| ApiError::bad_request(format!("invalid where clause: {e:#}")))?;
    if let Some(cols) = columns {
        for c in cols {
            ts.column_index(c).map_err(|e| ApiError::bad_request(format!("{e:#}")))?;
        }
    }
    if let Some(pr) = &pred {
        crate::predicate::CompiledPredicate::compile_with(pr, ts, &mut ValidateOnly)
            .map_err(|e| ApiError::bad_request(format!("invalid where clause: {e:#}")))?;
    }
    Ok(pred)
}

fn handle_entry(
    rec: &crate::engine::ShapeRecord,
    subscription: String,
    table: &TableRef,
    ts: &TableSchema,
    keys: HashSet<String>,
    offset: String,
) -> Arc<HandleEntry> {
    Arc::new(HandleEntry {
        stream_path: rec.stream_path.clone(),
        shape_id: rec.id.clone(),
        subscription,
        table: table.clone(),
        pk_cols: ts.pk_cols.iter().map(|&i| ts.columns[i].0.clone()).collect(),
        last_access: std::sync::Mutex::new(Instant::now()),
        state: tokio::sync::Mutex::new(HandleState { keys, offset }),
        live_inflight: std::sync::Mutex::new(HashMap::new()),
    })
}

/// `<shape id>h<stream scope>.<claim>`. The claim's nonce makes the handle unique across boots, where
/// a process counter repeated; the suffix keeps it disjoint from shape ids. The scope ties it to one
/// source's streams, whose shape ids repeat in every other source's.
fn mint_handle(engine: &Engine, shape_id: &str, subscription: &str) -> String {
    format!("{shape_id}h{}.{subscription}", engine.stream_scope_tag())
}

/// The shape id and claim a handle carries, when it was minted for this Engine's stream scope. A
/// claim must be engine-minted: a handle is client input, and it must not be able to name, and so
/// later release, a native subscriber's claim. An older handle, `<shape id>h<counter>`, carries
/// neither and is answered as an unknown handle.
fn parse_handle<'a>(handle: &'a str, scope: &str) -> Option<(&'a str, &'a str)> {
    let (shape_id, rest) = handle.split_once('h')?;
    let (handle_scope, subscription) = rest.split_once('.')?;
    let shape_number = shape_id.strip_prefix('s')?;
    let claim = subscription.strip_prefix(crate::engine::MINTED_SUB_PREFIX)?;
    let well_formed = handle_scope == scope
        && !shape_number.is_empty()
        && shape_number.bytes().all(|b| b.is_ascii_digit())
        && !claim.is_empty()
        && claim.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-');
    well_formed.then_some((shape_id, subscription))
}

/// The claim a resume holds until its handle is registered. A resume that fails gives it back, and
/// the client's refetch takes a new one; one that is cancelled does the same, detached, because `drop`
/// cannot await. Either is safe if the client resumes after all: the claim is its own to take again.
struct ResumeClaim<'a> {
    engine: &'a Engine,
    shape_id: String,
    subscription: String,
    armed: bool,
}

impl ResumeClaim<'_> {
    async fn give_back(mut self) {
        self.armed = false;
        self.engine.release_subscription(&self.shape_id, Some(&self.subscription)).await;
    }

    fn keep(mut self) {
        self.armed = false;
    }
}

impl Drop for ResumeClaim<'_> {
    fn drop(&mut self) {
        if self.armed {
            let (engine, shape_id, subscription) =
                (self.engine.clone(), self.shape_id.clone(), self.subscription.clone());
            self.engine.shutdown_token().spawn(async move {
                engine.release_subscription(&shape_id, Some(&subscription)).await;
            });
        }
    }
}

/// What a request with an unknown handle resent: everything a resume is judged on.
struct ResumeRequest<'a> {
    params: &'a ShapeParams,
    ts: &'a TableSchema,
    columns: &'a Option<Vec<String>>,
    raw_pairs: &'a [(String, String)],
    handle: &'a str,
    offset: &'a str,
}

/// Sort a failure the way [`ApiError`] already does: what is the client's or the handle's doing is a
/// refusal, and anything retryable stays retryable. Telling a client to drop its rows and download the
/// shape again because storage hiccupped is the reconnect storm a resume exists to prevent, and the
/// refetch would meet the same failure.
fn refused_or_retryable<T>(error: ApiError) -> ResumeOutcome<T> {
    if error.status.is_server_error() { Err(error) } else { Ok(None) }
}

/// Bring back a handle the registry does not know. Never serves rows from a stream the client was not
/// reading: see [`ResumeOutcome`] for what else can come of it.
async fn resume_handle(engine: &Engine, request: ResumeRequest<'_>) -> ResumeOutcome<Arc<HandleEntry>> {
    let scope = engine.stream_scope_tag();
    let Some((shape_id, subscription)) = parse_handle(request.handle, &scope) else { return Ok(None) };
    let work = || async {
        // A resume that finished between this request's lookup and its flight left the entry here.
        if let Some(entry) = engine.electric_handles.entries.lock().unwrap().get(request.handle) {
            return Ok(Some(entry.clone()));
        }
        try_resume(engine, &request, shape_id, subscription).await
    };
    engine.electric_handles.resumes.run(request.handle.to_string(), work).await
}

async fn try_resume(
    engine: &Engine,
    request: &ResumeRequest<'_>,
    shape_id: &str,
    subscription: &str,
) -> ResumeOutcome<Arc<HandleEntry>> {
    let ResumeRequest { params, ts, columns, raw_pairs, handle, offset } = *request;
    let _control = engine.admit_control()?;
    // A definition that no longer validates cannot be the handle's shape; the refetch says why.
    let Ok(pred) = resolve_definition(params, ts, columns, raw_pairs) else { return Ok(None) };
    // Exactly the snapshot's create, naming the handle's claim: a claim the catalog restored is
    // renewed, one that lapsed is taken again, and one this handle holds on another shape is refused.
    let created =
        engine.create_shape_as(&params.table, pred, columns.clone(), false, true, Some(subscription.to_string())).await;
    let rec = match created {
        Ok((rec, _)) => rec,
        Err(error)
            if error.downcast_ref::<crate::engine::SubscriptionConflict>().is_some()
                || error.downcast_ref::<crate::engine::GroupConflict>().is_some() =>
        {
            return Ok(None);
        }
        Err(error) => return refused_or_retryable(error.into()),
    };
    let claim = ResumeClaim { engine, shape_id: rec.id.clone(), subscription: subscription.to_string(), armed: true };
    if rec.id != shape_id {
        claim.give_back().await;
        return Ok(None);
    }
    let keys = match shared_keys_as_of(engine, &rec.stream_path, offset).await {
        Ok(Some(keys)) => keys,
        Ok(None) => {
            claim.give_back().await;
            return Ok(None);
        }
        Err(error) => {
            claim.give_back().await;
            return refused_or_retryable(error);
        }
    };
    let entry = handle_entry(&rec, subscription.to_string(), &params.table, ts, (*keys).clone(), offset.to_string());
    let entry = engine.electric_handles.entries.lock().unwrap().entry(handle.to_string()).or_insert(entry).clone();
    claim.keep();
    Ok(Some(entry))
}

/// [`keys_as_of`], shared by every resume of one stream at one offset. A stream is append-only, so the
/// answer is the same for all of them; a reconnecting fleet at the same offset is one rebuild.
async fn shared_keys_as_of(engine: &Engine, path: &str, offset: &str) -> ResumeOutcome<Arc<HashSet<String>>> {
    let rebuild = || async {
        keys_as_of(engine, path, offset).await.map(|keys| keys.map(Arc::new)).map_err(|error| {
            tracing::warn!("/v1/shape key-set rebuild of {path} at {offset} failed: {error:#}");
            ApiError::from(error)
        })
    };
    engine.electric_handles.rebuilds.run((path.to_string(), offset.to_string()), rebuild).await
}

async fn shape_inner(engine: Engine, p: ShapeParams, raw_pairs: &[(String, String)]) -> Result<Response, ApiError> {
    let offset = p.offset.clone().unwrap_or_else(|| "-1".into());
    let live = p.live.as_deref() == Some("true");
    // Checked first, before the table lookup: a draining engine's cheapest useful answer to a new
    // long-poll is "come back in a second", and it costs one atomic load to give.
    if live && engine.shutdown_token().is_shutting_down() {
        return Err(ApiError::draining());
    }
    // Middleware usually rejects managed standby/quiesce before this adapter runs. Re-check here
    // because an already-admitted shape operation can cross that boundary while validating or
    // creating its shared shape; it must receive the Electric retry contract, never a 500.
    if !engine.managed_public_ready() {
        return Err(ApiError::deployment_not_ready());
    }
    let columns = col_csv(&p.columns);
    let _ = &p.cursor; // accepted (cache-busting hint); we mint our own electric-cursor
    let _ = &p.replica; // accepted; we always send full rows (replica=full semantics)

    let Some(ts) = engine.table_schema(&p.table).await else {
        return Err(ApiError::bad_request(format!("unknown table '{}'", p.table)));
    };

    // A positioned read without a handle is invalid (Electric: "handle required") — silently serving a
    // fresh snapshot here would hand the client a log that doesn't match its offset.
    if offset != "-1" && p.handle.is_none() {
        return Err(ApiError::bad_request("offset was provided without a shape handle"));
    }

    // ---- Snapshot: offset=-1 -> create the shape and emit the current rows as inserts.
    if offset == "-1" {
        let _control = engine.admit_control()?;
        let pred = resolve_definition(&p, &ts, &columns, raw_pairs)?;
        // share = true: identical /v1/shape definitions collapse to ONE engine shape/stream. A
        // returning client's re-snapshot rejoins the retained shape — reactivated inside
        // `create_shape` if it went dormant — instead of re-backfilling from Postgres. The handle
        // minted below stays per-client (see the module docs).
        // The subscription is the ENGINE's to mint here, not the adapter's (ADR-0008): an Electric
        // client has no id of its own to renew or release with — the handle is its name for this
        // claim — and a name the adapter derived from its own counter would collide after a restart
        // with a claim the catalog restored. The minted id comes back with the record and is kept on
        // the handle, so the idle evictor releases exactly this claim.
        let (rec, subscription) = engine.create_shape_as(&p.table, pred, columns.clone(), false, true, None).await?;
        let (rows, tail) = match materialize(&engine, &rec.stream_path).await {
            Ok(v) => v,
            Err(e) => {
                // Failed after taking the create/join subscription: give it back, or the dead
                // subscription pins the shape active forever.
                engine.release_subscription(&rec.id, Some(&subscription)).await;
                return Err(e.into());
            }
        };

        let mut messages = Vec::with_capacity(rows.len() + 1);
        let mut keys = HashSet::with_capacity(rows.len());
        for (key, value) in &rows {
            messages.push(change_msg("insert", key, Some(encode_value(value)), None));
            keys.insert(key.clone());
        }
        messages.push(control_msg("up-to-date"));

        let schema_str = serde_json::to_string(&schema_json(&ts, &columns)).unwrap_or_default();
        // Handles are per-client even though the shape is shared: a unique handle id per snapshot
        // keeps each client's cursor state (key set / offset) private, so one client's live
        // long-poll never head-of-line blocks another's positioned read and offsets never thrash.
        // Each handle holds the one shape subscription its create/join took; the idle evictor releases
        // it with the handle, and a resume renews it by the name the handle carries.
        let handle_id = mint_handle(&engine, &rec.id, &subscription);
        engine
            .electric_handles
            .entries
            .lock()
            .unwrap()
            .insert(handle_id.clone(), handle_entry(&rec, subscription, &p.table, &ts, keys, tail.clone()));

        let mut headers = HeaderMap::new();
        headers.insert(HeaderName::from_static("electric-handle"), hv(&handle_id));
        headers.insert(HeaderName::from_static("electric-offset"), hv(&tail));
        headers.insert(HeaderName::from_static("electric-schema"), hv(&schema_str));
        headers.insert(HeaderName::from_static("electric-up-to-date"), hv(""));
        headers.insert(axum::http::header::CACHE_CONTROL, hv("no-store"));
        return Ok(respond(messages, headers, StatusCode::OK));
    }

    // ---- Live: long-poll from `offset`, emit insert/update/delete reconstructed against the key set.
    let handle = p.handle.clone().unwrap();
    let entry = {
        // Registry lock held only for the lookup; never across I/O or the per-handle lock.
        engine.electric_handles.entries.lock().unwrap().get(&handle).cloned()
    };
    let entry = match entry {
        Some(entry) => entry,
        None => {
            let request =
                ResumeRequest { params: &p, ts: &ts, columns: &columns, raw_pairs, handle: &handle, offset: &offset };
            match resume_handle(&engine, request).await? {
                Some(entry) => entry,
                None => return Ok(must_refetch()),
            }
        }
    };
    entry.touch();
    // ...and the same touch renews the engine-side LEASE this handle holds (ADR-0008). A `/v1/shape`
    // client's reads come through the engine, so unlike a native subscriber it does not have to
    // renew by hand — the poll is the renewal. In memory only: the claim is already in the catalog,
    // and a restart that lapses it is repaired by the resume, which renews it durably.
    engine.renew_subscription_local(&entry.shape_id, &entry.subscription).await;
    if entry.table != p.table {
        return Err(ApiError::bad_request(format!(
            "table '{}' does not match the shape of handle '{handle}' (table '{}')",
            p.table, entry.table
        )));
    }

    // Live requests at one (handle, offset) coalesce onto a single leader; everything else takes the
    // serialized (state-mutex) path directly. Only the leader / the serialized caller mutates state.
    let outcome = if live {
        let work_engine = engine.clone();
        let work_entry = entry.clone();
        let work_offset = offset.clone();
        coalesce_live(&entry, &offset, move || {
            let engine = work_engine.clone();
            let entry = work_entry.clone();
            let offset = work_offset.clone();
            async move { positioned_read(&engine, &entry, &offset, true).await }
        })
        .await?
    } else {
        positioned_read(&engine, &entry, &offset, false).await?
    };

    if outcome.retired {
        return Ok(must_refetch());
    }

    let mut headers = HeaderMap::new();
    headers.insert(HeaderName::from_static("electric-handle"), hv(&handle));
    headers.insert(HeaderName::from_static("electric-offset"), hv(&outcome.offset));
    headers.insert(HeaderName::from_static("electric-cursor"), hv(&outcome.cursor.to_string()));
    if outcome.up_to_date {
        headers.insert(HeaderName::from_static("electric-up-to-date"), hv(""));
    }
    headers.insert(axum::http::header::CACHE_CONTROL, hv("no-store"));
    match outcome.body {
        None => {
            let mut resp = (StatusCode::NO_CONTENT, headers).into_response();
            resp.extensions_mut().insert(BodyLen(0));
            Ok(resp)
        }
        Some(body) => {
            headers.insert(axum::http::header::CONTENT_TYPE, HeaderValue::from_static("application/json"));
            let len = body.len() as u64;
            let mut resp = (StatusCode::OK, headers, body).into_response();
            resp.extensions_mut().insert(BodyLen(len));
            Ok(resp)
        }
    }
}

#[cfg(test)]
#[path = "electric_resume_tests.rs"]
mod resume_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ds::EnvelopeHeaders;
    use crate::schema::{ColumnDef, TableDef};
    use crate::shutdown::ShutdownToken;
    use std::collections::BTreeMap;

    fn budget_config(bytes: u64) -> crate::config::Config {
        crate::config::Config::resolve(|key| {
            (key == "ELECTRIC_CIRCUITS_MEMORY_BUDGET_BYTES").then(|| bytes.to_string())
        })
        .unwrap()
    }

    async fn budget_engine(config: &crate::config::Config, store: Arc<crate::ds::ScriptedStore>) -> Engine {
        let binding =
            crate::store_identity::StoreBound::coupled_v1(&crate::store_identity::StreamScope::in_process_test_scope());
        let mut event = serde_json::to_value(&binding).unwrap();
        event.as_object_mut().unwrap().insert("t".into(), "storeBound".into());
        event.as_object_mut().unwrap().insert("eid".into(), "budget-binding".into());
        store.read_pages.lock().unwrap().push(("bound".into(), true, format!("[{event}]")));
        let ds = crate::ds::DsClient::with_test_store("scripted://budget-revision".into(), store.clone());
        let admission = Engine::admit_store(ds, binding, false).await.unwrap();
        store.read_pages.lock().unwrap().push(("bound".into(), true, format!("[{event}]")));
        let engine = Engine::new_with_config(admission, None, crate::engine::PostgresSetup::EngineManaged, config)
            .await
            .unwrap();
        let mut row = env("upsert", "one", "01");
        row.value = Some(serde_json::json!({"id":"one", "body":"x".repeat(1024)}));
        store
            .pages_by_offset
            .lock()
            .unwrap()
            .insert("-1".into(), ("01".into(), true, serde_json::to_string(&vec![row]).unwrap()));
        engine
    }

    #[tokio::test]
    async fn memory_budget_two_folds_backfill_and_sequencer_read_concurrently() {
        use std::{future::Future, sync::atomic::Ordering, task::Poll};
        let store = Arc::new(crate::ds::ScriptedStore::default());
        let page_bytes = store.caps.ds_read_max_bytes();
        let frame_bytes = crate::pg::backfill_config().append_bytes;
        let config = budget_config(3 * page_bytes + frame_bytes);
        let engine = budget_engine(&config, store.clone()).await;
        let budget = engine.metrics.memory_budget.clone();
        let mut pg = crate::pg::memory_budget_tests::BackfillFixture::new(engine.metrics.clone()).await;
        let gate = Arc::new(crate::ds::BodyGate::default());
        *store.body_gate.lock().unwrap() = Some(gate.clone());
        let ds = engine.shape_fold_input("shape/s1").await.unwrap();
        let mut first = Box::pin(drive_fold(&engine, "shape/s1", StreamFold::to_tail()));
        let mut second = Box::pin(drive_fold(&engine, "shape/s2", StreamFold::to_tail()));
        let mut sequencer = Box::pin(ds.read_budgeted("changes/0", "-1", true));
        let mut backfill = Box::pin(async {
            let scope = crate::pg::SettleScope::internal(&pg.table.table);
            let reader = crate::pg::backfill_reader(&mut pg.client, &pg.table, None, &scope).await.unwrap();
            reader.collect().await.unwrap().0
        });
        let mut pg_arrived = Box::pin(pg.arrived.notified());
        let mut pg_held = false;
        tokio::time::timeout(
            Duration::from_secs(5),
            std::future::poll_fn(|cx| {
                assert!(first.as_mut().poll(cx).is_pending());
                assert!(second.as_mut().poll(cx).is_pending());
                assert!(backfill.as_mut().poll(cx).is_pending());
                assert!(sequencer.as_mut().poll(cx).is_pending());
                assert_eq!(
                    budget.waits.load(Ordering::Relaxed),
                    0,
                    "two folds, a backfill and a sequencer page that fit must not wait for memory"
                );
                pg_held |= !pg_held && pg_arrived.as_mut().poll(cx).is_ready();
                if gate.arrived.load(Ordering::SeqCst) == 3 && pg_held {
                    assert_eq!(budget.held.load(Ordering::Relaxed), 3 * page_bytes + frame_bytes);
                    Poll::Ready(())
                } else {
                    Poll::Pending
                }
            }),
        )
        .await
        .expect("all four inputs must reach their held gates");
        gate.released.notify_waiters();
        pg.release.notify_one();
        let (first, second, rows, page) =
            tokio::time::timeout(Duration::from_secs(5), async { tokio::join!(first, second, backfill, sequencer) })
                .await
                .expect("released inputs must finish");
        assert_eq!(first.unwrap().rows.len(), 1);
        assert_eq!(second.unwrap().rows.len(), 1);
        assert_eq!(rows.len(), 1);
        assert_eq!(
            budget.held.load(Ordering::Relaxed),
            page_bytes,
            "retained decoded rows must not keep fold/backfill frame credits"
        );
        drop(page.unwrap());
        assert_eq!(budget.held.load(Ordering::Relaxed), 0);
    }

    /// Oversized page admission remains exclusive until the in-flight input owner drops.
    /// A completed map is retained result state, so it must not keep the page's reservation.
    #[tokio::test]
    async fn memory_budget_serializes_oversized_shape_folds_until_the_owner_drops() {
        use std::{future::Future, sync::atomic::Ordering, task::Poll};
        let store = Arc::new(crate::ds::ScriptedStore::default());
        let engine = budget_engine(&budget_config(128), store.clone()).await;
        let budget = engine.metrics.memory_budget.clone();
        let gate = Arc::new(crate::ds::BodyGate::default());
        *store.body_gate.lock().unwrap() = Some(gate.clone());
        let mut first = Box::pin(drive_fold(&engine, "shape/s1", StreamFold::to_tail()));
        let mut second = Box::pin(drive_fold(&engine, "shape/s2", StreamFold::to_tail()));
        std::future::poll_fn(|cx| {
            assert!(first.as_mut().poll(cx).is_pending());
            assert!(second.as_mut().poll(cx).is_pending());
            assert_eq!(
                gate.arrived.load(Ordering::SeqCst),
                1,
                "an oversized in-flight page must exclude a second fold page"
            );
            assert_eq!(budget.waits.load(Ordering::Relaxed), 1);
            Poll::Ready(())
        })
        .await;
        drop(first);
        std::future::poll_fn(|cx| {
            assert!(second.as_mut().poll(cx).is_pending());
            assert_eq!(
                gate.arrived.load(Ordering::SeqCst),
                2,
                "dropping the first owner must unblock the next fold page"
            );
            Poll::Ready(())
        })
        .await;
        gate.released.notify_one();
        let second = tokio::time::timeout(Duration::from_secs(5), second).await.unwrap().unwrap();
        assert_eq!(second.rows.len(), 1);
        assert_eq!(budget.held.load(Ordering::Relaxed), 0, "retained result is outside the ledger");
        *store.body_gate.lock().unwrap() = None;
        let third = drive_fold(&engine, "shape/s3", StreamFold::to_tail()).await.unwrap();
        assert_eq!(third.rows, second.rows);
        assert_eq!(budget.waits.load(Ordering::Relaxed), 1, "a retained result must not delay another fold");
    }

    async fn check_fold_lifecycle_between_pages(dormant: bool) {
        use std::{future::Future, task::Poll};
        {
            let store = Arc::new(crate::ds::ScriptedStore::default());
            let engine = budget_engine(&budget_config(1), store.clone()).await;
            store.pages_by_offset.lock().unwrap().get_mut("-1").unwrap().1 = false;
            let gate = Arc::new(crate::ds::BodyGate::default());
            *store.body_gate.lock().unwrap() = Some(gate.clone());
            let mut fold = Box::pin(drive_fold(&engine, "shape/s1", StreamFold::to_tail()));
            std::future::poll_fn(|cx| {
                assert!(fold.as_mut().poll(cx).is_pending());
                Poll::Ready(())
            })
            .await;
            if dormant {
                engine
                    .park_dormant_shape_for_test(
                        "s1",
                        "public.items",
                        crate::changelog::LogPosition { segment: 999, offset: "gone".into() },
                    )
                    .await;
            } else {
                engine.force_degraded();
            }
            *store.body_gate.lock().unwrap() = None;
            gate.released.notify_one();
            let result = tokio::time::timeout(Duration::from_secs(5), fold).await.unwrap();
            assert!(
                result.is_err(),
                "page two must refuse a {} shape",
                if dormant { "dormant, expired" } else { "degraded" }
            );
            assert_eq!(engine.metrics.memory_budget.held.load(std::sync::atomic::Ordering::Relaxed), 0);
        }
    }

    #[tokio::test]
    async fn memory_budget_fold_checks_degradation_between_pages() {
        check_fold_lifecycle_between_pages(false).await;
    }

    #[tokio::test]
    async fn memory_budget_fold_checks_dormancy_between_pages() {
        check_fold_lifecycle_between_pages(true).await;
    }

    #[tokio::test]
    async fn memory_budget_fold_releases_between_pages_while_retaining_rows() {
        use std::sync::atomic::Ordering;
        let store = Arc::new(crate::ds::ScriptedStore::default());
        let engine = budget_engine(&budget_config(1), store.clone()).await;
        {
            let mut pages = store.pages_by_offset.lock().unwrap();
            pages.get_mut("-1").unwrap().1 = false;
            let mut row = env("upsert", "two", "02");
            row.value = Some(serde_json::json!({"id":"two"}));
            pages.insert("01".into(), ("02".into(), true, serde_json::to_string(&vec![row]).unwrap()));
        }
        let fold = tokio::time::timeout(Duration::from_secs(5), drive_fold(&engine, "shape/s1", StreamFold::to_tail()))
            .await
            .expect("next page must not wait on the fold's own previous page")
            .unwrap();
        assert_eq!(fold.order, ["one", "two"]);
        assert_eq!(fold.rows.len(), 2);
        assert_eq!(fold.offset, "02");
        assert_eq!(engine.metrics.memory_budget.waits.load(Ordering::Relaxed), 0);
        assert_eq!(engine.metrics.memory_budget.held.load(Ordering::Relaxed), 0);
    }

    /// An Electric client's recreate signal is the protocol's own `must-refetch` (409 + the control
    /// message), not a 500. A shape retired because its dormant replay exceeded the budget is
    /// exactly that: the stream the client holds is gone and the answer is a fresh subscription.
    #[test]
    fn an_over_budget_reactivation_tells_an_electric_client_to_refetch() {
        let error: ApiError = anyhow::Error::new(crate::engine::ReactivationRecreate {
            shape: "s1".into(),
            reason: crate::engine::RecreateReason::OverBudget,
        })
        .into();
        assert!(error.must_refetch, "an Electric client must be told to refetch, not handed a 500");
        assert_eq!(error.status, StatusCode::CONFLICT);
    }

    /// A deferred reactivation is the one reactivation outcome that is NOT `must-refetch`: the
    /// shape still exists and is about to come back, so telling the client to throw its stream away
    /// would turn a retry into a full re-subscribe.
    #[test]
    fn a_deferred_reactivation_is_an_electric_retryable_response() {
        let error: ApiError =
            anyhow::Error::new(crate::engine::ReactivationDeferred::new("s1", "change-log HEAD unavailable")).into();
        assert!(!error.must_refetch, "the shape is still there; refetching would discard a live stream");
        assert_eq!(error.status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(error.retry_after, Some(1));
    }

    #[test]
    fn managed_deployment_not_ready_is_an_electric_retryable_response() {
        let error: ApiError = anyhow::Error::new(crate::engine::DeploymentNotReady).into();
        assert_eq!(error.status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(error.retry_after, Some(1));
        let response = error.into_response();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(response.headers().get(axum::http::header::RETRY_AFTER), Some(&HeaderValue::from_static("1")));
    }

    #[test]
    fn electric_schema_preserves_introspected_jsonb_type() {
        let columns = BTreeMap::from([
            ("id".to_string(), ColumnDef { ty: ColumnType::Int, pg_type: Some("int8".into()), has_default: false }),
            (
                "metadata".to_string(),
                ColumnDef { ty: ColumnType::Text, pg_type: Some("jsonb".into()), has_default: false },
            ),
        ]);
        let def = TableDef { columns, primary_key: vec!["id".into()], fingerprint: None };
        let schema = TableSchema::from_def(&"conversation".into(), &def).unwrap();

        assert_eq!(schema_json(&schema, &None)["metadata"]["type"], "jsonb");
    }

    fn env(op: &str, key: &str, offset: &str) -> Envelope {
        Envelope {
            type_: "change".into(),
            key: key.into(),
            value: if op == "delete" { None } else { Some(serde_json::json!({ "id": key })) },
            old: None,
            headers: EnvelopeHeaders {
                operation: op.into(),
                txid: None,
                offset: Some(offset.into()),
                lsn: None,
                seq: None,
                last: None,
            },
        }
    }

    fn page(envs: Vec<Envelope>, next: &str, up_to_date: bool) -> ReadResult {
        ReadResult { envelopes: envs, next_offset: Some(next.into()), up_to_date, closed: false }
    }

    fn op_and_key(msg: &serde_json::Value) -> (String, String) {
        (msg["headers"]["operation"].as_str().unwrap().to_string(), msg["key"].as_str().unwrap().to_string())
    }

    // M9: an empty non-up-to-date page mid-stream must not end the fold (that truncated snapshots);
    // the fold only stops on up-to-date or when the next offset stops advancing.
    #[test]
    fn fold_survives_empty_mid_stream_page() {
        let mut fold = StreamFold::to_tail();
        fold.apply_page(page(vec![env("upsert", "k1", "01")], "01", false));
        assert!(!fold.done, "non-empty non-up-to-date page must not end the fold");
        fold.apply_page(page(vec![], "02", false)); // empty page, but offset advanced
        assert!(!fold.done, "empty page with an advancing offset must not end the fold");
        fold.apply_page(page(vec![env("upsert", "k2", "03")], "03", true));
        assert!(fold.done);
        assert_eq!(fold.offset, "03");
        let mut keys: Vec<_> = fold.rows.keys().cloned().collect();
        keys.sort();
        assert_eq!(keys, vec!["k1", "k2"], "snapshot missed rows past the empty page");
    }

    #[test]
    fn fold_stops_when_offset_stalls() {
        let mut fold = StreamFold::to_tail();
        fold.apply_page(page(vec![env("upsert", "k1", "01")], "01", false));
        // Same next offset, no up-to-date: the stream is not advancing — stop (no infinite loop).
        fold.apply_page(page(vec![], "01", false));
        assert!(fold.done);
        assert_eq!(fold.offset, "01");
    }

    // The idle-live-poll bug: the ds server holds an idle long-poll far longer than our live
    // deadline, so each poll must be bounded by the *remaining* deadline or the up-to-date response
    // never fires (observed in-container: idle live requests hung >60s instead of ~20s).
    #[tokio::test(start_paused = true)]
    async fn live_poll_deadline_fires_through_hanging_read() {
        let deadline = Instant::now() + Duration::from_millis(200);
        let r = poll_live_until("00".into(), deadline, &ShutdownToken::new(), |_from| async {
            // Simulates the ds server parking an idle long-poll indefinitely.
            tokio::time::sleep(Duration::from_secs(3600)).await;
            Ok::<ReadResult, ()>(page(vec![], "01", false))
        })
        .await
        .unwrap();
        assert!(r.envelopes.is_empty());
        assert_eq!(r.next_offset, None, "mid-poll expiry must not advance the offset");
    }

    /// The graceful-shutdown property: a `live=true` request parked on an idle stream must return
    /// as soon as the token flips, not at its ~20 s deadline. Without this a single long-poll pins
    /// the pod's whole termination grace.
    #[tokio::test(start_paused = true)]
    async fn live_poll_returns_at_once_when_shutdown_begins() {
        let shutdown = ShutdownToken::new();
        let s2 = shutdown.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            s2.begin();
        });
        let deadline = Instant::now() + Duration::from_secs(20);
        let started = Instant::now();
        let r = poll_live_until("00".into(), deadline, &shutdown, |_from| async {
            tokio::time::sleep(Duration::from_secs(3600)).await;
            Ok::<ReadResult, ()>(page(vec![], "01", false))
        })
        .await
        .unwrap();
        assert!(r.envelopes.is_empty());
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "shutdown must end the poll immediately, not at the live deadline"
        );
    }

    /// A token that is ALREADY flipped when the poll starts must not enter the read at all.
    #[tokio::test(start_paused = true)]
    async fn live_poll_does_not_start_once_shutdown_has_begun() {
        let shutdown = ShutdownToken::new();
        shutdown.begin();
        let deadline = Instant::now() + Duration::from_secs(20);
        let r = poll_live_until("00".into(), deadline, &shutdown, |_from| async {
            panic!("the read must not be entered after shutdown has begun");
            #[allow(unreachable_code)]
            Ok::<ReadResult, ()>(page(vec![], "01", false))
        })
        .await
        .unwrap();
        assert_eq!(r.next_offset, None);
    }

    #[tokio::test(start_paused = true)]
    async fn live_poll_returns_data_before_deadline() {
        let deadline = Instant::now() + Duration::from_secs(20);
        let r = poll_live_until("00".into(), deadline, &ShutdownToken::new(), |_from| async {
            tokio::time::sleep(Duration::from_millis(50)).await;
            Ok::<ReadResult, ()>(page(vec![env("upsert", "k1", "01")], "01", false))
        })
        .await
        .unwrap();
        assert_eq!(r.envelopes.len(), 1, "data arriving before the deadline must be served");
        assert_eq!(r.next_offset.as_deref(), Some("01"));
    }

    #[tokio::test(start_paused = true)]
    async fn live_poll_deadline_after_empty_pages_keeps_advanced_offset() {
        let calls = Arc::new(AtomicU64::new(0));
        let deadline = Instant::now() + Duration::from_millis(500);
        let c = calls.clone();
        let r = poll_live_until("00".into(), deadline, &ShutdownToken::new(), move |from| {
            let c = c.clone();
            async move {
                if c.fetch_add(1, Ordering::SeqCst) == 0 {
                    assert_eq!(from, "00");
                    // Empty page but the offset advanced (e.g. skipped envelopes) — not data.
                    Ok::<ReadResult, ()>(page(vec![], "05", false))
                } else {
                    assert_eq!(from, "05", "later polls must resume from the advanced offset");
                    tokio::time::sleep(Duration::from_secs(3600)).await;
                    Ok(page(vec![], "06", false))
                }
            }
        })
        .await
        .unwrap();
        assert!(r.envelopes.is_empty());
        assert_eq!(
            r.next_offset.as_deref(),
            Some("05"),
            "the last empty page's offset must survive to the deadline response so the client resumes past it"
        );
    }

    // ---- the key set as of an offset ------------------------------------------------------------

    /// A page as durable-streams serves it from byte `start`, with the stream byte each item ends at.
    fn wire_page(items: &[Envelope], start: u64) -> (String, Vec<u64>) {
        let mut body = String::from("[");
        let mut ends = Vec::new();
        for (index, item) in items.iter().enumerate() {
            if index > 0 {
                body.push(',');
            }
            body.push_str(&serde_json::to_string(item).unwrap());
            ends.push(start + body.len() as u64);
        }
        body.push(']');
        (body, ends)
    }

    fn keys_of(keys: &HashSet<String>) -> Vec<&str> {
        let mut keys: Vec<_> = keys.iter().map(String::as_str).collect();
        keys.sort();
        keys
    }

    /// Items carry no offset of their own, so the cut is counted in bytes. A multi-byte value and an
    /// escaped key are where a count in anything else goes wrong.
    #[test]
    fn key_fold_stops_at_every_item_boundary_and_nowhere_else() {
        let mut wide = env("upsert", "雪", "");
        wide.value = Some(serde_json::json!({ "id": "雪", "note": "\"quoted\" ☃ \\ ," }));
        let items = [wide, env("upsert", "k\"2", ""), env("delete", "雪", ""), env("upsert", "k4", "")];
        let (body, ends) = wire_page(&items, 40);
        let next = *ends.last().unwrap();
        let expected: [&[&str]; 4] = [&["雪"], &["k\"2", "雪"], &["k\"2"], &["k\"2", "k4"]];
        for (end, expected) in ends.iter().zip(expected) {
            let mut keys = HashSet::new();
            assert_eq!(fold_keys_page(&mut keys, &body, 40, next, *end), KeyFold::Reached);
            let mut expected = expected.to_vec();
            expected.sort();
            assert_eq!(keys_of(&keys), expected);
            // One byte short of a boundary is inside the item that ends there.
            assert_eq!(fold_keys_page(&mut HashSet::new(), &body, 40, next, *end - 1), KeyFold::NotAPosition);
        }
    }

    /// A delete past the requested offset is not folded in: the client still holds that key, and the
    /// replay has to tell it otherwise. This is the whole reason not to fold to the tail.
    #[test]
    fn key_fold_leaves_a_later_delete_for_the_replay() {
        let items = [env("upsert", "k1", ""), env("upsert", "k2", ""), env("delete", "k2", "")];
        let (body, ends) = wire_page(&items, 0);
        let mut keys = HashSet::new();
        assert_eq!(fold_keys_page(&mut keys, &body, 0, ends[2], ends[1]), KeyFold::Reached);
        assert_eq!(keys_of(&keys), ["k1", "k2"]);
        let messages = apply_changes(&mut keys, &["id".into()], vec![env("delete", "k2", "")]);
        assert_eq!(op_and_key(&messages[0]), ("delete".into(), "k2".into()));
    }

    /// The other half of the same bug: a key absent at the offset and present at the tail is an
    /// `insert` to this client, which Electric's client would reject as an update of a missing row.
    #[test]
    fn key_fold_leaves_a_later_reinsert_for_the_replay() {
        let items = [env("upsert", "k1", ""), env("delete", "k1", ""), env("upsert", "k1", "")];
        let (body, ends) = wire_page(&items, 0);
        let mut keys = HashSet::new();
        assert_eq!(fold_keys_page(&mut keys, &body, 0, ends[2], ends[1]), KeyFold::Reached);
        assert!(keys.is_empty());
        let messages = apply_changes(&mut keys, &["id".into()], vec![env("upsert", "k1", "")]);
        assert_eq!(op_and_key(&messages[0]), ("insert".into(), "k1".into()));
    }

    #[test]
    fn key_fold_continues_past_a_page_that_ends_before_the_target() {
        let (first, first_ends) = wire_page(&[env("upsert", "k1", "")], 0);
        let (second, second_ends) = wire_page(&[env("upsert", "k2", "")], first_ends[0]);
        let target = second_ends[0];
        let mut keys = HashSet::new();
        assert_eq!(fold_keys_page(&mut keys, &first, 0, first_ends[0], target), KeyFold::Continue(first_ends[0]));
        assert_eq!(fold_keys_page(&mut keys, &second, first_ends[0], target, target), KeyFold::Reached);
        assert_eq!(keys_of(&keys), ["k1", "k2"]);
    }

    /// The arithmetic rests on how the store frames a page. A store that frames one differently is
    /// found out by its own next offset, and a resume falls back to a refetch rather than cut wrongly.
    /// The check comes first: a target inside the page, where a cut would land, is the usual case.
    #[test]
    fn key_fold_refuses_a_page_whose_length_disagrees_with_its_next_offset() {
        let (body, ends) = wire_page(&[env("upsert", "k1", ""), env("upsert", "k2", "")], 0);
        let spaced = body.replacen("},{", "}, {", 1);
        for target in [ends[0], ends[1], ends[1] + 100] {
            let mut keys = HashSet::new();
            // A store that counts one more byte per item than it serves.
            assert_eq!(fold_keys_page(&mut keys, &body, 0, ends[1] + 2, target), KeyFold::Misframed);
            assert_eq!(fold_keys_page(&mut keys, &spaced, 0, ends[1], target), KeyFold::Misframed);
            assert!(keys.is_empty(), "a misframed page must contribute nothing");
        }
        // Same length as its next offset says, but not items separated by commas.
        let semicolons = body.replacen("},{", "};{", 1);
        assert_eq!(fold_keys_page(&mut HashSet::new(), &semicolons, 0, ends[1], ends[1]), KeyFold::Misframed);
        assert_eq!(fold_keys_page(&mut HashSet::new(), "not a page", 0, 10, 10), KeyFold::Misframed);
    }

    /// A definite refusal is the protocol's 409. Everything retryable stays what it is everywhere else
    /// on this surface: the client keeps its handle and comes back.
    #[test]
    fn a_resume_refuses_only_what_is_about_the_handle() {
        let refused = [
            ApiError::must_refetch(),
            ApiError::bad_request("no such column"),
            anyhow::Error::new(crate::engine::ReactivationRecreate {
                shape: "s1".into(),
                reason: crate::engine::RecreateReason::OverBudget,
            })
            .into(),
        ];
        for error in refused {
            assert!(matches!(refused_or_retryable::<()>(error), Ok(None)));
        }
        let retryable = [
            anyhow::Error::new(crate::engine::ReactivationDeferred::new("s1", "change-log HEAD unavailable")),
            anyhow::Error::new(crate::engine::DeploymentNotReady),
            anyhow::anyhow!("GET shape/s1: 503"),
        ];
        for error in retryable {
            let outcome = refused_or_retryable::<()>(error.into());
            assert!(outcome.is_err_and(|error| error.status.is_server_error() && !error.must_refetch));
        }
    }

    #[test]
    fn key_fold_ignores_an_upsert_with_no_value_as_the_snapshot_fold_does() {
        let mut bare = env("upsert", "k1", "");
        bare.value = None;
        let (body, ends) = wire_page(&[bare], 0);
        let mut keys = HashSet::new();
        assert_eq!(fold_keys_page(&mut keys, &body, 0, ends[0], ends[0]), KeyFold::Reached);
        assert!(keys.is_empty());
    }

    // ---- handle identity --------------------------------------------------------------------------

    #[test]
    fn a_minted_handle_parses_back_to_its_shape_and_claim() {
        assert_eq!(parse_handle("s12habc.~3f9a-7", "abc"), Some(("s12", "~3f9a-7")));
    }

    #[test]
    fn a_handle_that_cannot_prove_its_claim_or_scope_is_refused() {
        for handle in [
            "s12h4",                // today's format: no scope, no claim
            "s12",                  // not a handle
            "s12hother.~3f9a-7",    // another source's stream scope
            "s12habc.native-claim", // a claim the engine did not mint: a handle must not name it
            "s12habc.~",            // an empty claim
            "s12habc.~3f\r\n",      // not a header value
            "shabc.~3f9a-7",        // no shape number
            "group-1habc.~3f9a-7",  // not an id this adapter mints
        ] {
            assert_eq!(parse_handle(handle, "abc"), None, "{handle}");
        }
    }

    // ---- shared flights ---------------------------------------------------------------------------

    #[tokio::test]
    async fn a_flight_keeps_nothing_after_its_last_caller() {
        let flights = Flights::<&str, u32>::default();
        assert_eq!(flights.run("k", || async { 1 }).await, 1);
        assert!(flights.slots.lock().unwrap().is_empty());
        assert_eq!(flights.run("k", || async { 2 }).await, 2, "a later caller must not read an old answer");
    }

    #[tokio::test]
    async fn concurrent_callers_of_a_flight_share_one_run() {
        let flights = Arc::new(Flights::<&str, u32>::default());
        let runs = Arc::new(AtomicU64::new(0));
        let callers: Vec<_> = (0..16)
            .map(|_| {
                let (flights, runs) = (flights.clone(), runs.clone());
                tokio::spawn(async move {
                    flights
                        .run("k", || async {
                            tokio::time::sleep(Duration::from_millis(50)).await;
                            runs.fetch_add(1, Ordering::SeqCst) as u32
                        })
                        .await
                })
            })
            .collect();
        for caller in callers {
            assert_eq!(caller.await.unwrap(), 0);
        }
        assert_eq!(runs.load(Ordering::SeqCst), 1);
        assert!(flights.slots.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn a_cancelled_flight_leaves_no_slot() {
        let flights = Arc::new(Flights::<&str, u32>::default());
        let runner = flights.clone();
        let cancelled = tokio::spawn(async move {
            runner.run("k", || std::future::pending()).await;
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        cancelled.abort();
        let _ = cancelled.await;
        assert!(flights.slots.lock().unwrap().is_empty());
    }

    #[test]
    fn apply_changes_classifies_against_the_key_set() {
        let mut keys: HashSet<String> = ["k1".to_string()].into_iter().collect();
        let msgs = apply_changes(
            &mut keys,
            &["id".into()],
            vec![env("upsert", "k1", "01"), env("upsert", "k2", "02"), env("delete", "k9", "03")],
        );
        assert_eq!(op_and_key(&msgs[0]), ("update".into(), "k1".into()));
        assert_eq!(op_and_key(&msgs[1]), ("insert".into(), "k2".into()));
        // delete of a key the client never had is suppressed
        assert_eq!(msgs.len(), 2);
    }

    #[test]
    fn apply_changes_composite_delete_carries_every_key_column() {
        let pk_cols = ["message_id".to_string(), "principal".to_string()];
        let key = crate::schema::join_key_components(["message-1", "principal-2"]);
        let mut keys = HashSet::from([key.clone()]);

        let messages = apply_changes(&mut keys, &pk_cols, vec![env("delete", &key, "01")]);

        assert_eq!(messages.len(), 1);
        assert_eq!(op_and_key(&messages[0]), ("delete".into(), key));
        assert_eq!(messages[0]["value"], serde_json::json!({"message_id": "message-1", "principal": "principal-2"}));
        assert!(keys.is_empty());
    }

    #[test]
    fn apply_changes_composite_delete_unescapes_key_components() {
        let pk_cols = ["message_id".to_string(), "principal".to_string()];
        for (message_id, principal) in [
            ("message\u{1f}1", r"principal\2"),
            (r"message\x1f1", "principal\\\u{1f}2"),
            ("", "雪\\"),
            ("message-1", ""),
        ] {
            let key = crate::schema::join_key_components([message_id, principal]);
            let mut keys = HashSet::from([key.clone()]);

            let messages = apply_changes(&mut keys, &pk_cols, vec![env("delete", &key, "01")]);

            assert_eq!(messages.len(), 1);
            assert_eq!(messages[0]["value"], serde_json::json!({"message_id": message_id, "principal": principal}));
        }
    }

    #[test]
    fn apply_changes_single_column_delete_preserves_the_bare_key() {
        for key in ["message-1", "message\u{1f}1", r"message\x1f1", ""] {
            let mut keys = HashSet::from([key.to_string()]);

            let messages = apply_changes(&mut keys, &["id".into()], vec![env("delete", key, "01")]);

            assert_eq!(messages.len(), 1);
            assert_eq!(messages[0]["value"], serde_json::json!({"id": key}));
        }
    }

    #[test]
    fn apply_changes_malformed_composite_delete_falls_back_to_the_first_key_column() {
        let pk_cols = ["message_id".to_string(), "principal".to_string()];
        for key in ["message-1", "message-1\u{1f}principal-2\u{1f}extra"] {
            let mut keys = HashSet::from([key.to_string()]);

            let messages = apply_changes(&mut keys, &pk_cols, vec![env("delete", key, "01")]);

            assert_eq!(messages.len(), 1);
            assert_eq!(messages[0]["value"], serde_json::json!({"message_id": key}));
        }
    }

    #[test]
    fn apply_changes_delete_preserves_a_supplied_value() {
        let pk_cols = ["message_id".to_string(), "principal".to_string()];
        let key = crate::schema::join_key_components(["message-1", "principal-2"]);
        let mut keys = HashSet::from([key.clone()]);
        let mut delete = env("delete", &key, "01");
        delete.value = Some(serde_json::json!({"message_id": "message-1", "principal": "principal-2", "count": 7}));

        let messages = apply_changes(&mut keys, &pk_cols, vec![delete]);

        assert_eq!(messages.len(), 1);
        assert_eq!(
            messages[0]["value"],
            serde_json::json!({"message_id": "message-1", "principal": "principal-2", "count": "7"})
        );
    }

    #[test]
    fn apply_changes_preserves_source_transaction_ids_for_each_live_operation() {
        let mut update = env("upsert", "k1", "01");
        update.headers.txid = Some("3259".into());
        let mut insert = env("upsert", "k2", "02");
        insert.headers.txid = Some("3260".into());
        let mut delete = env("delete", "k3", "03");
        delete.headers.txid = Some("3261".into());
        let mut keys: HashSet<String> = ["k1".into(), "k3".into()].into_iter().collect();

        let messages = apply_changes(&mut keys, &["id".into()], vec![update, insert, delete]);

        assert_eq!(messages[0]["headers"]["txid"], "3259");
        assert_eq!(messages[0]["headers"]["txids"], serde_json::json!([3259]));
        assert_eq!(messages[1]["headers"]["txid"], "3260");
        assert_eq!(messages[1]["headers"]["txids"], serde_json::json!([3260]));
        assert_eq!(messages[2]["headers"]["txid"], "3261");
        assert_eq!(messages[2]["headers"]["txids"], serde_json::json!([3261]));
    }

    #[test]
    fn apply_changes_keeps_opaque_transaction_ids_without_a_numeric_alias() {
        let mut change = env("upsert", "k1", "01");
        change.headers.txid = Some("library-write".into());
        let mut keys = HashSet::new();

        let messages = apply_changes(&mut keys, &["id".into()], vec![change]);

        assert_eq!(messages[0]["headers"]["txid"], "library-write");
        assert!(messages[0]["headers"].get("txids").is_none());
    }

    // ---- live-request coalescing --------------------------------------------------------------

    use std::sync::atomic::AtomicUsize;

    fn test_entry() -> Arc<HandleEntry> {
        Arc::new(HandleEntry {
            stream_path: "s".into(),
            shape_id: "s1".into(),
            subscription: "~test-1".into(),
            table: "t".into(),
            pk_cols: vec!["id".into()],
            last_access: std::sync::Mutex::new(Instant::now()),
            state: tokio::sync::Mutex::new(HandleState { keys: HashSet::new(), offset: "-1".into() }),
            live_inflight: std::sync::Mutex::new(HashMap::new()),
        })
    }

    /// Spawn one coalesced live request whose leader work bumps `calls`, sleeps `work_ms` of (paused)
    /// time, and publishes a canned outcome tagged with `offset`.
    fn spawn_live(
        entry: &Arc<HandleEntry>,
        offset: &'static str,
        calls: &Arc<AtomicUsize>,
        work_ms: u64,
    ) -> tokio::task::JoinHandle<Result<ReadOutcome, ApiError>> {
        let entry = entry.clone();
        let calls = calls.clone();
        tokio::spawn(async move {
            coalesce_live(&entry, offset, move || {
                let calls = calls.clone();
                async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    // Stands in for the ds long-poll: long enough that every spawned request has
                    // registered (leader or waiter) before the leader publishes.
                    tokio::time::sleep(Duration::from_millis(work_ms)).await;
                    Ok(ReadOutcome {
                        offset: format!("{offset}-next"),
                        up_to_date: true,
                        cursor: 1,
                        body: Some("[]".into()),
                        retired: false,
                    })
                }
            })
            .await
        })
    }

    // The write-fanout shape: N concurrent live requests on one handle at one offset must produce ONE
    // leader read, with every request receiving the same response (not N serialized long-polls).
    #[tokio::test]
    async fn concurrent_live_requests_coalesce_to_one_leader() {
        let entry = test_entry();
        let calls = Arc::new(AtomicUsize::new(0));
        let tasks: Vec<_> = (0..200).map(|_| spawn_live(&entry, "07", &calls, 500)).collect();
        for t in tasks {
            let out = t.await.unwrap().expect("every coalesced request gets the leader's response");
            assert_eq!(out.offset, "07-next");
            assert_eq!(out.body.as_deref(), Some("[]"));
        }
        assert_eq!(calls.load(Ordering::SeqCst), 1, "exactly one leader must run the read");
        assert!(entry.live_inflight.lock().unwrap().is_empty(), "in-flight slot must be cleared");
    }

    // Requests at *different* offsets are not identical — each gets its own leader.
    #[tokio::test]
    async fn live_requests_at_different_offsets_do_not_coalesce() {
        let entry = test_entry();
        let calls = Arc::new(AtomicUsize::new(0));
        let a = spawn_live(&entry, "07", &calls, 100);
        let b = spawn_live(&entry, "08", &calls, 100);
        assert_eq!(a.await.unwrap().unwrap().offset, "07-next");
        assert_eq!(b.await.unwrap().unwrap().offset, "08-next");
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert!(entry.live_inflight.lock().unwrap().is_empty());
    }

    // A leader whose request future is dropped (client disconnect mid-poll) must not strand waiters:
    // the watch channel closes and a waiter re-elects itself leader.
    #[tokio::test]
    async fn waiters_reelect_when_the_leader_is_cancelled() {
        let entry = test_entry();
        let calls = Arc::new(AtomicUsize::new(0));
        let leader = spawn_live(&entry, "07", &calls, 500);
        tokio::time::sleep(Duration::from_millis(50)).await; // let the leader register
        let waiter = spawn_live(&entry, "07", &calls, 500);
        tokio::time::sleep(Duration::from_millis(50)).await; // let the waiter subscribe
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        leader.abort();
        let out = waiter.await.unwrap().expect("waiter must recover from a cancelled leader");
        assert_eq!(out.offset, "07-next");
        assert_eq!(calls.load(Ordering::SeqCst), 2, "the waiter re-ran the read as the new leader");
        assert!(entry.live_inflight.lock().unwrap().is_empty());
    }
}
