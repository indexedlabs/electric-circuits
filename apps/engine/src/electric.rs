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
//!   offset** before replaying. Concurrent **live** requests at the *same* (handle, offset) are
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
//!   keeps its claim without knowing the id exists. (A handle does not survive a restart, so there
//!   is nothing to record: a returning client gets `must-refetch` and re-snapshots, taking a fresh
//!   claim.)
//! - Handles are evicted after sitting idle for `ELECTRIC_HANDLE_TTL` seconds (default 600). This is
//!   **handle-state cleanup only**: the per-handle cursor state is dropped and the shape subscription
//!   released — the underlying engine shape and its durable stream are retained and follow the
//!   retention lifecycle (idle → dormant → evicted; see [`crate::retention`]). A late request on the
//!   evicted handle gets the standard `409 must-refetch`; the client's re-snapshot rejoins the
//!   retained shape (reactivating it if dormant) rather than rebuilding from Postgres.
//! - Errors split by who must act: validation failures (bad `where`, unknown table/column, missing
//!   handle, table/handle mismatch) are 400 with an Electric-style `{"message": …}` body — the client
//!   treats 400 as fatal. Everything else (durable-streams hiccups, engine failures) is 500 so the
//!   client retries.
//! - Values are encoded as Postgres **text** (`bool`→`"true"`/`"false"`, text as-is); Electric's default
//!   value-mapper only coerces int/float, leaving these as strings — matching the oracle's stringified
//!   comparison.

use std::collections::{HashMap, HashSet};
use std::future::Future;
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

fn handles() -> &'static std::sync::Mutex<HashMap<String, Arc<HandleEntry>>> {
    static H: OnceLock<std::sync::Mutex<HashMap<String, Arc<HandleEntry>>>> = OnceLock::new();
    H.get_or_init(|| std::sync::Mutex::new(HashMap::new()))
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
pub(crate) async fn ttl_registry_heap_bytes() -> usize {
    let map = handles().lock().unwrap();
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

/// Spawn (once) the background evictor that cleans up handles idle longer than [`handle_ttl`]:
/// the per-handle cursor state is dropped and the shape's subscription released (`release_shape`)
/// — the shape itself is retained and ages through the retention lifecycle. A request arriving
/// with an evicted handle gets the standard `409 must-refetch` and re-snapshots (rejoining the
/// retained shape).
fn ensure_evictor(engine: &Engine) {
    static STARTED: OnceLock<()> = OnceLock::new();
    STARTED.get_or_init(|| {
        let engine = engine.clone();
        tokio::spawn(async move {
            let ttl = handle_ttl();
            let mut tick = tokio::time::interval(Duration::from_secs(60));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tick.tick().await;
                let idle: Vec<(String, Arc<HandleEntry>)> = handles()
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
                    // stream fine, and its next request gets 409 must-refetch (registry miss).
                    let Ok(guard) = entry.state.try_lock() else { continue };
                    if entry.last_access.lock().unwrap().elapsed() <= ttl {
                        continue; // touched between the scan and now
                    }
                    let Ok(_control) = engine.admit_control() else {
                        continue; // deployment handoff: keep the handle and its subscription intact
                    };
                    handles().lock().unwrap().remove(&id);
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

/// `true` iff stream offset `a` sorts after `b`. `"-1"` is the beginning; every other offset is the
/// durable-streams server's fixed-width zero-padded token, whose lexicographic order equals stream
/// order (`{seq:016}_{byte:016}`).
fn offset_after(a: &str, b: &str) -> bool {
    if a == "-1" {
        false
    } else if b == "-1" {
        true
    } else {
        a > b
    }
}

/// Page-driven fold of a shape stream into its current key→row map (pure, so unit-testable).
///
/// `until = Some(off)` rebuilds the state **as of** that offset: envelopes stamped after it are
/// ignored (used when a client retries an older offset — folding to the tail instead would drop
/// deletes / mis-classify inserts in the replayed window). `done` is set on `up-to-date`, when the
/// target offset is passed, or when the next offset stops advancing; an empty page mid-stream does
/// **not** end the fold (that truncated snapshots), but a `closed` page does: a retired stream is
/// terminal, so there is nothing further to wait for.
struct StreamFold {
    /// Key → current row value. When [`Self::keys_only`] is set the values are dropped as they are
    /// folded (stored as `Null`) — see [`Self::up_to`].
    rows: HashMap<String, serde_json::Value>,
    /// Keys in first-appearance (stream) order. `rows` alone would randomize the snapshot's
    /// row order per request (HashMap iteration), which readers observably depend on — the
    /// engine appends the backfill in a deterministic order and the adapter must not shuffle
    /// it. Deleted keys keep their slot here and are filtered at emission by map membership.
    order: Vec<String>,
    offset: String,
    until: Option<String>,
    /// Fold membership only: the caller wants the KEY SET, so a row's value is dropped the moment
    /// its key is recorded. Everything else about the fold is identical — a delete still removes the
    /// key, an upsert still adds it — so the key set is the same either way, at a fraction of the
    /// memory for a wide shape.
    keys_only: bool,
    done: bool,
}

impl StreamFold {
    fn to_tail() -> Self {
        StreamFold {
            rows: HashMap::new(),
            order: Vec::new(),
            offset: "-1".into(),
            until: None,
            keys_only: false,
            done: false,
        }
    }

    /// Rebuild the KEY SET as of `offset`. Values are never retained: the only consumer
    /// ([`keys_as_of`]) throws them away, and a `/v1/shape` catch-up over a wide shape should not
    /// hold a table's worth of JSON to work out which keys the client has.
    fn up_to(offset: &str) -> Self {
        StreamFold {
            rows: HashMap::new(),
            order: Vec::new(),
            offset: "-1".into(),
            until: Some(offset.to_string()),
            keys_only: true,
            done: false,
        }
    }

    /// The folded rows in stream (first-appearance) order.
    fn ordered_rows(&self) -> impl Iterator<Item = (&String, &serde_json::Value)> {
        self.order.iter().filter_map(|k| self.rows.get_key_value(k))
    }

    fn apply_page(&mut self, r: ReadResult) {
        for env in r.envelopes {
            if let (Some(target), Some(stamp)) = (self.until.as_deref(), env.headers.offset.as_deref()) {
                if offset_after(stamp, target) {
                    self.done = true;
                    return;
                }
            }
            match env.headers.operation.as_str() {
                "delete" => {
                    self.rows.remove(&env.key);
                }
                _ => {
                    if let Some(v) = env.value {
                        let v = if self.keys_only { serde_json::Value::Null } else { v };
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
    while !fold.done {
        let offset = fold.offset.clone();
        let r = engine.read_shape_stream(path, &offset, false).await?;
        fold.apply_page(r);
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

/// The key set a client holds when positioned at `offset` (fold `-1..=offset`).
async fn keys_as_of(engine: &Engine, path: &str, offset: &str) -> anyhow::Result<HashSet<String>> {
    let fold = drive_fold(engine, path, StreamFold::up_to(offset)).await?;
    Ok(fold.rows.into_keys().collect())
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
        st.keys = keys_as_of(engine, &entry.stream_path, offset).await?;
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
        match shape_inner(engine, p, &raw_pairs).await {
            Ok(resp) => resp,
            Err(e) => {
                tracing::warn!("/v1/shape error ({}): {}", e.status, e.message);
                e.into_response()
            }
        }
    };

    let status = resp.status().as_u16();
    let bytes = resp.extensions().get::<BodyLen>().map(|b| b.0).unwrap_or(0);
    crate::statsd::serve_shape(&root_table, live, status, start.elapsed(), bytes);
    resp
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
        // Resolve + validate `params` (Electric-compatible), then substitute `$N` into the where clause
        // BEFORE parsing, so the predicate/subquery machinery — and the shape's identity/signature —
        // are derived from the substituted text (distinct param values never collide onto one shape).
        let params = crate::params::parse_params(raw_pairs).map_err(ApiError::bad_request)?;
        let where_raw = p.where_.as_deref().unwrap_or("");
        let where_sub = crate::params::substitute(where_raw, &params).map_err(ApiError::bad_request)?;
        let pred = crate::where_sql::parse_where_typed(&where_sub, Some(&ts))
            .map_err(|e| ApiError::bad_request(format!("invalid where clause: {e:#}")))?;
        // Validate the request shape against the schema up front: these are client errors (400). A
        // failure past this point (stream creation, backfill, reads) is internal (500).
        if let Some(cols) = &columns {
            for c in cols {
                if ts.column_index(c).is_err() {
                    return Err(ApiError::bad_request(format!("unknown column '{c}'")));
                }
            }
        }
        if let Some(pr) = &pred {
            crate::predicate::CompiledPredicate::compile_with(pr, &ts, &mut ValidateOnly)
                .map_err(|e| ApiError::bad_request(format!("invalid where clause: {e:#}")))?;
        }
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
        // The suffix keeps handle ids disjoint from shape ids. Each handle holds the one shape
        // subscription its create/join took; the idle evictor releases it with the handle.
        let handle_id = format!("{}h{}", rec.id, next_cursor());
        handles().lock().unwrap().insert(
            handle_id.clone(),
            Arc::new(HandleEntry {
                stream_path: rec.stream_path.clone(),
                shape_id: rec.id.clone(),
                subscription: subscription.clone(),
                table: p.table.clone(),
                pk_cols: ts.pk_cols.iter().map(|&i| ts.columns[i].0.clone()).collect(),
                last_access: std::sync::Mutex::new(Instant::now()),
                state: tokio::sync::Mutex::new(HandleState { keys, offset: tail.clone() }),
                live_inflight: std::sync::Mutex::new(HashMap::new()),
            }),
        );

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
        let map = handles().lock().unwrap();
        match map.get(&handle) {
            Some(e) => e.clone(),
            None => return Ok(must_refetch()),
        }
    };
    entry.touch();
    // ...and the same touch renews the engine-side LEASE this handle holds (ADR-0008). A `/v1/shape`
    // client's reads come through the engine, so unlike a native subscriber it does not have to
    // renew by hand — the poll is the renewal. In memory only: a handle does not survive a restart.
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
mod tests {
    use super::*;
    use crate::ds::EnvelopeHeaders;
    use crate::schema::{ColumnDef, TableDef};
    use crate::shutdown::ShutdownToken;
    use std::collections::BTreeMap;

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

    // C2: rebuilding the key set as of the client's *requested* offset (not the tail).
    /// `up_to` is a KEY-SET fold: the values never reach memory, but membership is identical (a
    /// delete past the requested offset is still ignored, an upsert still adds the key).
    #[test]
    fn fold_up_to_keeps_keys_and_drops_values() {
        let mut fold = StreamFold::up_to("02");
        fold.apply_page(page(vec![env("upsert", "k1", "01"), env("upsert", "k2", "02")], "02", true));
        assert_eq!(fold.rows.len(), 2);
        assert!(fold.rows.values().all(|v| v.is_null()), "a key-set fold must not retain row values: {:?}", fold.rows);
        // The snapshot fold, by contrast, keeps them — it IS the response body.
        let mut snap = StreamFold::to_tail();
        snap.apply_page(page(vec![env("upsert", "k1", "01")], "01", true));
        assert!(snap.rows.values().all(|v| !v.is_null()));
    }

    #[test]
    fn fold_up_to_stops_at_the_requested_offset() {
        // Stream: insert k1 @01, insert k2 @02, delete k2 @03. A client at offset 02 holds {k1, k2}.
        let mut fold = StreamFold::up_to("02");
        fold.apply_page(page(
            vec![env("upsert", "k1", "01"), env("upsert", "k2", "02"), env("delete", "k2", "03")],
            "03",
            true,
        ));
        assert!(fold.done);
        let mut keys: Vec<_> = fold.rows.keys().cloned().collect();
        keys.sort();
        assert_eq!(keys, vec!["k1", "k2"], "the delete past the requested offset must not be folded in");
    }

    // C2: a delete in the replayed window must reach the client. Folding to the TAIL loses k2 (absent
    // at tail), so the replayed delete was silently dropped and the client kept a deleted row forever.
    #[test]
    fn replay_emits_delete_for_row_deleted_in_the_replayed_window() {
        let all = vec![env("upsert", "k1", "01"), env("upsert", "k2", "02"), env("delete", "k2", "03")];

        // Rebuild as of the client's offset (02)...
        let mut fold = StreamFold::up_to("02");
        fold.apply_page(page(all.clone(), "03", true));
        let mut keys: HashSet<String> = fold.rows.into_keys().collect();

        // ...then replay everything after it.
        let replay: Vec<Envelope> =
            all.into_iter().filter(|e| offset_after(e.headers.offset.as_deref().unwrap(), "02")).collect();
        let msgs = apply_changes(&mut keys, &["id".into()], replay);
        assert_eq!(msgs.len(), 1);
        assert_eq!(op_and_key(&msgs[0]), ("delete".into(), "k2".into()));
        assert!(!keys.contains("k2"));
    }

    // C2: an insert in the replayed window whose key exists at TAIL must be emitted as an insert (the
    // tail-state rebuild classified it as `update`, which Electric's client rejects as update-of-missing).
    #[test]
    fn replay_emits_insert_for_row_reinserted_after_the_replayed_offset() {
        let all = vec![env("upsert", "k1", "01"), env("delete", "k1", "02"), env("upsert", "k1", "03")];

        let mut fold = StreamFold::up_to("02");
        fold.apply_page(page(all.clone(), "03", true));
        let mut keys: HashSet<String> = fold.rows.into_keys().collect();
        assert!(keys.is_empty(), "as of offset 02 the client holds no rows");

        let replay: Vec<Envelope> =
            all.into_iter().filter(|e| offset_after(e.headers.offset.as_deref().unwrap(), "02")).collect();
        let msgs = apply_changes(&mut keys, &["id".into()], replay);
        assert_eq!(msgs.len(), 1);
        assert_eq!(op_and_key(&msgs[0]), ("insert".into(), "k1".into()));
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

    #[test]
    fn offset_ordering() {
        assert!(offset_after("02", "01"));
        assert!(!offset_after("01", "02"));
        assert!(!offset_after("-1", "01"));
        assert!(offset_after("01", "-1"));
        assert!(!offset_after("01", "01"));
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
