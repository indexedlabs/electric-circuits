//! Control-plane HTTP API (the swappable interface in front of the engine).

use axum::body::to_bytes;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::middleware::{Next, from_fn, from_fn_with_state};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post, put};
use axum::{Json, Router};
use std::time::Instant;

use serde::{Deserialize, Serialize};
use utoipa::{OpenApi, ToSchema};

use crate::engine::{Engine, ShapeRecord, TableSchemaInfo, TableStats};
use crate::predicate::PredicateJson;
use crate::schema::Schema;
use crate::table_ref::TableRef;

// These types are deliberately documentation-only. Runtime requests continue to use the
// validated domain types below; keeping the OpenAPI projection separate means a schema change
// cannot accidentally change parsing or engine behavior without a compiler/test failure.
#[derive(Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "lowercase")]
enum LeafOp {
    Eq,
    Neq,
    Lt,
    Lte,
    Gt,
    Gte,
    Like,
}

#[derive(Serialize, Deserialize, ToSchema)]
struct Subquery {
    /// Bare names are accepted as `public.<name>` by the runtime parser.
    #[schema(example = "public.projects")]
    table: String,
    project: String,
    #[serde(rename = "where")]
    where_: Option<Box<Predicate>>,
}

/// Recursive predicate grammar exposed by the native API. This intentionally mirrors the Rust
/// `PredicateJson` enum, including boxed recursion, instead of collapsing predicates to an
/// untyped `{}` as the tRPC OpenAPI bridge does for z.lazy schemas.
#[derive(Serialize, Deserialize, ToSchema)]
#[serde(untagged)]
enum Predicate {
    Leaf {
        col: String,
        op: LeafOp,
        value: serde_json::Value,
    },
    IsNull {
        col: String,
        #[serde(rename = "isNull")]
        is_null: bool,
    },
    And {
        and: Vec<Predicate>,
    },
    Or {
        or: Vec<Predicate>,
    },
    Not {
        not: Box<Predicate>,
    },
    In {
        col: String,
        #[serde(rename = "in")]
        subquery: Subquery,
        #[serde(default)]
        negated: bool,
    },
}

#[derive(Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
struct ShapeRequest {
    #[schema(example = "public.items")]
    table: String,
    #[serde(rename = "where")]
    where_: Option<Predicate>,
    columns: Option<Vec<String>>,
    #[serde(rename = "changesOnly")]
    changes_only: Option<bool>,
    subscription: Option<String>,
}

#[derive(Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
struct ShapeCreatedResponse {
    shape_id: String,
    table: String,
    stream_path: String,
    stream_url: String,
    subscription: String,
    lease_seconds: u64,
}

#[derive(Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
struct ShapeMetadataResponse {
    shape_id: String,
    table: String,
    stream_path: String,
    stream_url: String,
    state: String,
    subscriptions: usize,
}

#[derive(Serialize, Deserialize, ToSchema)]
struct DeleteResponse {
    ok: bool,
}

#[derive(Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
struct SubsetOrderBy {
    col: String,
    desc: Option<bool>,
}

#[derive(Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
struct SubsetQuery {
    #[schema(example = "public.items")]
    table: String,
    #[serde(rename = "where")]
    where_: Option<Predicate>,
    columns: Option<Vec<String>>,
    order_by: Option<SubsetOrderBy>,
    limit: Option<i64>,
    offset: Option<i64>,
}

#[derive(Serialize, Deserialize, ToSchema)]
struct SubsetResponse {
    rows: Vec<serde_json::Value>,
    /// `pg_current_wal_lsn()` at the page snapshot (LSN positioning for older clients).
    lsn: String,
    /// `pg_current_snapshot()::text` of the page snapshot (`xmin:xmax:xip,…`, full xid8 values). A
    /// live change is already in the page iff its transaction was visible to this snapshot.
    snapshot: String,
    /// `pg_current_wal_insert_lsn()` at the page snapshot: every change at or past it is newer than
    /// the page.
    horizon: String,
}

#[derive(Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "lowercase")]
enum AggregateFunction {
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

#[derive(Serialize, Deserialize, ToSchema)]
struct AggregateRequest {
    #[schema(example = "public.items")]
    table: String,
    #[serde(rename = "where")]
    where_: Option<Predicate>,
    #[serde(rename = "fn")]
    function: AggregateFunction,
    col: Option<String>,
    subscription: Option<String>,
}

#[derive(Serialize, Deserialize, ToSchema)]
struct ErrorResponse {
    error: String,
}

#[derive(Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
struct DeploymentPromoteRequest {
    coordination_key: String,
    owner_revision: String,
    /// Immutable revision of the receiving process. The endpoint refuses a request that does not
    /// name this process, preventing a quiescing incumbent from reclaiming ownership.
    successor_revision: String,
    generation: i64,
    handoff_id: String,
    source_commit_id: String,
}

#[derive(Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
struct SubsetFeedRequest {
    #[schema(example = "public.items")]
    table: String,
    #[serde(rename = "where")]
    where_: Option<Predicate>,
    columns: Option<Vec<String>>,
    subscription: Option<String>,
}

#[utoipa::path(
    post,
    path = "/v1/shapes",
    request_body = ShapeRequest,
    responses(
        (status = 200, description = "Shape created or renewed", body = ShapeCreatedResponse),
        (status = 400, description = "Invalid request", body = ErrorResponse),
        (status = 415, description = "Unsupported media type", body = ErrorResponse),
        (status = 422, description = "Request entity cannot be processed", body = ErrorResponse),
        (status = 409, description = "Subscription belongs to another shape", body = ErrorResponse),
        (status = 503, description = "Engine unavailable", body = ErrorResponse)
    )
)]
#[allow(dead_code)]
fn openapi_create_shape() {}

#[utoipa::path(
    get,
    path = "/v1/shapes/{id}",
    params(("id" = String, Path, description = "Shape id")),
    responses(
        (status = 200, description = "Shape metadata", body = ShapeMetadataResponse),
        (status = 404, description = "Shape not found", body = ErrorResponse),
        (status = 503, description = "Engine unavailable", body = ErrorResponse)
    )
)]
#[allow(dead_code)]
fn openapi_get_shape() {}

#[utoipa::path(
    delete,
    path = "/v1/shapes/{id}",
    params(
        ("id" = String, Path, description = "Shape id"),
        ("subscription" = Option<String>, Query, description = "Subscription claim to release")
    ),
    responses(
        (status = 200, description = "Release accepted", body = DeleteResponse),
        (status = 400, description = "Invalid request", body = ErrorResponse)
    )
)]
#[allow(dead_code)]
fn openapi_release_shape() {}

#[utoipa::path(
    post,
    path = "/v1/subsets/query",
    request_body = SubsetQuery,
    responses(
        (status = 200, description = "Snapshot rows", body = SubsetResponse),
        (status = 400, description = "Invalid request", body = ErrorResponse),
        (status = 415, description = "Unsupported media type", body = ErrorResponse),
        (status = 422, description = "Request entity cannot be processed", body = ErrorResponse),
        (status = 503, description = "Engine unavailable", body = ErrorResponse)
    )
)]
#[allow(dead_code)]
fn openapi_subset_query() {}

#[utoipa::path(
    post,
    path = "/v1/subset-feeds",
    request_body = SubsetFeedRequest,
    responses(
        (status = 200, description = "Changes-only shape feed", body = ShapeCreatedResponse),
        (status = 400, description = "Invalid request", body = ErrorResponse),
        (status = 415, description = "Unsupported media type", body = ErrorResponse),
        (status = 422, description = "Request entity cannot be processed", body = ErrorResponse),
        (status = 409, description = "Subscription belongs to another shape", body = ErrorResponse),
        (status = 503, description = "Engine unavailable", body = ErrorResponse)
    )
)]
#[allow(dead_code)]
fn openapi_subset_feed() {}

#[utoipa::path(
    post,
    path = "/v1/aggregates",
    request_body = AggregateRequest,
    responses(
        (status = 200, description = "Aggregate shape", body = ShapeCreatedResponse),
        (status = 400, description = "Invalid request", body = ErrorResponse),
        (status = 415, description = "Unsupported media type", body = ErrorResponse),
        (status = 422, description = "Request entity cannot be processed", body = ErrorResponse),
        (status = 409, description = "Subscription belongs to another shape", body = ErrorResponse),
        (status = 503, description = "Engine unavailable", body = ErrorResponse)
    )
)]
#[allow(dead_code)]
fn openapi_aggregate() {}

#[utoipa::path(
    post,
    path = "/_admin/deployment/promote",
    request_body = DeploymentPromoteRequest,
    responses(
        (status = 200, description = "Exact quiesced generation promoted"),
        (status = 409, description = "Ownership or receiving-revision conflict", body = ErrorResponse),
        (status = 503, description = "Ownership storage unavailable", body = ErrorResponse)
    )
)]
#[allow(dead_code)]
fn openapi_deployment_promote() {}

#[derive(OpenApi)]
#[openapi(
    info(
        title = "Electric Circuits Native API",
        version = "0.1.0",
        description = "Versioned Rust/Axum control-plane contract for native clients."
    ),
    paths(
        openapi_create_shape,
        openapi_get_shape,
        openapi_release_shape,
        openapi_subset_query,
        openapi_subset_feed,
        openapi_aggregate,
        openapi_deployment_promote
    ),
    components(schemas(
        LeafOp,
        Subquery,
        Predicate,
        ShapeRequest,
        ShapeCreatedResponse,
        ShapeMetadataResponse,
        DeleteResponse,
        SubsetOrderBy,
        SubsetQuery,
        SubsetResponse,
        AggregateFunction,
        AggregateRequest,
        SubsetFeedRequest,
        DeploymentPromoteRequest,
        ErrorResponse
    ))
)]
struct NativeApiDoc;

async fn openapi_json() -> Result<Response, AppError> {
    let body = NativeApiDoc::openapi().to_json().map_err(|e| AppError {
        status: StatusCode::INTERNAL_SERVER_ERROR,
        msg: format!("serialize OpenAPI: {e}"),
        retry_after: false,
    })?;
    let mut headers = HeaderMap::new();
    headers.insert(header::CONTENT_TYPE, HeaderValue::from_static("application/json"));
    Ok((headers, body).into_response())
}

pub fn router(engine: Engine) -> Router {
    router_with_introspection(engine, true)
}

/// `introspection = false` (`ELECTRIC_CIRCUITS_TRACE=0`) leaves the visualizer/introspection surface
/// unregistered — `/trace` (SSE), `/graph`(`/node`), `/state`(`/node`) all 404. With no route there
/// can be no `/trace` subscriber, so the per-envelope trace instrumentation stays on its
/// zero-subscriber fast path (one atomic load). The surface is unauthenticated when enabled.
pub fn router_with_introspection(engine: Engine, introspection: bool) -> Router {
    let mut r = Router::new()
        // Fleet surface: root probe + health state machine (CORS preflight is on the /v1/shape route).
        .route("/", get(|| async { StatusCode::OK }))
        .route("/v1/health", get(health_v1))
        // Native, versioned contract. The unversioned routes below remain for the visualizer and
        // existing internal callers; clients should use these routes and the generated document.
        // `purge=true` is intentionally omitted from the published DELETE contract: it remains a
        // legacy visualizer/operator escape hatch on this shared handler until a separately
        // authorizable admin route exists.
        .route("/v1/openapi.json", get(openapi_json))
        .route("/v1/shapes", post(create_shape))
        .route("/v1/shapes/{id}", get(get_shape).delete(release_shape))
        .route("/v1/subsets/query", post(query_subset))
        .route("/v1/subset-feeds", post(create_subset_feed))
        .route("/v1/aggregates", post(create_aggregate))
        .route("/changes/position", get(changes_position))
        .route("/changes/{segment}", get(read_changes))
        .route("/consumers", get(list_consumers))
        .route("/consumers/{id}", put(pin_consumer).delete(delete_consumer))
        // Kubernetes-shaped probes, deliberately split (see `ready` / the liveness note below).
        .route("/health", get(|| async { "ok" }))
        .route("/ready", get(ready))
        .route("/_admin/control-admission/close", post(close_control_admission))
        .route("/_admin/control-admission/open", post(open_control_admission))
        .route("/_admin/drained-through/{source_commit_id}", get(drained_through))
        .route("/_runtime/drained-through/{source_commit_id}", get(runtime_drained_through))
        .route("/_admin/deployment/status", get(deployment_status))
        .route("/_admin/deployment/quiesce", post(deployment_quiesce))
        .route("/_admin/deployment/promote", post(deployment_promote))
        .route("/schema", post(define_schema))
        .route("/shapes", post(create_shape))
        .route("/aggregate", post(create_aggregate))
        .route("/shapes/{id}", get(get_shape).delete(release_shape))
        .route("/shapes/{id}/rows", get(get_shape_rows))
        .route("/shapes/{id}/log", get(get_shape_log))
        .route("/query", post(query_subset))
        .route("/tables", get(list_tables))
        .route("/tables/{name}/offset", get(table_offset))
        .route("/tables/{name}/families", get(table_families))
        // Table schema (columns + pk), a parameterized single-row INSERT (the visualizer's add-row
        // action), and a by-primary-key DELETE (its delete-rows action). Both writes go to Postgres
        // so the changes are captured by logical replication and flow through the pipeline like any
        // other write.
        .route("/table/{table}/schema", get(get_table_schema))
        .route("/table/{table}/rows", post(insert_table_row).delete(delete_table_rows))
        .route("/subqueries", get(subquery_stats))
        .route("/replication/lsn", get(replication_lsn))
        // Operator recovery from a broken epoch under ELECTRIC_CIRCUITS_RESET_ON_SLOT_LOSS=false
        // (ADR-0004): retire every shape, bind a new epoch, resume ingest.
        .route("/epoch/reset", post(epoch_reset))
        .route("/metrics", get(get_metrics))
        .route("/metrics/reset", post(reset_metrics))
        .route("/memory", get(get_memory))
        .route("/metrics/prometheus", get(get_prometheus))
        // Electric-protocol adapter: lets Electric's official client + oracle harness read our shapes.
        // OPTIONS is the CORS preflight the fleet's browser-style clients send.
        .route("/v1/shape", get(crate::electric::shape).options(shape_options));
    if introspection {
        r = r
            .route("/graph", get(get_graph))
            .route("/graph/node", get(get_node_index))
            .route("/state", get(get_state))
            .route("/state/node", get(get_state_node))
            // Per-envelope pipeline trace (SSE) — best-effort, for visualization/debugging.
            .route("/trace", get(get_trace))
            // On-demand dbsp profiler dump for every dbsp circuit (membership + counts).
            // Heavy — diagnostic/attribution use only; never sampled in the background.
            .route("/debug/dbsp-profile", get(get_dbsp_profile));
    }
    r.layer(from_fn(normalize_extractor_rejection))
        .layer(from_fn_with_state(engine.clone(), require_managed_public_ready))
        .with_state(engine)
}

#[derive(Deserialize)]
struct ChangesReadQuery {
    #[serde(default = "changes_start_offset")]
    offset: String,
    live: Option<String>,
    /// A cursor is bound to its immutable query generation.  Older cursors are terminal even if
    /// an identically numbered path exists in the current namespace.
    generation: Option<String>,
}

#[derive(Deserialize)]
struct ConsumerPinRequest {
    segment: u32,
    offset: String,
}

#[derive(Serialize)]
struct ConsumerResponse {
    id: String,
    position: crate::changelog::LogPosition,
    pinned_segments: Vec<u32>,
    lag_segments: u32,
}

fn changes_start_offset() -> String {
    "-1".to_string()
}

/// A positioned, unmodified page of the segmented durable change log for external consumers.
async fn read_changes(
    State(engine): State<Engine>,
    Path(segment): Path<u32>,
    Query(query): Query<ChangesReadQuery>,
) -> Result<Response, AppError> {
    engine.ensure_not_degraded()?;
    if query.generation.as_deref().is_some_and(|generation| generation != engine.changes_generation()) {
        return Err(AppError {
            status: StatusCode::GONE,
            msg: "stale-generation: change-log position belongs to a previous query generation; re-sync".to_string(),
            retry_after: false,
        });
    }
    let live = query.live.as_deref() == Some("long-poll");
    let mut page = match if live {
        let deadline = Instant::now() + crate::electric::live_timeout();
        let read_engine = engine.clone();
        let offset = query.offset.clone();
        let shutdown = engine.shutdown_token();
        crate::electric::poll_live_until(offset, deadline, &shutdown, move |offset| {
            let engine = read_engine.clone();
            async move { engine.read_changes(segment, &offset, true).await }
        })
        .await
    } else {
        engine.read_changes(segment, &query.offset, false).await
    } {
        Ok(page) => page,
        Err(error) => {
            if let Some(gone) = error.chain().find_map(|cause| cause.downcast_ref::<crate::ds::StreamGone>()) {
                return Err(AppError {
                    status: StatusCode::from_u16(gone.status).unwrap_or(StatusCode::GONE),
                    msg: gone.to_string(),
                    retry_after: false,
                });
            }
            return Err(AppError::from(error));
        }
    };
    // The common reader contract reports an idle, deadline-bounded long-poll as caught up. The
    // storage poll may have been cancelled at our deadline before it could send its own 204.
    if live && page.envelopes.is_empty() && !page.closed {
        page.up_to_date = true;
    }
    let mut response = Json(page.envelopes).into_response();
    if let Some(offset) = page.next_offset {
        response.headers_mut().insert(
            "stream-next-offset",
            HeaderValue::from_str(&offset).unwrap_or_else(|_| HeaderValue::from_static("")),
        );
    }
    if page.up_to_date {
        response.headers_mut().insert("stream-up-to-date", HeaderValue::from_static("true"));
    }
    if page.closed {
        response.headers_mut().insert("stream-closed", HeaderValue::from_static("true"));
    }
    Ok(response)
}

/// Public current position and namespace of the segmented change log. The segment list is useful
/// to an external consumer only for diagnostics; positioned reads still follow each closed
/// segment's control pointer one hop at a time.
async fn changes_position(State(engine): State<Engine>) -> Result<Json<serde_json::Value>, AppError> {
    engine.ensure_not_degraded()?;
    Ok(Json(serde_json::json!({
        "generation": engine.changes_generation(),
        "position": engine.changes_position(),
        "segments": engine.changes_segments(),
    })))
}

async fn pin_consumer(
    State(engine): State<Engine>,
    Path(id): Path<String>,
    Json(request): Json<ConsumerPinRequest>,
) -> Result<Json<ConsumerResponse>, AppError> {
    engine.ensure_not_degraded()?;
    let position = crate::changelog::LogPosition { segment: request.segment, offset: request.offset };
    let result = engine.pin_consumer(id.clone(), position.clone()).await;
    if let Err(error) = result {
        let msg = error.to_string();
        return Err(AppError {
            status: if msg.contains("backwards") { StatusCode::CONFLICT } else { StatusCode::BAD_REQUEST },
            msg,
            retry_after: false,
        });
    }
    let (_, position, lag_segments) = engine
        .consumers()
        .into_iter()
        .find(|(consumer_id, _, _)| consumer_id == &id)
        .expect("durable pin is installed before the response");
    Ok(Json(ConsumerResponse {
        id,
        pinned_segments: engine
            .changes_segments()
            .keys()
            .copied()
            .filter(|segment| *segment >= position.segment)
            .collect(),
        position,
        lag_segments,
    }))
}

async fn list_consumers(State(engine): State<Engine>) -> Result<Json<Vec<ConsumerResponse>>, AppError> {
    engine.ensure_not_degraded()?;
    let segments = engine.changes_segments();
    Ok(Json(
        engine
            .consumers()
            .into_iter()
            .map(|(id, position, lag_segments)| ConsumerResponse {
                pinned_segments: segments.keys().copied().filter(|segment| *segment >= position.segment).collect(),
                id,
                position,
                lag_segments,
            })
            .collect(),
    ))
}

async fn delete_consumer(
    State(engine): State<Engine>,
    Path(id): Path<String>,
) -> Result<Json<serde_json::Value>, AppError> {
    engine.ensure_not_degraded()?;
    engine.remove_consumer(&id).await?;
    Ok(Json(serde_json::json!({ "ok": true })))
}

/// Liveness and private deployment control deliberately bypass this gate. Every other route is a
/// data/control surface and must independently refuse a liveness-healthy standby if a listener is
/// miswired around `/ready`.
async fn require_managed_public_ready(
    State(engine): State<Engine>,
    request: axum::extract::Request,
    next: Next,
) -> Response {
    let path = request.uri().path();
    if path == "/replication/lsn"
        || (path == "/epoch/reset" && engine.managed_recovery_owner())
        || matches!(
            path,
            "/" | "/health"
                | "/ready"
                | "/v1/health"
                | "/metrics"
                | "/memory"
                | "/metrics/prometheus"
                | "/v1/openapi.json"
        )
        || path.starts_with("/_admin/")
        || !engine.managed_deployment_enabled()
    {
        return next.run(request).await;
    }
    match engine.ensure_not_degraded() {
        Ok(()) => next.run(request).await,
        // `ensure_not_degraded` deliberately orders epoch/degradation latches before managed
        // readiness. Keep that taxonomy at the outer fence too: an active but broken engine must
        // report its durable diagnosis, while only transient deployment states get Retry-After.
        Err(error) => AppError::from(error).into_response(),
    }
}

/// Axum's built-in JSON and query extractors intentionally return plain-text rejection bodies.
/// The native contract is JSON, so normalize only those extractor responses on native DTO routes.
/// In particular, `/v1/shape` is the Electric protocol adapter and keeps its compatibility error
/// envelope/media type. Status semantics remain deliberate: malformed JSON/query is 400,
/// missing/wrong content type is 415, and valid JSON that cannot deserialize into the DTO is 422.
async fn normalize_extractor_rejection(request: axum::extract::Request, next: Next) -> Response {
    let native_route = is_native_dto_path(request.uri().path());
    let response = next.run(request).await;
    if !native_route {
        return response;
    }
    let status = response.status();
    let is_rejection = matches!(
        status,
        StatusCode::BAD_REQUEST | StatusCode::UNSUPPORTED_MEDIA_TYPE | StatusCode::UNPROCESSABLE_ENTITY
    );
    let is_plain_text = response
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.starts_with("text/plain"));
    if !is_rejection || !is_plain_text {
        return response;
    }

    let (_parts, body) = response.into_parts();
    let detail = to_bytes(body, 64 * 1024)
        .await
        .ok()
        .and_then(|bytes| String::from_utf8(bytes.to_vec()).ok())
        .filter(|text| !text.trim().is_empty())
        .unwrap_or_else(|| "invalid request".to_string());
    let mut mapped = (status, Json(ErrorResponse { error: detail })).into_response();
    mapped.headers_mut().insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    mapped
}

fn is_native_dto_path(path: &str) -> bool {
    matches!(path, "/v1/shapes" | "/v1/subsets/query" | "/v1/subset-feeds" | "/v1/aggregates")
        || path.strip_prefix("/v1/shapes/").is_some_and(|suffix| !suffix.is_empty())
}

/// SSE stream of per-envelope [`crate::trace::TraceEvent`]s (one JSON object per `data:` line).
/// Lossy by design: a lagging subscriber silently skips the events it missed rather than slowing
/// envelope processing.
async fn get_trace(State(engine): State<Engine>) -> impl IntoResponse {
    use tokio_stream::StreamExt;
    let rx = engine.trace_sender().subscribe();
    let stream = tokio_stream::wrappers::BroadcastStream::new(rx).filter_map(|item| match item {
        Ok(json) => Some(Ok::<_, std::convert::Infallible>(axum::response::sse::Event::default().data(json.as_str()))),
        // Lagged: drop the gap marker; the consumer treats trace as best-effort animation.
        Err(_) => None,
    });
    axum::response::sse::Sse::new(stream).keep_alive(axum::response::sse::KeepAlive::default())
}

/// Exact `/v1/health` JSON body for a status — no whitespace (the fleet's healthcheck string-compares
/// the body against `{"status":"active"}`).
fn health_json(status: &str) -> String {
    format!("{{\"status\":\"{status}\"}}")
}

fn page_should_continue(offset: &str, next: Option<&str>, up_to_date: bool, closed: bool) -> bool {
    !up_to_date && !closed && next.is_some_and(|candidate| candidate != offset)
}

/// `GET /v1/health` — `waiting`/`starting` → 202, `active` → 200, `degraded` → 503 (the engine lost
/// membership effects and only a restart fixes it; 503 keeps a load balancer from routing to it).
/// Caches are disabled so the fleet's 500ms poll always sees the live phase.
async fn health_v1(State(engine): State<Engine>) -> Response {
    let status = engine.health_status();
    let code = match status {
        "active" => StatusCode::OK,
        "degraded" => StatusCode::SERVICE_UNAVAILABLE,
        _ => StatusCode::ACCEPTED,
    };
    let mut headers = HeaderMap::new();
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-cache, no-store, must-revalidate"));
    headers.insert(header::CONTENT_TYPE, HeaderValue::from_static("application/json"));
    (code, headers, health_json(status)).into_response()
}

/// `GET /ready` — the **readiness** probe, and the only endpoint a load balancer should gate on.
///
/// 200 `{"status":"active"}` when every precondition for serving is met (Postgres connected, slot
/// verified, catalog restored, ingestor spawned, not degraded, epoch intact, not shutting down);
/// 503 with the word that says why otherwise — `waiting`, `starting`, `degraded`, `shutting_down`.
///
/// It is deliberately NOT `/health`, which stays pure **liveness**: "ok" while the process runs, so
/// a kubelet never restarts an engine that is merely waiting for Postgres to come up, or draining.
/// `/v1/health` is unchanged — it is the benchmarking-fleet's healthcheck and its status/code
/// mapping (202 while booting) is parity, not a probe contract.
async fn ready(State(engine): State<Engine>) -> Response {
    let status = engine.readiness_status();
    let code = if status == "active" { StatusCode::OK } else { StatusCode::SERVICE_UNAVAILABLE };
    let mut headers = HeaderMap::new();
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-cache, no-store, must-revalidate"));
    headers.insert(header::CONTENT_TYPE, HeaderValue::from_static("application/json"));
    (code, headers, health_json(status)).into_response()
}

fn require_private_admin(headers: &HeaderMap) -> Result<(), AppError> {
    require_private_admin_with_secret(headers, crate::config::control_secret())
}

pub(crate) fn require_private_admin_with_secret(headers: &HeaderMap, secret: Option<&str>) -> Result<(), AppError> {
    let Some(secret) = secret else {
        return Err(AppError {
            status: StatusCode::SERVICE_UNAVAILABLE,
            msg: "private admin authentication is not configured".to_string(),
            retry_after: false,
        });
    };
    let authorized = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .is_some_and(|provided| crate::config::secret_matches(secret, provided));
    if authorized {
        Ok(())
    } else {
        Err(AppError {
            status: StatusCode::UNAUTHORIZED,
            msg: "private admin authentication failed".to_string(),
            retry_after: false,
        })
    }
}

async fn close_control_admission(
    State(engine): State<Engine>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, AppError> {
    require_private_admin(&headers)?;
    engine.close_control_admission_with_receipt_barrier().await;
    Ok(Json(serde_json::json!({ "controlAdmission": "closed" })))
}

async fn open_control_admission(
    State(engine): State<Engine>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, AppError> {
    require_private_admin(&headers)?;
    if !engine.managed_recovery_owner() {
        return Err(anyhow::Error::new(crate::engine::DeploymentNotReady).into());
    }
    engine.open_control_admission();
    engine.ensure_control_admitted()?;
    Ok(Json(serde_json::json!({ "controlAdmission": "open" })))
}

async fn drained_through(
    State(engine): State<Engine>,
    headers: HeaderMap,
    Path(source_commit_id): Path<String>,
) -> Result<Json<serde_json::Value>, AppError> {
    require_private_admin(&headers)?;
    let parsed = uuid::Uuid::parse_str(&source_commit_id).map_err(|_| AppError {
        status: StatusCode::BAD_REQUEST,
        msg: "source_commit_id must be a UUID".to_string(),
        retry_after: false,
    })?;
    if parsed.to_string() != source_commit_id {
        return Err(AppError {
            status: StatusCode::BAD_REQUEST,
            msg: "source_commit_id must be a canonical lowercase UUID".to_string(),
            retry_after: false,
        });
    }
    let receipt = engine.source_drain_receipt(&source_commit_id);
    let last_receipt = engine.last_source_drain_receipt();
    Ok(Json(serde_json::json!({
        "sourceCommitId": source_commit_id,
        "drained": receipt.is_some(),
        "receipt": receipt,
        "lastReceipt": last_receipt,
    })))
}

fn require_runtime_gateway(
    headers: &HeaderMap,
    gateway: Option<&str>,
    controller: Option<&str>,
) -> Result<(), AppError> {
    let Some(secret) = gateway.filter(|secret| !secret.is_empty() && Some(*secret) != controller) else {
        return Err(AppError {
            status: StatusCode::SERVICE_UNAVAILABLE,
            msg: "runtime gateway authentication is not configured with a distinct secret".into(),
            retry_after: false,
        });
    };
    let authorized = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .is_some_and(|provided| crate::config::secret_matches(secret, provided));
    if !authorized {
        return Err(AppError {
            status: StatusCode::UNAUTHORIZED,
            msg: "runtime gateway authentication failed".into(),
            retry_after: false,
        });
    }
    Ok(())
}

#[derive(Deserialize)]
struct RuntimeDrainQuery {
    user_id: String,
    generation: String,
}

async fn runtime_drained_through(
    State(engine): State<Engine>,
    headers: HeaderMap,
    Path(source_commit_id): Path<String>,
    Query(query): Query<RuntimeDrainQuery>,
) -> Result<Response, AppError> {
    require_runtime_gateway(&headers, crate::config::secret(), crate::config::control_secret())?;
    let marker = crate::runtime_authority::AuthorityMarker::parse(&source_commit_id, &query.user_id, &query.generation)
        .ok_or_else(|| AppError {
            status: StatusCode::BAD_REQUEST,
            msg: "runtime marker requires canonical lowercase UUIDs and a 64-character lowercase hex generation".into(),
            retry_after: false,
        })?;
    if engine.readiness_status() != "active" {
        return Err(AppError {
            status: StatusCode::SERVICE_UNAVAILABLE,
            msg: "runtime drain receipt unavailable while engine is not ready".into(),
            retry_after: false,
        });
    }
    let receipt = engine.runtime_drain_receipt(&marker)?;
    Ok((
        [(header::CACHE_CONTROL, "no-store")],
        Json(serde_json::json!({
            "sourceCommitId": source_commit_id, "drained": receipt.is_some(), "receipt": receipt,
        })),
    )
        .into_response())
}

#[derive(Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
struct DeploymentTransitionReq {
    coordination_key: String,
    owner_revision: String,
    generation: i64,
    handoff_id: String,
    source_commit_id: String,
}

fn canonical_uuid(value: &str, name: &str) -> Result<uuid::Uuid, AppError> {
    let parsed = uuid::Uuid::parse_str(value).map_err(|_| AppError {
        status: StatusCode::BAD_REQUEST,
        msg: format!("{name} must be a UUID"),
        retry_after: false,
    })?;
    if parsed.to_string() != value {
        return Err(AppError {
            status: StatusCode::BAD_REQUEST,
            msg: format!("{name} must be a canonical lowercase UUID"),
            retry_after: false,
        });
    }
    Ok(parsed)
}

fn deployment_role(role: crate::engine::ManagedRole) -> &'static str {
    match role {
        crate::engine::ManagedRole::Active => "active",
        crate::engine::ManagedRole::Standby => "standby",
        crate::engine::ManagedRole::Quiescing => "quiescing",
        crate::engine::ManagedRole::Promoting => "promoting",
    }
}

async fn deployment_status(
    State(engine): State<Engine>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, AppError> {
    require_private_admin(&headers)?;
    let (key, revision, role, ownership) = engine.deployment_status().await?;
    Ok(Json(serde_json::json!({
        "coordinationKey": key,
        "revision": revision,
        "role": deployment_role(role),
        "generation": ownership.as_ref().map(|row| row.generation),
        "ownerRevision": ownership.as_ref().map(|row| &row.owner_revision),
        "phase": ownership.as_ref().map(|row| match row.phase { crate::deployment::OwnershipPhase::Active => "active", crate::deployment::OwnershipPhase::Quiesced => "quiesced" }),
        "handoffId": ownership.as_ref().and_then(|row| row.handoff_id.as_ref()),
        "sourceCommitId": ownership.as_ref().and_then(|row| row.source_commit_id.as_ref()),
        "controlAdmission": engine.ensure_control_admitted().is_ok(),
        "readiness": engine.readiness_status(),
    })))
}

async fn deployment_quiesce(
    State(engine): State<Engine>,
    headers: HeaderMap,
    Json(req): Json<DeploymentTransitionReq>,
) -> Result<Json<serde_json::Value>, AppError> {
    require_private_admin(&headers)?;
    let handoff_id = canonical_uuid(&req.handoff_id, "handoffId")?;
    let source_commit_id = canonical_uuid(&req.source_commit_id, "sourceCommitId")?;
    let ownership = engine
        .deployment_quiesce(&req.coordination_key, &req.owner_revision, req.generation, handoff_id, source_commit_id)
        .await?;
    Ok(Json(serde_json::json!({ "accepted": true, "phase": "quiesced", "generation": ownership.generation })))
}

async fn deployment_promote(
    State(engine): State<Engine>,
    headers: HeaderMap,
    Json(req): Json<DeploymentPromoteRequest>,
) -> Result<Json<serde_json::Value>, AppError> {
    require_private_admin(&headers)?;
    let handoff_id = canonical_uuid(&req.handoff_id, "handoffId")?;
    let source_commit_id = canonical_uuid(&req.source_commit_id, "sourceCommitId")?;
    let ownership = engine
        .deployment_promote(
            &req.coordination_key,
            &req.owner_revision,
            &req.successor_revision,
            req.generation,
            handoff_id,
            source_commit_id,
        )
        .await?;
    Ok(Json(serde_json::json!({ "accepted": true, "phase": "active", "generation": ownership.generation })))
}

/// `OPTIONS /v1/shape` — CORS preflight: 204 advertising the methods the adapter serves.
async fn shape_options() -> Response {
    let mut headers = HeaderMap::new();
    headers.insert(header::ACCESS_CONTROL_ALLOW_METHODS, HeaderValue::from_static("GET, POST, HEAD, DELETE, OPTIONS"));
    (StatusCode::NO_CONTENT, headers).into_response()
}

#[derive(Deserialize)]
struct DefineSchemaReq {
    schema: Schema,
}

#[derive(Deserialize)]
struct SubsetOrderByReq {
    col: String,
    #[serde(default)]
    desc: bool,
}

/// A one-shot subset query (the non-materialized counterpart to `/shapes`).
#[derive(Deserialize)]
struct QueryReq {
    /// Bare names are accepted as `public.<name>` sugar and canonicalised by `TableRef`'s
    /// `Deserialize`; `a.b.c` is a deserialization error (4xx), not a bad lookup.
    table: TableRef,
    #[serde(default, rename = "where")]
    where_: Option<PredicateJson>,
    #[serde(default)]
    columns: Option<Vec<String>>,
    #[serde(default, rename = "orderBy")]
    order_by: Option<SubsetOrderByReq>,
    #[serde(default)]
    limit: Option<i64>,
    #[serde(default)]
    offset: Option<i64>,
}

#[derive(Serialize)]
struct QueryResp {
    rows: Vec<serde_json::Value>,
    lsn: String,
    snapshot: String,
    horizon: String,
}

async fn query_subset(State(engine): State<Engine>, Json(req): Json<QueryReq>) -> Result<Json<QueryResp>, AppError> {
    engine.ensure_not_degraded()?;
    let order_by = req.order_by.map(|o| (o.col, o.desc));
    let page = engine.query_subset(&req.table, req.where_, req.columns, order_by, req.limit, req.offset).await?;
    Ok(Json(QueryResp { rows: page.rows, lsn: page.lsn, snapshot: page.snapshot, horizon: page.horizon }))
}

/// A changes-only feed uses the same shape lifecycle and subscription semantics as a materialized
/// shape, but deliberately skips its backfill. The distinction is explicit in the request sent to
/// the engine, rather than being inferred from a path in the engine core.
async fn create_subset_feed(
    State(engine): State<Engine>,
    Json(mut req): Json<CreateShapeReq>,
) -> Result<Json<ShapeResp>, AppError> {
    req.changes_only = true;
    create_shape(State(engine), Json(req)).await
}

async fn define_schema(
    State(engine): State<Engine>,
    Json(req): Json<DefineSchemaReq>,
) -> Result<Json<serde_json::Value>, AppError> {
    let _control = engine.admit_control()?;
    engine.define_schema(&req.schema).await?;
    Ok(Json(serde_json::json!({ "ok": true })))
}

#[derive(Deserialize)]
struct CreateShapeReq {
    table: TableRef,
    #[serde(default, rename = "where")]
    where_: Option<PredicateJson>,
    /// Optional output projection: column names to sync. Omitted = the full row.
    #[serde(default)]
    columns: Option<Vec<String>>,
    /// When true, skip the backfill and stream only future matching changes (a non-materialized live
    /// tail feed). Used by subset queries; a normal materialized shape leaves this false.
    #[serde(default, rename = "changesOnly")]
    changes_only: bool,
    /// The caller's **subscription id** (ADR-0008): a name for this claim on the shape.
    ///
    /// Repeating the create with the same id renews that subscription and returns the same handle
    /// instead of taking a second claim — so a caller whose success response was lost can simply ask
    /// again. Omitted, the engine mints one and returns it: the caller can then renew and release
    /// with it, but it had **no idempotency on this create**, because a repeat is indistinguishable
    /// from a new subscriber. An id another shape already holds is a `409`.
    #[serde(default)]
    subscription: Option<String>,
}

/// The checks every subscription id must pass, wherever it appears. Free-form (a uuid, a session
/// id, a device id — the engine never interprets it), with two limits: it must not be empty, and it
/// must fit in 128 bytes so a catalog event stays small. Control characters are refused because the
/// id travels through logs and a JSON catalog record.
///
/// The `~` prefix is deliberately NOT checked here — nor anywhere else: it is a MARKER, not a
/// reserved namespace (see [`validate_new_subscription`]).
fn validate_subscription(sub: Option<String>) -> Result<Option<String>, AppError> {
    let Some(sub) = sub else { return Ok(None) };
    let bad = |msg: &str| AppError { status: StatusCode::BAD_REQUEST, msg: msg.to_string(), retry_after: false };
    if sub.is_empty() {
        return Err(bad("subscription must not be empty (omit it to have one minted)"));
    }
    if sub.len() > 128 {
        return Err(bad("subscription must be at most 128 bytes"));
    }
    if sub.chars().any(char::is_control) {
        return Err(bad("subscription must not contain control characters"));
    }
    Ok(Some(sub))
}

/// [`validate_subscription`] for a CREATE, which is the only place an id can be brought into
/// existence. Identical to it today: the `~` prefix is a MARKER, not a namespace the engine
/// defends, so any well-formed id — minted-looking or not — is accepted.
///
/// The engine still mints `~<nonce>-<n>` ids for creates that name none, and the legacy anonymous
/// `DELETE` still releases `~` claims before named ones. It just no longer tries to tell a minted
/// `~` id from a caller-invented one. The only thing checking provenance ever bought was stopping a
/// caller from deliberately making its OWN claim the expendable one — which hurts nobody else; the
/// minted ids were never unguessable (a time/address nonce plus a counter), so it was not a security
/// boundary either; and the price was a history-sized in-memory set of every id ever minted, growing
/// with every anonymous create and compacted by nothing (ADR-0008).
///
/// It stays as a distinct call so the create paths keep one named place to hang a create-only rule.
fn validate_new_subscription(sub: Option<String>) -> Result<Option<String>, AppError> {
    validate_subscription(sub)
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ShapeResp {
    shape_id: String,
    table: TableRef,
    stream_path: String,
    stream_url: String,
    /// The subscription this create was recorded under (ADR-0008) — the caller's own id, or the one
    /// the engine minted. Release it with `DELETE /shapes/{id}?subscription=…`, renew it by
    /// repeating the create with it. Absent on `GET /shapes/{id}`, which belongs to no subscriber.
    #[serde(skip_serializing_if = "Option::is_none")]
    subscription: Option<String>,
    /// How long a subscription may go unrenewed before the engine releases it
    /// (`ELECTRIC_CIRCUITS_SUBSCRIPTION_LEASE_SECS`; `0` = leases never lapse).
    /// The renewal cadence is the server's to set, so clients read it from here rather than guess.
    #[serde(skip_serializing_if = "Option::is_none")]
    lease_seconds: Option<u64>,
    /// Retention lifecycle: `active` | `deactivating` | `dormant` | `reactivating` (see
    /// `crate::retention`). Shapes handed out by create are always active.
    #[serde(skip_serializing_if = "Option::is_none")]
    state: Option<&'static str>,
    /// Live subscriptions on the shape (`GET /shapes/{id}` only) — the count, never the ids. This
    /// is the number that explains a shape which will not go dormant.
    #[serde(skip_serializing_if = "Option::is_none")]
    subscriptions: Option<usize>,
}

impl ShapeResp {
    fn of(engine: &Engine, rec: ShapeRecord) -> Self {
        let stream_url = engine.stream_url(&rec.stream_path);
        ShapeResp {
            shape_id: rec.id,
            table: rec.table,
            stream_path: rec.stream_path,
            stream_url,
            subscription: None,
            lease_seconds: None,
            state: None,
            subscriptions: None,
        }
    }

    /// A create's answer: the record plus the subscription it was taken under and the lease window
    /// that subscription must be renewed within.
    fn created(engine: &Engine, rec: ShapeRecord, subscription: String) -> Self {
        ShapeResp {
            subscription: Some(subscription),
            lease_seconds: Some(engine.lease_seconds()),
            ..ShapeResp::of(engine, rec)
        }
    }
}

#[derive(serde::Serialize)]
struct TableInfo {
    table: TableRef,
    /// True when the table's schema drifted and the engine could not settle it: its shapes have
    /// been retired, its changes are dropped and creates on it are refused until a retry succeeds
    /// (ADR-0005). Watch this rather than guessing from a create failure.
    unresolved: bool,
}

#[derive(serde::Serialize)]
struct TablesResp {
    tables: Vec<TableInfo>,
}

/// Every table the engine tracks, with its schema-drift status.
async fn list_tables(State(engine): State<Engine>) -> Json<TablesResp> {
    let unresolved = engine.unresolved_tables().await;
    let tables = engine
        .tracked_tables()
        .await
        .into_iter()
        .map(|table| TableInfo { unresolved: unresolved.contains(&table), table })
        .collect();
    Json(TablesResp { tables })
}

async fn create_shape(
    State(engine): State<Engine>,
    Json(req): Json<CreateShapeReq>,
) -> Result<Json<ShapeResp>, AppError> {
    // Degradation outranks request validation: a degraded engine cannot safely answer for any
    // membership request, even one whose table name is invalid.
    engine.ensure_not_degraded()?;
    let _control = engine.admit_control()?;
    validate_create_shape_request(&engine, &req).await?;
    let subscription = validate_new_subscription(req.subscription)?;
    // share = true: identical reference shapes from multiple clients collapse to one maintained stream.
    let (rec, sub) =
        engine.create_shape_as(&req.table, req.where_, req.columns, req.changes_only, true, subscription).await?;
    Ok(Json(ShapeResp::created(&engine, rec, sub)))
}

enum CreateShapeRequestError {
    UnknownTable(TableRef),
    UnknownColumn { table: TableRef, column: String },
}

impl CreateShapeRequestError {
    fn into_app_error(self) -> AppError {
        let msg = match self {
            Self::UnknownTable(table) => format!("unknown table '{table}'"),
            Self::UnknownColumn { table, column } => format!("unknown column '{column}' on table '{table}'"),
        };
        AppError { status: StatusCode::BAD_REQUEST, msg, retry_after: false }
    }
}

/// Reject deterministic, caller-controlled schema-name errors before shape creation can acquire a
/// subscription or perform durable-stream work. Generic engine errors remain internal failures.
async fn validate_create_shape_request(engine: &Engine, req: &CreateShapeReq) -> Result<(), AppError> {
    let table = engine
        .table_schema(&req.table)
        .await
        .ok_or_else(|| CreateShapeRequestError::UnknownTable(req.table.clone()).into_app_error())?;
    for column in req.columns.as_deref().into_iter().flatten() {
        if !table.index.contains_key(column) {
            return Err(CreateShapeRequestError::UnknownColumn { table: req.table.clone(), column: column.clone() }
                .into_app_error());
        }
    }
    Ok(())
}

#[derive(Deserialize)]
struct AggregateReq {
    table: TableRef,
    #[serde(default, rename = "where")]
    where_: Option<PredicateJson>,
    #[serde(rename = "fn")]
    func: crate::engine::AggFn,
    #[serde(default)]
    col: Option<String>,
    /// The caller's subscription id — identical semantics to `CreateShapeReq::subscription`.
    #[serde(default)]
    subscription: Option<String>,
}

/// Create a scalar aggregation shape (electric-circuits extension; not in the Electric protocol).
async fn create_aggregate(
    State(engine): State<Engine>,
    Json(req): Json<AggregateReq>,
) -> Result<Json<ShapeResp>, AppError> {
    let _control = engine.admit_control()?;
    let subscription = validate_new_subscription(req.subscription)?;
    let (rec, sub) = engine.create_aggregate_as(&req.table, req.where_, req.func, req.col, subscription).await?;
    Ok(Json(ShapeResp::created(&engine, rec, sub)))
}

async fn get_shape(State(engine): State<Engine>, Path(id): Path<String>) -> Result<Json<ShapeResp>, AppError> {
    engine.ensure_not_degraded()?;
    match engine.get_shape(&id).await {
        Some(rec) => {
            let state = engine.shape_lifecycle(&rec.id).await;
            let subscriptions = Some(engine.subscription_count(&rec.id).await);
            Ok(Json(ShapeResp { state, subscriptions, ..ShapeResp::of(&engine, rec) }))
        }
        None => {
            Err(AppError { status: StatusCode::NOT_FOUND, msg: format!("shape {id} not found"), retry_after: false })
        }
    }
}

#[derive(Deserialize)]
struct ShapeRowsQuery {
    #[serde(default)]
    limit: Option<usize>,
}

#[derive(Serialize)]
struct ShapeRowEntry {
    key: String,
    value: serde_json::Value,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ShapeRowsResp {
    id: String,
    table: TableRef,
    changes_only: bool,
    /// Total materialized rows (before the display cap).
    count: usize,
    truncated: bool,
    rows: Vec<ShapeRowEntry>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ShapeLogEntry {
    op: String,
    key: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    value: Option<serde_json::Value>,
    /// Prior row on update/delete (REPLICA IDENTITY FULL) — lets a UI show what a delete removed.
    #[serde(skip_serializing_if = "Option::is_none")]
    old: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    lsn: Option<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ShapeLogResp {
    id: String,
    table: TableRef,
    changes_only: bool,
    /// Total envelopes on the stream (before the tail cap).
    total: usize,
    /// Oldest → newest; capped to the tail (`limit`).
    entries: Vec<ShapeLogEntry>,
}

/// The change log of an **existing** shape: the tail of its stream as-is (insert/update/delete
/// envelopes, oldest → newest). Drives the visualizer's feed-shape "live log" view, which polls
/// this. Read-only — creates no shape.
async fn get_shape_log(
    State(engine): State<Engine>,
    Path(id): Path<String>,
    Query(q): Query<ShapeRowsQuery>,
) -> Result<Json<ShapeLogResp>, AppError> {
    engine.ensure_not_degraded()?;
    let Some(rec) = engine.get_shape(&id).await else {
        return Err(AppError {
            status: StatusCode::NOT_FOUND,
            msg: format!("shape {id} not found"),
            retry_after: false,
        });
    };
    // A touch reactivates: if the shape is dormant, replay it live first so the log is current.
    engine.ensure_active(&id).await?;
    let limit = q.limit.unwrap_or(50).min(500);
    let mut entries: std::collections::VecDeque<ShapeLogEntry> = std::collections::VecDeque::new();
    let mut total = 0usize;
    // Walked over the WHOLE stream (not just the returned tail): which keys are live and their
    // last value. Lets the wire ops (upsert/delete) be reported as insert vs update exactly, and
    // gives a delete entry the row it removed.
    let mut live: std::collections::HashMap<String, Option<serde_json::Value>> = std::collections::HashMap::new();
    let mut offset = "-1".to_string();
    loop {
        let r = engine.read_shape_stream(&rec.stream_path, &offset, false).await?;
        for env in r.envelopes {
            total += 1;
            let (op, old) = if env.headers.operation == "delete" {
                let last = live.remove(&env.key).flatten();
                ("delete".to_string(), env.old.or(last))
            } else {
                let existed = live.insert(env.key.clone(), env.value.clone()).is_some();
                (if existed { "update".to_string() } else { "insert".to_string() }, env.old)
            };
            entries.push_back(ShapeLogEntry { op, key: env.key, value: env.value, old, lsn: env.headers.lsn });
            if entries.len() > limit {
                entries.pop_front();
            }
        }
        // Break when caught up, the stream is closed (retired: terminal, treat it as up-to-date),
        // the page was empty, or the offset failed to advance (a defensive guard against a non-empty
        // page with a missing/unchanged next offset looping forever).
        let closed = r.closed;
        let previous = offset.clone();
        let next = r.next_offset.clone();
        if let Some(n) = r.next_offset {
            offset = n;
        }
        if !page_should_continue(&previous, next.as_deref(), r.up_to_date, closed) {
            break;
        }
    }
    Ok(Json(ShapeLogResp {
        id: rec.id,
        table: rec.table,
        changes_only: rec.changes_only,
        total,
        entries: entries.into_iter().collect(),
    }))
}

/// The current contents of an **existing** shape, materialized by folding its stream — creates no new
/// shape (unlike `/v1/shape`). Drives the visualizer's live "contents" preview, which polls this.
async fn get_shape_rows(
    State(engine): State<Engine>,
    Path(id): Path<String>,
    Query(q): Query<ShapeRowsQuery>,
) -> Result<Json<ShapeRowsResp>, AppError> {
    engine.ensure_not_degraded()?;
    let Some(rec) = engine.get_shape(&id).await else {
        return Err(AppError {
            status: StatusCode::NOT_FOUND,
            msg: format!("shape {id} not found"),
            retry_after: false,
        });
    };
    // A touch reactivates: if the shape is dormant, replay it live first so the fold is current.
    engine.ensure_active(&id).await?;
    // Fold the shape's whole stream (catch-up reads from -1) into the current key→row map.
    let mut rows: std::collections::HashMap<String, serde_json::Value> = std::collections::HashMap::new();
    let mut offset = "-1".to_string();
    loop {
        let r = engine.read_shape_stream(&rec.stream_path, &offset, false).await?;
        for env in r.envelopes {
            if env.headers.operation == "delete" {
                rows.remove(&env.key);
            } else if let Some(v) = env.value {
                rows.insert(env.key, v);
            }
        }
        // Same breaks as get_shape_log: caught up, closed (retired), empty, or non-advancing.
        let closed = r.closed;
        let previous = offset.clone();
        let next = r.next_offset.clone();
        if let Some(n) = r.next_offset {
            offset = n;
        }
        if !page_should_continue(&previous, next.as_deref(), r.up_to_date, closed) {
            break;
        }
    }
    let count = rows.len();
    let limit = q.limit.unwrap_or(200).min(2000);
    let mut entries: Vec<ShapeRowEntry> = rows.into_iter().map(|(key, value)| ShapeRowEntry { key, value }).collect();
    // Deterministic order for a stable preview: by numeric key when possible, else lexicographic.
    entries.sort_by(|a, b| match (a.key.parse::<i64>(), b.key.parse::<i64>()) {
        (Ok(x), Ok(y)) => x.cmp(&y),
        _ => a.key.cmp(&b.key),
    });
    let truncated = entries.len() > limit;
    entries.truncate(limit);
    Ok(Json(ShapeRowsResp {
        id: rec.id,
        table: rec.table,
        changes_only: rec.changes_only,
        count,
        truncated,
        rows: entries,
    }))
}

#[derive(Deserialize)]
struct ReleaseShapeQuery {
    /// `?purge=true` force-drops the shape NOW (full teardown, stream deleted), bypassing the
    /// retention lifecycle — an admin/debug operation (the visualizer's trash button).
    #[serde(default)]
    purge: bool,
    /// Which subscription to release (ADR-0008) — the caller's own id, or the `~` one a create
    /// returned when it named none. Omitted = the **legacy anonymous** decrement, kept for callers
    /// that never learned their id; it is not retry-safe and prefers an engine-minted claim over an
    /// identified one. Ignored with `?purge=true`, which removes the whole shape.
    #[serde(default)]
    subscription: Option<String>,
}

/// `DELETE /shapes/{id}?subscription=…` = unsubscribe. Releases THAT subscription — repeating it is
/// a no-op, so a caller whose response was lost may safely retry — and the shape itself is retained
/// and follows the retention lifecycle (idle → dormant → evicted; see `crate::retention`). Without
/// `subscription` it is the legacy anonymous decrement.
///
/// With `?purge=true` it instead force-drops the shape immediately (subscribed clients recreate via
/// the normal 404 / must-refetch path).
///
/// Both forms are **durable before they are acknowledged** (ADR-0008): this answers only once the
/// `Left`/`Dropped` is in the restart contract, because a `200` here is a promise that the release or
/// the purge survives a restart — and `ELECTRIC_CIRCUITS_SHAPE_IDLE_SECS=0` is a supported setting in
/// which no lease will ever repair a record that went missing. So it blocks for as long as storage is
/// down; a client timeout means "no answer", never "not done", and retrying is safe (a repeat finds
/// the mutation applied and waits on the same barrier). Cancelling the request cancels nothing: the
/// record is the writer's, and a purge's teardown runs in a task the engine owns.
async fn release_shape(
    State(engine): State<Engine>,
    Path(id): Path<String>,
    Query(q): Query<ReleaseShapeQuery>,
) -> Result<Json<serde_json::Value>, AppError> {
    let _control = engine.admit_control()?;
    if q.purge {
        engine.purge_shape_durable(&id).await?;
    } else {
        let subscription = validate_subscription(q.subscription)?;
        engine.release_subscription_durable(&id, subscription.as_deref()).await?;
    }
    Ok(Json(serde_json::json!({ "ok": true })))
}

/// Resolve a `{table}`/`{name}` path segment to a table identity: a bare segment is the
/// `public.<name>` sugar, `schema.name` is taken as given, anything else is a 400 (never a
/// mis-resolved lookup).
fn path_table(raw: &str) -> Result<TableRef, AppError> {
    TableRef::parse(raw).map_err(|e| AppError {
        status: StatusCode::BAD_REQUEST,
        msg: format!("invalid table '{raw}': {e:#}"),
        retry_after: false,
    })
}

/// `GET /tables/{name}/offset` — the sequencer's position in the (segmented) change log.
///
/// The change log is a sequence of `changes/<n>` streams (ADR-0006), so a bare offset no longer
/// identifies a position: the answer carries the `segment`, its `path`, and the offset within it.
/// A consumer comparing progress must compare `(segment, offset)` — an offset from a later segment
/// can be lexicographically smaller than one from an earlier segment.
async fn table_offset(
    State(engine): State<Engine>,
    Path(name): Path<String>,
) -> Result<Json<serde_json::Value>, AppError> {
    let name = path_table(&name)?;
    match engine.table_offset(&name).await {
        Some(pos) => Ok(Json(serde_json::json!({
            "segment": pos.segment,
            "path": pos.path(),
            "offset": pos.offset,
        }))),
        None => Err(AppError {
            status: StatusCode::NOT_FOUND,
            msg: format!("no tailer for table {name}"),
            retry_after: false,
        }),
    }
}

async fn table_families(State(engine): State<Engine>, Path(name): Path<String>) -> Result<Json<TableStats>, AppError> {
    let name = path_table(&name)?;
    match engine.table_stats(&name).await {
        Some(stats) => Ok(Json(stats)),
        None => Err(AppError {
            status: StatusCode::NOT_FOUND,
            msg: format!("no tailer for table {name}"),
            retry_after: false,
        }),
    }
}

/// `GET /table/{table}/schema` — the table's columns (+ coarse/native types, pk flag) and primary key,
/// so the visualizer can render one input per column in its add-row form.
async fn get_table_schema(
    State(engine): State<Engine>,
    Path(table): Path<String>,
) -> Result<Json<TableSchemaInfo>, AppError> {
    let table = path_table(&table)?;
    match engine.table_schema_info(&table).await {
        Ok(info) => Ok(Json(info)),
        Err(e) => Err(AppError { status: StatusCode::NOT_FOUND, msg: format!("{e:#}"), retry_after: false }),
    }
}

/// Body of `POST /table/{table}/rows`: the new row as `column → value`, under either `columns` or
/// `values`. Omitted columns take their Postgres default / NULL.
#[derive(Deserialize)]
struct InsertRowReq {
    #[serde(default)]
    columns: Option<serde_json::Map<String, serde_json::Value>>,
    #[serde(default)]
    values: Option<serde_json::Map<String, serde_json::Value>>,
}

/// `POST /table/{table}/rows` — insert one row into the table's Postgres relation (parameterized,
/// identifier-quoted). The write is captured by logical replication and flows through the pipeline, so
/// the visualizer sees the change animate. Bad input (unknown column, type mismatch) is a 400.
async fn insert_table_row(
    State(engine): State<Engine>,
    Path(table): Path<String>,
    Json(req): Json<InsertRowReq>,
) -> Result<Json<serde_json::Value>, AppError> {
    let _control = engine.admit_control()?;
    let table = path_table(&table)?;
    let values = req.columns.or(req.values).unwrap_or_default();
    match engine.insert_row(&table, &values).await {
        Ok(v) => Ok(Json(v)),
        Err(e) => Err(AppError { status: StatusCode::BAD_REQUEST, msg: format!("{e:#}"), retry_after: false }),
    }
}

/// Body of `DELETE /table/{table}/rows`: the primary keys of the rows to delete — one
/// `pk column → value` object per row.
#[derive(Deserialize)]
struct DeleteRowsReq {
    keys: Vec<serde_json::Map<String, serde_json::Value>>,
}

/// `DELETE /table/{table}/rows` — delete rows from the table's Postgres relation by primary key
/// (parameterized, identifier-quoted; all keys in one statement). Like the insert, the deletes are
/// captured by logical replication and flow through the pipeline, so the visualizer sees them
/// animate. Bad input (unknown table, non-pk column, missing/NULL key part) is a 400.
async fn delete_table_rows(
    State(engine): State<Engine>,
    Path(table): Path<String>,
    Json(req): Json<DeleteRowsReq>,
) -> Result<Json<serde_json::Value>, AppError> {
    let _control = engine.admit_control()?;
    let table = path_table(&table)?;
    match engine.delete_rows(&table, &req.keys).await {
        Ok(v) => Ok(Json(v)),
        Err(e) => Err(AppError { status: StatusCode::BAD_REQUEST, msg: format!("{e:#}"), retry_after: false }),
    }
}

/// Full pipeline graph for the visualizer (`GET /graph`): tables, shapes with routing placement, and
/// the shared subquery node/edge DAG. Adds no cost to the hot path — reads in-memory topology only.
async fn get_graph(State(engine): State<Engine>) -> Json<crate::engine::EngineGraph> {
    Json(engine.graph().await)
}

#[derive(Deserialize)]
struct NodeIndexQuery {
    sig: String,
    #[serde(default)]
    cap: Option<usize>,
}

/// The live inner-set index of one subquery node (`GET /graph/node?sig=…`) — values + contributor
/// counts, for the visualizer's node-detail "index" view.
async fn get_node_index(
    State(engine): State<Engine>,
    Query(q): Query<NodeIndexQuery>,
) -> Result<Json<crate::engine::NodeIndex>, AppError> {
    match engine.node_index(&q.sig, q.cap.unwrap_or(500)).await {
        Some(idx) => Ok(Json(idx)),
        None => Err(AppError {
            status: StatusCode::NOT_FOUND,
            msg: format!("node {} not found", q.sig),
            retry_after: false,
        }),
    }
}

/// Full per-node state snapshot (`GET /state`): the live summary of every pipeline node, keyed by
/// graph node id. The visualizer seeds from this, then applies the incremental `{"type":"state"}`
/// events pushed on `/trace`.
async fn get_state(State(engine): State<Engine>) -> Json<crate::engine::StateSnapshot> {
    Json(engine.state_snapshot().await)
}

#[derive(Deserialize)]
struct StateNodeQuery {
    id: String,
}

/// Deep state dump of one node (`GET /state/node?id=<node-id>`): a family router's routing-index
/// contents, an aggregate's fold internals, or a subquery node's inner-set index.
async fn get_state_node(
    State(engine): State<Engine>,
    Query(q): Query<StateNodeQuery>,
) -> Result<Json<serde_json::Value>, AppError> {
    match engine.dump_node(&q.id).await {
        Some(v) => Ok(Json(v)),
        None => {
            Err(AppError { status: StatusCode::NOT_FOUND, msg: format!("node {} not found", q.id), retry_after: false })
        }
    }
}

async fn subquery_stats(State(engine): State<Engine>) -> Json<serde_json::Value> {
    let nodes = engine.subquery_stats().await;
    Json(serde_json::json!({ "nodes": nodes }))
}

async fn replication_lsn(State(engine): State<Engine>) -> Json<serde_json::Value> {
    // Read the change-log position ONCE: three separate reads could straddle a rotation and report
    // a segment with an offset that belongs to a different one.
    let changes = engine.changes_position();
    Json(serde_json::json!({
        "lsn": engine.replication_lsn(),
        "sync": engine.replication_sync(),
        // Deferred subquery flip batches not yet propagated. Convergence barrier = sync caught up
        // + per-table offsets at tail + pendingFlips == 0. An abandoned batch never decrements, so
        // pendingFlips can only reach 0 when every computed effect really did land.
        "pendingFlips": engine.pending_flips(),
        // Backfill/subset snapshots currently waiting for transactions the sequencer already fanned
        // out to become visible to new snapshots (see `pg::SequencedXids`). Diagnostic only.
        "visibilityWaits": engine.settle_waits_active(),
        // The settle record (size, peak, bytes, transactions dropped at its bound) and counters
        // (poller ticks/failures, checks, retakes, timeouts, admission rejections) plus the settle
        // wait-duration distribution. Diagnostic only.
        "settle": engine.settle_stats_json(),
        // Flip batches abandoned after exhausting their retries; non-zero means the engine is
        // degraded (its membership-bearing routes answer 503) and must be restarted.
        "flipFailures": engine.flip_failures(),
        // Which replication slot, in which cluster, this engine is bound to (ADR-0004), and whether
        // that binding still holds. `state: "broken"` is the refuse policy's degraded state: ingest
        // is stopped, shape routes answer 503, and `POST /epoch/reset` is the way out.
        "epoch": engine.epoch_json(),
        // The INGESTOR's position in the segmented change log (ADR-0006): which segment it appends
        // to, and the tail offset of its last append. Additive — the four fields above are
        // untouched — and it is what a convergence barrier reads to know which `changes/<n>` to
        // HEAD for the tail (the sequencer's own position is `GET /tables/{name}/offset`).
        "changes": {
            "segment": changes.segment,
            "path": changes.path(),
            "offset": changes.offset,
        },
    }))
}

/// `POST /epoch/reset` — the operator's half of `ELECTRIC_CIRCUITS_RESET_ON_SLOT_LOSS=false`:
/// retire every shape, bind a new epoch on a fresh slot, and let ingest resume.
///
/// Refused with 409 unless the epoch is actually broken. It is a destructive operation (every shape
/// stream is closed and deleted), and on a healthy engine there is nothing it could fix — the slot
/// it would drop is the one the ingestor is streaming from.
async fn epoch_reset(State(engine): State<Engine>) -> Result<Json<serde_json::Value>, AppError> {
    let _control = engine.admit_control()?;
    let Some(reason) = engine.epoch_broken() else {
        return Err(AppError {
            status: StatusCode::CONFLICT,
            msg: "the epoch is not broken; nothing to reset".to_string(),
            retry_after: false,
        });
    };
    engine.reset_epoch(reason).await?;
    Ok(Json(serde_json::json!({ "ok": true, "epoch": engine.epoch_json() })))
}

async fn get_metrics(State(engine): State<Engine>) -> Json<serde_json::Value> {
    Json(engine.metrics.snapshot())
}

async fn reset_metrics(State(engine): State<Engine>) -> Json<serde_json::Value> {
    engine.metrics.reset();
    Json(serde_json::json!({ "ok": true }))
}

/// JSON memory snapshot — process RSS/virtual + engine cardinalities. Recomputes cardinalities fresh so
/// the harness reads the exact state right after creating a batch of shapes (and republishes the OTel
/// gauges in the same pass).
///
/// The HTTP handler is one call site for `Engine::mem_bytes` — the expensive `heap_bytes`/`MemBytes`
/// byte walk (Phase 0 self-accounting) runs here and in the slower diagnostic memory logger, never
/// on the 500ms background sampler (see `mem::spawn_sampler` / `Engine::mem_cardinalities`'s doc
/// comments).
async fn get_memory(State(engine): State<Engine>) -> Json<serde_json::Value> {
    let card = engine.mem_cardinalities().await.with_bytes(engine.mem_bytes().await);
    crate::mem::publish(&engine.memory_gauges, &card);
    Json(crate::mem::snapshot_json(&engine.memory_gauges, &card))
}

/// Diagnostic: dbsp profiler dump for every dbsp circuit the engine runs (see
/// `Engine::dbsp_profile_dump`). Heavy — on-demand only, introspection-gated.
async fn get_dbsp_profile(State(engine): State<Engine>) -> Json<serde_json::Value> {
    Json(engine.dbsp_profile_dump().await)
}

/// OpenTelemetry metrics in Prometheus exposition format (what an OTel collector's prometheus receiver
/// scrapes). Reflects the last published sample (refreshed by the background sampler + every `/memory`).
async fn get_prometheus(State(engine): State<Engine>) -> Response {
    ([(axum::http::header::CONTENT_TYPE, "text/plain; version=0.0.4")], engine.prometheus_text()).into_response()
}

pub(crate) struct AppError {
    pub(crate) status: StatusCode,
    pub(crate) msg: String,
    pub(crate) retry_after: bool,
}

impl From<anyhow::Error> for AppError {
    fn from(e: anyhow::Error) -> Self {
        let ownership_error = e.downcast_ref::<crate::deployment::OwnershipError>();
        // A degradation is the one engine failure that is not a 500: the request was fine, the
        // engine is not. Matched by type, never by message text — lost membership effects
        // (`Degraded`) and a broken epoch (`EpochBroken`, ADR-0004) alike.
        let retry_after = e.downcast_ref::<crate::engine::CreateRaced>().is_some()
            // A snapshot that could not settle is waiting on Postgres (a synchronous standby), not
            // broken: the same request succeeds once the held commit is visible.
            || e.downcast_ref::<crate::pg::SnapshotUnsettled>().is_some()
            // A deferred reactivation is the caller's to retry, not evidence of a broken engine:
            // the shape is still there, parked back as dormant, and the next touch replays it.
            || e.downcast_ref::<crate::engine::ReactivationDeferred>().is_some()
            || e.downcast_ref::<crate::engine::ControlAdmissionClosed>().is_some()
            || e.downcast_ref::<crate::engine::DeploymentNotReady>().is_some()
            || e.downcast_ref::<crate::deployment::OwnershipBackend>().is_some()
            || matches!(ownership_error, Some(crate::deployment::OwnershipError::PrecloseRequired))
            || matches!(ownership_error, Some(crate::deployment::OwnershipError::FreshReceiptRequired));
        let status = if retry_after
            || e.downcast_ref::<crate::engine::Degraded>().is_some()
            || e.downcast_ref::<crate::engine::EpochBroken>().is_some()
            || e.downcast_ref::<crate::engine::EpochResetting>().is_some()
            || matches!(ownership_error, Some(crate::deployment::OwnershipError::Disabled))
        {
            StatusCode::SERVICE_UNAVAILABLE
        // A subscription id that already names another shape is the caller's conflict to resolve,
        // not a server fault and not something a retry changes (ADR-0008).
        } else if e.downcast_ref::<crate::engine::SubscriptionConflict>().is_some()
            || matches!(ownership_error, Some(crate::deployment::OwnershipError::Conflict))
        {
            StatusCode::CONFLICT
        } else if e.downcast_ref::<crate::engine::SchemaDefinitionUnsupported>().is_some() {
            StatusCode::BAD_REQUEST
        } else if e.downcast_ref::<crate::engine::ReactivationRecreate>().is_some() {
            // GONE, not CONFLICT. The create/join path never reaches here in the ordinary case —
            // `retry_create` redoes the attempt as a fresh create and the client receives a 2xx
            // carrying a NEW shape id, which the iOS SDK already treats as a replacement and
            // reseeds from. This status is what a *read* route (rows/log) and an exhausted
            // fall-through return, and 410 is the code that SDK's stream reader already maps to
            // "gone, get a fresh one"; 409 it classifies as a terminal failure with no reseed.
            StatusCode::GONE
        } else {
            StatusCode::INTERNAL_SERVER_ERROR
        };
        AppError { status, msg: format!("{e:#}"), retry_after }
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let mut response = (self.status, Json(serde_json::json!({ "error": self.msg }))).into_response();
        if self.retry_after {
            response.headers_mut().insert(header::RETRY_AFTER, HeaderValue::from_static("1"));
        }
        response
    }
}

#[cfg(test)]
mod tests {
    use axum::body::Body;
    use axum::http::{HeaderMap, HeaderValue, Request, StatusCode, header};
    use tower::ServiceExt;

    use super::{
        AppError, CreateShapeReq, Predicate, ShapeRequest, SubsetFeedRequest, health_json, page_should_continue,
        require_private_admin_with_secret, require_runtime_gateway, router_with_introspection,
    };
    use crate::predicate::PredicateJson;
    use crate::{ds::DsClient, engine::Engine};

    // The fleet's healthcheck does an awk string-compare against the exact body, so byte-for-byte
    // exactness (no whitespace) matters more than JSON equivalence.
    #[test]
    fn health_body_is_exact() {
        assert_eq!(health_json("waiting"), r#"{"status":"waiting"}"#);
        assert_eq!(health_json("starting"), r#"{"status":"starting"}"#);
        assert_eq!(health_json("active"), r#"{"status":"active"}"#);
    }

    #[test]
    fn empty_advanced_pages_continue_rows_and_log_folds() {
        assert!(page_should_continue("offset-1", Some("offset-2"), false, false));
        assert!(!page_should_continue("offset-2", Some("offset-2"), false, false));
        assert!(!page_should_continue("offset-1", Some("offset-2"), true, false));
    }

    /// The iOS SDK's one "gone, get a fresh one" vocabulary is 404/410 (`StreamMaterialization`
    /// turns those into `.notFound`/`.gone` and reseeds); 409 it classifies as a terminal
    /// `ClientError.http` with no reseed. 410 also cannot be confused with an unknown shape id or
    /// route, and it keeps 409 for the ElectricSQL protocol's own must-refetch code.
    #[test]
    fn over_budget_reactivation_is_gone_not_a_conflict() {
        let err = AppError::from(anyhow::Error::new(crate::engine::ReactivationRecreate {
            shape: "shape/s1".into(),
            reason: crate::engine::RecreateReason::OverBudget,
        }));
        assert_eq!(err.status, StatusCode::GONE);
    }

    /// A reactivation that DEFERRED — a transient HEAD failure on the change log, a replay page
    /// the store never finished — is not a bug in the engine and not a state the caller can act on
    /// by recreating: the shape is parked back as dormant and the next touch retries it. Answering
    /// it with a bare 500 tells a client that backs off on 5xx the same thing a panic would, and
    /// tells an operator watching error rates nothing. It is a retryable refusal, with the
    /// `Retry-After` every other retryable refusal on these routes carries.
    #[tokio::test]
    async fn a_deferred_reactivation_is_retryable_not_a_server_fault() {
        let store = std::sync::Arc::new(crate::ds::ScriptedStore { head_status: 503, ..Default::default() });
        let engine =
            Engine::new_for_in_process_test(DsClient::with_test_store("scripted://provider".into(), store.clone()));
        engine
            .park_dormant_shape_for_test(
                "s1",
                "users",
                crate::changelog::LogPosition { segment: 0, offset: "0".into() },
            )
            .await;
        let app = router_with_introspection(engine, false);
        let response = app.oneshot(Request::get("/shapes/s1/rows").body(Body::empty()).unwrap()).await.unwrap();
        assert_eq!(
            response.status(),
            StatusCode::SERVICE_UNAVAILABLE,
            "a deferred reactivation is a retryable refusal, not a server fault"
        );
        assert_eq!(
            response.headers().get(header::RETRY_AFTER).and_then(|v| v.to_str().ok()),
            Some("1"),
            "a caller told to retry must be told when"
        );
    }

    /// The three answers a reactivation can produce, side by side, because the value of each one
    /// is that it is not the other two: retry this exact shape, get a replacement, or resolve a
    /// name you gave to two shapes. A 500 is none of them.
    #[test]
    fn reactivation_outcomes_map_to_distinct_actionable_statuses() {
        let deferred = AppError::from(anyhow::Error::new(crate::engine::ReactivationDeferred::new(
            "s1",
            "change-log HEAD unavailable",
        )));
        assert_eq!(deferred.status, StatusCode::SERVICE_UNAVAILABLE);
        assert!(deferred.retry_after, "a retryable refusal carries Retry-After");

        let recreate = AppError::from(anyhow::Error::new(crate::engine::ReactivationRecreate {
            shape: "shape/s1".into(),
            reason: crate::engine::RecreateReason::JoinTimedOut,
        }));
        assert_eq!(recreate.status, StatusCode::GONE);
        assert!(!recreate.retry_after, "the same shape is not coming back; retrying it is not the action");

        let conflict = AppError::from(anyhow::Error::new(crate::engine::SubscriptionConflict {
            subscription: "sub-1".into(),
            shape: "s2".into(),
        }));
        assert_eq!(conflict.status, StatusCode::CONFLICT);
        assert!(!conflict.retry_after);
    }

    #[test]
    fn private_admin_requires_its_dedicated_control_secret() {
        let mut headers = HeaderMap::new();
        headers.insert(header::AUTHORIZATION, HeaderValue::from_static("Bearer gateway-secret"));
        assert_eq!(
            require_private_admin_with_secret(&headers, Some("controller-secret")).unwrap_err().status,
            StatusCode::UNAUTHORIZED
        );

        headers.insert(header::AUTHORIZATION, HeaderValue::from_static("Bearer controller-secret"));
        assert!(require_private_admin_with_secret(&headers, Some("controller-secret")).is_ok());
        assert_eq!(
            require_private_admin_with_secret(&headers, None).unwrap_err().status,
            StatusCode::SERVICE_UNAVAILABLE
        );
    }

    #[test]
    fn runtime_gateway_requires_a_configured_distinct_bearer() {
        let mut headers = HeaderMap::new();
        assert_eq!(
            require_runtime_gateway(&headers, Some("gateway"), Some("controller")).unwrap_err().status,
            StatusCode::UNAUTHORIZED
        );
        for invalid in ["Bearer controller", "Bearer wrong", "gateway"] {
            headers.insert(header::AUTHORIZATION, HeaderValue::from_static(invalid));
            assert_eq!(
                require_runtime_gateway(&headers, Some("gateway"), Some("controller")).unwrap_err().status,
                StatusCode::UNAUTHORIZED
            );
        }
        headers.insert(header::AUTHORIZATION, HeaderValue::from_static("Bearer gateway"));
        assert!(require_runtime_gateway(&headers, Some("gateway"), Some("controller")).is_ok());
        for gateway in [None, Some("")] {
            assert_eq!(
                require_runtime_gateway(&headers, gateway, Some("controller")).unwrap_err().status,
                StatusCode::SERVICE_UNAVAILABLE
            );
        }
        assert_eq!(
            require_runtime_gateway(&headers, Some("gateway"), Some("gateway")).unwrap_err().status,
            StatusCode::SERVICE_UNAVAILABLE
        );
    }

    #[tokio::test]
    async fn private_admin_routes_reject_the_gateway_secret_and_accept_the_control_secret() {
        crate::config::set_globals(
            "http-route-test",
            "http-route-test",
            Some("gateway-secret"),
            Some("controller-secret"),
        );
        let engine = Engine::new_for_in_process_test(DsClient::new_for_in_process_test("http://127.0.0.1:1"));
        let app = router_with_introspection(engine, false);

        let gateway_response = app
            .clone()
            .oneshot(
                Request::post("/_admin/control-admission/close")
                    .header(header::AUTHORIZATION, "Bearer gateway-secret")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(gateway_response.status(), StatusCode::UNAUTHORIZED);

        let control_response = app
            .oneshot(
                Request::post("/_admin/control-admission/close")
                    .header(header::AUTHORIZATION, "Bearer controller-secret")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(control_response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn deployment_promote_requires_the_receiving_successor_revision_in_its_json_contract() {
        crate::config::set_globals(
            "http-route-test",
            "http-route-test",
            Some("gateway-secret"),
            Some("controller-secret"),
        );
        let engine = Engine::new_for_in_process_test(DsClient::new_for_in_process_test("http://127.0.0.1:1"));
        let app = router_with_introspection(engine, false);
        let body = serde_json::json!({
            "coordinationKey": "a".repeat(64),
            "ownerRevision": "revision-a",
            "generation": 1,
            "handoffId": "00000000-0000-0000-0000-000000000000",
            "sourceCommitId": "00000000-0000-0000-0000-000000000000"
        })
        .to_string();
        let response = app
            .oneshot(
                Request::post("/_admin/deployment/promote")
                    .header(header::AUTHORIZATION, "Bearer controller-secret")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
    }

    #[tokio::test]
    async fn managed_ownership_routes_report_an_unreachable_coordinator_as_retryable() {
        crate::config::set_globals(
            "http-route-test",
            "http-route-test",
            Some("gateway-secret"),
            Some("controller-secret"),
        );
        let pg_url = "postgres://127.0.0.1:1/unreachable".to_string();
        let auth = "Bearer controller-secret";

        let status_engine =
            Engine::new_pg_for_in_process_test(DsClient::new_for_in_process_test("http://127.0.0.1:1"), pg_url.clone());
        status_engine.install_managed_role_for_test(false);
        let status = router_with_introspection(status_engine, false)
            .oneshot(
                Request::get("/_admin/deployment/status")
                    .header(header::AUTHORIZATION, auth)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(status.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(status.headers().get(header::RETRY_AFTER), Some(&HeaderValue::from_static("1")));

        let promote_engine =
            Engine::new_pg_for_in_process_test(DsClient::new_for_in_process_test("http://127.0.0.1:1"), pg_url.clone());
        promote_engine.install_managed_role_for_test(false);
        let promote = router_with_introspection(promote_engine, false)
            .oneshot(
                Request::post("/_admin/deployment/promote")
                    .header(header::AUTHORIZATION, auth)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "coordinationKey": "a".repeat(64),
                            "ownerRevision": "revision-a",
                            "successorRevision": "test-revision",
                            "generation": 1,
                            "handoffId": "00000000-0000-0000-0000-000000000000",
                            "sourceCommitId": "00000000-0000-0000-0000-000000000000"
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(promote.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(promote.headers().get(header::RETRY_AFTER), Some(&HeaderValue::from_static("1")));

        let quiesce_engine =
            Engine::new_pg_for_in_process_test(DsClient::new_for_in_process_test("http://127.0.0.1:1"), pg_url);
        quiesce_engine.install_managed_role_for_test(false);
        quiesce_engine.close_control_admission_with_receipt_barrier().await;
        quiesce_engine.record_source_drain_receipt_for_test("00000000-0000-0000-0000-000000000000");
        let quiesce = router_with_introspection(quiesce_engine, false)
            .oneshot(
                Request::post("/_admin/deployment/quiesce")
                    .header(header::AUTHORIZATION, auth)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "coordinationKey": "a".repeat(64),
                            "ownerRevision": "test-revision",
                            "generation": 1,
                            "handoffId": "00000000-0000-0000-0000-000000000000",
                            "sourceCommitId": "00000000-0000-0000-0000-000000000000"
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(quiesce.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(quiesce.headers().get(header::RETRY_AFTER), Some(&HeaderValue::from_static("1")));
    }

    #[test]
    fn retry_after_is_reserved_for_retryable_unavailability() {
        let degraded: AppError = anyhow::Error::new(crate::engine::Degraded).into();
        let preclose: AppError = anyhow::Error::new(crate::deployment::OwnershipError::PrecloseRequired).into();
        let fresh: AppError = anyhow::Error::new(crate::deployment::OwnershipError::FreshReceiptRequired).into();
        assert!(!degraded.retry_after);
        assert!(preclose.retry_after);
        assert!(fresh.retry_after);
    }

    #[tokio::test]
    async fn managed_standby_cannot_reset_but_active_owner_keeps_diagnostics_and_recovery_reachable() {
        let active = Engine::new_for_in_process_test(DsClient::new_for_in_process_test("http://127.0.0.1:1"));
        active.install_managed_role_for_test(true);
        active.force_degraded();
        let active_app = router_with_introspection(active, false);
        assert_eq!(
            active_app
                .clone()
                .oneshot(Request::get("/replication/lsn").body(Body::empty()).unwrap())
                .await
                .unwrap()
                .status(),
            StatusCode::OK
        );
        // No broken epoch is installed, so reaching the reset handler is its stable 409—not the
        // standby middleware's retryable 503.
        assert_eq!(
            active_app.oneshot(Request::post("/epoch/reset").body(Body::empty()).unwrap()).await.unwrap().status(),
            StatusCode::CONFLICT
        );

        let standby = Engine::new_for_in_process_test(DsClient::new_for_in_process_test("http://127.0.0.1:1"));
        standby.install_managed_role_for_test(false);
        let standby_app = router_with_introspection(standby, false);
        assert_eq!(
            standby_app.oneshot(Request::post("/epoch/reset").body(Body::empty()).unwrap()).await.unwrap().status(),
            StatusCode::SERVICE_UNAVAILABLE
        );
    }

    #[tokio::test]
    async fn managed_data_routes_preserve_degradation_taxonomy_before_readiness_fencing() {
        let degraded = Engine::new_for_in_process_test(DsClient::new_for_in_process_test("http://127.0.0.1:1"));
        degraded.install_managed_role_for_test(true);
        degraded.force_degraded();
        let response = router_with_introspection(degraded, false)
            .oneshot(Request::get("/tables").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(response.headers().get(header::RETRY_AFTER), None);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert!(std::str::from_utf8(&body).unwrap().contains("degraded: subquery membership effects were lost"));

        let broken = Engine::new_for_in_process_test(DsClient::new_for_in_process_test("http://127.0.0.1:1"));
        broken.install_managed_role_for_test(true);
        assert!(broken.latch_epoch_break(crate::engine::EpochBreakReason::SlotLost, "test_slot"));
        let response = router_with_introspection(broken, false)
            .oneshot(Request::get("/tables").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(response.headers().get(header::RETRY_AFTER), None);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert!(std::str::from_utf8(&body).unwrap().contains("epoch broken"));

        let standby = Engine::new_for_in_process_test(DsClient::new_for_in_process_test("http://127.0.0.1:1"));
        standby.install_managed_role_for_test(false);
        let response = router_with_introspection(standby, false)
            .oneshot(Request::get("/tables").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(response.headers().get(header::RETRY_AFTER), Some(&HeaderValue::from_static("1")));
    }

    #[test]
    fn documented_requests_round_trip_into_runtime_requests() {
        let shape = ShapeRequest {
            table: "public.items".to_string(),
            where_: None,
            columns: Some(vec!["id".to_string()]),
            changes_only: Some(false),
            subscription: Some("ios".to_string()),
        };
        let runtime: CreateShapeReq = serde_json::from_value(serde_json::to_value(shape).unwrap()).unwrap();
        assert_eq!(runtime.table.to_string(), "public.items");
        assert_eq!(runtime.columns, Some(vec!["id".to_string()]));
        assert!(!runtime.changes_only);

        let feed = SubsetFeedRequest {
            table: "public.items".to_string(),
            where_: None,
            columns: None,
            subscription: Some("ios".to_string()),
        };
        let wire = serde_json::to_value(feed).unwrap();
        assert!(wire.get("changesOnly").is_none(), "changesOnly is server-controlled for subset feeds");
        let runtime: CreateShapeReq = serde_json::from_value(wire).unwrap();
        assert!(!runtime.changes_only, "the runtime request defaults changesOnly to false before the route override");
    }

    #[test]
    fn populated_recursive_predicate_dto_round_trips_to_runtime_grammar() {
        let wire = serde_json::json!({
            "and": [
                {"col": "status", "op": "eq", "value": "open"},
                {"not": {"isNull": true, "col": "archived_at"}},
                {"or": [
                    {"col": "priority", "op": "gte", "value": 2},
                    {"col": "project_id", "in": {
                        "table": "public.projects",
                        "project": "id",
                        "where": {"col": "active", "op": "eq", "value": true}
                    }, "negated": false}
                ]}
            ]
        });
        let dto: Predicate = serde_json::from_value(wire.clone()).unwrap();
        let runtime: PredicateJson = serde_json::from_value(serde_json::to_value(dto).unwrap()).unwrap();
        assert_eq!(serde_json::to_value(runtime).unwrap(), wire);
    }
}
