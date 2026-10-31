//! **Per-transaction atomic emission survives chunking** (ADR-0003), driven against a real engine
//! (library mode) + sequencer and a fake durable-streams server that hands the change log out one
//! page at a time.
//!
//! A commit too large for one request body reaches the change log as several appends, and
//! durable-streams exposes each append atomically — so a reader long-polling the segment tail sees
//! chunk 1 on its own. Splitting the page into transactions by `(txid, lsn)` alone would make that
//! chunk look like a whole transaction and flush it to the shape streams, and a subscriber would
//! observe a fraction of a commit. The ingestor therefore marks the LAST envelope of every
//! transaction (`headers.last`), and the sequencer holds an unterminated trailing run back.
//!
//! Holding is not free of consequences, and each test here pins one of them down:
//!   1. an incomplete run is not flushed, a re-delivered prefix does not double-apply, and the
//!      completed transaction is flushed once — with publication pinned while held and released
//!      after;
//!   2. the "already held, skip it" filter applies to the held transaction ONLY: complete
//!      transactions that follow it in the same page — including ones whose `seq` restarts at 0 —
//!      are fanned out untouched (they are acknowledged, so nothing would ever re-deliver them);
//!   3. a page that completes one held run and starts another re-pins to ITS OWN page, so a
//!      catch-up over consecutive chunked commits does not freeze the checkpoint at the first one;
//!   4. progress made before a hold is checkpointed even though the hold pins the position — the
//!      de-duplication highwater moves on its own, and a crash must not re-apply what it covers.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use axum::Router;
use axum::extract::{Request, State};
use axum::http::{Method, StatusCode};
use axum::response::{IntoResponse, Response};
use electric_circuits_engine::ds::{DsClient, Envelope};
use electric_circuits_engine::engine::Engine;
use electric_circuits_engine::schema::Schema;
use electric_circuits_engine::table_ref::TableRef;

/// One recorded append: the stream path and the envelopes it carried.
type ShapeAppend = (String, Vec<Envelope>);
/// One page of the scripted change log: `next-offset` and the JSON body served at a given offset.
type Page = (String, String);

/// A durable-streams stub whose change log is a **script**: a map from the offset a reader asks for
/// to the page it gets. An offset with no page parks, like a real long-poll — which is what "the
/// ingestor has not appended the next chunk yet" looks like.
#[derive(Clone, Default)]
struct FakeLog {
    pages: Arc<Mutex<HashMap<String, Page>>>,
    /// Every POST to a `shape/*` stream — i.e. the per-transaction flushes.
    appends: Arc<Mutex<Vec<ShapeAppend>>>,
    /// Every event appended to the durable catalog (`meta/catalog`).
    catalog: Arc<Mutex<Vec<serde_json::Value>>>,
}

impl FakeLog {
    fn serve(&self, at: &str, next: &str, envs: &[String]) {
        self.pages.lock().unwrap().insert(at.to_string(), (next.to_string(), format!("[{}]", envs.join(","))));
    }

    fn shape_flushes(&self, path: &str) -> Vec<Vec<Envelope>> {
        self.appends.lock().unwrap().iter().filter(|(p, _)| p == path).map(|(_, e)| e.clone()).collect()
    }

    /// The `Offset` checkpoints the sequencer has written, as `(position offset, highwater)`.
    fn checkpoints(&self) -> Vec<(String, Option<serde_json::Value>)> {
        self.catalog
            .lock()
            .unwrap()
            .iter()
            .filter(|e| e.get("t").and_then(|t| t.as_str()) == Some("offset"))
            .map(|e| (e["pos"]["offset"].as_str().unwrap_or_default().to_string(), e.get("highwater").cloned()))
            .collect()
    }
}

/// One envelope of a scripted transaction, exactly as the ingestor stamps it.
fn env_json(txid: u32, lsn: &str, key: &str, seq: u32, last: bool) -> String {
    let marker = if last { r#","last":true"# } else { "" };
    format!(
        r#"{{"type":"public.t","key":"{key}","value":{{"id":"{key}"}},"headers":{{"operation":"insert","txid":"{txid}","lsn":"{lsn}","seq":{seq}{marker}}}}}"#
    )
}

/// [`env_json`] with the commit time the ingestor stamps from the pgoutput `Commit` message.
fn env_json_at(txid: u32, lsn: &str, key: &str, seq: u32, last: bool, commit_ts: i64) -> String {
    env_json(txid, lsn, key, seq, last).replacen(r#""seq":"#, &format!(r#""commit_ts":{commit_ts},"seq":"#), 1)
}

async fn ds_handler(State(log): State<FakeLog>, req: Request) -> Response {
    let path = req
        .uri()
        .path()
        .trim_start_matches("/")
        .rsplit_once("/queries/test-query/")
        .map_or_else(|| req.uri().path().trim_start_matches('/').to_string(), |(_, logical)| logical.to_string());
    let query = req.uri().query().unwrap_or("").to_string();
    match *req.method() {
        Method::PUT | Method::DELETE => StatusCode::OK.into_response(),
        // The change log's boot walk HEADs the current segment (ADR-0006): present, never closed.
        Method::HEAD => ([("stream-next-offset", "tip")]).into_response(),
        Method::POST => {
            if req.headers().get("stream-closed").is_some() || path.starts_with("changes") {
                return StatusCode::OK.into_response();
            }
            let body = match axum::body::to_bytes(req.into_body(), usize::MAX).await {
                Ok(b) => b,
                Err(_) => return StatusCode::BAD_REQUEST.into_response(),
            };
            if path.starts_with("shape") {
                if let Ok(envs) = serde_json::from_slice::<Vec<Envelope>>(&body) {
                    log.appends.lock().unwrap().push((path, envs));
                }
            } else if path.starts_with("meta")
                && let Ok(events) = serde_json::from_slice::<Vec<serde_json::Value>>(&body)
            {
                log.catalog.lock().unwrap().extend(events);
            }
            StatusCode::OK.into_response()
        }
        Method::GET if path.starts_with("changes") => {
            let at = query.split('&').find_map(|kv| kv.strip_prefix("offset=")).unwrap_or("-1").to_string();
            let page = log.pages.lock().unwrap().get(&at).cloned();
            match page {
                Some((next, body)) => ([("stream-next-offset", next.as_str())], body).into_response(),
                // Nothing appended past here yet: park, like a real long-poll.
                None => {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                    (StatusCode::NO_CONTENT, [("stream-next-offset", at.as_str())]).into_response()
                }
            }
        }
        Method::GET => ([("stream-next-offset", "tip"), ("stream-up-to-date", "1")], "[]").into_response(),
        _ => StatusCode::METHOD_NOT_ALLOWED.into_response(),
    }
}

/// A library-mode engine with one match-all shape on `public.t`, reading the scripted log.
async fn boot() -> (Engine, FakeLog, String, TableRef) {
    let state = FakeLog::default();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let ds_url = format!("http://{}", listener.local_addr().unwrap());
    let app = Router::new().fallback(ds_handler).with_state(state.clone());
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });

    let engine = Engine::new_for_in_process_test(DsClient::new_for_in_process_test(&ds_url));
    let schema: Schema = serde_json::from_value(serde_json::json!({
        "tables": { "t": { "columns": { "id": { "type": "text" } }, "primaryKey": "id" } }
    }))
    .unwrap();
    engine.define_schema(&schema).await.unwrap();
    let t = TableRef::parse("t").unwrap();
    let shape = engine.create_shape(&t, None, None, false, false).await.unwrap();
    (engine, state, shape.stream_path, t)
}

async fn wait_for(mut cond: impl FnMut() -> bool, what: &str) {
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while std::time::Instant::now() < deadline {
        if cond() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("timed out waiting for {what}");
}

async fn wait_for_progress_or_shutdown(engine: &Engine, table: &TableRef, expected: &str) {
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while std::time::Instant::now() < deadline {
        if engine.shutdown_token().is_shutting_down() {
            return;
        }
        if engine.table_offset(table).await.unwrap().offset != expected {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("timed out waiting for the sequencer to consume the malformed page or request shutdown");
}

/// Chunk 1 alone flushes nothing and pins publication; a re-delivered prefix does not double-apply;
/// the marked final chunk flushes the transaction once, whole, and releases the pin.
#[tokio::test]
async fn a_chunked_transaction_is_flushed_once_and_only_when_complete() {
    let (engine, log, stream, t) = boot().await;
    // Only chunk 1 is on the log.
    log.serve("-1", "01", &[env_json(100, "0/10", "1", 0, false), env_json(100, "0/10", "2", 1, false)]);

    tokio::time::sleep(Duration::from_millis(400)).await;
    assert!(log.shape_flushes(&stream).is_empty(), "a chunk of an incomplete transaction must not be flushed");
    // Publication is pinned where the held run began: `processed` is the restart point,
    // `GET /tables/{name}/offset` and the segment-deletion floor.
    assert_eq!(engine.table_offset(&t).await.unwrap().offset, "-1", "pinned while held");

    // The ingestor failed on a later chunk, so Postgres re-delivered the WHOLE transaction: the
    // same seqs arrive again ahead of the rest. Still nothing, and no double-apply.
    log.serve("01", "02", &[env_json(100, "0/10", "1", 0, false), env_json(100, "0/10", "2", 1, false)]);
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert!(log.shape_flushes(&stream).is_empty(), "a re-delivered prefix is still not a transaction");

    // The final chunk, marked.
    log.serve("02", "03", &[env_json(100, "0/10", "3", 2, false), env_json(100, "0/10", "4", 3, true)]);
    wait_for(|| !log.shape_flushes(&stream).is_empty(), "the completed transaction to be flushed").await;

    let flushes = log.shape_flushes(&stream);
    assert_eq!(flushes.len(), 1, "one transaction, one flush: {flushes:?}");
    let keys: Vec<&str> = flushes[0].iter().map(|e| e.key.as_str()).collect();
    assert_eq!(keys, vec!["1", "2", "3", "4"], "the whole transaction, in order, exactly once");

    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(log.shape_flushes(&stream).len(), 1, "and nothing further for it");
    assert_ne!(engine.table_offset(&t).await.unwrap().offset, "-1", "the pin is released once it completes");
}

/// The de-duplication that folds a re-delivered prefix into a held run applies to the HELD
/// transaction only.
///
/// After a reconnect Postgres re-delivers the interrupted transaction, and the page can carry
/// complete transactions after it whose `seq` restarts at 0. Filtering the whole page on
/// "seq greater than the last one held" would drop those outright — and they are acknowledged, so
/// nothing would ever deliver them again. That is silent, permanent data loss.
#[tokio::test]
async fn complete_transactions_following_a_held_run_are_never_filtered_by_its_seqs() {
    let (_engine, log, stream, _t) = boot().await;
    // B is huge: its first chunk ends at seq 1001, unmarked.
    log.serve("-1", "01", &[env_json(200, "0/20", "b1", 1000, false), env_json(200, "0/20", "b2", 1001, false)]);
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(log.shape_flushes(&stream).is_empty());

    // B's tail, then two complete single-envelope transactions whose seqs start at 0 again.
    log.serve(
        "01",
        "02",
        &[
            env_json(200, "0/20", "b3", 1002, true),
            env_json(201, "0/21", "c1", 0, true),
            env_json(202, "0/22", "d1", 0, true),
        ],
    );
    wait_for(|| log.shape_flushes(&stream).len() >= 3, "B, C and D to be flushed").await;

    let flushes = log.shape_flushes(&stream);
    assert_eq!(flushes.len(), 3, "three transactions, three flushes: {flushes:?}");
    let per_txn: Vec<Vec<&str>> = flushes.iter().map(|f| f.iter().map(|e| e.key.as_str()).collect()).collect();
    assert_eq!(per_txn, vec![vec!["b1", "b2", "b3"], vec!["c1"], vec!["d1"]], "each once, in order");

    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(log.shape_flushes(&stream).len(), 3, "nothing is emitted twice");
}

/// A page that completes one held run and starts another must re-pin to ITS OWN page.
///
/// Keeping the first page's pin would freeze the published position — and with it the checkpoint,
/// which only fires when the position moves — for the whole of a catch-up over consecutive chunked
/// commits, so a crash would re-apply every transaction flushed in between.
#[tokio::test]
async fn a_new_hold_after_a_completed_one_re_pins_to_its_own_page() {
    let (engine, log, stream, t) = boot().await;
    log.serve("-1", "01", &[env_json(300, "0/30", "a1", 0, false)]);
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(engine.table_offset(&t).await.unwrap().offset, "-1");

    // A completes here, and B's first chunk starts a NEW hold.
    log.serve("01", "02", &[env_json(300, "0/30", "a2", 1, true), env_json(301, "0/31", "b1", 0, false)]);
    wait_for(|| !log.shape_flushes(&stream).is_empty(), "A to be flushed").await;

    let flushes = log.shape_flushes(&stream);
    assert_eq!(flushes.len(), 1);
    assert_eq!(flushes[0].iter().map(|e| e.key.as_str()).collect::<Vec<_>>(), vec!["a1", "a2"]);
    // The pin followed the new hold instead of staying on page 1.
    assert_eq!(
        engine.table_offset(&t).await.unwrap().offset,
        "01",
        "the pin moved to the page the NEW held run began in"
    );
    // B is still held: nothing more is flushed.
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(log.shape_flushes(&stream).len(), 1);
}

/// Progress made before a hold is checkpointed even though the hold pins the position.
///
/// When the pinned position happens to equal the last checkpointed one — the common case on the
/// first page — a position-only checkpoint trigger never fires, so the de-duplication highwater that
/// covers the already-applied transaction never reaches the catalog and a crash re-applies it.
/// Aggregate and subquery contributor weights are not idempotent, so that is a correctness bug, not
/// a performance one.
#[tokio::test]
async fn the_highwater_is_checkpointed_even_while_the_position_is_pinned() {
    let (engine, log, stream, t) = boot().await;
    // One complete transaction, then the first chunk of a large one — all in the very first page,
    // so the pin lands exactly on the position the sequencer started (and last checkpointed) at.
    log.serve("-1", "01", &[env_json(400, "0/40", "a1", 0, true), env_json(401, "0/41", "b1", 0, false)]);
    wait_for(|| !log.shape_flushes(&stream).is_empty(), "A to be flushed").await;
    assert_eq!(engine.table_offset(&t).await.unwrap().offset, "-1", "pinned at the start position");

    // The checkpoint cadence is ~2 s; the highwater has moved even though the position has not.
    wait_for(
        || log.checkpoints().iter().any(|(_, hw)| hw.is_some()),
        "a checkpoint carrying the highwater of the applied transaction",
    )
    .await;
    let (pos, hw) = log.checkpoints().into_iter().find(|(_, hw)| hw.is_some()).unwrap();
    assert_eq!(pos, "-1", "written at the pinned position");
    // A's commit LSN is 0x40 and its last seq 0 — what a restart must de-duplicate against.
    assert_eq!(hw.unwrap(), serde_json::json!([0x40, 0]));
    // ...and B, still held, was not part of it.
    assert_eq!(log.shape_flushes(&stream).len(), 1);
}

/// A fan-out failure is terminal: the malformed committed envelope must remain at the replay
/// boundary, with no highwater/processed/checkpoint advancement that could make the effect vanish.
#[tokio::test]
async fn a_process_envelope_failure_fails_closed_without_progress() {
    let (engine, log, stream, t) = boot().await;
    let malformed = r#"{"type":"public.t","key":"bad","value":{"id":"bad"},"headers":{"operation":"bogus","txid":"500","lsn":"0/50","seq":1,"last":true}}"#;
    log.serve("-1", "01", &[env_json(500, "0/50", "ok", 0, false), malformed.to_string()]);

    wait_for_progress_or_shutdown(&engine, &t, "-1").await;
    assert!(log.shape_flushes(&stream).is_empty(), "a failed envelope must not flush a partial effect");
    assert_eq!(engine.table_offset(&t).await.unwrap().offset, "-1", "failed work stays at the replay boundary");
    assert!(
        log.checkpoints().iter().all(|(offset, _)| offset == "-1"),
        "no checkpoint may advance past the failed envelope: {:?}",
        log.checkpoints()
    );
    assert!(
        log.checkpoints().iter().all(|(_, highwater)| highwater.is_none()),
        "the failed transaction must not advance its highwater: {:?}",
        log.checkpoints()
    );

    engine.shutdown_token().begin();
}

/// If a held transaction completes on the same page as a later processing failure, the safe
/// replay boundary is still the page where the held transaction began, not the failing page.
#[tokio::test]
async fn a_failure_after_a_held_prefix_rewinds_to_the_held_boundary() {
    let (engine, log, stream, t) = boot().await;
    let malformed = r#"{"type":"public.t","key":"bad","value":{"id":"bad"},"headers":{"operation":"bogus","txid":"501","lsn":"0/51","seq":0,"last":true}}"#;
    log.serve("-1", "01", &[env_json(500, "0/50", "b0", 0, false)]);
    wait_for(|| log.shape_flushes(&stream).is_empty(), "the held prefix to remain unflushed").await;
    assert_eq!(engine.table_offset(&t).await.unwrap().offset, "-1", "the held prefix pins the boundary");

    log.serve("01", "02", &[env_json(500, "0/50", "b1", 1, true), malformed.to_string()]);
    wait_for_progress_or_shutdown(&engine, &t, "-1").await;
    wait_for(|| !log.shape_flushes(&stream).is_empty(), "B's completed transaction to flush").await;
    let flushes = log.shape_flushes(&stream);
    let keys: Vec<&str> = flushes[0].iter().map(|e| e.key.as_str()).collect();
    assert_eq!(keys, vec!["b0", "b1"], "B must be emitted exactly once before C fails");
    assert_eq!(engine.table_offset(&t).await.unwrap().offset, "-1", "replay must include B's held prefix");
    assert!(
        log.checkpoints().iter().all(|(offset, _)| offset == "-1"),
        "checkpoint crossed the held boundary: {:?}",
        log.checkpoints()
    );
    assert!(
        log.checkpoints().iter().all(|(_, highwater)| highwater.as_ref() == Some(&serde_json::json!([0x50, 1]))),
        "B's completed highwater must accompany the held boundary: {:?}",
        log.checkpoints()
    );
    engine.shutdown_token().begin();
}

/// Every shape envelope a commit produces carries that commit's time (`headers.commit_ts`), across
/// the chunks of a held transaction; a commit whose changes carry none emits none.
#[tokio::test]
async fn shape_output_carries_the_commit_time_of_its_transaction() {
    let (_engine, log, stream, _t) = boot().await;
    const AT: i64 = 1_793_404_800_123;
    log.serve("-1", "01", &[env_json_at(500, "0/50", "1", 0, false, AT)]);
    log.serve("01", "02", &[env_json_at(500, "0/50", "2", 1, true, AT), env_json(501, "0/51", "3", 0, true)]);
    wait_for(|| log.shape_flushes(&stream).len() >= 2, "both transactions to be flushed").await;

    let stamps: Vec<Vec<(String, Option<i64>)>> = log
        .shape_flushes(&stream)
        .iter()
        .map(|f| f.iter().map(|e| (e.key.clone(), e.headers.commit_ts)).collect())
        .collect();
    assert_eq!(
        stamps,
        vec![vec![("1".to_string(), Some(AT)), ("2".to_string(), Some(AT))], vec![("3".to_string(), None)]]
    );
}
