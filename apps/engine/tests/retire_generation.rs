//! `retire-generation` against a Durable Streams stand-in, driven through the engine binary: the
//! dry run, the confirmed run, and each refusal (`#v-retire-generation` on the Circuits deployment
//! page). Each case asserts on the exit code, what the command printed and what the store
//! received — never on the command's internals.
//!
//! The stand-in keeps every stream's events and closed flag and logs every request, so "deleted
//! nothing" is checked as "the store saw no close and no delete", not as a missing log line.

use std::collections::{BTreeMap, HashSet};
use std::sync::{Arc, Mutex};

use axum::Router;
use axum::extract::{Request, State};
use axum::http::{Method, StatusCode};
use axum::response::{IntoResponse, Response};
use serde_json::{Value, json};

/// The in-process test store's generation (`StoreIdentityV1::in_process_test_identity`).
const STORE_GENERATION: &str = "ff8b5fa6-e786-4994-8da0-f14e9e79f318";
/// The in-process test scope's stack namespace: the one the command is configured for.
const NAMESPACE: &str = "test-stack";
/// The rest of the in-process test store's identity, which the deployed-configuration test sets
/// through the environment instead.
const STORE_ID: &str = "2bc96d0b-9740-4f50-97c6-754b2b27d6b0";
const FILESYSTEM_UUID: &str = "253f14d5-cbee-4df8-9e3c-e44c6e41501b";
const DAY: u64 = 86_400;

fn prefix(namespace: &str, query: &str) -> String {
    format!("circuits/v1/{namespace}/stores/{STORE_GENERATION}/queries/{query}/")
}

fn v1() -> String {
    prefix(NAMESPACE, "dashboard-conversation-v1")
}

fn v1_catalog() -> String {
    format!("{}meta/catalog", v1())
}

fn now() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs()
}

#[derive(Default, Clone)]
struct Stream {
    events: Vec<Value>,
    closed: bool,
}

#[derive(Default)]
struct StoreState {
    streams: BTreeMap<String, Stream>,
    /// `"<METHOD> <path>"` for every request, in arrival order (`CLOSE` for a closing POST).
    requests: Vec<String>,
    /// Paths whose DELETE answers 503.
    fail_delete: HashSet<String>,
    /// On the nth HEAD of this path, append one event to it first: an engine writing the catalog.
    append_on_head: Option<(String, usize)>,
    heads: BTreeMap<String, usize>,
    /// The body `GET /_admin/ready` answers; `None` answers 404 (the in-process mode never asks).
    ready: Option<String>,
    /// Answer reads in pages of this many events (`None`: the whole stream in one page).
    page_size: Option<usize>,
    /// A read of this path at this offset answers 404, as a proxy or failover can mid-read.
    missing_page: Option<(String, usize)>,
    /// On the nth GET of this path, append one event to it first: an engine writing the catalog
    /// while it is read.
    append_on_get: Option<(String, usize)>,
    gets: BTreeMap<String, usize>,
}

#[derive(Clone)]
struct Store {
    url: String,
    state: Arc<Mutex<StoreState>>,
}

impl Store {
    async fn start() -> Store {
        let state = Arc::new(Mutex::new(StoreState::default()));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let app = Router::new().fallback(handle).with_state(state.clone());
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        Store { url, state }
    }

    fn put(&self, path: &str, events: Vec<Value>) {
        self.state.lock().unwrap().streams.insert(path.to_string(), Stream { events, closed: false });
    }

    fn paths(&self) -> Vec<String> {
        self.state.lock().unwrap().streams.keys().cloned().collect()
    }

    fn requests(&self) -> Vec<String> {
        self.state.lock().unwrap().requests.clone()
    }

    /// Every request that changes a stream other than by appending to it.
    fn destructive_requests(&self) -> Vec<String> {
        self.requests().into_iter().filter(|r| r.starts_with("DELETE ") || r.starts_with("CLOSE ")).collect()
    }
}

async fn handle(State(state): State<Arc<Mutex<StoreState>>>, req: Request) -> Response {
    let path = req.uri().path().trim_start_matches('/').to_string();
    // `-1` (or no offset) is the beginning; any other offset is an event index this stand-in issued.
    let offset = req
        .uri()
        .query()
        .and_then(|q| q.split('&').find_map(|kv| kv.strip_prefix("offset=")))
        .and_then(|o| o.parse::<usize>().ok())
        .unwrap_or(0);
    let method = req.method().clone();
    let closing = req.headers().get("stream-closed").is_some();
    let body = axum::body::to_bytes(req.into_body(), usize::MAX).await.unwrap();
    let mut st = state.lock().unwrap();
    let label = if method == Method::POST && closing { "CLOSE".to_string() } else { method.to_string() };
    st.requests.push(format!("{label} {path}"));
    if method == Method::GET && path == "_admin/ready" {
        return match &st.ready {
            Some(body) => ([("content-type", "application/json")], body.clone()).into_response(),
            None => StatusCode::NOT_FOUND.into_response(),
        };
    }
    match method {
        Method::PUT => {
            st.streams.entry(path).or_default();
            StatusCode::CREATED.into_response()
        }
        Method::HEAD => {
            let n = {
                let count = st.heads.entry(path.clone()).or_default();
                *count += 1;
                *count
            };
            if st.append_on_head.as_ref().is_some_and(|(p, at)| *p == path && *at == n)
                && let Some(stream) = st.streams.get_mut(&path)
            {
                stream.events.push(json!({"t": "offset", "eid": "late-writer", "pos": {"segment": 1, "offset": "-1"}}));
            }
            match st.streams.get(&path) {
                None => StatusCode::NOT_FOUND.into_response(),
                Some(stream) => {
                    let mut res = ([("stream-next-offset", stream.events.len().to_string())]).into_response();
                    if stream.closed {
                        res.headers_mut().insert("stream-closed", "true".parse().unwrap());
                    }
                    res
                }
            }
        }
        Method::GET => {
            let n = {
                let count = st.gets.entry(path.clone()).or_default();
                *count += 1;
                *count
            };
            if st.append_on_get.as_ref().is_some_and(|(p, at)| *p == path && *at == n)
                && let Some(stream) = st.streams.get_mut(&path)
            {
                stream.events.push(json!({"t": "offset", "eid": "late-writer", "pos": {"segment": 1, "offset": "-1"}}));
            }
            match st.streams.get(&path) {
                None => StatusCode::NOT_FOUND.into_response(),
                Some(_) if st.missing_page.as_ref().is_some_and(|(p, at)| *p == path && *at == offset) => {
                    StatusCode::NOT_FOUND.into_response()
                }
                Some(stream) => {
                    let total = stream.events.len();
                    let end = st.page_size.map_or(total, |size| (offset + size).min(total));
                    let page = &stream.events[offset.min(total)..end];
                    let mut res = (
                        [("stream-next-offset", end.to_string()), ("content-type", "application/json".to_string())],
                        serde_json::to_string(page).unwrap(),
                    )
                        .into_response();
                    if end == total {
                        res.headers_mut().insert("stream-up-to-date", "true".parse().unwrap());
                    }
                    res
                }
            }
        }
        Method::POST => {
            let Some(stream) = st.streams.get_mut(&path) else {
                return StatusCode::NOT_FOUND.into_response();
            };
            if closing {
                stream.closed = true;
                return StatusCode::NO_CONTENT.into_response();
            }
            if stream.closed {
                return (StatusCode::CONFLICT, [("stream-closed", "true")]).into_response();
            }
            let events: Vec<Value> = serde_json::from_slice(&body).unwrap();
            stream.events.extend(events);
            ([("stream-next-offset", stream.events.len().to_string())]).into_response()
        }
        Method::DELETE => {
            if st.fail_delete.contains(&path) {
                return (StatusCode::SERVICE_UNAVAILABLE, "retirement in progress").into_response();
            }
            match st.streams.remove(&path) {
                Some(_) => StatusCode::NO_CONTENT.into_response(),
                None => StatusCode::NOT_FOUND.into_response(),
            }
        }
        _ => StatusCode::METHOD_NOT_ALLOWED.into_response(),
    }
}

struct Run {
    code: i32,
    stdout: String,
    stderr: String,
}

/// The engine binary with the store environment and nothing else.
fn engine_command(store: &Store, program: &str) -> tokio::process::Command {
    let mut command = tokio::process::Command::new(program);
    command.env_clear().env("ELECTRIC_CIRCUITS_DS_URL", &store.url).env("ELECTRIC_CIRCUITS_DS_IN_PROCESS_TEST", "1");
    command
}

/// The engine binary configured as a deployment configures it: the full store identity, its own
/// namespace and query generation, and no in-process test flag. `store_id` is what the command
/// expects the store to attest.
fn deployed_command(store: &Store, store_id: &str, args: &[&str]) -> tokio::process::Command {
    let mut command = tokio::process::Command::new(env!("CARGO_BIN_EXE_electric-circuits-engine"));
    command.arg("retire-generation").args(args).env_clear();
    for (name, value) in [
        ("ELECTRIC_CIRCUITS_DS_URL", store.url.as_str()),
        ("ELECTRIC_CIRCUITS_DS_STORE_ID", store_id),
        ("ELECTRIC_CIRCUITS_DS_STORE_GENERATION", STORE_GENERATION),
        ("ELECTRIC_CIRCUITS_DS_PROTOCOL_VERSION", "1"),
        ("ELECTRIC_CIRCUITS_DS_LAYOUT_VERSION", "1"),
        ("ELECTRIC_CIRCUITS_DS_DURABILITY_MODE", "wal"),
        ("ELECTRIC_CIRCUITS_DS_WAL_SHARDS", "2"),
        ("ELECTRIC_CIRCUITS_DS_STREAM_LANES", "1"),
        ("ELECTRIC_CIRCUITS_DS_FILESYSTEM_UUID", FILESYSTEM_UUID),
        ("ELECTRIC_CIRCUITS_DS_NAMESPACE", "indexed-prod"),
        ("ELECTRIC_CIRCUITS_QUERY_GENERATION", "dashboard-conversation-v2"),
    ] {
        command.env(name, value);
    }
    command
}

/// What a ready store with the in-process test identity answers on `GET /_admin/ready`.
fn readiness() -> String {
    json!({
        "contract_version": "durable-streams-store-ready-v1",
        "status": "ready",
        "artifact_digest": "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        "manifest": {
            "store_id": STORE_ID,
            "store_generation": STORE_GENERATION,
            "protocol_version": 1,
            "layout_version": 1,
            "durability_mode": "wal",
            "wal_shard_count": 2,
            "stream_lane_count": 1,
            "filesystem_uuid": FILESYSTEM_UUID,
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

async fn retire(store: &Store, args: &[&str]) -> Run {
    finish(engine_command(store, env!("CARGO_BIN_EXE_electric-circuits-engine")).arg("retire-generation").args(args))
        .await
}

async fn finish(command: &mut tokio::process::Command) -> Run {
    let out = command.output().await.expect("run the engine binary");
    Run {
        code: out.status.code().expect("the command exits rather than being killed"),
        stdout: String::from_utf8(out.stdout).unwrap(),
        stderr: String::from_utf8(out.stderr).unwrap(),
    }
}

fn shape(id: &str, at: u64) -> Value {
    json!({
        "t": "created",
        "rec": {
            "id": id,
            "table": "public.conversations",
            "stream_path": format!("shape/{id}"),
            "changes_only": false,
            "where_json": null,
            "columns": null,
            "family_key": null,
            "is_subquery": false,
            "aggregate": null,
            "fingerprint": null,
        },
        "sig": null,
        "subscription": format!("sub-{id}"),
        "at": at,
    })
}

/// Event zero of a catalog written for `query` in the in-process test store.
fn store_bound(query: &str) -> Value {
    json!({
        "t": "storeBound",
        "store": {
            "store_id": "2bc96d0b-9740-4f50-97c6-754b2b27d6b0",
            "store_generation": STORE_GENERATION,
            "protocol_version": 1,
            "layout_version": 1,
            "durability_mode": "wal",
            "wal_shard_count": 2,
            "stream_lane_count": 1,
            "filesystem_uuid": "253f14d5-cbee-4df8-9e3c-e44c6e41501b",
        },
        "stack_namespace": NAMESPACE,
        "ingest_epoch": "coupled-v1",
        "query_generation": query,
    })
}

/// A stopped generation, bound to `query`, whose newest event is `last` (unix seconds): live,
/// dormant, pending and retired shapes, a group, and a change log whose segment 0 was deleted and
/// segment 1 is current.
fn catalog_events(query: &str, last: u64) -> Vec<Value> {
    let t = last - 10;
    let mut events = vec![
        store_bound(query),
        shape("s1", t),
        shape("s2", t),
        json!({"t": "dormant", "id": "s2", "resume": {"segment": 1, "offset": "-1"},
               "gate": {"lsn": 0, "xmin": 0, "xmax": 0, "xip": []}}),
        shape("s3", t),
        json!({"t": "dropped", "id": "s3"}),
        shape("s4", t),
        json!({"t": "dropped", "id": "s4"}),
        json!({"t": "retired", "id": "s4"}),
        json!({"t": "groupCreated", "group": {
            "id": "g1", "stream_path": "shape/group-abc-1", "subscription": "sub-g1", "members": [],
            "lsn": "0/0", "snapshot": "1:1:", "horizon": "0/0", "replay_fence": "0/0"}}),
        json!({"t": "changesRotated", "segment": 0, "at": t}),
        json!({"t": "changesRotated", "segment": 1, "at": t + 1}),
        json!({"t": "changesSegmentDeleted", "segment": 0}),
        json!({"t": "offset", "pos": {"segment": 1, "offset": "-1"}}),
        json!({"t": "joined", "id": "s1", "subscription": "sub-renewed", "at": last}),
    ];
    for (n, event) in events.iter_mut().enumerate() {
        event["eid"] = json!(format!("v1-{n}"));
    }
    events
}

/// The streams `catalog_events` still names, plus the change-log segment a crashed rotation left
/// unrecorded (`changes/2`), in the order the command lists them.
fn named(prefix: &str) -> Vec<String> {
    ["changes/1", "changes/2", "shape/group-abc-1", "shape/s1", "shape/s2", "shape/s3"]
        .iter()
        .map(|p| format!("{prefix}{p}"))
        .collect()
}

/// The v1 generation as storage holds it, beside the live v2 generation and a stream under the v1
/// prefix that the v1 catalog does not name.
async fn store_with_v1(last: u64) -> Store {
    let store = Store::start().await;
    store.put(&v1_catalog(), catalog_events("dashboard-conversation-v1", last));
    for path in named(&v1()) {
        store.put(&path, vec![]);
    }
    store.put(&format!("{}shape/s99", v1()), vec![]);
    let v2 = prefix(NAMESPACE, "dashboard-conversation-v2");
    store.put(&format!("{v2}meta/catalog"), catalog_events("dashboard-conversation-v2", now()));
    store.put(&format!("{v2}shape/s1"), vec![]);
    store
}

#[tokio::test(flavor = "multi_thread")]
async fn a_dry_run_prints_every_named_stream_and_deletes_nothing() {
    let store = store_with_v1(now() - 20 * DAY).await;
    let before = store.paths();

    let run = retire(&store, &[&v1_catalog()]).await;

    assert_eq!(run.code, 0, "stderr: {}", run.stderr);
    let would: Vec<&str> = run.stdout.lines().filter_map(|l| l.strip_prefix("would delete ")).collect();
    let mut expected = named(&v1());
    expected.push(v1_catalog());
    assert_eq!(would, expected, "stdout: {}", run.stdout);
    assert!(run.stdout.contains("Re-run with --confirm"), "stdout: {}", run.stdout);
    let evidence: Vec<Value> = run
        .stdout
        .lines()
        .filter_map(|l| l.strip_prefix("evidence "))
        .map(|e| serde_json::from_str(e).unwrap())
        .collect();
    assert_eq!(evidence.len(), 2, "stdout: {}", run.stdout);
    assert_eq!(evidence[0]["t"], "storeBound");
    assert_eq!(evidence[0]["query_generation"], "dashboard-conversation-v1");
    assert_eq!(evidence[0]["store"]["store_generation"], STORE_GENERATION);
    assert_eq!(evidence[1]["t"], "offset");
    assert_eq!(evidence[1]["pos"], json!({"segment": 1, "offset": "-1"}));
    assert_eq!(store.destructive_requests(), Vec::<String>::new());
    assert_eq!(store.paths(), before);
}

#[tokio::test(flavor = "multi_thread")]
async fn confirm_deletes_every_named_stream_and_then_the_catalog() {
    let store = store_with_v1(now() - 20 * DAY).await;

    let run = retire(&store, &[&v1_catalog(), "--confirm"]).await;

    assert_eq!(run.code, 0, "stdout: {}\nstderr: {}", run.stdout, run.stderr);
    let deletes: Vec<String> =
        store.requests().into_iter().filter_map(|r| r.strip_prefix("DELETE ").map(str::to_string)).collect();
    let mut expected = named(&v1());
    expected.push(v1_catalog());
    assert_eq!(deletes, expected, "every named stream, each once, and the catalog last");
    let v2 = prefix(NAMESPACE, "dashboard-conversation-v2");
    assert_eq!(
        store.paths(),
        vec![format!("{v1}shape/s99", v1 = v1()), format!("{v2}meta/catalog"), format!("{v2}shape/s1")],
        "only what the v1 catalog names is deleted"
    );
}

/// A deployment starts the engine as `/bin/sh -ec <script>` and a one-off task overrides only the
/// command, so the command must run as one `exec` shell string.
#[tokio::test(flavor = "multi_thread")]
async fn it_runs_as_a_shell_command_override_of_the_engine_task() {
    let store = store_with_v1(now() - 20 * DAY).await;
    let line = format!("exec {} retire-generation {}", env!("CARGO_BIN_EXE_electric-circuits-engine"), v1_catalog());

    let run = finish(engine_command(&store, "/bin/sh").args(["-ec", &line])).await;

    assert_eq!(run.code, 0, "stdout: {}\nstderr: {}", run.stdout, run.stderr);
    assert_eq!(run.stdout.lines().filter(|l| l.starts_with("would delete ")).count(), named(&v1()).len() + 1);
    assert_eq!(store.destructive_requests(), Vec::<String>::new());
}

/// The generation the command runs with is never retired, whether the path names it or the
/// catalog's `StoreBound` does, and however long the catalog has been quiet.
#[tokio::test(flavor = "multi_thread")]
async fn the_catalog_of_its_own_configured_generation_is_refused() {
    // The in-process test store's ELECTRIC_CIRCUITS_QUERY_GENERATION is `test-query`.
    let own = prefix(NAMESPACE, "test-query");
    let store = store_with_v1(now() - 20 * DAY).await;
    store.put(&format!("{own}meta/catalog"), catalog_events("test-query", now() - 20 * DAY));
    store.put(&format!("{own}shape/s1"), vec![]);
    // A catalog stored under another generation's path that says it was written for this one.
    store.put(&v1_catalog(), catalog_events("test-query", now() - 20 * DAY));
    let before = store.paths();

    for catalog in [format!("{own}meta/catalog"), v1_catalog()] {
        let run = retire(&store, &[&catalog, "--confirm"]).await;

        assert_eq!(run.code, 78, "{catalog}\nstdout: {}\nstderr: {}", run.stdout, run.stderr);
        assert!(run.stderr.contains("the generation this engine runs with"), "stderr: {}", run.stderr);
    }
    assert_eq!(store.destructive_requests(), Vec::<String>::new());
    assert_eq!(store.paths(), before);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_per_source_catalog_is_refused() {
    let store = Store::start().await;
    let hex = "0123456789abcdef0123456789abcdef01234567";
    let source = prefix(&format!("source-{hex}"), &format!("query-{hex}"));
    store.put(&format!("{source}meta/catalog"), catalog_events(&format!("query-{hex}"), now() - 20 * DAY));
    store.put(&format!("{source}shape/s1"), vec![]);

    let run = retire(&store, &[&format!("{source}meta/catalog"), "--confirm"]).await;

    assert_eq!(run.code, 78, "stdout: {}\nstderr: {}", run.stdout, run.stderr);
    assert!(run.stderr.contains("per-source catalog"), "stderr: {}", run.stderr);
    assert_eq!(store.requests(), Vec::<String>::new(), "a per-source path never reaches the store");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_catalog_appended_to_in_the_last_seven_days_is_refused() {
    let store = store_with_v1(now() - 6 * DAY).await;
    let before = store.paths();

    let run = retire(&store, &[&v1_catalog(), "--confirm"]).await;

    assert_eq!(run.code, 78, "stdout: {}\nstderr: {}", run.stdout, run.stderr);
    assert!(run.stderr.contains("last 7 days"), "stderr: {}", run.stderr);
    assert_eq!(store.destructive_requests(), Vec::<String>::new());
    assert_eq!(store.paths(), before);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_catalog_that_records_no_time_is_refused() {
    let store = Store::start().await;
    store.put(&v1_catalog(), vec![json!({"t": "offset", "eid": "e0", "pos": {"segment": 0, "offset": "-1"}})]);

    let run = retire(&store, &[&v1_catalog(), "--confirm"]).await;

    assert_eq!(run.code, 78, "stdout: {}\nstderr: {}", run.stdout, run.stderr);
    assert!(run.stderr.contains("records a time"), "stderr: {}", run.stderr);
    assert_eq!(store.destructive_requests(), Vec::<String>::new());
}

#[tokio::test(flavor = "multi_thread")]
async fn a_catalog_naming_a_stream_outside_its_prefix_is_refused() {
    let store = store_with_v1(now() - 20 * DAY).await;
    let mut events = catalog_events("dashboard-conversation-v1", now() - 20 * DAY);
    let mut escape = shape("s5", now() - 20 * DAY);
    escape["rec"]["stream_path"] = json!("../../dashboard-conversation-v2/meta/catalog");
    escape["eid"] = json!("v1-escape");
    events.push(escape);
    store.put(&v1_catalog(), events);
    let before = store.paths();

    let run = retire(&store, &[&v1_catalog(), "--confirm"]).await;

    assert_eq!(run.code, 78, "stdout: {}\nstderr: {}", run.stdout, run.stderr);
    assert!(run.stderr.contains("outside the catalog's own prefix"), "stderr: {}", run.stderr);
    assert_eq!(store.destructive_requests(), Vec::<String>::new());
    assert_eq!(store.paths(), before);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_catalog_appended_to_while_the_command_reads_it_is_refused() {
    let store = store_with_v1(now() - 20 * DAY).await;
    // The first HEAD takes the tail; the second, after the read, finds an engine has appended.
    store.state.lock().unwrap().append_on_head = Some((v1_catalog(), 2));
    let before = store.paths();

    let run = retire(&store, &[&v1_catalog(), "--confirm"]).await;

    assert_eq!(run.code, 78, "stdout: {}\nstderr: {}", run.stdout, run.stderr);
    assert!(run.stderr.contains("an engine is writing it"), "stderr: {}", run.stderr);
    assert_eq!(store.destructive_requests(), Vec::<String>::new());
    assert_eq!(store.paths(), before);
}

/// The last guard runs after every stream is gone: a catalog that moved meanwhile is kept, and the
/// run reports a partial retirement with its own exit code (3): neither a refusal (78) nor the
/// retryable failure (1), because a re-run once the writer goes quiet could delete a live catalog.
#[tokio::test(flavor = "multi_thread")]
async fn a_catalog_that_moves_while_its_streams_are_deleted_is_kept() {
    let store = store_with_v1(now() - 20 * DAY).await;
    // HEADs 1 and 2 bracket the read; HEAD 3, just before the catalog's delete, finds an append.
    store.state.lock().unwrap().append_on_head = Some((v1_catalog(), 3));

    let run = retire(&store, &[&v1_catalog(), "--confirm"]).await;

    assert_eq!(run.code, 3, "stdout: {}\nstderr: {}", run.stdout, run.stderr);
    assert!(run.stdout.contains("streams deleted; the catalog was kept because it moved"), "stdout: {}", run.stdout);
    assert!(run.stdout.contains("Find the writer before any re-run"), "stdout: {}", run.stdout);
    let left = store.paths();
    assert!(left.contains(&v1_catalog()), "the moved catalog survives");
    assert!(named(&v1()).iter().all(|p| !left.contains(p)), "every named stream was deleted: {left:?}");
    assert!(!store.destructive_requests().iter().any(|r| r.ends_with(&v1_catalog())), "the catalog was never closed");
}

/// A task retires only its own stack's generations: a catalog in another namespace of the same
/// store is refused before anything is read.
#[tokio::test(flavor = "multi_thread")]
async fn a_catalog_in_another_namespace_is_refused() {
    let store = store_with_v1(now() - 20 * DAY).await;
    let other = prefix("other-stack", "dashboard-conversation-v1");
    store.put(&format!("{other}meta/catalog"), catalog_events("dashboard-conversation-v1", now() - 20 * DAY));
    store.put(&format!("{other}shape/s1"), vec![]);

    let run = retire(&store, &[&format!("{other}meta/catalog"), "--confirm"]).await;

    assert_eq!(run.code, 78, "stdout: {}\nstderr: {}", run.stdout, run.stderr);
    assert!(run.stderr.contains("namespace 'other-stack'"), "stderr: {}", run.stderr);
    assert_eq!(store.requests(), Vec::<String>::new(), "the other namespace is never read");
}

/// An engine that died between creating `changes/0` and recording its rotation left a segment the
/// catalog never names; it is still this generation's, so it is planned and deleted.
#[tokio::test(flavor = "multi_thread")]
async fn an_unrecorded_first_change_log_segment_is_deleted() {
    let store = Store::start().await;
    let mut events = vec![store_bound("dashboard-conversation-v1"), shape("s1", now() - 20 * DAY)];
    for (n, event) in events.iter_mut().enumerate() {
        event["eid"] = json!(format!("crashed-{n}"));
    }
    store.put(&v1_catalog(), events);
    store.put(&format!("{}shape/s1", v1()), vec![]);
    store.put(&format!("{}changes/0", v1()), vec![]);
    let before = store.paths();

    let dry = retire(&store, &[&v1_catalog()]).await;

    assert_eq!(dry.code, 0, "stdout: {}\nstderr: {}", dry.stdout, dry.stderr);
    let would: Vec<&str> = dry.stdout.lines().filter_map(|l| l.strip_prefix("would delete ")).collect();
    let expected = [format!("{}changes/0", v1()), format!("{}shape/s1", v1()), v1_catalog()];
    assert_eq!(would, expected, "stdout: {}", dry.stdout);
    assert_eq!(store.destructive_requests(), Vec::<String>::new(), "the dry run closed or deleted something");
    assert_eq!(store.paths(), before, "the dry run changed the store");

    let run = retire(&store, &[&v1_catalog(), "--confirm"]).await;

    assert_eq!(run.code, 0, "stdout: {}\nstderr: {}", run.stdout, run.stderr);
    assert_eq!(store.paths(), Vec::<String>::new(), "nothing of the generation is left");
}

/// The command as a deployment runs it: the store identity, namespace and query generation come
/// from the environment, and the store must attest its identity before anything is read. The
/// configured generation is refused however old its catalog is, a stopped one is planned, and a
/// store that attests another identity is never read.
#[tokio::test(flavor = "multi_thread")]
async fn the_deployed_configuration_refuses_its_own_generation_and_plans_a_stopped_one() {
    let store = Store::start().await;
    store.state.lock().unwrap().ready = Some(readiness());
    let v1 = prefix("indexed-prod", "dashboard-conversation-v1");
    let v2 = prefix("indexed-prod", "dashboard-conversation-v2");
    store.put(&format!("{v1}meta/catalog"), catalog_events("dashboard-conversation-v1", now() - 20 * DAY));
    for path in named(&v1) {
        store.put(&path, vec![]);
    }
    // The configured generation's catalog, quiet for as long as v1's: only the generation refuses it.
    store.put(&format!("{v2}meta/catalog"), catalog_events("dashboard-conversation-v2", now() - 20 * DAY));
    store.put(&format!("{v2}shape/s1"), vec![]);
    let before = store.paths();

    let own = finish(&mut deployed_command(&store, STORE_ID, &[&format!("{v2}meta/catalog"), "--confirm"])).await;
    assert_eq!(own.code, 78, "stdout: {}\nstderr: {}", own.stdout, own.stderr);
    assert!(own.stderr.contains("'dashboard-conversation-v2', the generation this engine runs with"), "{}", own.stderr);

    let stopped = finish(&mut deployed_command(&store, STORE_ID, &[&format!("{v1}meta/catalog")])).await;
    assert_eq!(stopped.code, 0, "stdout: {}\nstderr: {}", stopped.stdout, stopped.stderr);
    let would: Vec<&str> = stopped.stdout.lines().filter_map(|l| l.strip_prefix("would delete ")).collect();
    let mut expected = named(&v1);
    expected.push(format!("{v1}meta/catalog"));
    assert_eq!(would, expected, "stdout: {}", stopped.stdout);

    let reads_before_mismatch = store.requests().len();
    let other = "00000000-0000-4000-8000-000000000000";
    let mismatch = finish(&mut deployed_command(&store, other, &[&format!("{v1}meta/catalog"), "--confirm"])).await;
    assert_eq!(mismatch.code, 78, "stdout: {}\nstderr: {}", mismatch.stdout, mismatch.stderr);
    assert!(mismatch.stderr.contains("did not attest the identity"), "stderr: {}", mismatch.stderr);
    assert!(mismatch.stderr.contains("identity mismatch"), "stderr: {}", mismatch.stderr);
    assert_eq!(
        store.requests()[reads_before_mismatch..].to_vec(),
        vec!["GET _admin/ready".to_string()],
        "a store that attests another identity is never read"
    );

    assert!(store.requests().iter().any(|r| r == "GET _admin/ready"), "the store was asked to attest");
    assert_eq!(store.destructive_requests(), Vec::<String>::new());
    assert_eq!(store.paths(), before);
}

/// The catalog is the only index of its streams, so an event the fold cannot decode (here a
/// `Created` missing a required field) could hide one: the run is refused and nothing is deleted.
#[tokio::test(flavor = "multi_thread")]
async fn a_catalog_with_an_undecodable_event_is_refused() {
    let store = store_with_v1(now() - 20 * DAY).await;
    let mut events = catalog_events("dashboard-conversation-v1", now() - 20 * DAY);
    let mut broken = shape("s9", now() - 20 * DAY);
    broken["rec"].as_object_mut().unwrap().remove("changes_only");
    broken["eid"] = json!("v1-broken");
    events.push(broken);
    store.put(&v1_catalog(), events);
    store.put(&format!("{}shape/s9", v1()), vec![]);
    let before = store.paths();

    let run = retire(&store, &[&v1_catalog(), "--confirm"]).await;

    assert_eq!(run.code, 78, "stdout: {}\nstderr: {}", run.stdout, run.stderr);
    assert!(run.stderr.contains("1 catalog events could not be decoded"), "stderr: {}", run.stderr);
    assert!(run.stderr.contains("created v1-broken: missing field `changes_only`"), "names the event: {}", run.stderr);
    assert_eq!(store.destructive_requests(), Vec::<String>::new());
    assert_eq!(store.paths(), before);
}

/// A catalog the fold refuses outright (here an event written before ADR-0008, with no `eid`)
/// cannot be planned, and a re-run will not change that: it is a refusal, not a retryable failure.
#[tokio::test(flavor = "multi_thread")]
async fn a_catalog_this_engine_cannot_read_is_refused() {
    let store = store_with_v1(now() - 20 * DAY).await;
    let mut events = catalog_events("dashboard-conversation-v1", now() - 20 * DAY);
    events.push(json!({"t": "offset", "pos": {"segment": 1, "offset": "-1"}}));
    store.put(&v1_catalog(), events);
    let before = store.paths();

    let run = retire(&store, &[&v1_catalog(), "--confirm"]).await;

    assert_eq!(run.code, 78, "stdout: {}\nstderr: {}", run.stdout, run.stderr);
    assert!(run.stderr.contains("this engine cannot read the catalog"), "stderr: {}", run.stderr);
    assert_eq!(store.destructive_requests(), Vec::<String>::new());
    assert_eq!(store.paths(), before);
}

/// A catalog read over several pages must reach the tail a `HEAD` reported. A later page answered
/// 404 (a proxy or failover mid-read) ends the read as if it were up to date; planning from that
/// prefix would delete the catalog and leave the later pages' streams with no index at all.
#[tokio::test(flavor = "multi_thread")]
async fn a_catalog_read_that_ends_early_is_refused() {
    let store = store_with_v1(now() - 20 * DAY).await;
    store.state.lock().unwrap().page_size = Some(4);
    let before = store.paths();

    // Paged reads that reach the tail plan exactly what a single page does.
    let whole = retire(&store, &[&v1_catalog()]).await;
    assert_eq!(whole.code, 0, "stdout: {}\nstderr: {}", whole.stdout, whole.stderr);
    let would: Vec<&str> = whole.stdout.lines().filter_map(|l| l.strip_prefix("would delete ")).collect();
    let mut expected = named(&v1());
    expected.push(v1_catalog());
    assert_eq!(would, expected, "stdout: {}", whole.stdout);

    // The second page answers 404 while the HEAD keeps reporting the same tail.
    store.state.lock().unwrap().missing_page = Some((v1_catalog(), 4));
    let run = retire(&store, &[&v1_catalog(), "--confirm"]).await;

    assert_eq!(run.code, 78, "stdout: {}\nstderr: {}", run.stdout, run.stderr);
    let tail = catalog_events("dashboard-conversation-v1", now()).len();
    assert!(
        run.stderr.contains(&format!("ended at offset Some(\"4\"), but the catalog's tail is Some(\"{tail}\")"))
            || run.stderr.contains(&format!("ended at offset None, but the catalog's tail is Some(\"{tail}\")")),
        "names both offsets: {}",
        run.stderr
    );
    assert_eq!(store.destructive_requests(), Vec::<String>::new());
    assert_eq!(store.paths(), before);
}

/// A writer appending between the first HEAD and the read makes the read end past that HEAD's
/// tail. It is reported as a live writer, not as a missing page, so the operator does not mistake
/// it for a transient fault and re-run into a generation that is still being written.
#[tokio::test(flavor = "multi_thread")]
async fn a_catalog_appended_to_before_it_is_read_is_refused_as_written() {
    let store = store_with_v1(now() - 20 * DAY).await;
    store.state.lock().unwrap().append_on_get = Some((v1_catalog(), 1));
    let before = store.paths();

    let run = retire(&store, &[&v1_catalog(), "--confirm"]).await;

    assert_eq!(run.code, 78, "stdout: {}\nstderr: {}", run.stdout, run.stderr);
    assert!(run.stderr.contains("an engine is writing it"), "the writer message: {}", run.stderr);
    assert!(!run.stderr.contains("a page was missing"), "not the missing-page message: {}", run.stderr);
    assert_eq!(store.destructive_requests(), Vec::<String>::new());
    assert_eq!(store.paths(), before);
}

/// Run outside the engine's task definition, with no store configured, the command cannot do
/// anything a re-run would change: it refuses rather than reporting a retryable failure.
#[tokio::test(flavor = "multi_thread")]
async fn a_run_with_no_store_configured_is_refused() {
    let run = finish(
        tokio::process::Command::new(env!("CARGO_BIN_EXE_electric-circuits-engine"))
            .arg("retire-generation")
            .arg(v1_catalog())
            .env_clear(),
    )
    .await;

    assert_eq!(run.code, 78, "stdout: {}\nstderr: {}", run.stdout, run.stderr);
    assert!(run.stderr.contains("no Durable Streams store is configured"), "stderr: {}", run.stderr);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_failed_delete_keeps_the_catalog_and_a_rerun_finishes() {
    let store = store_with_v1(now() - 20 * DAY).await;
    let stuck = format!("{}shape/s2", v1());
    store.state.lock().unwrap().fail_delete.insert(stuck.clone());

    let first = retire(&store, &[&v1_catalog(), "--confirm"]).await;

    assert_eq!(first.code, 1, "stdout: {}\nstderr: {}", first.stdout, first.stderr);
    assert!(first.stdout.contains(&format!("failed {stuck}")), "stdout: {}", first.stdout);
    let left = store.paths();
    assert!(left.contains(&v1_catalog()), "the catalog is kept while a named stream remains");
    assert!(left.contains(&stuck));
    assert!(!left.contains(&format!("{}shape/s1", v1())), "the other deletes still went through");

    store.state.lock().unwrap().fail_delete.clear();
    let second = retire(&store, &[&v1_catalog(), "--confirm"]).await;

    assert_eq!(second.code, 0, "stdout: {}\nstderr: {}", second.stdout, second.stderr);
    let left = store.paths();
    assert!(!left.contains(&v1_catalog()) && !left.contains(&stuck), "left: {left:?}");
}
