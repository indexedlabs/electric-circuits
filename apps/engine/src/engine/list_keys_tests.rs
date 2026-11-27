//! The list rule (`ELECTRIC_CIRCUITS_LIST_KEYS`): a filter whose `OR` has an equality in every
//! branch is filed under every one of those keys instead of on the fallback scan list.
//!
//! Filing decides only which shapes a change is OFFERED to; each offered shape still evaluates its
//! whole filter. So the rule is right exactly when it never withholds a change from a shape whose
//! output that change would alter, and the proof is a differential one: the same shapes and the
//! same row stream through filed lookup and through the scan list must give identical change
//! output and identical membership. A wrong rule drops rows silently, and the row it is most
//! likely to drop is a removal: a row that leaves a shape because its key changed no longer
//! carries the key the shape is filed under, and reaches it only through its old image.

use std::collections::BTreeMap;

use super::*;
use crate::predicate::{AccessLeaf, AccessPlan};
use crate::schema::{TableDef, TableSchema};

/// A link table: two ends, each with a Space and an id, plus columns no key is made of.
fn links() -> TableSchema {
    let def: TableDef = serde_json::from_value(serde_json::json!({
        "columns": {
            "id": {"type":"int"},
            "from_space": {"type":"text"},
            "to_space": {"type":"text"},
            "from_id": {"type":"text"},
            "kind": {"type":"text"},
            "hidden": {"type":"bool"},
            "n": {"type":"int"}
        },
        "primaryKey": "id"
    }))
    .unwrap();
    TableSchema::from_def(&"links".into(), &def).unwrap()
}

fn compile(ts: &TableSchema, j: serde_json::Value) -> Arc<CompiledPredicate> {
    Arc::new(CompiledPredicate::compile(&serde_json::from_value::<PredicateJson>(j).unwrap(), ts).unwrap())
}

fn eq(col: &str, value: impl Into<serde_json::Value>) -> serde_json::Value {
    serde_json::json!({"col": col, "op": "eq", "value": value.into()})
}

fn any_of(col: &str, values: &[String]) -> serde_json::Value {
    serde_json::json!({"or": values.iter().map(|v| eq(col, v.as_str())).collect::<Vec<_>>()})
}

fn link(id: i64, from_space: Option<&str>, to_space: Option<&str>) -> serde_json::Value {
    serde_json::json!({
        "id": id, "from_space": from_space, "to_space": to_space,
        "from_id": "r0", "kind": "a", "hidden": false, "n": 0
    })
}

/// A replicated change as the ingestor writes it: `REPLICA IDENTITY FULL` puts the old row on
/// every update and delete.
fn change(op: &str, key: i64, value: Option<serde_json::Value>, old: Option<serde_json::Value>, seq: u64) -> Envelope {
    Envelope {
        type_: "public.links".into(),
        key: key.to_string(),
        value,
        old,
        headers: EnvelopeHeaders {
            operation: op.into(),
            txid: Some((1000 + seq).to_string()),
            offset: None,
            lsn: Some(format!("0/{:X}", 0x1000 + seq)),
            seq: Some(0),
            last: Some(true),
            commit_ts: None,
        },
    }
}

fn subq() -> SubqueryHandle {
    let ds = DsClient::new_for_in_process_test("http://127.0.0.1:1");
    let registry = Arc::new(Mutex::new(SubqueryRegistry::new(ds.clone(), None)));
    let (flip_tx, flip_rx) = mpsc::unbounded_channel();
    let pending_flips = Arc::new(PendingFlips::default());
    let (trace_tx, _) = tokio::sync::broadcast::channel(16);
    let degrade = DegradeState::new();
    let shutdown = crate::shutdown::ShutdownToken::new();
    let group_lanes = emission::GroupEmissionLanes::new(ds, shutdown.clone(), Default::default());
    spawn_flip_propagator(
        registry.clone(),
        flip_rx,
        pending_flips.clone(),
        degrade.clone(),
        trace_tx,
        group_lanes.clone(),
        shutdown,
    );
    SubqueryHandle { registry, flip_tx, pending_flips, degrade, group_lanes }
}

/// How a world's two indexes file their shapes.
#[derive(Clone, Copy, Debug, PartialEq)]
enum Filing {
    /// Every shape on the scan list: the reference, with no index to be wrong.
    ScanList,
    /// The rule as it is with the setting off: one necessary conjunct or the scan list.
    Conjunct,
    /// The list rule.
    ListKeys,
}

/// The plain tier of one table: standalone shapes and COUNT aggregates with their indexes, and
/// everything it has emitted per stream.
struct World {
    filing: Filing,
    shapes: HashMap<String, StandaloneShape>,
    shape_index: StandaloneIndex,
    aggregates: HashMap<String, AggShape>,
    agg_index: StandaloneIndex,
}

impl World {
    fn new(filing: Filing) -> World {
        let index = || StandaloneIndex::new(filing == Filing::ListKeys);
        World { filing, shapes: HashMap::new(), shape_index: index(), aggregates: HashMap::new(), agg_index: index() }
    }

    fn file(filing: Filing, index: &mut StandaloneIndex, sid: &str, pred: &CompiledPredicate) {
        if filing == Filing::ScanList {
            index.scan.push(sid.to_string());
            index.placed.insert(sid.to_string(), AccessPlan::Scan);
        } else {
            index.insert(sid, pred);
        }
    }

    fn add_shape(&mut self, sid: &str, pred: &Arc<CompiledPredicate>) {
        Self::file(self.filing, &mut self.shape_index, sid, pred);
        self.shapes.insert(
            sid.to_string(),
            StandaloneShape {
                pred: pred.clone(),
                stream_path: format!("shape/{sid}"),
                gate: crate::pg::SnapshotGate::passthrough(),
                out_cols: None,
            },
        );
    }

    fn add_count(&mut self, sid: &str, pred: &Arc<CompiledPredicate>) {
        Self::file(self.filing, &mut self.agg_index, sid, pred);
        self.aggregates.insert(
            sid.to_string(),
            AggShape {
                pred: pred.clone(),
                func: AggFn::Count,
                col: None,
                stream_path: format!("agg/{sid}"),
                gate: crate::pg::SnapshotGate::passthrough(),
                count: 0,
                nn_count: 0,
                sum: Default::default(),
                multiset: std::collections::BTreeMap::new(),
                last: None,
            },
        );
    }

    fn remove_shape(&mut self, sid: &str) {
        self.shapes.remove(sid);
        self.shape_index.remove(sid);
    }

    /// One change through the sequencer's per-envelope step, as Postgres mode runs it. Returns
    /// what it staged per stream, as JSON in emission order.
    async fn apply(&mut self, ts: &TableSchema, subq: &SubqueryHandle, env: Envelope) -> BTreeMap<String, Vec<String>> {
        let (trace_tx, _) = tokio::sync::broadcast::channel::<Arc<String>>(1);
        let families: HashMap<Vec<usize>, KeyRouter> = HashMap::new();
        let mut pending: HashMap<String, Vec<Envelope>> = HashMap::new();
        process_envelope(
            &crate::metrics::Metrics::default(),
            ts,
            &self.shapes,
            &self.shape_index,
            &families,
            &mut self.aggregates,
            &self.agg_index,
            env,
            &mut pending,
            subq,
            &trace_tx,
            false,
        )
        .await
        .unwrap();
        pending
            .into_iter()
            .map(|(path, envs)| (path, envs.iter().map(|e| serde_json::to_string(e).unwrap()).collect()))
            .collect()
    }
}

/// Deterministic generator (SplitMix64), so a failing seed replays.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
    fn chance(&mut self, percent: usize) -> bool {
        self.below(100) < percent
    }
}

/// Small domains, so rows collide with the keys shapes are filed under and with each other.
const SPACES: [&str; 6] = ["s0", "s1", "s2", "s3", "s4", "s5"];
const IDS: [&str; 4] = ["r0", "r1", "r2", "r3"];
const KINDS: [&str; 3] = ["a", "b", "c"];

fn random_cell(rng: &mut Rng, col: &str) -> serde_json::Value {
    // One cell in five is NULL: a NULL key equals nothing, so the row reaches no key's shapes.
    if col != "id" && rng.chance(20) {
        return serde_json::Value::Null;
    }
    match col {
        "from_space" | "to_space" => SPACES[rng.below(SPACES.len())].into(),
        "from_id" => IDS[rng.below(IDS.len())].into(),
        "kind" => KINDS[rng.below(KINDS.len())].into(),
        "hidden" => rng.chance(50).into(),
        "n" => (rng.below(6) as i64).into(),
        other => panic!("no domain for column {other}"),
    }
}

const KEY_COLS: [&str; 4] = ["from_space", "to_space", "from_id", "kind"];
const ALL_COLS: [&str; 6] = ["from_space", "to_space", "from_id", "kind", "hidden", "n"];

fn random_eq(rng: &mut Rng) -> serde_json::Value {
    let col = if rng.chance(10) { "hidden" } else { KEY_COLS[rng.below(KEY_COLS.len())] };
    let mut value = random_cell(rng, col);
    // A NULL literal is kept rare here (it equals nothing, so the branch is dead weight); the
    // unfilable generator below produces it on purpose.
    while value.is_null() {
        value = random_cell(rng, col);
    }
    eq(col, value)
}

/// A leaf or small tree no key can be made of.
fn random_unfilable(rng: &mut Rng) -> serde_json::Value {
    let col = KEY_COLS[rng.below(KEY_COLS.len())];
    match rng.below(7) {
        0 => serde_json::json!({"col": col, "op": "neq", "value": SPACES[rng.below(3)]}),
        1 => serde_json::json!({"col": col, "isNull": true}),
        2 => serde_json::json!({"col": col, "isNull": false}),
        3 => serde_json::json!({"col": "n", "op": if rng.chance(50) { "gt" } else { "lte" }, "value": rng.below(6)}),
        4 => serde_json::json!({"col": col, "op": "like", "value": "s%"}),
        5 => serde_json::json!({"not": random_eq(rng)}),
        // `col = NULL` is UNKNOWN on every row.
        _ => serde_json::json!({"col": col, "op": "eq", "value": null}),
    }
}

/// A tree that has a key cover: an equality, an `OR` of covered branches, or an `AND` with at
/// least one covered member beside anything else.
fn random_covered(rng: &mut Rng, depth: usize) -> serde_json::Value {
    if depth == 0 {
        return random_eq(rng);
    }
    match rng.below(4) {
        0 => random_eq(rng),
        1 | 2 => {
            let branches: Vec<_> = (0..1 + rng.below(4)).map(|_| random_covered(rng, depth - 1)).collect();
            serde_json::json!({"or": branches})
        }
        _ => {
            let mut members = vec![random_covered(rng, depth - 1)];
            for _ in 0..rng.below(3) {
                members.push(if rng.chance(50) { random_unfilable(rng) } else { random_covered(rng, depth - 1) });
            }
            let at = rng.below(members.len());
            members.swap(0, at);
            serde_json::json!({"and": members})
        }
    }
}

/// Any tree at all: covered or not, including `OR`s that mix both.
fn random_any(rng: &mut Rng, depth: usize) -> serde_json::Value {
    if depth == 0 {
        return if rng.chance(50) { random_eq(rng) } else { random_unfilable(rng) };
    }
    match rng.below(5) {
        0 => random_unfilable(rng),
        1 => random_covered(rng, depth),
        2 => serde_json::json!({"or": (0..1 + rng.below(3)).map(|_| random_any(rng, depth - 1)).collect::<Vec<_>>()}),
        3 => serde_json::json!({"and": (0..1 + rng.below(3)).map(|_| random_any(rng, depth - 1)).collect::<Vec<_>>()}),
        _ => serde_json::json!({"not": random_any(rng, depth - 1)}),
    }
}

/// A filter of the kind the rule is for: an `OR` at the top, or in the top-level `AND` chain.
fn random_list_filter(rng: &mut Rng) -> serde_json::Value {
    let list = serde_json::json!({
        "or": (0..1 + rng.below(4)).map(|_| random_covered(rng, 2)).collect::<Vec<_>>()
    });
    if rng.chance(40) {
        return list;
    }
    let mut chain = vec![list];
    for _ in 0..1 + rng.below(2) {
        chain.push(if rng.chance(50) { random_unfilable(rng) } else { random_eq(rng) });
    }
    let at = rng.below(chain.len());
    chain.swap(0, at);
    serde_json::json!({"and": chain})
}

/// An `OR` with a covered branch beside one no key can be made of: it must stay on the scan list.
fn random_mixed_or(rng: &mut Rng) -> serde_json::Value {
    let mut branches = vec![random_covered(rng, 1), random_unfilable(rng)];
    for _ in 0..rng.below(2) {
        branches.push(random_covered(rng, 1));
    }
    let at = rng.below(branches.len());
    branches.swap(0, at);
    serde_json::json!({"or": branches})
}

fn random_row(rng: &mut Rng, id: i64) -> serde_json::Value {
    let mut row = serde_json::Map::new();
    row.insert("id".into(), id.into());
    for col in ALL_COLS {
        row.insert(col.into(), random_cell(rng, col));
    }
    row.into()
}

/// What the stream exercised, so a run that never moved a key or never met a NULL cannot pass.
#[derive(Default)]
struct Seen {
    inserts: usize,
    deletes: usize,
    /// Updates that changed a column a shape is filed under.
    key_moves: usize,
    /// Updates that moved a value from one end's Space column to the other's.
    column_moves: usize,
    /// Changes whose old or new image has a NULL in a key column.
    null_keys: usize,
}

fn has_null_key(row: &serde_json::Value) -> bool {
    KEY_COLS.iter().any(|c| row[*c].is_null())
}

/// The next change against `table`, and the table after it.
fn random_change(rng: &mut Rng, table: &mut BTreeMap<i64, serde_json::Value>, seen: &mut Seen, seq: u64) -> Envelope {
    const MAX_ROWS: i64 = 24;
    let roll = rng.below(100);
    if table.is_empty() || (roll < 25 && (table.len() as i64) < MAX_ROWS) {
        let id = (0..MAX_ROWS).find(|id| !table.contains_key(id)).unwrap();
        let row = random_row(rng, id);
        seen.inserts += 1;
        seen.null_keys += has_null_key(&row) as usize;
        table.insert(id, row.clone());
        return change("insert", id, Some(row), None, seq);
    }
    let id = *table.keys().nth(rng.below(table.len())).unwrap();
    let old = table[&id].clone();
    if roll < 40 {
        table.remove(&id);
        seen.deletes += 1;
        seen.null_keys += has_null_key(&old) as usize;
        return change("delete", id, None, Some(old), seq);
    }
    let mut new = old.clone();
    if rng.chance(25) {
        // The value changes ends: a row that was filed-for by its `from` end is now by its `to`.
        let (from, to) = (old["from_space"].clone(), old["to_space"].clone());
        new["from_space"] = to;
        new["to_space"] = from;
    } else {
        for _ in 0..1 + rng.below(3) {
            let col = ALL_COLS[rng.below(ALL_COLS.len())];
            new[col] = random_cell(rng, col);
        }
    }
    seen.key_moves += KEY_COLS.iter().any(|c| old[*c] != new[*c]) as usize;
    seen.column_moves += (old["from_space"] != new["from_space"]
        && (new["to_space"] == old["from_space"] || new["from_space"] == old["to_space"]))
        as usize;
    seen.null_keys += (has_null_key(&old) || has_null_key(&new)) as usize;
    table.insert(id, new.clone());
    change("update", id, Some(new), Some(old), seq)
}

/// Fold one stream's change output into the rows a reader of that stream holds.
fn fold(held: &mut BTreeMap<String, BTreeMap<String, serde_json::Value>>, staged: &BTreeMap<String, Vec<String>>) {
    for (path, envs) in staged {
        let rows = held.entry(path.clone()).or_default();
        for env in envs {
            let env: Envelope = serde_json::from_str(env).unwrap();
            match env.headers.operation.as_str() {
                "delete" => {
                    rows.remove(&env.key);
                }
                _ => {
                    rows.insert(env.key.clone(), env.value.clone().unwrap());
                }
            }
        }
    }
}

/// **The differential test.** The same shapes and the same row stream through three filings: the
/// scan list alone, the rule with the setting off, and the list rule. After every change all
/// three must have staged exactly the same envelopes on exactly the same streams, and at the end
/// every reader holds exactly the rows its filter is TRUE on.
///
/// The stream inserts, deletes, and updates that move a key between values, between the two
/// ends' columns, and to and from NULL; the shapes include `OR`s that mix a keyed branch with one
/// no key can be made of, which must stay on the scan list. Shapes also leave mid-stream.
#[tokio::test]
async fn filed_lookup_and_the_scan_list_emit_the_same_changes() {
    let ts = links();
    let subq = subq();
    let (mut total_keys, mut total_scan, mut total_changes) = (0usize, 0usize, 0usize);
    let mut seen = Seen::default();
    for seed in 1..=12u64 {
        let mut rng = Rng(seed);
        let mut worlds = [World::new(Filing::ScanList), World::new(Filing::Conjunct), World::new(Filing::ListKeys)];
        let mut filters: BTreeMap<String, (serde_json::Value, Arc<CompiledPredicate>)> = BTreeMap::new();
        for i in 0..60 {
            let filter = match rng.below(10) {
                0..=5 => random_list_filter(&mut rng),
                6 | 7 => random_mixed_or(&mut rng),
                _ => random_any(&mut rng, 3),
            };
            let pred = compile(&ts, filter.clone());
            let sid = format!("s{i}");
            for world in &mut worlds {
                world.add_shape(&sid, &pred);
                // Every third filter also backs a COUNT, which folds only the changes it is offered.
                if i % 3 == 0 {
                    world.add_count(&format!("c{i}"), &pred);
                }
            }
            filters.insert(sid, (filter, pred));
        }
        for (sid, plan) in &worlds[2].shape_index.placed {
            match plan {
                AccessPlan::Keys(_) => total_keys += 1,
                AccessPlan::Scan => total_scan += 1,
                AccessPlan::Leaf(_) => {}
            }
            // An OR with a branch no key can be made of must not be filed under any key.
            let keyed = |branch: &serde_json::Value| {
                let plan = compile(&ts, branch.clone()).access_plan(&|_, _| 0);
                matches!(plan, AccessPlan::Keys(_) | AccessPlan::Leaf(AccessLeaf::Eq { .. }))
            };
            if let Some(branches) = filters[sid].0.get("or").and_then(|b| b.as_array())
                && !branches.iter().all(keyed)
            {
                assert_eq!(plan, &AccessPlan::Scan, "seed {seed}: {sid} has an unkeyed branch: {}", filters[sid].0);
            }
        }

        let mut table: BTreeMap<i64, serde_json::Value> = BTreeMap::new();
        let mut held: BTreeMap<String, BTreeMap<String, serde_json::Value>> = BTreeMap::new();
        for step in 0..400u64 {
            // Shapes leave mid-stream: a removed shape must leave no key behind that still routes.
            if step == 200 {
                for sid in ["s3", "s11", "s40"] {
                    for world in &mut worlds {
                        world.remove_shape(sid);
                    }
                    filters.remove(sid);
                    held.remove(&format!("shape/{sid}"));
                }
            }
            let env = random_change(&mut rng, &mut table, &mut seen, seed * 1000 + step);
            let reference = worlds[0].apply(&ts, &subq, env.clone()).await;
            for world in &mut worlds[1..] {
                let staged = world.apply(&ts, &subq, env.clone()).await;
                assert_eq!(
                    staged,
                    reference,
                    "seed {seed} step {step}: {:?} filing emitted differently from the scan list for {}",
                    world.filing,
                    serde_json::to_string(&env).unwrap()
                );
            }
            fold(&mut held, &reference);
            total_changes += 1;
        }

        // Membership, from first principles: a reader holds a row exactly while its filter is TRUE on it.
        for (sid, (filter, pred)) in &filters {
            let expected: BTreeMap<String, serde_json::Value> = table
                .iter()
                .filter(|(_, row)| pred.matches(&ts.row_from_json(row.as_object().unwrap()).unwrap()))
                .map(|(id, row)| (id.to_string(), row.clone()))
                .collect();
            let got = held.get(&format!("shape/{sid}")).cloned().unwrap_or_default();
            assert_eq!(got, expected, "seed {seed}: {sid} holds the wrong rows for {filter}");
        }
        for world in &worlds {
            for (cid, agg) in &world.aggregates {
                let sid = cid.replacen('c', "s", 1);
                let Some((_, pred)) = filters.get(&sid) else { continue };
                let expected =
                    table.values().filter(|r| pred.matches(&ts.row_from_json(r.as_object().unwrap()).unwrap())).count();
                assert_eq!(agg.count, expected as i64, "seed {seed}: {cid} count under {:?}", world.filing);
            }
        }
    }
    // The run is only evidence if it exercised what the rule can get wrong.
    assert!(total_keys >= 200, "only {total_keys} shapes were filed under a list of keys");
    assert!(total_scan >= 100, "only {total_scan} shapes stayed on the scan list");
    assert!(total_changes == 4800 && seen.inserts >= 300 && seen.deletes >= 300, "thin stream");
    assert!(seen.key_moves >= 1000, "only {} updates moved a key", seen.key_moves);
    assert!(seen.column_moves >= 200, "only {} updates moved a value between columns", seen.column_moves);
    assert!(seen.null_keys >= 1000, "only {} changes had a NULL key", seen.null_keys);
}

/// **The safety line.** A reader who loses a row must be told so by the change that takes it
/// away. Each of these changes leaves the reader's keys behind, so the row's new image names
/// none of them; the shape is reached through the old image alone and is sent the delete in the
/// same step. A filing that looked rows up only under their new values would send nothing here.
#[tokio::test]
async fn a_row_that_leaves_a_filed_key_is_deleted_from_the_shape_it_left() {
    let ts = links();
    let subq = subq();
    // A reader of two Spaces, by either end of the link; a second reader of a third Space.
    let reader = compile(
        &ts,
        serde_json::json!({"or": [eq("from_space", "s1"), eq("from_space", "s2"), eq("to_space", "s1"), eq("to_space", "s2")]}),
    );
    let other = compile(&ts, serde_json::json!({"or": [eq("from_space", "s9"), eq("to_space", "s9")]}));
    let mut world = World::new(Filing::ListKeys);
    world.add_shape("reader", &reader);
    world.add_shape("other", &other);
    assert!(matches!(world.shape_index.placed["reader"], AccessPlan::Keys(ref keys) if keys.len() == 4));
    assert!(world.shape_index.scan.is_empty(), "both readers are filed, so only lookups can reach them");

    let ops = |staged: &BTreeMap<String, Vec<String>>, path: &str| -> Vec<String> {
        staged
            .get(path)
            .map(|envs| {
                envs.iter()
                    .map(|e| {
                        let e: Envelope = serde_json::from_str(e).unwrap();
                        format!("{} {}", e.headers.operation, e.key)
                    })
                    .collect()
            })
            .unwrap_or_default()
    };

    // The row moves to another reader's Space: the old value's shape is told to drop it.
    let a = link(1, Some("s1"), None);
    let staged = world.apply(&ts, &subq, change("insert", 1, Some(a.clone()), None, 1)).await;
    assert_eq!(ops(&staged, "shape/reader"), ["upsert 1"]);
    let b = link(1, Some("s9"), None);
    let staged = world.apply(&ts, &subq, change("update", 1, Some(b.clone()), Some(a), 2)).await;
    assert_eq!(ops(&staged, "shape/reader"), ["delete 1"], "the reader who lost the row is told");
    assert_eq!(ops(&staged, "shape/other"), ["upsert 1"]);

    // The key moves between columns and away: filed under `to_space`, then under neither.
    let c = link(2, None, Some("s2"));
    world.apply(&ts, &subq, change("insert", 2, Some(c.clone()), None, 3)).await;
    let d = link(2, Some("s2"), None);
    let staged = world.apply(&ts, &subq, change("update", 2, Some(d.clone()), Some(c), 4)).await;
    assert_eq!(ops(&staged, "shape/reader"), ["upsert 2"], "still the reader's, by its other end");
    let e = link(2, Some("s7"), Some("s7"));
    let staged = world.apply(&ts, &subq, change("update", 2, Some(e), Some(d), 5)).await;
    assert_eq!(ops(&staged, "shape/reader"), ["delete 2"]);

    // The key becomes NULL: a NULL key equals nothing, so the row leaves.
    let f = link(3, Some("s2"), Some("s1"));
    world.apply(&ts, &subq, change("insert", 3, Some(f.clone()), None, 6)).await;
    let g = link(3, None, None);
    let staged = world.apply(&ts, &subq, change("update", 3, Some(g.clone()), Some(f), 7)).await;
    assert_eq!(ops(&staged, "shape/reader"), ["delete 3"]);
    // ... and a change to a row with no key at all reaches no filed shape.
    let staged = world.apply(&ts, &subq, change("delete", 3, None, Some(g), 8)).await;
    assert!(staged.is_empty(), "a row with NULL keys is in no reader's shape: {staged:?}");

    // The row is deleted: only its old image exists.
    let staged = world.apply(&ts, &subq, change("delete", 1, None, Some(b), 9)).await;
    assert_eq!(ops(&staged, "shape/other"), ["delete 1"]);
    assert!(!staged.contains_key("shape/reader"));
}

/// The cost the rule removes. With one list filter per account, a changed row is offered to the
/// shapes filed under its own values and to no others, however many accounts there are; with the
/// setting off every one of those filters is on the scan list and is offered every change.
#[test]
fn a_change_is_offered_only_to_the_shapes_filed_under_its_values() {
    let ts = links();
    let row = |from: &str, to: &str| ts.row_from_json(link(1, Some(from), Some(to)).as_object().unwrap()).unwrap();
    for accounts in [1_000usize, 10_000] {
        let mut filed = StandaloneIndex::new(true);
        let mut unfiled = StandaloneIndex::new(false);
        for a in 0..accounts {
            // An account reads two Spaces, each by either end of a link.
            let spaces = [format!("home-{a}"), format!("shared-{a}")];
            let filter =
                compile(&ts, serde_json::json!({"or": [any_of("from_space", &spaces), any_of("to_space", &spaces)]}));
            filed.insert(&format!("s{a}"), &filter);
            unfiled.insert(&format!("s{a}"), &filter);
        }
        assert_eq!(unfiled.scan.len(), accounts, "today every list filter is on the scan list");
        assert_eq!(filed.scan.len(), 0, "with the rule none is");

        // An insert between two accounts' Spaces: offered to exactly those two.
        let mut offered = filed.candidates(&[Tup2(row("home-7", "shared-9"), 1)]);
        offered.sort();
        assert_eq!(offered, ["s7", "s9"], "{accounts} accounts");
        // A move: the account the row left and the one it reached, and no third.
        let mut offered = filed.candidates(&[Tup2(row("home-7", "home-7"), -1), Tup2(row("home-8", "home-8"), 1)]);
        offered.sort();
        assert_eq!(offered, ["s7", "s8"], "{accounts} accounts");
        // A row in nobody's Space is offered to nobody.
        assert!(filed.candidates(&[Tup2(row("nowhere", "nowhere"), 1)]).is_empty());
        assert_eq!(unfiled.candidates(&[Tup2(row("nowhere", "nowhere"), 1)]).len(), accounts);
    }
}

/// A link filter as a gateway writes it: branches by link type, each testing an end against the
/// reader's Spaces and shared ids, under row-state conditions. Every reader's filter names the
/// same type constants, so a shape filed under those would be offered every row of that type.
/// Among a branch's alternatives the index files under the keys the fewest other filters name,
/// which is never the constants: a reader's own keys are named by that reader's filters alone.
#[test]
fn a_link_filter_is_filed_under_its_readers_keys_not_under_shared_constants() {
    let ts = links();
    let filter = |a: usize| {
        let spaces = [format!("home-{a}")];
        let shared = [format!("rec-{a}-0"), format!("rec-{a}-1")];
        let end = |side: &str| serde_json::json!({"or": [any_of(&format!("{side}_space"), &spaces)]});
        serde_json::json!({"and": [
            {"col": "n", "isNull": false},
            eq("hidden", false),
            {"or": [
                {"and": [eq("kind", "a"), {"or": [any_of("from_space", &spaces), any_of("from_id", &shared)]}]},
                {"and": [{"or": [eq("kind", "b"), eq("kind", "c")]}, end("from"), end("to")]},
            ]},
        ]})
    };
    let mut worst = Vec::new();
    for accounts in [200usize, 2_000] {
        let mut index = StandaloneIndex::new(true);
        for a in 0..accounts {
            index.insert(&format!("s{a}"), &compile(&ts, filter(a)));
        }
        assert_eq!(index.scan.len(), 0);
        let on_constants = index
            .placed
            .values()
            .filter(|plan| {
                let AccessPlan::Keys(keys) = plan else { panic!("not filed under keys: {plan:?}") };
                keys.iter().any(|(col, _)| {
                    *col == ts.column_index("kind").unwrap() || *col == ts.column_index("hidden").unwrap()
                })
            })
            .count();
        assert_eq!(on_constants, 0, "of {accounts} shapes, filed under a constant every filter names");
        // The busiest a row can be: it carries the shared constants and one account's keys.
        let row = ts
            .row_from_json(
                serde_json::json!({"id": 1, "from_space": "home-5", "to_space": "home-5", "from_id": "rec-5-0", "kind": "a", "hidden": false, "n": 1})
                    .as_object()
                    .unwrap(),
            )
            .unwrap();
        worst.push(index.candidates(&[Tup2(row, 1)]).len());
    }
    assert_eq!(worst, [1, 1], "shapes a row is offered to, at 200 and at 2,000 accounts");
}

/// The other way round: the equality beside the list is the reader's key and the list is of
/// constants every filter names (`project = 12 AND (status = 'todo' OR status = 'doing')`). The
/// shape stays filed under its own equality, as it is with the setting off.
#[test]
fn a_filter_whose_list_is_of_shared_constants_keeps_its_own_equality() {
    let ts = links();
    let mut index = StandaloneIndex::new(true);
    for project in 0..200 {
        let filter = serde_json::json!({"and": [
            eq("from_id", format!("p{project}")),
            {"or": [eq("kind", "a"), eq("kind", "b")]},
        ]});
        index.insert(&format!("s{project}"), &compile(&ts, filter));
    }
    let from_id = ts.column_index("from_id").unwrap();
    for (sid, plan) in &index.placed {
        let project = sid.replacen('s', "p", 1);
        assert_eq!(plan, &AccessPlan::Keys(vec![(from_id, Value::Text(project))]), "{sid}");
    }
    let row = ts
        .row_from_json(
            serde_json::json!({"id": 1, "from_space": null, "to_space": null, "from_id": "p7", "kind": "a", "hidden": false, "n": 1})
                .as_object()
                .unwrap(),
        )
        .unwrap();
    assert_eq!(index.candidates(&[Tup2(row, 1)]), ["s7"]);
}

/// Removing a shape removes it from every key it was filed under, and reclaims emptied keys.
#[test]
fn removing_a_filed_shape_clears_every_one_of_its_keys() {
    let ts = links();
    let mut index = StandaloneIndex::new(true);
    let two =
        compile(&ts, serde_json::json!({"or": [eq("from_space", "s1"), eq("to_space", "s1"), eq("from_space", "s2")]}));
    let one = compile(&ts, serde_json::json!({"or": [eq("from_space", "s1")]}));
    index.insert("two", &two);
    index.insert("one", &one);
    let row = |from: &str, to: &str| ts.row_from_json(link(1, Some(from), Some(to)).as_object().unwrap()).unwrap();
    let mut offered = index.candidates(&[Tup2(row("s1", "s1"), 1)]);
    offered.sort();
    assert_eq!(offered, ["one", "two"], "a shape filed under two of a row's values is offered it once");

    index.remove("two");
    assert_eq!(index.candidates(&[Tup2(row("s1", "s1"), 1)]), ["one"]);
    assert!(index.candidates(&[Tup2(row("s2", "s0"), 1)]).is_empty());
    assert!(index.candidates(&[Tup2(row("s0", "s1"), 1)]).is_empty());
    index.remove("one");
    assert!(index.eq.is_empty() && index.placed.is_empty() && index.scan.is_empty(), "nothing is left behind");
}

/// With the setting off, nothing is filed differently: a list filter is on the scan list and a
/// filter with one equality beside its list is filed under that equality, as before the rule.
#[test]
fn with_the_setting_off_filing_is_unchanged() {
    let ts = links();
    let mut index = StandaloneIndex::default();
    index.insert("list", &compile(&ts, serde_json::json!({"or": [eq("from_space", "s1"), eq("to_space", "s1")]})));
    index.insert(
        "beside",
        &compile(
            &ts,
            serde_json::json!({"and": [eq("kind", "a"), {"or": [eq("from_space", "s1"), eq("to_space", "s1")]}]}),
        ),
    );
    assert_eq!(index.placed["list"], AccessPlan::Scan);
    assert_eq!(
        index.placed["beside"],
        AccessPlan::Leaf(AccessLeaf::Eq { col: ts.column_index("kind").unwrap(), value: Value::Text("a".into()) })
    );
    assert_eq!(index.scan, ["list"]);
}

#[test]
fn the_setting_is_off_unless_it_says_one_or_true() {
    for on in ["1", "true", "TRUE", " true "] {
        assert!(parse_list_keys(Some(on)), "{on:?}");
    }
    for off in [None, Some(""), Some("0"), Some("false"), Some("yes"), Some("on")] {
        assert!(!parse_list_keys(off), "{off:?}");
    }
}

/// The cost check, as a measurement: one account's 1,000-row write against 1,000 and 10,000
/// other accounts' list filters, through the sequencer's per-envelope step. Run with
/// `cargo test --release -p electric-circuits-engine --features test-support --lib
/// list_rule_cost -- --ignored --nocapture`; `LIST_KEYS_COST=off` measures the setting off.
#[tokio::test]
#[ignore = "a measurement, not a gate"]
async fn list_rule_cost_of_a_1000_row_write() {
    let ts = links();
    let subq = subq();
    let on = std::env::var("LIST_KEYS_COST").map_or(true, |v| v != "off");
    for accounts in [1_000usize, 10_000] {
        let mut world = World::new(if on { Filing::ListKeys } else { Filing::Conjunct });
        for a in 0..accounts {
            let spaces = [format!("home-{a}"), format!("shared-{a}")];
            let filter =
                compile(&ts, serde_json::json!({"or": [any_of("from_space", &spaces), any_of("to_space", &spaces)]}));
            world.add_shape(&format!("s{a}"), &filter);
        }
        let mut emitted = 0usize;
        let started = std::time::Instant::now();
        for i in 0..1_000i64 {
            let row = link(i, Some("home-0"), Some("shared-0"));
            let staged = world.apply(&ts, &subq, change("insert", i, Some(row), None, i as u64)).await;
            emitted += staged.values().map(Vec::len).sum::<usize>();
        }
        let elapsed = started.elapsed();
        println!(
            "list rule {}: {accounts} list filters ({} on the scan list), 1000-row write: {:.1} ms total, {:.2} us/row, {emitted} envelopes emitted",
            if on { "on" } else { "off" },
            world.shape_index.scan.len(),
            elapsed.as_secs_f64() * 1e3,
            elapsed.as_secs_f64() * 1e6 / 1_000.0,
        );
        assert_eq!(emitted, 1_000, "the writer's own shape receives every row, and no other does");
    }
}
