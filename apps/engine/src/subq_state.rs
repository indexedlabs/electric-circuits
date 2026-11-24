//! The subquery registry's durable state: what a graceful stop writes and the next boot restores.
//!
//! A subquery shape's membership is decided by inner-set **nodes** that live in memory (their
//! contributors, by inner-row pk) and by each shape's **key set** (what its stream delivered). A
//! node re-seeded from Postgres at boot cannot tell which values entered or left while the engine
//! was down, so without this state every subquery shape is dropped at restore and its clients
//! reload everything. With it, the registry is rebuilt as it stood at the final checkpoint and the
//! change log is replayed from there through the ordinary live path.
//!
//! **What is written** ([`RegistryState`]): per node its signature, inner table (with that table's
//! schema fingerprint), seed gate and contributors as `(pk, value)`; per shape its gate, stream
//! position and key set; per group lane its stream position. Primary keys are written as strings —
//! the registry's `u32` ids are process-local.
//!
//! **Where**: a stream of its own, `meta/subq-state/<nonce>`, as a sequence of JSON records — a
//! header, the state in bounded chunks, a trailer. The final `Offset` checkpoint names it with a
//! [`StatePointer`]: the stream, its record count, its size and a SHA-256 over every record.
//!
//! **Order**: the pointer is made durable FIRST, then the stream is written. Durable-streams has
//! no list API, so a stream no catalog record names could never be found again; an `Offset` that
//! names a stream which is missing or incomplete is harmless, because the checksum cannot match.
//!
//! **When it is trusted** ([`read`]): only when the catalog's LAST `Offset` names it and every
//! record is present, in order, with the pointer's count, size and checksum, the header's version
//! is this one and its position is that checkpoint's. Anything else is [`Loaded::Rejected`] and
//! the shapes are dropped exactly as before. A transport failure is an error instead: the boot
//! retries rather than dropping shapes over an outage.
//!
//! The registry half is `SubqueryRegistry::export_state` / `restore_shape`; the sequencer decides
//! when the state may be written (`engine::sequencer`), and the catalog restore when it is used
//! (`engine::catalog`).

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::changelog::LogPosition;
use crate::ds::DsClient;
use crate::pg::SnapshotGate;
use crate::predicate::SubquerySig;
use crate::schema::SchemaFingerprint;
use crate::table_ref::TableRef;
use crate::value::{JSON_EXACT_INT_MAX, Value};

/// The format this build writes and the only one it reads.
pub(crate) const VERSION: u32 = 1;

/// Every state stream lives under this prefix.
pub(crate) const STREAM_PREFIX: &str = "meta/subq-state/";

/// Roughly how large one contributor or key chunk record gets before the next one starts. Small
/// against every store's page, so a read never has to frame an oversized value.
const CHUNK_BYTES: usize = 64 * 1024;

/// Roughly how many bytes of records one append carries.
const APPEND_BYTES: usize = 512 * 1024;

/// What an `Offset` checkpoint says about the registry state written with it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct StatePointer {
    pub(crate) stream: String,
    pub(crate) version: u32,
    /// How many records the stream holds, header and trailer included.
    pub(crate) records: u64,
    /// The summed size of those records in their canonical encoding.
    pub(crate) bytes: u64,
    /// SHA-256 over the canonical encoding of every record, in order (lower-case hex).
    pub(crate) sha256: String,
}

/// One inner-set node as it stood at the checkpoint.
#[derive(Clone, Debug)]
pub(crate) struct NodeState {
    pub(crate) sig: SubquerySig,
    pub(crate) table: TableRef,
    /// The inner table's schema fingerprint when the state was written (`None`: the source has
    /// none — library mode). A node whose table has moved since cannot be restored.
    pub(crate) fingerprint: Option<SchemaFingerprint>,
    pub(crate) gate: SnapshotGate,
    /// `(inner-row pk, projected value)`, sorted by pk.
    pub(crate) contributors: Vec<(Arc<str>, Value)>,
}

/// One subquery shape as it stood at the checkpoint.
#[derive(Clone, Debug)]
pub(crate) struct ShapeState {
    pub(crate) shape_id: String,
    pub(crate) gate: SnapshotGate,
    /// `SubqueryShape::position`.
    pub(crate) position: u64,
    /// Envelopes appended so far (the visualizer's counter).
    pub(crate) emitted: u64,
    /// The shape's key set, as pk strings, sorted.
    pub(crate) keys: Vec<Arc<str>>,
}

/// The registry at one checkpoint: nodes by signature, shapes by id, group lanes by stream path.
#[derive(Clone, Debug, Default)]
pub(crate) struct RegistryState {
    pub(crate) nodes: Vec<NodeState>,
    pub(crate) shapes: Vec<ShapeState>,
    pub(crate) group_positions: Vec<(String, u64)>,
}

impl RegistryState {
    /// Nothing to restore: no shape is registered.
    pub(crate) fn is_empty(&self) -> bool {
        self.shapes.is_empty()
    }

    /// The state indexed for a restore: nodes by signature, shapes by id.
    pub(crate) fn indexed(self) -> IndexedState {
        IndexedState {
            nodes: self.nodes.into_iter().map(|node| (node.sig.clone(), node)).collect(),
            shapes: self.shapes.into_iter().map(|shape| (shape.shape_id.clone(), shape)).collect(),
            group_positions: self.group_positions.into_iter().collect(),
        }
    }
}

/// [`RegistryState`] keyed for lookup.
#[derive(Default)]
pub(crate) struct IndexedState {
    pub(crate) nodes: HashMap<SubquerySig, NodeState>,
    pub(crate) shapes: HashMap<String, ShapeState>,
    pub(crate) group_positions: HashMap<String, u64>,
}

/// A cell on the wire. Self-describing, so a value comes back as the variant it was — and never
/// through a JSON number a parser might round: an integer past 2^53 and a float's bits are strings.
#[derive(Serialize, Deserialize)]
enum Cell {
    #[serde(rename = "n")]
    Null,
    #[serde(rename = "i")]
    Int(i64),
    #[serde(rename = "l")]
    Long(String),
    #[serde(rename = "t")]
    Text(String),
    #[serde(rename = "b")]
    Bool(bool),
    #[serde(rename = "f")]
    Float(String),
}

impl Cell {
    fn of(value: &Value) -> Cell {
        match value {
            Value::Null => Cell::Null,
            Value::Int(i) if i.unsigned_abs() <= JSON_EXACT_INT_MAX => Cell::Int(*i),
            Value::Int(i) => Cell::Long(i.to_string()),
            Value::Text(s) => Cell::Text(s.clone()),
            Value::Bool(b) => Cell::Bool(*b),
            Value::Float(f) => Cell::Float(format!("{:016x}", f.into_inner().to_bits())),
        }
    }

    fn into_value(self) -> Result<Value> {
        Ok(match self {
            Cell::Null => Value::Null,
            Cell::Int(i) => Value::Int(i),
            Cell::Long(s) => Value::Int(s.parse().with_context(|| format!("'{s}' is not an integer"))?),
            Cell::Text(s) => Value::Text(s),
            Cell::Bool(b) => Value::Bool(b),
            Cell::Float(bits) => Value::Float(dbsp::algebra::F64::new(f64::from_bits(
                u64::from_str_radix(&bits, 16).with_context(|| format!("'{bits}' is not a float's bits"))?,
            ))),
        })
    }
}

/// One record of a state stream.
#[derive(Serialize, Deserialize)]
#[serde(tag = "k", rename_all = "camelCase")]
enum Record {
    /// First: what follows, and the checkpoint it belongs to.
    Header {
        version: u32,
        pos: LogPosition,
        #[serde(default)]
        highwater: Option<(u64, u64)>,
        nodes: u64,
        shapes: u64,
    },
    Node {
        sig: SubquerySig,
        table: TableRef,
        #[serde(default)]
        fingerprint: Option<SchemaFingerprint>,
        gate: SnapshotGate,
    },
    /// Contributors of the node most recently named, continued.
    Contributors {
        sig: SubquerySig,
        entries: Vec<(String, Cell)>,
    },
    Shape {
        id: String,
        gate: SnapshotGate,
        position: u64,
        emitted: u64,
    },
    /// Keys of the shape most recently named, continued.
    Keys {
        id: String,
        keys: Vec<String>,
    },
    Group {
        path: String,
        position: u64,
    },
    /// Last: how many records came before it.
    End {
        records: u64,
    },
}

/// A record as it is hashed and stored. Encoding through [`serde_json::Value`] sorts every
/// object's keys, so what is hashed when the state is written is byte for byte what a read hashes
/// after the store has parsed the record and printed it again.
fn canonical(record: &Record) -> Result<(serde_json::Value, Vec<u8>)> {
    let value = serde_json::to_value(record).context("encoding a subquery state record")?;
    let bytes = serde_json::to_vec(&value).context("encoding a subquery state record")?;
    Ok((value, bytes))
}

/// `items` cut into runs of roughly [`CHUNK_BYTES`], by each item's `size`.
fn chunks<'a, T>(items: &'a [T], size: impl Fn(&T) -> usize + 'a) -> impl Iterator<Item = &'a [T]> + 'a {
    let mut start = 0usize;
    std::iter::from_fn(move || {
        if start >= items.len() {
            return None;
        }
        let (mut end, mut bytes) = (start, 0usize);
        while end < items.len() && bytes < CHUNK_BYTES {
            bytes += size(&items[end]);
            end += 1;
        }
        let chunk = &items[start..end];
        start = end;
        Some(chunk)
    })
}

/// Every record of `state` but the trailer, in the order the stream holds them. One record is
/// built at a time, so the encoded state is never held whole.
fn records<'a>(
    state: &'a RegistryState,
    pos: &'a LogPosition,
    highwater: Option<(u64, u64)>,
) -> impl Iterator<Item = Record> + 'a {
    let header = std::iter::once(Record::Header {
        version: VERSION,
        pos: pos.clone(),
        highwater,
        nodes: state.nodes.len() as u64,
        shapes: state.shapes.len() as u64,
    });
    let nodes = state.nodes.iter().flat_map(|node| {
        let named = Record::Node {
            sig: node.sig.clone(),
            table: node.table.clone(),
            fingerprint: node.fingerprint.clone(),
            gate: node.gate.clone(),
        };
        let size =
            |(pk, value): &(Arc<str>, Value)| pk.len() + 16 + if let Value::Text(s) = value { s.len() } else { 8 };
        std::iter::once(named).chain(chunks(&node.contributors, size).map(move |chunk| Record::Contributors {
            sig: node.sig.clone(),
            entries: chunk.iter().map(|(pk, value)| (pk.to_string(), Cell::of(value))).collect(),
        }))
    });
    let shapes = state.shapes.iter().flat_map(|shape| {
        let named = Record::Shape {
            id: shape.shape_id.clone(),
            gate: shape.gate.clone(),
            position: shape.position,
            emitted: shape.emitted,
        };
        std::iter::once(named).chain(chunks(&shape.keys, |key: &Arc<str>| key.len() + 4).map(move |chunk| {
            Record::Keys { id: shape.shape_id.clone(), keys: chunk.iter().map(|key| key.to_string()).collect() }
        }))
    });
    let groups =
        state.group_positions.iter().map(|(path, position)| Record::Group { path: path.clone(), position: *position });
    header.chain(nodes).chain(shapes).chain(groups)
}

/// The running count, size and checksum of a stream's records, in the order they are stored.
#[derive(Default)]
struct Tally {
    records: u64,
    bytes: u64,
    hasher: Sha256,
}

impl Tally {
    fn add(&mut self, encoded: &[u8]) {
        self.records += 1;
        self.bytes += encoded.len() as u64;
        self.hasher.update(encoded);
    }

    fn sha256(self) -> String {
        format!("{:x}", self.hasher.finalize())
    }
}

/// Name the stream `state` will be written to at this checkpoint and compute what it will hold:
/// the pointer the final `Offset` carries. Nothing is written.
pub(crate) fn seal(
    state: &RegistryState,
    stream: String,
    pos: &LogPosition,
    highwater: Option<(u64, u64)>,
) -> Result<StatePointer> {
    let mut tally = Tally::default();
    for record in records(state, pos, highwater) {
        tally.add(&canonical(&record)?.1);
    }
    let before = tally.records;
    tally.add(&canonical(&Record::End { records: before })?.1);
    let (records, bytes) = (tally.records, tally.bytes);
    Ok(StatePointer { stream, version: VERSION, records, bytes, sha256: tally.sha256() })
}

/// A fresh stream name. Unique per call, so a stream is never written twice.
pub(crate) fn new_stream_name() -> String {
    format!("{STREAM_PREFIX}{}", uuid::Uuid::new_v4().simple())
}

/// Write `state` to the stream `pointer` names. Called only after the `Offset` carrying `pointer`
/// is durable (see the module docs). Any failure leaves a stream the pointer's checksum does not
/// match, which a restore rejects. One append-sized batch of records is held at a time.
pub(crate) async fn write(
    ds: &DsClient,
    state: &RegistryState,
    pointer: &StatePointer,
    pos: &LogPosition,
    highwater: Option<(u64, u64)>,
) -> Result<()> {
    ds.ensure_stream(&pointer.stream).await.with_context(|| format!("creating {}", pointer.stream))?;
    let mut tally = Tally::default();
    let mut batch: Vec<serde_json::Value> = Vec::new();
    let mut batch_bytes = 0usize;
    for record in records(state, pos, highwater) {
        let (value, encoded) = canonical(&record)?;
        tally.add(&encoded);
        batch_bytes += encoded.len();
        batch.push(value);
        if batch_bytes >= APPEND_BYTES {
            ds.append_json(&pointer.stream, &batch)
                .await
                .with_context(|| format!("appending to {}", pointer.stream))?;
            batch.clear();
            batch_bytes = 0;
        }
    }
    // The trailer is appended only if what went before it is what the pointer promised: a stream
    // that ends without one is rejected whatever else it holds.
    let before = tally.records;
    let (trailer, encoded) = canonical(&Record::End { records: before })?;
    tally.add(&encoded);
    if tally.records != pointer.records || tally.bytes != pointer.bytes || tally.sha256() != pointer.sha256 {
        bail!("the subquery state changed between sealing its pointer and writing it");
    }
    batch.push(trailer);
    ds.append_json(&pointer.stream, &batch).await.with_context(|| format!("appending to {}", pointer.stream))?;
    Ok(())
}

/// What [`read`] found in the stream a pointer names.
pub(crate) enum Loaded {
    /// The state, verified against the pointer and the checkpoint.
    State(RegistryState),
    /// The stream cannot be used; why. The caller drops the shapes, loudly, as it always did.
    Rejected(String),
}

/// Read the stream `pointer` names and verify it: every record present and in order, the count,
/// size and checksum the pointer promised, this build's version, and the checkpoint `(pos,
/// highwater)` the boot is resuming from. `Err` is a storage failure only (the boot retries).
pub(crate) async fn read(
    ds: &DsClient,
    pointer: &StatePointer,
    pos: &LogPosition,
    highwater: Option<(u64, u64)>,
) -> Result<Loaded> {
    if pointer.version != VERSION {
        return Ok(Loaded::Rejected(format!(
            "it was written in format {}, this build reads {VERSION}",
            pointer.version
        )));
    }
    if !pointer.stream.starts_with(STREAM_PREFIX) {
        return Ok(Loaded::Rejected(format!("'{}' is not a subquery state stream", pointer.stream)));
    }
    let mut decoder = Decoder::default();
    let mut tally = Tally::default();
    let mut offset = "-1".to_string();
    loop {
        let (body, next, up_to_date, _permit) = ds
            .read_json_body(&pointer.stream, &offset)
            .await
            .with_context(|| format!("reading subquery state {}", pointer.stream))?;
        let page: Vec<serde_json::Value> = if body.trim().is_empty() {
            Vec::new()
        } else {
            match serde_json::from_str(&body) {
                Ok(page) => page,
                Err(e) => return Ok(Loaded::Rejected(format!("a page is not a JSON array ({e})"))),
            }
        };
        drop(body);
        for value in page {
            let encoded = match serde_json::to_vec(&value) {
                Ok(encoded) => encoded,
                Err(e) => return Ok(Loaded::Rejected(format!("record {} cannot be encoded ({e})", tally.records))),
            };
            tally.add(&encoded);
            // Checked as the stream is read, so a stream that is not this state at all is given up
            // on before it is decoded to its end.
            if tally.records > pointer.records || tally.bytes > pointer.bytes {
                return Ok(Loaded::Rejected(format!(
                    "it holds more than the {} records ({} bytes) its checkpoint named",
                    pointer.records, pointer.bytes
                )));
            }
            let record = match serde_json::from_value::<Record>(value) {
                Ok(record) => record,
                Err(e) => {
                    return Ok(Loaded::Rejected(format!("record {} is not a state record ({e})", tally.records)));
                }
            };
            if let Err(e) = decoder.accept(record) {
                return Ok(Loaded::Rejected(format!("record {}: {e:#}", tally.records)));
            }
        }
        match next {
            Some(next) if !up_to_date && next != offset => offset = next,
            _ => break,
        }
    }
    if tally.records != pointer.records || tally.bytes != pointer.bytes {
        return Ok(Loaded::Rejected(format!(
            "it holds {} records ({} bytes), its checkpoint named {} ({} bytes): the stream is missing or was \
             not written to the end",
            tally.records, tally.bytes, pointer.records, pointer.bytes
        )));
    }
    let sha256 = tally.sha256();
    if sha256 != pointer.sha256 {
        return Ok(Loaded::Rejected(format!("its checksum is {sha256}, its checkpoint named {}", pointer.sha256)));
    }
    match decoder.finish(pos, highwater) {
        Ok(state) => Ok(Loaded::State(state)),
        Err(e) => Ok(Loaded::Rejected(format!("{e:#}"))),
    }
}

/// Folds a stream's records back into a [`RegistryState`], refusing anything out of place.
#[derive(Default)]
struct Decoder {
    header: Option<(LogPosition, Option<(u64, u64)>, u64, u64)>,
    state: RegistryState,
    seen: u64,
    ended: bool,
}

impl Decoder {
    fn accept(&mut self, record: Record) -> Result<()> {
        if self.ended {
            bail!("a record follows the trailer");
        }
        if self.header.is_none() && !matches!(record, Record::Header { .. }) {
            bail!("the stream does not start with a header");
        }
        match record {
            Record::Header { version, pos, highwater, nodes, shapes } => {
                if self.header.is_some() {
                    bail!("a second header");
                }
                if version != VERSION {
                    bail!("written in format {version}, this build reads {VERSION}");
                }
                self.header = Some((pos, highwater, nodes, shapes));
            }
            Record::Node { sig, table, fingerprint, gate } => {
                self.state.nodes.push(NodeState { sig, table, fingerprint, gate, contributors: Vec::new() });
            }
            Record::Contributors { sig, entries } => {
                let node =
                    self.state.nodes.last_mut().filter(|node| node.sig == sig).context("contributors of no node")?;
                for (pk, cell) in entries {
                    node.contributors.push((Arc::from(pk), cell.into_value()?));
                }
            }
            Record::Shape { id, gate, position, emitted } => {
                self.state.shapes.push(ShapeState { shape_id: id, gate, position, emitted, keys: Vec::new() });
            }
            Record::Keys { id, keys } => {
                let shape =
                    self.state.shapes.last_mut().filter(|shape| shape.shape_id == id).context("keys of no shape")?;
                shape.keys.extend(keys.into_iter().map(Arc::from));
            }
            Record::Group { path, position } => self.state.group_positions.push((path, position)),
            Record::End { records } => {
                if records != self.seen {
                    bail!("the trailer counts {records} records, {} came before it", self.seen);
                }
                self.ended = true;
            }
        }
        self.seen += 1;
        Ok(())
    }

    fn finish(self, pos: &LogPosition, highwater: Option<(u64, u64)>) -> Result<RegistryState> {
        let (at, at_highwater, nodes, shapes) = self.header.context("the stream is empty")?;
        if !self.ended {
            bail!("the stream has no trailer: it was not written to the end");
        }
        if &at != pos || at_highwater != highwater {
            bail!(
                "it was written at checkpoint {at} (highwater {at_highwater:?}), the catalog resumes from {pos} \
                 (highwater {highwater:?})"
            );
        }
        if nodes != self.state.nodes.len() as u64 || shapes != self.state.shapes.len() as u64 {
            bail!(
                "the header counts {nodes} nodes and {shapes} shapes, the stream holds {} and {}",
                self.state.nodes.len(),
                self.state.shapes.len()
            );
        }
        Ok(self.state)
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::ds::ScriptedStore;

    pub(crate) fn store() -> (DsClient, Arc<ScriptedStore>) {
        let store =
            Arc::new(ScriptedStore { read_appended: true, streams: Some(Default::default()), ..Default::default() });
        (DsClient::with_test_store("scripted://subq-state".into(), store.clone()), store)
    }

    fn pos(offset: &str) -> LogPosition {
        LogPosition { segment: 3, offset: offset.into() }
    }

    fn sample() -> RegistryState {
        RegistryState {
            nodes: vec![
                NodeState {
                    sig: "records|id|L(visible,Eq,true)".into(),
                    table: TableRef::from("records"),
                    fingerprint: None,
                    gate: SnapshotGate::parse("100:104:101,103", "0/1A0"),
                    contributors: vec![
                        (Arc::from("1"), Value::Int(1)),
                        (Arc::from("2"), Value::Int(9_007_199_254_740_993)),
                        (Arc::from("3"), Value::Text("three".into())),
                        (Arc::from("4"), Value::Null),
                        (Arc::from("5"), Value::Bool(true)),
                        (Arc::from("6"), Value::Float(dbsp::algebra::F64::new(0.1))),
                    ],
                },
                NodeState {
                    sig: "empty".into(),
                    table: TableRef::from("grants"),
                    fingerprint: None,
                    gate: SnapshotGate::passthrough(),
                    contributors: Vec::new(),
                },
            ],
            shapes: vec![
                ShapeState {
                    shape_id: "s1".into(),
                    gate: SnapshotGate::parse("100:104:", "0/1B0"),
                    position: 0x1_0000_0010,
                    emitted: 7,
                    keys: vec![Arc::from("10"), Arc::from("11")],
                },
                ShapeState {
                    shape_id: "s2".into(),
                    gate: SnapshotGate::passthrough(),
                    position: 0,
                    emitted: 0,
                    keys: Vec::new(),
                },
            ],
            group_positions: vec![("shape/group-1".into(), 0x2A)],
        }
    }

    async fn written(ds: &DsClient, state: &RegistryState, at: &LogPosition, hw: Option<(u64, u64)>) -> StatePointer {
        let pointer = seal(state, new_stream_name(), at, hw).unwrap();
        write(ds, state, &pointer, at, hw).await.unwrap();
        pointer
    }

    fn state_of(loaded: Loaded) -> RegistryState {
        match loaded {
            Loaded::State(state) => state,
            Loaded::Rejected(why) => panic!("the state was rejected: {why}"),
        }
    }

    fn rejection(loaded: Loaded) -> String {
        match loaded {
            Loaded::State(_) => panic!("the state was accepted"),
            Loaded::Rejected(why) => why,
        }
    }

    /// Every variant of a value, a gate with in-progress xids, an integer past 2^53 and a float all
    /// come back exactly as they were written.
    #[tokio::test]
    async fn state_comes_back_exactly_as_it_was_written() {
        let (ds, _store) = store();
        let state = sample();
        let pointer = written(&ds, &state, &pos("42"), Some((0x10, 3))).await;
        let read_back = state_of(read(&ds, &pointer, &pos("42"), Some((0x10, 3))).await.unwrap());
        assert_eq!(read_back.nodes.len(), 2);
        assert_eq!(read_back.nodes[0].sig, state.nodes[0].sig);
        assert_eq!(read_back.nodes[0].table, state.nodes[0].table);
        assert_eq!(read_back.nodes[0].contributors, state.nodes[0].contributors);
        assert!(read_back.nodes[1].contributors.is_empty());
        assert_eq!(read_back.shapes.iter().map(|shape| shape.shape_id.as_str()).collect::<Vec<_>>(), ["s1", "s2"]);
        assert_eq!((read_back.shapes[0].emitted, read_back.shapes[1].position), (7, 0));
        assert_eq!(read_back.shapes[0].keys, state.shapes[0].keys);
        assert_eq!(read_back.shapes[0].position, 0x1_0000_0010);
        assert_eq!(read_back.group_positions, state.group_positions);
        // `SnapshotGate` has no `PartialEq`: compare what a gate decides. 102 committed before the
        // node's seed snapshot, 103 was still in progress when it was taken.
        let gate = &read_back.nodes[0].gate;
        assert!(gate.should_skip(0, Some(102)) && !gate.should_skip(0, Some(103)));
    }

    /// A state larger than one chunk and one append is split, and still verifies.
    #[tokio::test]
    async fn a_large_state_is_written_in_bounded_pieces() {
        let (ds, store) = store();
        let mut state = sample();
        state.shapes[0].keys = (0..200_000).map(|i| Arc::from(format!("key-{i:08}"))).collect();
        let pointer = written(&ds, &state, &pos("42"), None).await;
        let appends: Vec<usize> = store
            .appended
            .lock()
            .unwrap()
            .iter()
            .filter(|(path, _, _)| path.ends_with(&pointer.stream))
            .map(|(_, _, body)| body.len())
            .collect();
        assert!(appends.len() > 1, "more than one append: {appends:?}");
        assert!(appends.iter().all(|bytes| *bytes < 2 * APPEND_BYTES), "every append is bounded: {appends:?}");
        let read_back = state_of(read(&ds, &pointer, &pos("42"), None).await.unwrap());
        assert_eq!(read_back.shapes[0].keys.len(), 200_000);
    }

    /// The pointer is the only authority: a stream that is missing, short, altered, or from another
    /// checkpoint is rejected, never partly used.
    #[tokio::test]
    async fn a_stream_that_does_not_match_its_pointer_is_rejected() {
        let (ds, store) = store();
        let state = sample();
        let at = pos("42");
        let pointer = written(&ds, &state, &at, Some((0x10, 3))).await;

        let missing = StatePointer { stream: new_stream_name(), ..pointer.clone() };
        assert!(rejection(read(&ds, &missing, &at, Some((0x10, 3))).await.unwrap()).contains("missing"));

        let wrong_sum = StatePointer { sha256: "0".repeat(64), ..pointer.clone() };
        assert!(rejection(read(&ds, &wrong_sum, &at, Some((0x10, 3))).await.unwrap()).contains("checksum"));

        let other_format = StatePointer { version: VERSION + 1, ..pointer.clone() };
        assert!(rejection(read(&ds, &other_format, &at, Some((0x10, 3))).await.unwrap()).contains("format"));

        // The same stream, read by a boot that resumes from another checkpoint.
        assert!(rejection(read(&ds, &pointer, &pos("43"), Some((0x10, 3))).await.unwrap()).contains("checkpoint"));
        assert!(rejection(read(&ds, &pointer, &at, Some((0x10, 4))).await.unwrap()).contains("checkpoint"));

        // A stream the writer did not finish: the pointer was made durable first, then the process
        // died part-way through the appends.
        let short = seal(&state, new_stream_name(), &at, None).unwrap();
        ds.ensure_stream(&short.stream).await.unwrap();
        let (header, _) =
            canonical(&Record::Header { version: VERSION, pos: at.clone(), highwater: None, nodes: 2, shapes: 2 })
                .unwrap();
        ds.append_json(&short.stream, &[header]).await.unwrap();
        assert!(rejection(read(&ds, &short, &at, None).await.unwrap()).contains("not written to the end"));

        // One more record than the pointer named.
        ds.append_json(&pointer.stream, &[serde_json::json!({"k": "group", "path": "shape/group-9", "position": 1})])
            .await
            .unwrap();
        assert!(rejection(read(&ds, &pointer, &at, Some((0x10, 3))).await.unwrap()).contains("more than"));
        drop(store);
    }

    /// A read the store cannot answer is an error, not a rejection: the boot retries instead of
    /// dropping every subquery shape over an outage.
    #[tokio::test]
    async fn a_storage_failure_is_an_error_not_a_rejection() {
        let (ds, store) = store();
        let state = sample();
        let pointer = written(&ds, &state, &pos("42"), None).await;
        store.nonlive_read_status.store(503, std::sync::atomic::Ordering::Relaxed);
        assert!(read(&ds, &pointer, &pos("42"), None).await.is_err());
    }
}
