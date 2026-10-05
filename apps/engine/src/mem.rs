//! Memory probes, exported via OpenTelemetry.
//!
//! Answers "how does engine memory evolve as shapes are created, for different deployment sizes?".
//! We track two layers:
//!   1. **Process memory** — resident (RSS) and virtual bytes (`memory-stats`, cross-platform).
//!   2. **Engine cardinalities** — the in-memory structures whose growth drives RSS: shapes, per-table
//!      tailers, shared family circuits (the M× join-trace amplifier), standalone circuits, and the
//!      subquery registry's nodes + contributor-pk sets.
//!
//! Both are published as OpenTelemetry observable gauges through a Prometheus exporter
//! (`GET /metrics/prometheus`, the format an OTel collector scrapes) and, when configured, an OTLP
//! HTTP/protobuf exporter. They are also exposed as JSON (`GET /memory`) for the benchmark harness.
//! OTel observable-gauge callbacks are synchronous, so a background sampler refreshes
//! a lock-free [`Gauges`] snapshot that the callbacks (and the JSON endpoint) read.
//!
//! A third layer, JSON-only (Phase 0 of the memory-reduction effort, no OTel gauges to avoid metric
//! churn): byte-level self-accounting, a lower-bound owned-heap estimate (see
//! [`crate::heap_size::HeapSize`]) per major structure. The gap between the sum of these and
//! `process.rss_bytes` is the allocator/pinning term this instrumentation exists to isolate.
//!
//! The byte-level walk (`Engine::mem_bytes`) is expensive — it locks engine state, round-trips a
//! `SequencerCmd::MemBytes` command to the sequencer task, locks the subquery registry, and walks
//! roughly the engine's entire owned heap. It runs on the explicit `GET /memory` endpoint and, when
//! enabled, on the slower structured memory logger; it must never run on the 500ms background OTel
//! sampler (`spawn_sampler` below), which calls `Engine::mem_cardinalities` exclusively. See
//! `engine::Engine::mem_cardinalities` / `mem_bytes` for the split and `http::get_memory` for the
//! endpoint that combines both layers.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use opentelemetry::metrics::MeterProvider as _;
use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider, Temporality};
use prometheus::{Registry, TextEncoder};

/// The six byte-level self-accounting terms (Phase 0 of the memory-reduction effort), computed by
/// [`crate::engine::Engine::mem_bytes`] — the on-demand diagnostic counterpart to
/// [`Cardinalities`]/[`crate::engine::Engine::mem_cardinalities`]. Deliberately its own type (not
/// folded into `Cardinalities` at the source) so the cheap-count path has no fields to leave zeroed
/// by convention — `mem_cardinalities` simply never constructs one of these.
#[derive(Clone, Default)]
pub struct HeapBytes {
    pub bytes_shape_records: usize,
    pub bytes_executors: usize,
    pub bytes_retention: usize,
    pub bytes_subquery_registry: usize,
    pub bytes_membership_circuit: usize,
    /// Split of `bytes_membership_circuit` (Task 1.3): the raw upsert-map integrals
    /// (CONTRIBUTORS + FEEDS `(id,pk)→value` maps — the operators' own input integrals).
    pub bytes_circuit_integral: usize,
    /// Split of `bytes_membership_circuit` (Task 1.3): the derived MEMBERS relation snapshot
    /// (`(node,value)`), published for `contains`/introspection reads.
    pub bytes_circuit_snapshots: usize,
    /// Host-side per-feed key sets (Task 2.2, dbsp-ds-dh6): the delete gate's Roaring bitmaps,
    /// moved OUT of the membership circuit. Replaces the feed component that used to live in
    /// `bytes_membership_circuit`/`bytes_circuit_integral`; ~10–19× lighter than the in-circuit
    /// relation it supplants (owned-heap floor: serialized bitmap payloads + the outer HashMap).
    pub bytes_feed_sets: usize,
    /// The global pk dictionary (Task 2.1): once-per-distinct-pk string storage + forward/reverse
    /// index. Append-only (no eviction in v1); reported so the string-interning trade is visible.
    pub bytes_pk_dict: usize,
    pub bytes_electric_adapter: usize,
}

/// Engine-internal cardinalities, computed from in-memory state by [`crate::engine::Engine::mem_cardinalities`].
#[derive(Clone, Default, serde::Serialize)]
pub struct Cardinalities {
    pub shapes: usize,
    /// Shapes currently dormant (retention lifecycle: stream retained, engine state dropped).
    pub shapes_dormant: usize,
    pub tailers: usize,
    pub tables: usize,
    pub families: usize,
    pub family_shapes: usize,
    pub standalone: usize,
    pub subquery_nodes: usize,
    pub subquery_contributors: usize,
    pub subquery_distinct_values: usize,
    pub subquery_shapes: usize,
    pub subquery_edges: usize,
    /// Total pks delivered across all shapes' host-side per-feed key sets (Task 2.2, the delete
    /// gate moved out of the membership circuit). The row-count analogue of `subquery_contributors`;
    /// `bytes_feed_sets` measures its byte cost.
    pub subquery_feed_entries: usize,
    /// `ShapeRecord`s in the shape registry (`Engine.state.shapes`).
    pub bytes_shape_records: usize,
    /// Per-table executor structures: standalone shapes + their conjunct index, family routers
    /// (`RoutedShape`s), aggregate folds + their conjunct index.
    pub bytes_executors: usize,
    /// Per-shape retention lifecycle records (`Engine.lives`) — dominated by dormant shapes'
    /// resume offsets + snapshot gates.
    pub bytes_retention: usize,
    /// The cross-table subquery registry's own structures: nodes, templates (incl. their
    /// `pk_nodes` inverted index), shapes, and edges — excludes the membership circuit itself
    /// (see `bytes_membership_circuit`).
    pub bytes_subquery_registry: usize,
    /// Measured owned/on-disk bytes of the dbsp membership circuit's published snapshots (subquery
    /// inner sets — the CONTRIBUTORS relation), via dbsp's `BatchReader::approximate_byte_size`
    /// (exact in-memory columnar bytes when resident, on-disk file size when spilled). Equals
    /// `bytes_circuit_integral + bytes_circuit_snapshots`. See `SubqueryRegistry::circuit_bytes`.
    /// NOTE: this covers only the host-published snapshots (which share the operators' own
    /// integrals via dbsp's trace cache); dbsp's non-published incremental state (z1 delayed
    /// traces, `distinct` integrals) roughly doubles it and is measurable only via the profiler.
    /// The per-feed key sets left this term in Task 2.2 — see `bytes_feed_sets`.
    pub bytes_membership_circuit: usize,
    /// Raw contributor upsert-map integral term of `bytes_membership_circuit`.
    pub bytes_circuit_integral: usize,
    /// Derived MEMBERS relation snapshot term of `bytes_membership_circuit`.
    pub bytes_circuit_snapshots: usize,
    /// Host-side per-feed key sets (Task 2.2, dbsp-ds-dh6): the delete gate's Roaring bitmaps,
    /// moved out of the membership circuit. Replaces the feed component formerly folded into
    /// `bytes_membership_circuit`. See `SubqueryRegistry::feed_sets_bytes`.
    pub bytes_feed_sets: usize,
    /// The global pk dictionary (Task 2.1): amortized once-per-distinct-pk string storage plus its
    /// forward/reverse index — the append-only cost of keying the circuit by `u32` pk ids instead
    /// of heap strings. See `SubqueryRegistry::pk_dict_bytes`.
    pub bytes_pk_dict: usize,
    /// The `/v1/shape` (Electric-protocol) adapter's TTL handle registry: per-handle cursor
    /// state (known-keys sets, in-flight live-poll map).
    pub bytes_electric_adapter: usize,
}

impl Cardinalities {
    /// Fold in the on-demand byte-level terms computed by `Engine::mem_bytes`. The only caller is
    /// the `/memory` HTTP handler — the counts alone (as returned by `mem_cardinalities`, all
    /// `bytes_*` left at their `Default` zero) are what the 500ms background sampler publishes.
    pub fn with_bytes(mut self, bytes: HeapBytes) -> Self {
        self.bytes_shape_records = bytes.bytes_shape_records;
        self.bytes_executors = bytes.bytes_executors;
        self.bytes_retention = bytes.bytes_retention;
        self.bytes_subquery_registry = bytes.bytes_subquery_registry;
        self.bytes_membership_circuit = bytes.bytes_membership_circuit;
        self.bytes_circuit_integral = bytes.bytes_circuit_integral;
        self.bytes_circuit_snapshots = bytes.bytes_circuit_snapshots;
        self.bytes_feed_sets = bytes.bytes_feed_sets;
        self.bytes_pk_dict = bytes.bytes_pk_dict;
        self.bytes_electric_adapter = bytes.bytes_electric_adapter;
        self
    }
}

/// Lock-free snapshot the OTel gauge callbacks and `/memory` read. Updated by the sampler and on demand.
#[derive(Default)]
pub(crate) struct Gauges {
    rss_bytes: AtomicU64,
    virtual_bytes: AtomicU64,
    shapes: AtomicU64,
    shapes_dormant: AtomicU64,
    tailers: AtomicU64,
    tables: AtomicU64,
    families: AtomicU64,
    family_shapes: AtomicU64,
    standalone: AtomicU64,
    subquery_nodes: AtomicU64,
    subquery_contributors: AtomicU64,
    subquery_distinct_values: AtomicU64,
    subquery_shapes: AtomicU64,
    subquery_edges: AtomicU64,
    subquery_feed_entries: AtomicU64,
    samples: AtomicU64,
}

/// Current process resident + virtual memory in bytes (0 if unavailable on this platform).
pub fn process_memory() -> (u64, u64) {
    match memory_stats::memory_stats() {
        Some(s) => (s.physical_mem as u64, s.virtual_mem as u64),
        None => (0, 0),
    }
}

/// Jemalloc counters for transient allocator slack. Returns `(allocated, resident, retained)`;
/// zeroes on targets where the process uses the platform allocator.
#[cfg(unix)]
pub fn allocator_memory() -> (u64, u64, u64) {
    let _ = tikv_jemalloc_ctl::epoch::advance();
    (
        tikv_jemalloc_ctl::stats::allocated::read().unwrap_or(0) as u64,
        tikv_jemalloc_ctl::stats::resident::read().unwrap_or(0) as u64,
        tikv_jemalloc_ctl::stats::retained::read().unwrap_or(0) as u64,
    )
}

#[cfg(unix)]
#[derive(Clone, Copy, Debug, Default, serde::Serialize)]
pub struct AllocatorConfig {
    pub background_thread: Option<bool>,
    pub dirty_decay_ms: Option<i64>,
    pub muzzy_decay_ms: Option<i64>,
}

#[cfg(unix)]
pub fn allocator_config() -> AllocatorConfig {
    // These options are read-only `opt.*` values: unlike `MALLOC_CONF`, querying them verifies
    // what jemalloc initialized with, including the prefixed `_rjem_` build used by this crate.
    unsafe {
        AllocatorConfig {
            background_thread: tikv_jemalloc_ctl::raw::read(b"opt.background_thread\0").ok(),
            dirty_decay_ms: tikv_jemalloc_ctl::raw::read(b"opt.dirty_decay_ms\0").ok(),
            muzzy_decay_ms: tikv_jemalloc_ctl::raw::read(b"opt.muzzy_decay_ms\0").ok(),
        }
    }
}

#[cfg(not(unix))]
#[derive(Clone, Copy, Debug, Default, serde::Serialize)]
pub struct AllocatorConfig;

#[cfg(not(unix))]
pub fn allocator_config() -> AllocatorConfig {
    AllocatorConfig
}

#[cfg(not(unix))]
pub fn allocator_memory() -> (u64, u64, u64) {
    (0, 0, 0)
}

#[cfg(all(test, unix))]
mod allocator_tests {
    #[test]
    fn jemalloc_decay_config_is_reported() {
        let config = super::allocator_config();
        assert_eq!(config.dirty_decay_ms, Some(1000));
        assert_eq!(config.muzzy_decay_ms, Some(1000));
    }
}

/// cgroup memory counters for the container. ECS exposes task-level memory in Container
/// Insights, but these files give us the engine's own 5-second view and preserve the `oom_kill`
/// edge that a one-minute CloudWatch sample can miss. ECS EC2 hosts may still mount cgroup v1,
/// so the probe supports both layouts and reports which one was found.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CgroupMemory {
    pub version: Option<&'static str>,
    pub current_bytes: Option<u64>,
    pub max_bytes: Option<u64>,
    pub high_events: Option<u64>,
    pub oom_events: Option<u64>,
    pub oom_kill_events: Option<u64>,
    /// cgroup-v1 allocation-failure counter (`memory.failcnt`).
    pub failcnt: Option<u64>,
    /// cgroup-v1's current OOM pressure bit (`memory.oom_control: under_oom`).
    pub under_oom: Option<u64>,
}

fn read_cgroup_bytes(path: &str) -> Option<u64> {
    std::fs::read_to_string(path).ok()?.trim().parse().ok()
}

fn parse_cgroup_event(contents: &str, key: &str) -> Option<u64> {
    contents.lines().find_map(|line| {
        let mut fields = line.split_whitespace();
        let name = fields.next()?;
        let value = fields.next()?;
        (name == key).then(|| value.trim().parse().ok()).flatten()
    })
}

fn read_cgroup_event(path: &str, key: &str) -> Option<u64> {
    parse_cgroup_event(&std::fs::read_to_string(path).ok()?, key)
}

fn read_cgroup_v2() -> Option<CgroupMemory> {
    if !std::path::Path::new("/sys/fs/cgroup/memory.current").exists() {
        return None;
    }
    Some(CgroupMemory {
        version: Some("v2"),
        current_bytes: read_cgroup_bytes("/sys/fs/cgroup/memory.current"),
        max_bytes: read_cgroup_bytes("/sys/fs/cgroup/memory.max"),
        high_events: read_cgroup_event("/sys/fs/cgroup/memory.events", "high"),
        oom_events: read_cgroup_event("/sys/fs/cgroup/memory.events", "oom"),
        oom_kill_events: read_cgroup_event("/sys/fs/cgroup/memory.events", "oom_kill"),
        failcnt: None,
        under_oom: None,
    })
}

fn read_cgroup_v1() -> Option<CgroupMemory> {
    let current_bytes = read_cgroup_bytes("/sys/fs/cgroup/memory/memory.usage_in_bytes");
    let max_bytes = read_cgroup_bytes("/sys/fs/cgroup/memory/memory.limit_in_bytes");
    if current_bytes.is_none() && max_bytes.is_none() {
        return None;
    }

    let oom_control = "/sys/fs/cgroup/memory/memory.oom_control";
    Some(CgroupMemory {
        version: Some("v1"),
        current_bytes,
        max_bytes,
        high_events: None,
        oom_events: read_cgroup_event(oom_control, "oom"),
        oom_kill_events: read_cgroup_event(oom_control, "oom_kill"),
        failcnt: read_cgroup_bytes("/sys/fs/cgroup/memory/memory.failcnt"),
        under_oom: read_cgroup_event(oom_control, "under_oom"),
    })
}

/// Read the Linux cgroup memory files. On non-Linux hosts without a mounted memory controller all
/// fields are `None`; the sampler still reports process RSS and engine cardinalities.
pub fn cgroup_memory() -> CgroupMemory {
    read_cgroup_v2().or_else(read_cgroup_v1).unwrap_or_default()
}

fn log_memory_snapshot(card: &Cardinalities, bytes: Option<&HeapBytes>) {
    let (rss_bytes, virtual_bytes) = process_memory();
    let (allocator_allocated_bytes, allocator_resident_bytes, allocator_retained_bytes) = allocator_memory();
    let cgroup = cgroup_memory();
    let (
        bytes_shape_records,
        bytes_executors,
        bytes_retention,
        bytes_subquery_registry,
        bytes_membership_circuit,
        bytes_circuit_integral,
        bytes_circuit_snapshots,
        bytes_feed_sets,
        bytes_pk_dict,
        bytes_electric_adapter,
    ) = bytes.map_or((0, 0, 0, 0, 0, 0, 0, 0, 0, 0), |b| {
        (
            b.bytes_shape_records,
            b.bytes_executors,
            b.bytes_retention,
            b.bytes_subquery_registry,
            b.bytes_membership_circuit,
            b.bytes_circuit_integral,
            b.bytes_circuit_snapshots,
            b.bytes_feed_sets,
            b.bytes_pk_dict,
            b.bytes_electric_adapter,
        )
    });

    tracing::info!(
        target: "electric_circuits_engine::memory",
        event = "memory_snapshot",
        rss_bytes,
        virtual_bytes,
        allocator_allocated_bytes,
        allocator_resident_bytes,
        allocator_retained_bytes,
        cgroup_memory_current_bytes = cgroup.current_bytes.unwrap_or(0),
        cgroup_memory_max_bytes = cgroup.max_bytes.unwrap_or(0),
        cgroup_memory_high_events = cgroup.high_events.unwrap_or(0),
        cgroup_memory_oom_events = cgroup.oom_events.unwrap_or(0),
        cgroup_memory_oom_kill_events = cgroup.oom_kill_events.unwrap_or(0),
        cgroup_memory_failcnt = cgroup.failcnt.unwrap_or(0),
        cgroup_memory_under_oom = cgroup.under_oom.unwrap_or(0),
        cgroup_memory_version = cgroup.version.unwrap_or("unavailable"),
        cgroup_memory_available = cgroup.current_bytes.is_some(),
        shapes = card.shapes,
        shapes_dormant = card.shapes_dormant,
        tailers = card.tailers,
        tables = card.tables,
        families = card.families,
        family_shapes = card.family_shapes,
        standalone = card.standalone,
        subquery_nodes = card.subquery_nodes,
        subquery_contributors = card.subquery_contributors,
        subquery_distinct_values = card.subquery_distinct_values,
        subquery_shapes = card.subquery_shapes,
        subquery_edges = card.subquery_edges,
        subquery_feed_entries = card.subquery_feed_entries,
        bytes_sampled = bytes.is_some(),
        bytes_shape_records,
        bytes_executors,
        bytes_retention,
        bytes_subquery_registry,
        bytes_membership_circuit,
        bytes_circuit_integral,
        bytes_circuit_snapshots,
        bytes_feed_sets,
        bytes_pk_dict,
        bytes_electric_adapter,
        "engine memory snapshot",
    );
}

/// Emit cheap process/cardinality snapshots at `interval`, with an optional slower owned-heap walk.
/// The byte walk is deliberately not part of the 500 ms OTel sampler; it runs only at the explicitly
/// configured diagnostic period and is marked with `bytes_sampled=true` in the structured log.
pub fn spawn_memory_logger(
    engine: crate::engine::Engine,
    interval: Duration,
    bytes_interval: Duration,
    shutdown: crate::shutdown::ShutdownToken,
) {
    if interval.is_zero() {
        return;
    }
    tokio::spawn(async move {
        let mut next_bytes_sample = std::time::Instant::now();
        loop {
            let card = engine.mem_cardinalities().await;
            let bytes = if !bytes_interval.is_zero() && std::time::Instant::now() >= next_bytes_sample {
                next_bytes_sample = std::time::Instant::now() + bytes_interval;
                Some(engine.mem_bytes().await)
            } else {
                None
            };
            log_memory_snapshot(&card, bytes.as_ref());

            tokio::select! {
                _ = shutdown.wait() => break,
                _ = tokio::time::sleep(interval) => {}
            }
        }
    });
}

/// Refresh the published gauges from a freshly-measured process memory + engine cardinalities. Called by
/// the background sampler and by `/memory` so the JSON read and the OTel scrape agree.
pub(crate) fn publish(g: &Gauges, card: &Cardinalities) {
    let (rss, virt) = process_memory();
    g.rss_bytes.store(rss, Ordering::Relaxed);
    g.virtual_bytes.store(virt, Ordering::Relaxed);
    g.shapes.store(card.shapes as u64, Ordering::Relaxed);
    g.shapes_dormant.store(card.shapes_dormant as u64, Ordering::Relaxed);
    g.tailers.store(card.tailers as u64, Ordering::Relaxed);
    g.tables.store(card.tables as u64, Ordering::Relaxed);
    g.families.store(card.families as u64, Ordering::Relaxed);
    g.family_shapes.store(card.family_shapes as u64, Ordering::Relaxed);
    g.standalone.store(card.standalone as u64, Ordering::Relaxed);
    g.subquery_nodes.store(card.subquery_nodes as u64, Ordering::Relaxed);
    g.subquery_contributors.store(card.subquery_contributors as u64, Ordering::Relaxed);
    g.subquery_distinct_values.store(card.subquery_distinct_values as u64, Ordering::Relaxed);
    g.subquery_shapes.store(card.subquery_shapes as u64, Ordering::Relaxed);
    g.subquery_edges.store(card.subquery_edges as u64, Ordering::Relaxed);
    g.subquery_feed_entries.store(card.subquery_feed_entries as u64, Ordering::Relaxed);
    g.samples.fetch_add(1, Ordering::Relaxed);
}

/// JSON snapshot for `GET /memory` (RSS measured fresh; cardinalities from the last publish).
///
/// The `bytes_*` fields are read directly from `card` (the just-computed snapshot), not from
/// [`Gauges`] — they are JSON-only (no OTel gauge), so there is nothing published to read back;
/// every other field mirrors the last [`publish`] call, same as before.
pub(crate) fn snapshot_json(g: &Gauges, card: &Cardinalities) -> serde_json::Value {
    let (rss, virt) = process_memory();
    let (allocator_allocated, allocator_resident, allocator_retained) = allocator_memory();
    let allocator_config = allocator_config();
    g.rss_bytes.store(rss, Ordering::Relaxed);
    g.virtual_bytes.store(virt, Ordering::Relaxed);
    serde_json::json!({
        "process": {
            "rss_bytes": rss,
            "rss_mib": rss / (1024 * 1024),
            "virtual_bytes": virt,
            "allocator_allocated_bytes": allocator_allocated,
            "allocator_resident_bytes": allocator_resident,
            "allocator_retained_bytes": allocator_retained,
            "allocator_config": allocator_config,
        },
        "cardinalities": {
            "shapes": g.shapes.load(Ordering::Relaxed),
            "shapes_dormant": g.shapes_dormant.load(Ordering::Relaxed),
            "tailers": g.tailers.load(Ordering::Relaxed),
            "tables": g.tables.load(Ordering::Relaxed),
            "families": g.families.load(Ordering::Relaxed),
            "family_shapes": g.family_shapes.load(Ordering::Relaxed),
            "standalone": g.standalone.load(Ordering::Relaxed),
            "subquery_nodes": g.subquery_nodes.load(Ordering::Relaxed),
            "subquery_contributors": g.subquery_contributors.load(Ordering::Relaxed),
            "subquery_distinct_values": g.subquery_distinct_values.load(Ordering::Relaxed),
            "subquery_shapes": g.subquery_shapes.load(Ordering::Relaxed),
            "subquery_edges": g.subquery_edges.load(Ordering::Relaxed),
            "subquery_feed_entries": g.subquery_feed_entries.load(Ordering::Relaxed),
            "bytes_shape_records": card.bytes_shape_records,
            "bytes_executors": card.bytes_executors,
            "bytes_retention": card.bytes_retention,
            "bytes_subquery_registry": card.bytes_subquery_registry,
            "bytes_membership_circuit": card.bytes_membership_circuit,
            "bytes_circuit_integral": card.bytes_circuit_integral,
            "bytes_circuit_snapshots": card.bytes_circuit_snapshots,
            "bytes_feed_sets": card.bytes_feed_sets,
            "bytes_pk_dict": card.bytes_pk_dict,
            "bytes_electric_adapter": card.bytes_electric_adapter,
        },
        "samples": g.samples.load(Ordering::Relaxed),
    })
}

/// Shape counts from the last published cardinality snapshot (refreshed by the background sampler):
/// `(total, family_shapes, standalone)`. Used by the StatsD periodic sampler for the
/// `electric.shapes.*` gauges without re-locking engine state on the poll path.
pub(crate) fn published_shape_counts(g: &Gauges) -> (u64, u64, u64) {
    (g.shapes.load(Ordering::Relaxed), g.family_shapes.load(Ordering::Relaxed), g.standalone.load(Ordering::Relaxed))
}

/// One Engine's exporters, observations and cumulative series.
pub(crate) struct Telemetry {
    pub(crate) provider: SdkMeterProvider,
    registry: Registry,
}
impl Telemetry {
    pub(crate) fn prometheus_text(&self) -> String {
        let mut text = String::new();
        let _ = TextEncoder::new().encode_utf8(&self.registry.gather(), &mut text);
        text
    }
    pub(crate) fn shutdown(&self) {
        if let Err(error) = self.provider.force_flush() {
            tracing::warn!(%error, "Engine telemetry flush failed");
        }
        if let Err(error) = self.provider.shutdown() {
            tracing::warn!(%error, "Engine telemetry shutdown failed");
        }
    }
}

/// Build a provider for this Engine. Captures only metric state, never the Engine.
pub(crate) fn init_otel(
    metrics: Arc<crate::metrics::Metrics>,
    gauges: Arc<Gauges>,
    source_id: Option<&str>,
) -> Telemetry {
    let registry = Registry::new();
    let exporter = opentelemetry_prometheus::exporter()
        .with_registry(registry.clone())
        .build()
        .expect("build prometheus exporter");
    let mut provider_builder = SdkMeterProvider::builder().with_reader(exporter);
    if std::env::var("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT")
        .or_else(|_| std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT"))
        .ok()
        .is_some_and(|endpoint| !endpoint.trim().is_empty())
    {
        match opentelemetry_otlp::MetricExporter::builder()
            .with_http()
            .with_temporality(Temporality::Cumulative)
            .build()
        {
            Ok(otlp_exporter) => {
                let reader = PeriodicReader::builder(otlp_exporter, opentelemetry_sdk::runtime::Tokio)
                    .with_interval(Duration::from_secs(10))
                    .with_timeout(Duration::from_secs(5))
                    .build();
                provider_builder = provider_builder.with_reader(reader);
                tracing::info!(
                    target: "electric_circuits_engine::telemetry",
                    "OTLP metrics export enabled from OTEL_EXPORTER_OTLP_* configuration"
                );
            }
            Err(error) => tracing::error!(
                target: "electric_circuits_engine::telemetry",
                error = %error,
                "OTLP metrics exporter initialization failed; Prometheus metrics remain enabled"
            ),
        }
    }

    if let Some(source) = source_id {
        provider_builder =
            provider_builder.with_resource(opentelemetry_sdk::Resource::new([opentelemetry::KeyValue::new(
                "source_id",
                source.to_owned(),
            )]));
    }
    let attributes: Vec<_> =
        source_id.into_iter().map(|source| opentelemetry::KeyValue::new("source_id", source.to_owned())).collect();
    let provider = provider_builder.build();
    let meter = provider.meter("electric_circuits_engine");

    // One observable gauge per metric; each callback reads the lock-free published snapshot.
    macro_rules! gauge {
        ($name:expr, $desc:expr, $field:ident, $unit:expr) => {{
            let b = meter.u64_observable_gauge($name).with_description($desc);
            let b = if $unit.is_empty() { b } else { b.with_unit($unit) };
            let gauges = gauges.clone();
            let attributes = attributes.clone();
            b.with_callback(move |obs| obs.observe(gauges.$field.load(Ordering::Relaxed), &attributes)).build();
        }};
    }
    gauge!("engine_process_resident_memory", "Resident set size of the engine process", rss_bytes, "By");
    gauge!("engine_process_virtual_memory", "Virtual memory of the engine process", virtual_bytes, "By");
    gauge!("engine_shapes", "Registered shapes (all kinds)", shapes, "");
    gauge!(
        "engine_shapes_dormant",
        "Dormant shapes (retention: stream retained, engine state dropped)",
        shapes_dormant,
        ""
    );
    gauge!("engine_tailers", "Per-table replication tailers", tailers, "");
    gauge!("engine_tables", "Tables with a known schema", tables, "");
    gauge!("engine_family_circuits", "Shared equality family circuits (each holds the base table once)", families, "");
    gauge!("engine_family_shapes", "Shapes attached to family circuits", family_shapes, "");
    gauge!("engine_standalone_circuits", "Standalone per-shape circuits", standalone, "");
    gauge!("engine_subquery_nodes", "Maintained subquery inner-set nodes (shared)", subquery_nodes, "");
    gauge!("engine_subquery_contributors", "Total contributor pks across subquery nodes", subquery_contributors, "");
    gauge!("engine_subquery_distinct_values", "Distinct values across subquery nodes", subquery_distinct_values, "");
    gauge!("engine_subquery_shapes", "Subquery (cross-table) shapes", subquery_shapes, "");
    gauge!("engine_subquery_edges", "Subquery dependency edges", subquery_edges, "");
    gauge!(
        "engine_subquery_feed_entries",
        "Total pks delivered across subquery shapes' feed sets",
        subquery_feed_entries,
        ""
    );

    // The engine's own counters and gauges (`crate::metrics`, served as JSON at `GET /metrics`) are
    // exported here too, so `/metrics/prometheus` is a complete scrape target rather than the
    // memory/cardinality half of one. Counters are observable COUNTERS (monotonic within an Engine;
    // `POST /metrics/reset` is a benchmark affordance and shows up as a reset, which is exactly what
    // it is); the gauges below describe the world right now.
    //
    // NOTE ON NAMES: the Prometheus exporter appends the unit suffix and, for a counter, `_total`,
    // so the names below are the OTel ones WITHOUT those suffixes — `engine_txn_spills` + counter →
    // `engine_txn_spills_total`; `engine_replication_slot_retained_wal` + gauge + `By` →
    // `engine_replication_slot_retained_wal_bytes`; `engine_txn_spill` + counter + `By` →
    // `engine_txn_spill_bytes_total` (both suffixes, in that order). Spelling a suffix in the name
    // too produces `…_total_total` / `…_bytes_bytes`.
    macro_rules! engine_counter {
        ($name:expr, $desc:expr, $field:ident, $unit:expr) => {{
            let b = meter.u64_observable_counter($name).with_description($desc);
            let b = if $unit.is_empty() { b } else { b.with_unit($unit) };
            let metrics = metrics.clone();
            let attributes = attributes.clone();
            b.with_callback(move |obs| obs.observe(metrics.$field.load(Ordering::Relaxed), &attributes)).build();
        }};
    }
    macro_rules! engine_gauge {
        ($name:expr, $desc:expr, $field:ident, $unit:expr) => {{
            let b = meter.u64_observable_gauge($name).with_description($desc);
            let b = if $unit.is_empty() { b } else { b.with_unit($unit) };
            let metrics = metrics.clone();
            let attributes = attributes.clone();
            b.with_callback(move |obs| obs.observe(metrics.$field.load(Ordering::Relaxed), &attributes)).build();
        }};
    }
    engine_counter!("engine_envelopes_processed", "Table change events fanned out", envelopes, "");
    engine_counter!("engine_shape_appends", "Appends to shape streams", shape_appends, "");
    engine_counter!("engine_family_steps", "Family circuit transactions", family_steps, "");
    engine_counter!("engine_shapes_dormanted", "Retention: active -> dormant transitions", shapes_dormanted, "");
    engine_counter!("engine_shapes_reactivated", "Retention: dormant -> active transitions", shapes_reactivated, "");
    engine_counter!("engine_shapes_evicted", "Retention: dormant shapes evicted (stream deleted)", shapes_evicted, "");
    engine_counter!(
        "engine_retention_pressure",
        "Sweeps where a cap was exceeded with nothing dormant to evict",
        retention_pressure,
        ""
    );
    engine_counter!("engine_schema_drift", "Tables whose dependents were retired (ADR-0005)", schema_drift, "");
    engine_counter!(
        "engine_schema_unresolved",
        "Drifts that could not be resolved (table parked)",
        schema_unresolved,
        ""
    );
    engine_counter!("engine_epoch_breaks", "Slots the engine could no longer vouch for (ADR-0004)", epoch_breaks, "");
    engine_counter!("engine_epoch_resets", "New epochs bound (every shape retired, fresh slot)", epoch_resets, "");
    engine_counter!(
        "engine_changes_rotations",
        "Change-log segments closed and succeeded (ADR-0006)",
        changes_rotations,
        ""
    );
    engine_counter!("engine_changes_segments_deleted", "Rotated-out segments retired", changes_segments_deleted, "");
    engine_counter!(
        "engine_catalog_restore_retired",
        "Shape records retired during catalog restore because their streams were missing or closed",
        catalog_restore_retired,
        ""
    );
    engine_counter!("engine_txn_spills", "Transactions whose buffer outgrew the memory cap (ADR-0003)", txn_spills, "");
    engine_counter!("engine_txn_spill", "Bytes ever written to transaction spill files", txn_spill_bytes, "By");
    engine_counter!(
        "engine_txn_chunked_appends",
        "Chunk appends made by commits too large for one append",
        txn_chunked_appends,
        ""
    );
    engine_counter!(
        "engine_backfill_chunked_appends",
        "Chunk appends made by backfills too large for one append",
        backfill_chunked_appends,
        ""
    );
    engine_counter!(
        "engine_sequencer_orphan_fragments",
        "Incomplete transaction fragments discarded by the sequencer",
        sequencer_orphan_fragments,
        ""
    );
    engine_gauge!(
        "engine_changes_segments_retained",
        "Change-log segments that exist right now",
        changes_segments_retained,
        ""
    );
    engine_gauge!(
        "engine_sequencer_held_run",
        "1 while the sequencer holds an incomplete transaction (ADR-0003)",
        sequencer_held_run,
        ""
    );
    engine_gauge!("engine_shutdown_in_progress", "1 once a graceful shutdown has begun", shutdown_in_progress, "");
    engine_gauge!(
        "engine_replication_slot_retained_wal",
        "WAL Postgres retains for this engine's slot (pg_current_wal_lsn - restart_lsn)",
        replication_slot_retained_wal_bytes,
        "By"
    );
    engine_gauge!(
        "engine_replication_confirmed_flush_lag",
        "Ingest lag in WAL bytes (pg_current_wal_lsn - confirmed_flush_lsn)",
        replication_confirmed_flush_lag_bytes,
        "By"
    );
    engine_gauge!("engine_replication_slot_active", "1 while a walsender holds the slot", replication_slot_active, "");

    Telemetry { provider, registry }
}

/// Spawn the background sampler: every `interval`, recompute engine cardinalities and republish the
/// gauges so the OTel scrape reflects current state without a `/memory` poll.
///
/// Deliberately calls `mem_cardinalities` only — cheap counts, no `heap_bytes` walk, no
/// `SequencerCmd::MemBytes` round-trip. Do not change this to call `mem_bytes` (or any function
/// that does): that byte-level walk is reserved for `GET /memory` and the slower diagnostic logger
/// (see the module doc comment above and `Engine::mem_bytes`'s doc comment for why).
pub fn spawn_sampler(engine: crate::engine::Engine, interval: Duration) {
    let shutdown = engine.shutdown_token();
    shutdown.clone().spawn_background(async move {
        loop {
            let card = engine.mem_cardinalities().await;
            publish(&engine.memory_gauges, &card);
            tokio::select! { _ = shutdown.wait() => break, _ = tokio::time::sleep(interval) => {} }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::parse_cgroup_event;

    #[test]
    fn source_memory_publication_and_registries_are_independent() {
        use super::*;
        let alpha = Arc::new(Gauges::default());
        let beta = Arc::new(Gauges::default());
        let alpha_provider = init_otel(Arc::new(crate::metrics::Metrics::default()), alpha.clone(), Some("alpha"));
        let beta_provider = init_otel(Arc::new(crate::metrics::Metrics::default()), beta.clone(), Some("beta"));
        let card_a = Cardinalities { shapes: 7, ..Default::default() };
        let card_b = Cardinalities { shapes: 2, ..Default::default() };
        publish(&alpha, &card_a);
        publish(&beta, &card_b);
        assert_eq!(snapshot_json(&alpha, &card_a)["cardinalities"]["shapes"], 7);
        assert_eq!(snapshot_json(&beta, &card_b)["cardinalities"]["shapes"], 2);
        let a = alpha_provider.prometheus_text();
        let b = beta_provider.prometheus_text();
        assert!(a.contains("source_id=\"alpha\""));
        assert!(!a.contains("source_id=\"beta\""));
        assert!(b.contains("source_id=\"beta\""));
        assert!(!b.contains("source_id=\"alpha\""));
        alpha_provider.shutdown();
        drop(alpha_provider);
        assert_eq!(beta_provider.prometheus_text(), b);
        beta_provider.shutdown();
    }

    #[test]
    fn cgroup_event_parser_accepts_only_the_named_counter() {
        let contents = "high 3\noom 1\noom_kill 2\n";
        assert_eq!(parse_cgroup_event(contents, "oom_kill"), Some(2));
    }

    #[test]
    fn cgroup_event_parser_ignores_malformed_values() {
        let contents = "oom_kill nope\n";
        assert_eq!(parse_cgroup_event(contents, "oom_kill"), None);
    }
}
