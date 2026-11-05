//! `retire-generation <catalog stream path> [--confirm]`: delete what one stopped engine
//! generation left in Durable Streams.
//!
//! A generation is one catalog namespace (`circuits/v1/<namespace>/stores/<store generation>/
//! queries/<query generation>/`). When a deployment moves to a new query generation the old
//! catalog and every stream it names stay on disk, because nothing tracks them any more. This
//! one-shot command folds the old catalog exactly as a boot would and deletes what the fold still
//! names — live and dormant shape streams, group streams, shapes whose retirement never landed,
//! change-log segments (and any later segment a rotation created but never recorded) — and then
//! the catalog itself.
//!
//! It is deliberately hard to aim at the wrong thing:
//!
//! - it takes the exact physical path of one `meta/catalog` stream, never a prefix or a pattern;
//! - it refuses the catalog of the generation it runs with (`ELECTRIC_CIRCUITS_QUERY_GENERATION`),
//!   named either by the path or by the catalog's `StoreBound`, however long it has been quiet;
//! - it refuses a per-source catalog (`circuits/v1/source-…`): those belong to the sources
//!   supervisor (circuitsd), which retires its own;
//! - it refuses a catalog any of whose events records a time within the last 7 days, or that
//!   records no time at all — the store keeps no last-append time, so the catalog's own events are
//!   the only witness that no engine is writing it — and a catalog whose tail moves while the
//!   command runs;
//! - it refuses a catalog in another store generation or stack namespace than the engine's own;
//! - it refuses a catalog naming any stream outside the catalog's own prefix;
//! - without `--confirm` it deletes nothing and prints what it would delete.
//!
//! Every run first prints the catalog's `StoreBound` and last `Offset` checkpoint events
//! (`evidence <event>` lines): the record of the generation that a dry run's log keeps once the
//! catalog is gone.
//!
//! It resolves the store connection from the engine's own environment, so in a deployment it runs
//! as a one-off task from the engine's task definition with only the command overridden. The
//! Indexed task starts the engine through `/bin/sh -ec <script>`, which a command override keeps,
//! so the override is one shell string:
//! `["exec /usr/local/bin/electric-circuits-engine retire-generation <catalog stream path>"]`
//! (append ` --confirm` for the deleting run).
//!
//! Deletion uses the engine's own retirement (close, then delete; a 404 or 410 counts as done), so a
//! re-run after a partial failure picks up where the last one stopped. The catalog goes last, and
//! only once every stream it names is gone.
//!
//! | exit | meaning |
//! |---|---|
//! | `0` | a dry run printed its plan, or every named stream and the catalog were deleted |
//! | `1` | storage failed, or some deletes failed and the catalog was kept for a re-run |
//! | `3` | partial: the named streams were deleted, the catalog was kept because it moved; find the writer before any re-run |
//! | `78` | refused: a bad argument or path, or one of the guards above |

use std::io::Write;

use anyhow::{Context, Result, bail};

use crate::config::Config;
use crate::ds::DsClient;
use crate::engine::{CATALOG_STREAM, fold_catalog_at};
use crate::store_identity::{StreamScope, validate_identifier};

/// The command's name on the engine binary's command line.
pub const COMMAND: &str = "retire-generation";

/// A catalog with an event this recent (seconds) may still have an engine writing it.
pub const QUIET_SECS: u64 = 7 * 24 * 60 * 60;

/// Exit code for a refusal: the request is wrong, and running it again unchanged will not help.
pub const EXIT_REFUSED: i32 = crate::pg::EXIT_CONFIG;

/// Exit code for a storage failure or failed deletes; a re-run continues it.
pub const EXIT_FAILED: i32 = 1;

/// Exit code when every named stream was deleted but the catalog moved before its own delete and
/// was kept. Not retryable: something appended to the catalog, and a re-run once that writer goes
/// quiet could delete a live generation's catalog. Find the writer first.
pub const EXIT_CATALOG_MOVED: i32 = 3;

/// Upper bound on the walk past the last recorded change-log segment. A rotation records each new
/// segment before the next one can begin, so more than one unrecorded successor is already a
/// broken log; the bound only stops a pathological store from looping the command.
const MAX_UNRECORDED_SEGMENTS: u32 = 1_000;

/// Why the command will not run. Each one is a decision for the operator, not a transient failure.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Refusal {
    #[error("usage: retire-generation <catalog stream path> [--confirm] ({0})")]
    Usage(String),
    #[error(
        "'{0}' is not a catalog stream path; expected \
         circuits/v1/<namespace>/stores/<store generation>/queries/<query generation>/meta/catalog"
    )]
    NotACatalogPath(String),
    #[error("'{0}' is a per-source catalog; the sources supervisor retires its own streams")]
    PerSource(String),
    #[error(
        "'{path}' is in store generation {path_generation}, but this engine is configured for store generation {configured}"
    )]
    OtherStore { path: String, path_generation: String, configured: String },
    #[error(
        "'{path}' is in namespace '{path_namespace}', but this engine is configured for namespace '{configured}' \
         (ELECTRIC_CIRCUITS_DS_NAMESPACE)"
    )]
    OtherNamespace { path: String, path_namespace: String, configured: String },
    #[error(
        "the catalog belongs to query generation '{0}', the generation this engine runs with \
         (ELECTRIC_CIRCUITS_QUERY_GENERATION); a running engine's own catalog is never retired"
    )]
    OwnGeneration(String),
    #[error("there is no catalog stream at '{0}'")]
    NoCatalog(String),
    #[error(
        "the catalog's newest event is from unix second {newest_at}, {age_secs} s ago; a catalog appended to in the last \
         7 days may still have an engine writing it"
    )]
    RecentlyAppended { newest_at: u64, age_secs: u64 },
    #[error("no event in the catalog records a time, so nothing shows it has been quiet for 7 days")]
    UnknownAge,
    #[error("the catalog names stream '{0}', which is outside the catalog's own prefix")]
    OutsidePrefix(String),
    #[error("the catalog's tail moved from {before:?} to {after:?} while this command ran; an engine is writing it")]
    CatalogMoved { before: Option<String>, after: Option<String> },
}

/// Is this error (or anything it was contextualised from) a [`Refusal`]?
pub fn refusal(e: &anyhow::Error) -> Option<&Refusal> {
    e.chain().find_map(|c| c.downcast_ref::<Refusal>())
}

/// The command line after the command name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Args {
    pub catalog: CatalogPath,
    pub confirm: bool,
}

/// Parse `<catalog stream path> [--confirm]`, in either order.
pub fn parse_args(args: &[String]) -> Result<Args, Refusal> {
    let mut confirm = false;
    let mut path = None;
    for arg in args {
        match arg.as_str() {
            "--confirm" if !confirm => confirm = true,
            "--confirm" => return Err(Refusal::Usage("--confirm given twice".into())),
            flag if flag.starts_with('-') => return Err(Refusal::Usage(format!("unknown option '{flag}'"))),
            _ if path.is_some() => return Err(Refusal::Usage("more than one catalog path".into())),
            _ => path = Some(arg.as_str()),
        }
    }
    let path = path.ok_or_else(|| Refusal::Usage("missing the catalog stream path".into()))?;
    Ok(Args { catalog: CatalogPath::parse(path)?, confirm })
}

/// The exact physical path of one engine catalog, split into the scope that qualifies it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogPath {
    physical: String,
    stack_namespace: String,
    store_generation: String,
    query_generation: String,
}

impl CatalogPath {
    /// Accept only `circuits/v1/<namespace>/stores/<generation>/queries/<query generation>/meta/catalog`,
    /// and refuse a per-source namespace.
    pub fn parse(path: &str) -> Result<Self, Refusal> {
        let not_a_catalog = || Refusal::NotACatalogPath(path.to_string());
        let parts: Vec<&str> = path.split('/').collect();
        let ["circuits", "v1", namespace, "stores", generation, "queries", query, "meta", "catalog"] = parts[..] else {
            return Err(not_a_catalog());
        };
        if validate_identifier("stack_namespace", namespace).is_err()
            || validate_identifier("query_generation", query).is_err()
            || generation.is_empty()
        {
            return Err(not_a_catalog());
        }
        if namespace.starts_with(crate::sources::SOURCE_NAMESPACE_PREFIX) {
            return Err(Refusal::PerSource(path.to_string()));
        }
        Ok(Self {
            physical: path.to_string(),
            stack_namespace: namespace.to_string(),
            store_generation: generation.to_string(),
            query_generation: query.to_string(),
        })
    }

    pub fn as_str(&self) -> &str {
        &self.physical
    }

    /// The query generation the path names.
    pub fn query_generation(&self) -> &str {
        &self.query_generation
    }

    /// Everything this generation owns lives under this prefix (it ends with `/`).
    pub fn prefix(&self) -> &str {
        self.physical.strip_suffix(CATALOG_STREAM).expect("parse checked the suffix")
    }

    /// The scope that qualifies this catalog's logical paths in the configured store. Refused when
    /// the path belongs to another store generation or another stack namespace than the ones the
    /// engine is configured for: a task only retires generations of its own stack.
    pub fn scope(&self, configured: &StreamScope) -> Result<StreamScope> {
        let store = &configured.store;
        if store.store_generation != self.store_generation {
            bail!(Refusal::OtherStore {
                path: self.physical.clone(),
                path_generation: self.store_generation.clone(),
                configured: store.store_generation.clone(),
            });
        }
        if configured.stack_namespace != self.stack_namespace {
            bail!(Refusal::OtherNamespace {
                path: self.physical.clone(),
                path_namespace: self.stack_namespace.clone(),
                configured: configured.stack_namespace.clone(),
            });
        }
        let scope = StreamScope::new(self.stack_namespace.clone(), store.clone(), self.query_generation.clone())?;
        // The scope must reproduce the path byte for byte, or the deletes would land elsewhere.
        if scope.qualify(CATALOG_STREAM)? != self.physical {
            bail!(Refusal::NotACatalogPath(self.physical.clone()));
        }
        Ok(scope)
    }
}

/// What a confirmed run would delete, checked against every guard.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Plan {
    /// The catalog's physical path; deleted last.
    pub catalog: String,
    /// Every stream to delete before the catalog: `(logical, physical)`, sorted by logical path.
    pub streams: Vec<(String, String)>,
    /// The newest wall-clock second any catalog event records.
    pub newest_at: u64,
    /// The catalog's `StoreBound` and last checkpoint events, kept as evidence.
    pub evidence: Vec<serde_json::Value>,
    /// The catalog's tail offset when it was read; the catalog is deleted only if it is unchanged.
    tail: Option<String>,
}

/// The result of a confirmed run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Retired {
    /// Physical paths deleted (or already gone), in order.
    pub deleted: Vec<String>,
    /// Physical paths whose delete failed, with the error. Non-empty means the catalog was kept.
    pub failed: Vec<(String, String)>,
    /// Whether the catalog itself was deleted.
    pub catalog_deleted: bool,
    /// `Some((before, after))` when every stream was deleted but the catalog's tail had moved
    /// since it was read, so the catalog was kept.
    pub catalog_moved: Option<(Option<String>, Option<String>)>,
}

/// Read the catalog `ds` is scoped to and decide what a confirmed run would delete. Deletes
/// nothing. `own_generation` is the query generation this process is configured with; `now` is
/// unix seconds.
pub async fn plan(ds: &DsClient, catalog: &CatalogPath, own_generation: &str, now: u64) -> Result<Plan> {
    if ds.physical_path(CATALOG_STREAM)? != catalog.as_str() {
        bail!("the Durable Streams client is not scoped to {}", catalog.as_str());
    }
    if catalog.query_generation() == own_generation {
        bail!(Refusal::OwnGeneration(own_generation.to_string()));
    }
    let Some(head) = ds.head(CATALOG_STREAM).await.context("checking the catalog stream")? else {
        bail!(Refusal::NoCatalog(catalog.as_str().to_string()));
    };
    let fold = fold_catalog_at(ds).await.context("reading the catalog")?;
    if let Some(bound) = fold.store_bound()
        && bound.query_generation == own_generation
    {
        bail!(Refusal::OwnGeneration(bound.query_generation.clone()));
    }
    let newest_at = fold.newest_at().ok_or(Refusal::UnknownAge)?;
    let age_secs = now.saturating_sub(newest_at);
    if age_secs < QUIET_SECS {
        bail!(Refusal::RecentlyAppended { newest_at, age_secs });
    }

    let mut logical = fold.named_streams();
    // A rotation creates segment n+1 before it records it, so a crash can leave successors the
    // catalog never names. They are this generation's too.
    let last_segment = logical
        .iter()
        .filter_map(|p| p.strip_prefix(crate::changelog::CHANGES_PREFIX)?.strip_prefix('/')?.parse::<u32>().ok())
        .max();
    if let Some(last) = last_segment {
        for n in last.saturating_add(1)..last.saturating_add(1).saturating_add(MAX_UNRECORDED_SEGMENTS) {
            let path = crate::changelog::segment_path(n);
            if ds.head(&path).await.with_context(|| format!("checking {path}"))?.is_none() {
                break;
            }
            logical.insert(path);
        }
    }

    let mut streams = Vec::with_capacity(logical.len());
    for path in logical {
        match ds.physical_path(&path) {
            Ok(physical) if physical.starts_with(catalog.prefix()) && physical != catalog.as_str() => {
                streams.push((path, physical))
            }
            _ => bail!(Refusal::OutsidePrefix(path)),
        }
    }

    ensure_tail(ds, &head.next_offset).await?;
    Ok(Plan {
        catalog: catalog.as_str().to_string(),
        streams,
        newest_at,
        evidence: fold.evidence_events()?,
        tail: head.next_offset,
    })
}

/// Delete every stream in `plan`, then the catalog. A stream whose delete fails is reported and
/// the rest still go; the catalog is then kept, so a re-run finds what is left.
pub async fn execute(ds: &DsClient, plan: &Plan, out: &mut dyn Write) -> Result<Retired> {
    let mut retired = Retired { deleted: Vec::new(), failed: Vec::new(), catalog_deleted: false, catalog_moved: None };
    for (n, (logical, physical)) in plan.streams.iter().enumerate() {
        match ds.retire_stream(logical).await {
            Ok(()) => {
                writeln!(out, "deleted {physical}")?;
                retired.deleted.push(physical.clone());
            }
            Err(e) => {
                writeln!(out, "failed {physical}: {e:#}")?;
                retired.failed.push((physical.clone(), format!("{e:#}")));
            }
        }
        if (n + 1) % 100 == 0 {
            tracing::info!(done = n + 1, total = plan.streams.len(), "retire-generation: progress");
        }
    }
    if !retired.failed.is_empty() {
        return Ok(retired);
    }
    // The streams are already gone, so a moved tail here is not a refusal: keep the catalog, which
    // is what an engine appending to it would need, and report a partial retirement.
    let after = ds.head(CATALOG_STREAM).await.context("re-checking the catalog stream")?.map(|head| head.next_offset);
    if after.as_ref() != Some(&plan.tail) {
        retired.catalog_moved = Some((plan.tail.clone(), after.flatten()));
        return Ok(retired);
    }
    ds.retire_stream(CATALOG_STREAM).await.with_context(|| format!("deleting {}", plan.catalog))?;
    writeln!(out, "deleted {}", plan.catalog)?;
    retired.catalog_deleted = true;
    Ok(retired)
}

/// Refuse unless the catalog's tail is still `expected`: an engine appending to it is not stopped.
async fn ensure_tail(ds: &DsClient, expected: &Option<String>) -> Result<()> {
    let after = ds.head(CATALOG_STREAM).await.context("re-checking the catalog stream")?;
    let after = after.map(|head| head.next_offset);
    if after.as_ref() != Some(expected) {
        bail!(Refusal::CatalogMoved { before: expected.clone(), after: after.flatten() });
    }
    Ok(())
}

/// Plan, print, and with `confirm` delete. Returns the exit code ([`EXIT_REFUSED`] for a refusal).
pub async fn run(ds: &DsClient, args: &Args, own_generation: &str, now: u64, out: &mut dyn Write) -> i32 {
    match run_inner(ds, args, own_generation, now, out).await {
        Ok(code) => code,
        Err(e) => report_error(&e),
    }
}

async fn run_inner(ds: &DsClient, args: &Args, own_generation: &str, now: u64, out: &mut dyn Write) -> Result<i32> {
    let plan = plan(ds, &args.catalog, own_generation, now).await?;
    let age_days = now.saturating_sub(plan.newest_at) / 86_400;
    writeln!(out, "catalog {} (newest event {age_days} days ago)", plan.catalog)?;
    if plan.evidence.is_empty() {
        writeln!(out, "evidence: the catalog has no storeBound and no checkpoint event")?;
    }
    for event in &plan.evidence {
        writeln!(out, "evidence {event}")?;
    }
    if !args.confirm {
        for (_, physical) in &plan.streams {
            writeln!(out, "would delete {physical}")?;
        }
        writeln!(out, "would delete {}", plan.catalog)?;
        writeln!(
            out,
            "dry run: {} streams and the catalog would be deleted; nothing was. Re-run with --confirm to delete them.",
            plan.streams.len()
        )?;
        return Ok(0);
    }
    let retired = execute(ds, &plan, out).await?;
    if retired.catalog_deleted {
        writeln!(out, "retired {} streams and the catalog", retired.deleted.len())?;
        Ok(0)
    } else if let Some((before, after)) = &retired.catalog_moved {
        writeln!(
            out,
            "{} streams deleted; the catalog was kept because it moved (tail {before:?} -> {after:?}) while they were \
             deleted, so an engine may be writing it. Find the writer before any re-run.",
            retired.deleted.len()
        )?;
        Ok(EXIT_CATALOG_MOVED)
    } else {
        writeln!(
            out,
            "{} streams deleted, {} failed; the catalog was kept so a re-run can finish",
            retired.deleted.len(),
            retired.failed.len()
        )?;
        Ok(EXIT_FAILED)
    }
}

/// The query generation the engine's configuration runs with: `ELECTRIC_CIRCUITS_QUERY_GENERATION`,
/// or the fixed in-process test scope's.
pub fn own_generation(config: &Config) -> Option<String> {
    if let Some(connection) = &config.ds_connection {
        return Some(connection.scope.query_generation.clone());
    }
    config.ds_in_process_test_url.as_ref().map(|_| StreamScope::in_process_test_scope().query_generation)
}

/// Connect to the store the engine's configuration names, scoped to `catalog`'s generation.
pub async fn connect(config: &Config, catalog: &CatalogPath) -> Result<DsClient> {
    if let Some(connection) = &config.ds_connection {
        let scope = catalog.scope(&connection.scope)?;
        let connection = crate::ds::DsConnectionConfig::new(
            connection.base_url.clone(),
            connection.ca_bundle_path.clone(),
            connection.client_certificate_path.clone(),
            connection.client_key_path.clone(),
            scope,
        )?;
        return DsClient::connect(connection).await;
    }
    #[cfg(feature = "test-support")]
    {
        if let Some(base_url) = &config.ds_in_process_test_url {
            let scope = catalog.scope(&StreamScope::in_process_test_scope())?;
            return Ok(DsClient::new_for_in_process_test_with_scope(base_url.clone(), scope));
        }
    }
    bail!("ELECTRIC_CIRCUITS_DS_URL and its store identity configuration are required")
}

/// The whole command after argument parsing: connect, run, and map the outcome to an exit code.
pub async fn command(config: &Config, args: &Args, out: &mut dyn Write) -> i32 {
    let Some(own_generation) = own_generation(config) else {
        return report_error(&anyhow::anyhow!(
            "ELECTRIC_CIRCUITS_DS_URL and its store identity configuration are required"
        ));
    };
    match connect(config, &args.catalog).await {
        Ok(ds) => run(&ds, args, &own_generation, crate::changelog::now_secs(), out).await,
        Err(e) => report_error(&e),
    }
}

fn report_error(e: &anyhow::Error) -> i32 {
    if let Some(refusal) = refusal(e) {
        eprintln!("{COMMAND} refused: {refusal}");
        EXIT_REFUSED
    } else {
        eprintln!("{COMMAND} failed: {e:#}");
        EXIT_FAILED
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const V1: &str = "circuits/v1/indexed-prod/stores/ff8b5fa6-e786-4994-8da0-f14e9e79f318/queries/dashboard-conversation-v1/meta/catalog";

    fn args(list: &[&str]) -> Result<Args, Refusal> {
        parse_args(&list.iter().map(|s| s.to_string()).collect::<Vec<_>>())
    }

    #[test]
    fn parses_a_catalog_path_with_or_without_confirm() {
        let dry = args(&[V1]).unwrap();
        assert!(!dry.confirm);
        assert_eq!(dry.catalog.as_str(), V1);
        assert_eq!(
            dry.catalog.prefix(),
            "circuits/v1/indexed-prod/stores/ff8b5fa6-e786-4994-8da0-f14e9e79f318/queries/dashboard-conversation-v1/"
        );
        assert!(args(&[V1, "--confirm"]).unwrap().confirm);
        assert!(args(&["--confirm", V1]).unwrap().confirm);
    }

    #[test]
    fn refuses_bad_command_lines() {
        assert!(matches!(args(&[]), Err(Refusal::Usage(_))));
        assert!(matches!(args(&[V1, V1]), Err(Refusal::Usage(_))));
        assert!(matches!(args(&[V1, "--force"]), Err(Refusal::Usage(_))));
        assert!(matches!(args(&[V1, "--confirm", "--confirm"]), Err(Refusal::Usage(_))));
    }

    #[test]
    fn refuses_anything_but_one_exact_catalog_path() {
        for path in [
            "",
            "meta/catalog",
            "/circuits/v1/indexed-prod/stores/g/queries/dashboard-conversation-v1/meta/catalog",
            "circuits/v1/indexed-prod/stores/g/queries/dashboard-conversation-v1/",
            "circuits/v1/indexed-prod/stores/g/queries/dashboard-conversation-v1/shape/s1",
            "circuits/v1/indexed-prod/stores/g/queries/dashboard-conversation-v1/meta/catalog/",
            "circuits/v1/indexed-prod/stores//queries/dashboard-conversation-v1/meta/catalog",
            "circuits/v1/../stores/g/queries/dashboard-conversation-v1/meta/catalog",
            "circuits/v1/indexed-prod/stores/g/queries/*/meta/catalog",
            "circuits/v2/indexed-prod/stores/g/queries/dashboard-conversation-v1/meta/catalog",
        ] {
            assert!(matches!(CatalogPath::parse(path), Err(Refusal::NotACatalogPath(_))), "{path:?} was accepted");
        }
    }

    #[test]
    fn refuses_a_per_source_catalog() {
        let source = format!(
            "circuits/v1/source-{h}/stores/ff8b5fa6-e786-4994-8da0-f14e9e79f318/queries/query-{h}/meta/catalog",
            h = "0123456789abcdef0123456789abcdef01234567"
        );
        assert_eq!(CatalogPath::parse(&source), Err(Refusal::PerSource(source.clone())));
    }

    #[test]
    fn refuses_a_catalog_in_another_store_generation() {
        let mut configured = StreamScope::in_process_test_scope();
        configured.stack_namespace = "indexed-prod".into();
        configured.store.store_generation = "00000000-0000-4000-8000-000000000000".into();
        let e = CatalogPath::parse(V1).unwrap().scope(&configured).unwrap_err();
        assert!(matches!(refusal(&e), Some(Refusal::OtherStore { .. })), "{e:#}");
    }

    #[test]
    fn scopes_only_a_catalog_in_its_own_namespace() {
        let mut configured = StreamScope::in_process_test_scope();
        configured.stack_namespace = "indexed-prod".into();
        let scope = CatalogPath::parse(V1).unwrap().scope(&configured).unwrap();
        assert_eq!(scope.qualify(CATALOG_STREAM).unwrap(), V1);
        configured.stack_namespace = "indexed-dev".into();
        let e = CatalogPath::parse(V1).unwrap().scope(&configured).unwrap_err();
        assert!(matches!(refusal(&e), Some(Refusal::OtherNamespace { .. })), "{e:#}");
    }
}
