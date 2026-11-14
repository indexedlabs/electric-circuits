//! The shared membership kernel — the mechanics of "rows enter/leave a shape because a
//! *related* table changed", used by the subquery registry (the one membership
//! implementation; row data lives in Postgres):
//!
//!  * **candidate-row resolution** from pooled Postgres ([`query_rows_by_col`],
//!    [`query_rows_by_values`], [`query_rows_all`]) — parallel across the flip-worker pool, bounded by
//!    `ELECTRIC_DB_POOL_SIZE`;
//!  * the **latest-row-per-pk fold** used before an absolute membership evaluation
//!    ([`latest_rows_by_pk`]);
//!  * [`fold_refcount_flips`], the reference refcount fold the flip-semantics regression test
//!    pins `SubqueryNode::reconcile_row` against.
//!
//! Emission itself is already shared: both paths hand their decided `(Row, ±1)` sets to
//! [`super::output::translate_output`], whose per-pk grouping (net positive → `upsert`, else
//! `delete`) *is* the absolute-emission invariant (see `docs/ARCHITECTURE.md` §6).

use super::*;

use crate::subquery::{Flip, FlipDir};

/// Fold a batch of weighted group contributions into a refcounted group map and report the
/// values whose membership **flipped** (refcount crossed zero). `Enter` = the value's refcount
/// went `≤0 → >0`, `Leave` = `>0 → ≤0`; values whose refcount changes without crossing zero
/// produce no flip. Callers must feed exactly-once deltas (the sequencer's `(lsn, seq)`
/// highwater guarantees this) — refcounts, unlike the registry's pk-sets, are not idempotent.
#[allow(dead_code)] // retained as the reference implementation the flip-semantics regression
// test pins `SubqueryNode::reconcile_row` against (the two must agree on flips forever).
pub(crate) fn fold_refcount_flips(
    groups: &mut HashMap<Value, i64>,
    contributions: impl IntoIterator<Item = (Value, ZWeight)>,
) -> Vec<Flip> {
    let mut flips = Vec::new();
    for (v, w) in contributions {
        let e = groups.entry(v.clone()).or_insert(0);
        let was = *e;
        *e += w;
        let now = *e;
        if now <= 0 {
            groups.remove(&v);
        }
        if was <= 0 && now > 0 {
            flips.push(Flip { value: v, dir: FlipDir::Enter });
        } else if was > 0 && now <= 0 {
            flips.push(Flip { value: v, dir: FlipDir::Leave });
        }
    }
    flips
}

/// Query candidate rows where `col = value` from Postgres on a pooled connection — row data
/// lives in Postgres, never engine-side. Concurrency is bounded by the shared pool
/// (`ELECTRIC_DB_POOL_SIZE`); reads see PG-current state, which converges under absolute
/// per-pk emission exactly as deferred flip propagation always has.
///
/// The read's [`SnapshotGate`](crate::pg::SnapshotGate) is returned WITH the rows: it is the
/// only thing that says *how old* these rows are, and the emission tail needs exactly that to
/// tell a candidate that is still current from one a live commit has already superseded (see
/// `subquery::EmissionSource::QueryBack`). Discarding it — as this used to — is what let an
/// older read become a shape stream's last word.
pub(crate) async fn query_rows_by_col(
    pg_pool: &Option<crate::pg::Pool>,
    ts: &TableSchema,
    col: usize,
    value: &Value,
) -> Result<(Vec<Row>, crate::pg::SnapshotGate)> {
    let pool = pg_pool.as_ref().context("membership query-back requires postgres")?;
    let mut client = pool.get().await?;
    let where_sql = value_eq_sql(&ts.columns[col].0, value, ts.pg_types.get(col).and_then(|o| o.as_deref()));
    // `collect`, deliberately: a query-back's RESULT is the candidate set — there is no stream to
    // append it to, and it is one key's worth of rows, not a table's. An engine-internal read: never
    // refused at the settle admission cap (a refusal would only become flip retries).
    let scope = crate::pg::SettleScope::internal(&ts.table);
    let (rows, fences) =
        crate::pg::backfill_where_reader(&mut client, ts, Some(where_sql), &scope).await?.collect().await?;
    Ok((rows, fences.gate))
}

/// [`query_rows_by_col`] for a SET of values: `<ts> WHERE <col> IN (<values>)` in one read, with
/// the read's snapshot gate. The coalesced form of a walk round's query-backs (see
/// `subquery::propagate_flips`): every dependent that a round's flips reach through the same table
/// and column shares this one read, however many flipped nodes and values reached it. One value
/// is exactly [`query_rows_by_col`].
pub(crate) async fn query_rows_by_values(
    pg_pool: &Option<crate::pg::Pool>,
    ts: &TableSchema,
    col: usize,
    values: &[Value],
) -> Result<(Vec<Row>, crate::pg::SnapshotGate)> {
    if let [value] = values {
        return query_rows_by_col(pg_pool, ts, col, value).await;
    }
    let pool = pg_pool.as_ref().context("membership query-back requires postgres")?;
    let mut client = pool.get().await?;
    let where_sql = values_in_sql(&ts.columns[col].0, values, ts.pg_types.get(col).and_then(|o| o.as_deref()));
    // `collect`, for the same reason as `query_rows_by_col`: the result is the candidate set.
    let scope = crate::pg::SettleScope::internal(&ts.table);
    let (rows, fences) =
        crate::pg::backfill_where_reader(&mut client, ts, Some(where_sql), &scope).await?.collect().await?;
    Ok((rows, fences.gate))
}

/// Query all rows of `ts` (full re-derive) from Postgres on a pooled connection, with the read's
/// snapshot gate (see [`query_rows_by_col`]).
pub(crate) async fn query_rows_all(
    pg_pool: &Option<crate::pg::Pool>,
    ts: &TableSchema,
) -> Result<(Vec<Row>, crate::pg::SnapshotGate)> {
    let pool = pg_pool.as_ref().context("membership query-back requires postgres")?;
    let mut client = pool.get().await?;
    // `collect`: a full re-derive's result IS the in-memory candidate set (see `query_rows_by_col`).
    let scope = crate::pg::SettleScope::internal(&ts.table);
    let (rows, fences) = crate::pg::backfill_where_reader(&mut client, ts, None, &scope).await?.collect().await?;
    Ok((rows, fences.gate))
}

/// Build a `WHERE col = value` fragment + params for a move query-back (the LIVE re-derive path).
/// Text is parameterized; other scalars are inlined (mirrors the SQL emitter). A text param is cast to
/// the column's native Postgres type (`$1::text::uuid`) when known — same as the backfill emitters, so
/// a uuid/timestamptz column doesn't hit tokio-postgres's `String -> uuid` refusal (which used to fail
/// this path SILENTLY, dropping live subquery move-ins) — else the `col::text = $1` fallback. NULL
/// never reaches here (handled by full re-derive).
pub(crate) fn value_eq_sql(col: &str, value: &Value, pg_type: Option<&str>) -> (String, Vec<String>) {
    let name = crate::pg::quote_ident(col);
    match value {
        Value::Null => (format!("{name} IS NULL"), Vec::new()),
        Value::Int(i) => (format!("{name} = {i}"), Vec::new()),
        Value::Float(f) => (format!("{name} = {}", f.into_inner()), Vec::new()),
        Value::Bool(b) => (format!("{name} = {}", if *b { "true" } else { "false" }), Vec::new()),
        Value::Text(s) => (crate::sql::text_param_cmp(&name, "=", 1, pg_type), vec![s.clone()]),
    }
}

/// `col IN (…)` over several non-NULL flipped values: the same per-value comparison as
/// [`value_eq_sql`] — text cast to the column's native type when known, else compared as text —
/// as one list, so a set of values costs one index probe per value in ONE statement instead of
/// one statement each. Text values are numbered params `$1..$n`; other scalars are inlined.
pub(crate) fn values_in_sql(col: &str, values: &[Value], pg_type: Option<&str>) -> (String, Vec<String>) {
    let name = crate::pg::quote_ident(col);
    let mut params = Vec::new();
    let mut inlined = Vec::new();
    for value in values {
        match value {
            // A NULL flip is a full re-derive, never a filtered read; `= NULL` would match nothing.
            Value::Null => {}
            Value::Int(i) => inlined.push(i.to_string()),
            Value::Float(f) => inlined.push(f.into_inner().to_string()),
            Value::Bool(b) => inlined.push(if *b { "true" } else { "false" }.to_string()),
            Value::Text(s) => params.push(s.clone()),
        }
    }
    let mut terms = Vec::new();
    if !inlined.is_empty() {
        terms.push(format!("{name} IN ({})", inlined.join(", ")));
    }
    if !params.is_empty() {
        let list = |cast: &str| (1..=params.len()).map(|n| format!("${n}{cast}")).collect::<Vec<_>>().join(", ");
        terms.push(match pg_type {
            Some(ty) => format!("{name} IN ({})", list(&format!("::text::{}", crate::pg::quote_ident(ty)))),
            None => format!("{name}::text IN ({})", list("")),
        });
    }
    match terms.len() {
        0 => ("FALSE".to_string(), params),
        1 => (terms.pop().unwrap_or_default(), params),
        _ => (format!("({})", terms.join(" OR ")), params),
    }
}

/// Fold a Z-set delta down to each touched pk's **latest** state: the `+1` row if present
/// (insert/update — the row still exists), else the `-1` row (delete). The `bool` is
/// "row still exists". This is the front half of every absolute membership evaluation: decide
/// per touched pk from its latest row, never from history.
pub(crate) fn latest_rows_by_pk(ts: &TableSchema, delta: &[Tup2<Row, ZWeight>]) -> Vec<(Row, bool)> {
    let mut by_pk: HashMap<String, (Row, bool)> = HashMap::new();
    for Tup2(row, w) in delta {
        let pk = ts.key_string(row).unwrap_or_default();
        if *w > 0 {
            by_pk.insert(pk, (row.clone(), true));
        } else {
            by_pk.entry(pk).or_insert_with(|| (row.clone(), false));
        }
    }
    by_pk.into_values().collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A coalesced query-back keeps each value's own comparison: integers inlined, text as numbered
    /// params cast to the column's native type (or compared as text when the type is unknown).
    #[test]
    fn values_in_sql_lists_each_value_with_its_own_comparison() {
        let ints = values_in_sql("record_id", &[Value::Int(1), Value::Int(2)], Some("int8"));
        assert_eq!(ints, ("\"record_id\" IN (1, 2)".to_string(), Vec::new()));
        let uuids = values_in_sql("record_id", &[Value::Text("a".into()), Value::Text("b".into())], Some("uuid"));
        assert_eq!(uuids.0, "\"record_id\" IN ($1::text::\"uuid\", $2::text::\"uuid\")");
        assert_eq!(uuids.1, vec!["a".to_string(), "b".to_string()]);
        let untyped = values_in_sql("c", &[Value::Text("a".into()), Value::Text("b".into())], None);
        assert_eq!(untyped.0, "\"c\"::text IN ($1, $2)");
        let mixed = values_in_sql("c", &[Value::Int(1), Value::Text("a".into())], Some("text"));
        assert_eq!(mixed.0, "(\"c\" IN (1) OR \"c\" IN ($1::text::\"text\"))");
    }
}
