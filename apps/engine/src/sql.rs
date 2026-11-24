//! Compile a shape's predicate to a parameterized SQL `WHERE` fragment, so backfill can read only the
//! rows a shape needs (`SELECT … WHERE <predicate>`) instead of the whole table. Mirrors the
//! TypeScript `predicateToSql` in `@electric-circuits/protocol` (and therefore the oracle's `WHERE`), so
//! the SQL filter agrees with the engine's `CompiledPredicate::matches` (three-valued NULL included).
//!
//! Numeric / boolean / null literals are inlined directly (they are typed Rust scalars — only digits,
//! `.`, `-`, `e`, `true`/`false`, `NULL` — so there is no injection surface); only **text** literals
//! are sent as bound parameters (`$1`, `$2`, …), which both escapes them and matches text columns
//! cleanly. The caller still applies `matches()` as the final authority, so the SQL only needs to be a
//! sound *superset* filter; mirroring the proven compiler keeps it exact.
//!
//! The JSON emitter ([`predicate_json_to_sql`]) prints two kinds of `OR` differently from the
//! TypeScript mirror — sibling `IN (subquery)` tests as one `IN` over a `UNION ALL`, and an `OR`
//! that tests the table's key with `IN` as one key union — so Postgres can drive the table from a
//! set of keys instead of scanning it. The rows are the same (see `JsonSql::or`, and
//! `conformance-sql-or-union.test.ts`, which runs both forms over generated predicates).

use std::collections::HashMap;

use crate::pg::quote_ident;
use crate::predicate::{CompiledPredicate, LeafOp, PredicateJson, SubqueryJson};
use crate::schema::TableSchema;
use crate::table_ref::TableRef;
use crate::value::Value;

/// Compare a text-bound param `$n` to column `name` (already quoted). When the column's native Postgres
/// type is known, cast the (text) param to it — `name = $n::text::uuid` — so the comparison stays
/// index-eligible (a plain `name::text = $n` defeats the btree index). `None` (library mode / unknown
/// column) falls back to the text-vs-text form, which is always correct for our coarse-Text columns.
/// Shared with the subquery live move query-back (`subquery::value_eq_sql`) so every text/uuid param
/// bind uses the identical form.
pub(crate) fn text_param_cmp(name: &str, op: &str, n: usize, pg_type: Option<&str>) -> String {
    match pg_type {
        Some(ty) => format!("{name} {op} ${n}::text::{}", quote_ident(ty)),
        None => format!("{name}::text {op} ${n}"),
    }
}

/// Build a `WHERE` fragment + ordered text parameters for `pred`. Returns `None` for `MatchAll`
/// (no `WHERE` at all). Placeholders are numbered `$1..$n` in the order text literals appear.
pub fn predicate_to_sql(pred: &CompiledPredicate, ts: &TableSchema) -> Option<(String, Vec<String>)> {
    if matches!(pred, CompiledPredicate::MatchAll) {
        return None;
    }
    let mut params: Vec<String> = Vec::new();
    let text = build(pred, ts, &mut params);
    Some((text, params))
}

fn op_sql(op: LeafOp) -> &'static str {
    match op {
        LeafOp::Eq => "=",
        LeafOp::Neq => "<>",
        LeafOp::Lt => "<",
        LeafOp::Lte => "<=",
        LeafOp::Gt => ">",
        LeafOp::Gte => ">=",
        LeafOp::Like => "LIKE",
    }
}

fn build(p: &CompiledPredicate, ts: &TableSchema, params: &mut Vec<String>) -> String {
    match p {
        // Only reachable at the top level (compile never nests MatchAll); treat as the identity.
        CompiledPredicate::MatchAll => "TRUE".to_string(),
        CompiledPredicate::Cmp { col, op, value } => {
            let name = quote_ident(&ts.columns[*col].0);
            let o = op_sql(*op);
            match value {
                Value::Null => format!("{name} {o} NULL"),
                Value::Int(i) => format!("{name} {o} {i}"),
                Value::Float(f) => format!("{name} {o} {}", f.into_inner()),
                Value::Bool(b) => format!("{name} {o} {}", if *b { "true" } else { "false" }),
                Value::Text(s) => {
                    // Bind text as a `$n` param, cast to the column's native type when known. Our schema
                    // coarsens uuid/timestamptz/… to `Text`, so the real Postgres column may be uuid;
                    // binding a Rust `String` against a uuid param is refused by tokio-postgres
                    // (`cannot convert String -> uuid`). `$n::text::uuid` sends the param as text and
                    // parses it to the native type (index-eligible); unknown type → `col::text` fallback.
                    //
                    // An ORDERING comparison on a collatable text column carries an explicit
                    // `COLLATE "C"`: the engine's own evaluator compares Rust strings, i.e. by code
                    // point, so a backfill (or a subset page's keyset cursor) run under a different
                    // database collation would select a different set of rows than the live path
                    // matches. Equality needs no collation — it is byte equality under every
                    // deterministic collation — so `=`/`<>` keep the plain, index-eligible form.
                    params.push(s.clone());
                    let ordering = matches!(op, LeafOp::Lt | LeafOp::Lte | LeafOp::Gt | LeafOp::Gte);
                    let lhs = if ordering && ts.is_collatable_text(*col) {
                        std::borrow::Cow::Owned(format!("{name} collate \"C\""))
                    } else {
                        std::borrow::Cow::Borrowed(name.as_str())
                    };
                    text_param_cmp(&lhs, o, params.len(), ts.pg_types.get(*col).and_then(|o| o.as_deref()))
                }
            }
        }
        CompiledPredicate::IsNull { col, is_null } => {
            let name = quote_ident(&ts.columns[*col].0);
            format!("{name} IS {}NULL", if *is_null { "" } else { "NOT " })
        }
        CompiledPredicate::And(v) => {
            if v.is_empty() {
                "TRUE".to_string()
            } else {
                let parts: Vec<String> = v.iter().map(|p| build(p, ts, params)).collect();
                format!("({})", parts.join(" AND "))
            }
        }
        CompiledPredicate::Or(v) => {
            if v.is_empty() {
                "FALSE".to_string()
            } else {
                let parts: Vec<String> = v.iter().map(|p| build(p, ts, params)).collect();
                format!("({})", parts.join(" OR "))
            }
        }
        CompiledPredicate::Not(b) => format!("(NOT {})", build(b, ts, params)),
        // Subquery SQL is emitted from the raw `PredicateJson` (which carries the inner table /
        // projection / where) via `predicate_json_to_sql`; the compiled form drops those details.
        CompiledPredicate::InSubquery { .. } => {
            unreachable!("subquery SQL must be built from PredicateJson, not the compiled predicate")
        }
    }
}

/// Build a `WHERE` fragment + text parameters directly from a **JSON** predicate, supporting
/// `IN (SELECT …)` subqueries (which the compiled form can't reconstruct). Mirrors the TS
/// `predicateToSql` shape; literal handling matches `predicate_to_sql` above (numbers/bools/null
/// inlined, text bound as `$n`). `start_param` is the next placeholder index (1-based). Returns
/// `None` for an empty/match-all predicate is *not* applicable here — pass a real predicate.
///
/// Two rewrites of an `OR` change the SQL text and never the rows it selects (see
/// [`JsonSql::or`]): sibling `IN (subquery)` tests on one column become one `IN` over a
/// `UNION ALL`, and an `OR` that tests the table's key with `IN` gathers its other IN-constrained
/// branches into that union. Collatable projections are left alone because schemas carry no collation.
/// The predicate itself — and with it the shape's identity — is untouched.
pub fn predicate_json_to_sql(
    pred: &PredicateJson,
    start_param: usize,
    schemas: &HashMap<TableRef, TableSchema>,
    table: &TableRef,
) -> (String, Vec<String>) {
    let mut params: Vec<String> = Vec::new();
    let text = JsonSql { start: start_param, params: &mut params, schemas }.build(pred, table, Use::Filter);
    (text, params)
}

/// A branch that is a plain (not negated) `IN (subquery)`: its column and its subquery.
fn plain_in(branch: &PredicateJson) -> Option<(&str, &SubqueryJson)> {
    match branch {
        PredicateJson::In { col, subquery, negated: false } => Some((col.as_str(), subquery)),
        _ => None,
    }
}

/// A positive membership constraint. `where_sql` lowers a literal IN list to an OR of equality
/// leaves on one column; recognize that existing representation without extending the predicate.
/// An AND is constrained when any conjunct is constrained (including through nested ANDs).
fn has_positive_in(branch: &PredicateJson) -> bool {
    match branch {
        PredicateJson::In { negated: false, .. } => true,
        PredicateJson::And { and } => and.iter().any(has_positive_in),
        PredicateJson::Or { or } => {
            let Some(PredicateJson::Leaf { col, op: LeafOp::Eq, .. }) = or.first() else { return false };
            or.iter().all(|p| matches!(p, PredicateJson::Leaf { col: c, op: LeafOp::Eq, .. } if c == col))
        }
        _ => false,
    }
}

/// Without column collation metadata, matching names alone cannot establish compatibility for
/// collatable projections: their UNION can change the collation used by the outer IN. Only known
/// non-collatable built-in types qualify; text, domains, and unclassified types keep the old SQL.
fn union_type<'a>(schema: &'a TableSchema, column: &str) -> Option<&'a str> {
    schema.pg_type_of(column).filter(|ty| {
        matches!(
            *ty,
            "bool"
                | "int2"
                | "int4"
                | "int8"
                | "float4"
                | "float8"
                | "numeric"
                | "uuid"
                | "date"
                | "time"
                | "timetz"
                | "timestamp"
                | "timestamptz"
                | "interval"
                | "bytea"
        )
    })
}

/// How the value of the expression being written is used.
///
/// The key-union rewrite ([`JsonSql::or`], rule 2) turns a branch that is UNKNOWN for a row into
/// FALSE for that row. That is invisible wherever a row is kept only when the expression is TRUE —
/// a `WHERE`, and every `AND`/`OR` operand of one — and visible under a `NOT`, where NOT UNKNOWN
/// drops the row and NOT FALSE keeps it. So that rewrite is made only in a [`Use::Filter`].
#[derive(Clone, Copy, PartialEq, Eq)]
enum Use {
    /// The expression keeps a row only when TRUE: FALSE and UNKNOWN are the same outcome.
    Filter,
    /// The expression is under a `NOT`, which tells FALSE from UNKNOWN.
    Negated,
    /// A key OR with a broad branch must retain main's SQL, including all its descendants.
    Verbatim,
}

/// The JSON emitter's state: where placeholders start, the text params bound so far, and every
/// table's compiled schema (for native types and keys).
struct JsonSql<'a> {
    start: usize,
    params: &'a mut Vec<String>,
    schemas: &'a HashMap<TableRef, TableSchema>,
}

impl JsonSql<'_> {
    /// `table` is the table the current predicate's columns belong to; it switches to the inner table
    /// when descending into a subquery, so each leaf's param is cast to the right column's native type.
    fn build(&mut self, p: &PredicateJson, table: &TableRef, used: Use) -> String {
        let schemas = self.schemas;
        let pg_type = |col: &str| schemas.get(table).and_then(|ts| ts.pg_type_of(col)).map(str::to_string);
        // Same rule as the compiled emitter: an ORDERING comparison on a collatable text column is
        // spelled `COLLATE "C"` so Postgres agrees with the engine's code-point comparison.
        let lhs = |col: &str, op: LeafOp| {
            let name = quote_ident(col);
            let ordering = matches!(op, LeafOp::Lt | LeafOp::Lte | LeafOp::Gt | LeafOp::Gte);
            let collatable =
                schemas.get(table).and_then(|ts| ts.index.get(col).map(|&i| ts.is_collatable_text(i))).unwrap_or(false);
            if ordering && collatable { format!("{name} collate \"C\"") } else { name }
        };
        match p {
            PredicateJson::Leaf { col, op, value } => {
                let name = quote_ident(col);
                let o = op_sql(*op);
                match value {
                    serde_json::Value::Null => format!("{name} {o} NULL"),
                    serde_json::Value::Bool(b) => format!("{name} {o} {}", if *b { "true" } else { "false" }),
                    serde_json::Value::Number(n) => format!("{name} {o} {n}"),
                    serde_json::Value::String(s) => {
                        // Bind as text, cast to the column's native type when known (see `text_param_cmp`).
                        self.params.push(s.clone());
                        text_param_cmp(&lhs(col, *op), o, self.start + self.params.len() - 1, pg_type(col).as_deref())
                    }
                    other => {
                        // Arrays/objects are not valid leaf literals; stringify defensively as a param.
                        self.params.push(other.to_string());
                        text_param_cmp(&lhs(col, *op), o, self.start + self.params.len() - 1, pg_type(col).as_deref())
                    }
                }
            }
            PredicateJson::IsNull { col, is_null } => {
                let name = quote_ident(col);
                format!("{name} IS {}NULL", if *is_null { "" } else { "NOT " })
            }
            PredicateJson::And { and } => {
                if and.is_empty() {
                    "TRUE".to_string()
                } else {
                    let parts: Vec<String> = and.iter().map(|p| self.build(p, table, used)).collect();
                    format!("({})", parts.join(" AND "))
                }
            }
            PredicateJson::Or { or } => {
                if or.is_empty() {
                    "FALSE".to_string()
                } else {
                    self.or(or, table, used)
                }
            }
            PredicateJson::Not { not } => {
                let inner_use = if used == Use::Verbatim { Use::Verbatim } else { Use::Negated };
                format!("(NOT {})", self.build(not, table, inner_use))
            }
            PredicateJson::In { col, subquery, negated } => {
                let op = if *negated { "NOT IN" } else { "IN" };
                format!("{} {op} ({})", quote_ident(col), self.select(subquery, used))
            }
        }
    }

    /// `SELECT <project> FROM <table>[ WHERE <where>]` for one subquery. The inner where's columns
    /// belong to the subquery's table, so the context switches and their params are cast to the
    /// inner table's native types. A subquery's `WHERE` is a filter however the `IN` is used: the
    /// set it selects is the projection of the rows it is TRUE for.
    fn select(&mut self, subquery: &SubqueryJson, used: Use) -> String {
        let inner_use = if used == Use::Verbatim { Use::Verbatim } else { Use::Filter };
        let inner = match &subquery.where_ {
            Some(w) => format!(" WHERE {}", self.build(w, &subquery.table, inner_use)),
            None => String::new(),
        };
        // The inner table is schema-qualified like every other FROM (`"schema"."name"`).
        format!("SELECT {} FROM {}{inner}", quote_ident(&subquery.project), subquery.table.quote_qualified())
    }

    /// An `OR`, with two rewrites that let Postgres drive the table from a set of keys instead of
    /// scanning it to test every branch on every row. Both keep the rows exactly:
    ///
    /// 1. **Same-column `IN` siblings** — `c IN (s1) OR c IN (s2)` becomes `c IN (s1 UNION ALL s2)`,
    ///    when every subquery projects the same known non-collatable SQL type (so the union is that
    ///    type, and each comparison is the one the separate tests made). Three-valued logic agrees:
    ///    both forms are
    ///    TRUE iff `c` equals a value of some subquery, FALSE iff neither subquery holds `c` or a
    ///    NULL (with `c` itself not NULL, or both subqueries empty), and UNKNOWN otherwise.
    /// 2. **The key in an `IN`** — when a branch is `k IN (…)` on the table's single-column primary
    ///    key, each other branch `B` must be a positive IN (subquery or literal list), or an AND
    ///    containing one. Only then does it join as `SELECT k FROM <table> WHERE B`. A broad branch
    ///    vetoes rewriting the whole key OR, including descendants, retaining main's SQL. The
    ///    key is unique and never NULL, so a row is in `SELECT k … WHERE B` exactly when `B` is TRUE
    ///    for it. `B` UNKNOWN becomes FALSE, which is why this is done only in a [`Use::Filter`].
    ///    Every key subquery must project the key's known non-collatable type.
    ///
    /// Left alone: `NOT IN` (its NULL semantics are not a union's), `IN` on different non-key
    /// columns, a key that is composite or of unknown type, and any projection whose type is unknown
    /// or differs from its siblings', and projections whose collation compatibility is unknown.
    /// A union of ONE member is not a rewrite either, and an `OR` neither rule applies to is written
    /// exactly as before.
    fn or(&mut self, branches: &[PredicateJson], table: &TableRef, used: Use) -> String {
        if used == Use::Verbatim {
            let parts: Vec<String> = branches.iter().map(|p| self.build(p, table, used)).collect();
            return format!("({})", parts.join(" OR "));
        }
        let schemas = self.schemas;
        // The SQL type a subquery projects: its column's native type, known only from introspection
        // (`None` in library mode, so neither rewrite applies there).
        let projects =
            |subquery: &SubqueryJson| schemas.get(&subquery.table).and_then(|ts| union_type(ts, &subquery.project));

        // Rule 2: the table's single-column key, tested with `IN` by at least one branch.
        if used == Use::Filter
            && let Some(ts) = schemas.get(table)
            && let [key] = ts.pk_cols.as_slice()
        {
            let key_name = ts.columns[*key].0.as_str();
            let keyed: Vec<&SubqueryJson> =
                branches.iter().filter_map(plain_in).filter(|(col, _)| *col == key_name).map(|(_, s)| s).collect();
            if !keyed.is_empty() && branches.len() > 1 && !branches.iter().all(has_positive_in) {
                return self.or(branches, table, Use::Verbatim);
            }
            if !keyed.is_empty()
                && branches.len() > 1
                && let Some(key_type) = union_type(ts, key_name)
                && keyed.iter().all(|s| projects(s) == Some(key_type))
            {
                let members: Vec<String> = branches
                    .iter()
                    .map(|branch| match plain_in(branch) {
                        Some((col, subquery)) if col == key_name => self.select(subquery, used),
                        _ => format!(
                            "SELECT {} FROM {} WHERE {}",
                            quote_ident(key_name),
                            table.quote_qualified(),
                            self.build(branch, table, Use::Filter)
                        ),
                    })
                    .collect();
                return format!("({} IN ({}))", quote_ident(key_name), members.join(" UNION ALL "));
            }
        }

        // Rule 1: same-column `IN` siblings, each column's union where its first sibling stood.
        let mut merged: Vec<&str> = Vec::new();
        let mut parts: Vec<String> = Vec::with_capacity(branches.len());
        for branch in branches {
            let Some((col, _)) = plain_in(branch) else {
                parts.push(self.build(branch, table, used));
                continue;
            };
            if merged.contains(&col) {
                continue;
            }
            let siblings: Vec<&SubqueryJson> =
                branches.iter().filter_map(plain_in).filter(|(c, _)| *c == col).map(|(_, s)| s).collect();
            let projected = projects(siblings[0]);
            if siblings.len() > 1 && projected.is_some() && siblings.iter().all(|s| projects(s) == projected) {
                merged.push(col);
                let members: Vec<String> = siblings.into_iter().map(|s| self.select(s, used)).collect();
                parts.push(format!("{} IN ({})", quote_ident(col), members.join(" UNION ALL ")));
            } else {
                parts.push(self.build(branch, table, used));
            }
        }
        format!("({})", parts.join(" OR "))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::predicate::PredicateJson;
    use crate::schema::{ColumnDef, ColumnType, TableDef};
    use std::collections::BTreeMap;

    fn schema() -> TableSchema {
        let mut columns = BTreeMap::new();
        columns.insert("id".to_string(), ColumnDef { ty: ColumnType::Int, pg_type: None, has_default: false });
        columns.insert("name".to_string(), ColumnDef { ty: ColumnType::Text, pg_type: None, has_default: false });
        columns.insert("score".to_string(), ColumnDef { ty: ColumnType::Float, pg_type: None, has_default: false });
        columns.insert("active".to_string(), ColumnDef { ty: ColumnType::Bool, pg_type: None, has_default: false });
        let def = TableDef { columns, primary_key: vec!["id".to_string()], fingerprint: None };
        TableSchema::from_def(&"users".into(), &def).unwrap()
    }

    fn sql(json: serde_json::Value) -> (String, Vec<String>) {
        let ts = schema();
        let pj: PredicateJson = serde_json::from_value(json).unwrap();
        let cp = CompiledPredicate::compile(&pj, &ts).unwrap();
        predicate_to_sql(&cp, &ts).unwrap()
    }

    /// A table whose Postgres types are KNOWN, for the collation tests below.
    fn introspected(pg_type_of_label: &str) -> TableSchema {
        let mut columns = BTreeMap::new();
        columns.insert(
            "id".to_string(),
            ColumnDef { ty: ColumnType::Int, pg_type: Some("int4".to_string()), has_default: false },
        );
        columns.insert(
            "label".to_string(),
            ColumnDef { ty: ColumnType::Text, pg_type: Some(pg_type_of_label.to_string()), has_default: false },
        );
        let def = TableDef { columns, primary_key: vec!["id".to_string()], fingerprint: None };
        TableSchema::from_def(&"users".into(), &def).unwrap()
    }

    fn sql_on(ts: &TableSchema, json: serde_json::Value) -> String {
        let pj: PredicateJson = serde_json::from_value(json).unwrap();
        let cp = CompiledPredicate::compile(&pj, ts).unwrap();
        predicate_to_sql(&cp, ts).unwrap().0
    }

    /// An ORDERING comparison on a genuinely collatable text column carries `COLLATE "C"`, so
    /// Postgres compares it the way the engine's own evaluator (Rust string ordering) and the subset
    /// client (code points) do, whatever the database's default collation is.
    #[test]
    fn a_text_range_comparison_is_collated_c() {
        let ts = introspected("text");
        let w = sql_on(&ts, serde_json::json!({"col": "label", "op": "gt", "value": "m"}));
        assert_eq!(w, r#""label" collate "C" > $1::text::"text""#);
    }

    /// Equality is byte equality under every deterministic collation, so it keeps the plain,
    /// index-eligible form — collating it would defeat the column's btree index for nothing.
    #[test]
    fn text_equality_is_never_collated() {
        let ts = introspected("text");
        let w = sql_on(&ts, serde_json::json!({"col": "label", "op": "eq", "value": "m"}));
        assert_eq!(w, r#""label" = $1::text::"text""#);
    }

    /// Our coarse `Text` also covers types whose order is collation-INDEPENDENT and which refuse a
    /// collation outright; asking for one would make every query on the table an error.
    #[test]
    fn a_non_collatable_text_mapped_column_is_not_collated() {
        let ts = introspected("uuid");
        let w = sql_on(&ts, serde_json::json!({"col": "label", "op": "gt", "value": "m"}));
        assert_eq!(w, r#""label" > $1::text::"uuid""#);
    }

    /// An UNKNOWN Postgres type is not collated either: the declaration says "text", but the column
    /// underneath may be a uuid. The code-point ordering guarantee needs introspection.
    #[test]
    fn a_column_with_no_known_pg_type_is_not_collated() {
        let ts = schema();
        let w = sql_on(&ts, serde_json::json!({"col": "name", "op": "gt", "value": "m"}));
        assert_eq!(w, r#""name"::text > $1"#);
    }

    #[test]
    fn leaf_text_is_parameterized() {
        let (w, p) = sql(serde_json::json!({"col": "name", "op": "eq", "value": "Alice"}));
        assert_eq!(w, r#""name"::text = $1"#);
        assert_eq!(p, vec!["Alice".to_string()]);
    }

    #[test]
    fn numeric_bool_null_are_inlined() {
        assert_eq!(sql(serde_json::json!({"col": "id", "op": "gte", "value": 3})).0, r#""id" >= 3"#);
        assert_eq!(sql(serde_json::json!({"col": "score", "op": "lt", "value": 1.5})).0, r#""score" < 1.5"#);
        assert_eq!(sql(serde_json::json!({"col": "active", "op": "eq", "value": true})).0, r#""active" = true"#);
        // a null literal in a leaf compares as NULL (SQL three-valued -> never TRUE), matching matches()
        assert_eq!(sql(serde_json::json!({"col": "name", "op": "neq", "value": null})).0, r#""name" <> NULL"#);
    }

    #[test]
    fn and_or_not_compose_with_ordered_placeholders() {
        let (w, p) = sql(serde_json::json!({
            "and": [
                { "col": "name", "op": "eq", "value": "a" },
                { "or": [ { "col": "id", "op": "gt", "value": 5 }, { "col": "name", "op": "eq", "value": "b" } ] },
                { "not": { "col": "active", "op": "eq", "value": false } }
            ]
        }));
        assert_eq!(w, r#"("name"::text = $1 AND ("id" > 5 OR "name"::text = $2) AND (NOT "active" = false))"#);
        assert_eq!(p, vec!["a".to_string(), "b".to_string()]);
    }

    // No schemas -> pg_type unknown -> the `col::text` fallback form (exercised by these tests).
    fn jsql(json: serde_json::Value) -> (String, Vec<String>) {
        let pj: PredicateJson = serde_json::from_value(json).unwrap();
        predicate_json_to_sql(&pj, 1, &HashMap::new(), &"t".into())
    }

    /// A table with a real Postgres type per column (as introspection would produce).
    fn typed_schema(name: &str, cols: &[(&str, ColumnType, &str)]) -> TableSchema {
        let mut columns = BTreeMap::new();
        for (c, ty, pg) in cols {
            columns.insert(c.to_string(), ColumnDef { ty: *ty, pg_type: Some(pg.to_string()), has_default: false });
        }
        let def = TableDef { columns, primary_key: vec![cols[0].0.to_string()], fingerprint: None };
        TableSchema::from_def(&TableRef::parse(name).unwrap(), &def).unwrap()
    }

    #[test]
    fn text_param_cast_to_native_type_when_known() {
        // uuid column -> `$n::text::uuid` (index-eligible), not `col::text = $n`.
        let ts = typed_schema("projects", &[("id", ColumnType::Text, "uuid"), ("owner_id", ColumnType::Text, "uuid")]);
        let pj: PredicateJson =
            serde_json::from_value(serde_json::json!({"col":"owner_id","op":"eq","value":"u1"})).unwrap();
        let cp = CompiledPredicate::compile(&pj, &ts).unwrap();
        let (w, p) = predicate_to_sql(&cp, &ts).unwrap();
        assert_eq!(w, r#""owner_id" = $1::text::"uuid""#);
        assert_eq!(p, vec!["u1".to_string()]);

        // Same via the JSON/subquery emitter, casting the INNER table's column to its native type.
        let mut schemas = HashMap::new();
        schemas.insert(TableRef::parse("projects").unwrap(), ts);
        schemas.insert(
            TableRef::parse("issues").unwrap(),
            typed_schema("issues", &[("id", ColumnType::Text, "uuid"), ("project_id", ColumnType::Text, "uuid")]),
        );
        let outer: PredicateJson = serde_json::from_value(serde_json::json!({
            "col": "project_id",
            "in": { "table": "projects", "project": "id", "where": { "col": "owner_id", "op": "eq", "value": "u1" } }
        }))
        .unwrap();
        let (w, p) = predicate_json_to_sql(&outer, 1, &schemas, &"issues".into());
        assert_eq!(w, r#""project_id" IN (SELECT "id" FROM "public"."projects" WHERE "owner_id" = $1::text::"uuid")"#);
        assert_eq!(p, vec!["u1".to_string()]);
    }

    #[test]
    fn json_emitter_in_subquery() {
        let (w, p) = jsql(serde_json::json!({
            "col": "parent_id",
            "in": { "table": "parent", "project": "id", "where": { "col": "active", "op": "eq", "value": true } }
        }));
        assert_eq!(w, r#""parent_id" IN (SELECT "id" FROM "public"."parent" WHERE "active" = true)"#);
        assert!(p.is_empty());
    }

    #[test]
    fn json_emitter_not_in_and_text_params() {
        let (w, p) = jsql(serde_json::json!({
            "col": "pid", "negated": true,
            "in": { "table": "p", "project": "id", "where": { "col": "name", "op": "eq", "value": "x" } }
        }));
        assert_eq!(w, r#""pid" NOT IN (SELECT "id" FROM "public"."p" WHERE "name"::text = $1)"#);
        assert_eq!(p, vec!["x".to_string()]);
    }

    #[test]
    fn json_emitter_nested_subquery_and_param_numbering() {
        let (w, p) = jsql(serde_json::json!({
            "and": [
                { "col": "tag", "op": "eq", "value": "a" },
                { "col": "l3", "in": { "table": "level_3", "project": "id", "where": {
                    "col": "l2", "in": { "table": "level_2", "project": "id", "where": { "col": "name", "op": "eq", "value": "b" } } } } }
            ]
        }));
        assert_eq!(
            w,
            r#"("tag"::text = $1 AND "l3" IN (SELECT "id" FROM "public"."level_3" WHERE "l2" IN (SELECT "id" FROM "public"."level_2" WHERE "name"::text = $2)))"#
        );
        assert_eq!(p, vec!["a".to_string(), "b".to_string()]);
    }

    #[test]
    fn text_value_with_quote_is_a_param_not_inlined() {
        // Injection-style input stays a bound parameter (no inlining), so it can't break the SQL.
        let (w, p) = sql(serde_json::json!({"col": "name", "op": "eq", "value": "x'); DROP TABLE users;--"}));
        assert_eq!(w, r#""name"::text = $1"#);
        assert_eq!(p, vec!["x'); DROP TABLE users;--".to_string()]);
    }

    // --- the OR rewrites (`JsonSql::or`) ------------------------------------------------------------
    //
    // An app scope in miniature, introspected (every column's Postgres type known): `events` and
    // `records` keyed by a uuid `id`, `grants` linking users to both, `legacy` keyed by an int4 with
    // an int8 column, and `pairs` with a composite key.

    fn app_schemas() -> HashMap<TableRef, TableSchema> {
        use ColumnType::{Int, Text};
        let mut schemas = HashMap::new();
        for ts in [
            typed_schema(
                "events",
                &[
                    ("id", Text, "uuid"),
                    ("record_id", Text, "uuid"),
                    ("owner_id", Text, "uuid"),
                    ("name", Text, "text"),
                    ("deleted_at", Text, "timestamptz"),
                ],
            ),
            typed_schema(
                "records",
                &[
                    ("id", Text, "uuid"),
                    ("owner_id", Text, "uuid"),
                    ("recurring_parent_id", Text, "uuid"),
                    ("original_start_time", Text, "timestamptz"),
                    ("visible", ColumnType::Bool, "bool"),
                ],
            ),
            typed_schema(
                "grants",
                &[
                    ("id", Text, "uuid"),
                    ("user_id", Text, "uuid"),
                    ("record_id", Text, "uuid"),
                    ("event_id", Text, "uuid"),
                ],
            ),
            typed_schema("legacy", &[("id", Int, "int4"), ("record_id", Text, "text"), ("big", Int, "int8")]),
        ] {
            schemas.insert(ts.table.clone(), ts);
        }
        let mut columns = BTreeMap::new();
        for (c, ty, pg) in [("a", Text, "uuid"), ("b", Text, "uuid"), ("c", Int, "int4")] {
            columns.insert(c.to_string(), ColumnDef { ty, pg_type: Some(pg.to_string()), has_default: false });
        }
        let def = TableDef { columns, primary_key: vec!["a".to_string(), "b".to_string()], fingerprint: None };
        let pairs = TableSchema::from_def(&TableRef::parse("pairs").unwrap(), &def).unwrap();
        schemas.insert(pairs.table.clone(), pairs);
        schemas
    }

    fn app_sql(table: &str, json: serde_json::Value) -> (String, Vec<String>) {
        let pj: PredicateJson = serde_json::from_value(json).unwrap();
        predicate_json_to_sql(&pj, 1, &app_schemas(), &TableRef::parse(table).unwrap())
    }

    /// `col IN (SELECT id FROM records WHERE visible = true)`: a uuid projection.
    fn on_visible_record(col: &str) -> serde_json::Value {
        serde_json::json!({"col": col, "in": {"table": "records", "project": "id",
            "where": {"col": "visible", "op": "eq", "value": true}}})
    }
    const VISIBLE: &str = r#"SELECT "id" FROM "public"."records" WHERE "visible" = true"#;

    /// `col IN (SELECT <project> FROM grants WHERE user_id = <user>)`: a uuid projection.
    fn granted(col: &str, project: &str, user: &str) -> serde_json::Value {
        serde_json::json!({"col": col, "in": {"table": "grants", "project": project,
            "where": {"col": "user_id", "op": "eq", "value": user}}})
    }

    #[test]
    fn same_column_in_siblings_become_one_union() {
        let (w, p) = app_sql(
            "events",
            serde_json::json!({"or": [on_visible_record("record_id"), granted("record_id", "record_id", "u1")]}),
        );
        assert_eq!(
            w,
            format!(
                r#"("record_id" IN ({VISIBLE} UNION ALL SELECT "record_id" FROM "public"."grants" WHERE "user_id" = $1::text::"uuid"))"#
            )
        );
        assert_eq!(p, vec!["u1".to_string()]);
    }

    /// A union stands where its first member stood; the other branches keep their order, and the
    /// placeholders stay numbered in the order their params were bound.
    #[test]
    fn a_union_stands_where_its_first_member_stood() {
        let (w, p) = app_sql(
            "events",
            serde_json::json!({"or": [
                {"col": "name", "op": "eq", "value": "a"},
                granted("record_id", "record_id", "u1"),
                {"col": "deleted_at", "isNull": true},
                on_visible_record("record_id"),
                granted("record_id", "record_id", "u2"),
            ]}),
        );
        let grants =
            |n: usize| format!(r#"SELECT "record_id" FROM "public"."grants" WHERE "user_id" = ${n}::text::"uuid""#);
        assert_eq!(
            w,
            format!(
                r#"("name" = $1::text::"text" OR "record_id" IN ({} UNION ALL {VISIBLE} UNION ALL {}) OR "deleted_at" IS NULL)"#,
                grants(2),
                grants(3)
            )
        );
        assert_eq!(p, vec!["a".to_string(), "u1".to_string(), "u2".to_string()]);
    }

    /// `NOT IN` is never a union member: over a NULL in its subquery it is UNKNOWN for every row,
    /// which no union of `IN`s reproduces. The plain `IN` siblings beside it still merge.
    #[test]
    fn not_in_is_left_alone() {
        let not_in =
            serde_json::json!({"col": "record_id", "negated": true, "in": {"table": "grants", "project": "record_id"}});
        let (w, _) = app_sql("events", serde_json::json!({"or": [not_in.clone(), on_visible_record("record_id")]}));
        assert_eq!(
            w,
            format!(
                r#"("record_id" NOT IN (SELECT "record_id" FROM "public"."grants") OR "record_id" IN ({VISIBLE}))"#
            )
        );

        let (w, _) = app_sql(
            "events",
            serde_json::json!({"or": [not_in, on_visible_record("record_id"), granted("record_id", "record_id", "u1")]}),
        );
        assert_eq!(
            w,
            format!(
                r#"("record_id" NOT IN (SELECT "record_id" FROM "public"."grants") OR "record_id" IN ({VISIBLE} UNION ALL SELECT "record_id" FROM "public"."grants" WHERE "user_id" = $1::text::"uuid"))"#
            )
        );
    }

    /// `IN` tests on two different non-key columns are not one set.
    #[test]
    fn in_tests_on_different_columns_are_left_alone() {
        let (w, _) = app_sql(
            "events",
            serde_json::json!({"or": [on_visible_record("record_id"), granted("owner_id", "user_id", "u1")]}),
        );
        assert_eq!(
            w,
            format!(
                r#"("record_id" IN ({VISIBLE}) OR "owner_id" IN (SELECT "user_id" FROM "public"."grants" WHERE "user_id" = $1::text::"uuid"))"#
            )
        );
    }

    /// A uuid projection and a text one would make a union of another type (or none at all), and
    /// each `IN` compare differently: left alone. So is a column whose type is not known.
    #[test]
    fn projections_of_different_or_unknown_types_are_left_alone() {
        let legacy = serde_json::json!({"col": "record_id", "in": {"table": "legacy", "project": "record_id"}});
        let (w, _) = app_sql("events", serde_json::json!({"or": [on_visible_record("record_id"), legacy]}));
        assert_eq!(
            w,
            format!(r#"("record_id" IN ({VISIBLE}) OR "record_id" IN (SELECT "record_id" FROM "public"."legacy"))"#)
        );

        // Library mode: no Postgres types at all.
        let (w, _) = jsql(serde_json::json!({"or": [
            {"col": "parent_id", "in": {"table": "a", "project": "id"}},
            {"col": "parent_id", "in": {"table": "b", "project": "id"}},
        ]}));
        assert_eq!(
            w,
            r#"("parent_id" IN (SELECT "id" FROM "public"."a") OR "parent_id" IN (SELECT "id" FROM "public"."b"))"#
        );
    }

    /// The week-query shape: key subqueries plus a parent IN and an original-start range.
    /// Literal IN lists already reach the printer as same-column equality ORs.
    #[test]
    fn the_week_query_gathers_a_parent_in_and_time_range() {
        for (parent, expected_parent, expected_params) in [
            (
                "recurring_parent_id IN (SELECT id FROM records WHERE visible = true)",
                format!(r#""recurring_parent_id" IN ({VISIBLE})"#),
                vec!["u1", "2026-11-16", "2026-11-23"],
            ),
            (
                "recurring_parent_id IN ('p1', 'p2')",
                r#"("recurring_parent_id" = $2::text::"uuid" OR "recurring_parent_id" = $3::text::"uuid")"#.to_string(),
                vec!["u1", "p1", "p2", "2026-11-16", "2026-11-23"],
            ),
            (
                "recurring_parent_id IN ('p1')",
                r#"("recurring_parent_id" = $2::text::"uuid")"#.to_string(),
                vec!["u1", "p1", "2026-11-16", "2026-11-23"],
            ),
        ] {
            let pj = crate::where_sql::parse_where(&format!(
                "id IN (SELECT record_id FROM grants WHERE user_id = 'u1') OR \
                 ({parent} AND original_start_time >= '2026-11-16' AND original_start_time < '2026-11-23')"
            ))
            .unwrap()
            .unwrap();
            let (w, params) = predicate_json_to_sql(&pj, 1, &app_schemas(), &"records".into());
            let end = expected_params.len();
            assert_eq!(
                w,
                format!(
                    r#"("id" IN (SELECT "record_id" FROM "public"."grants" WHERE "user_id" = $1::text::"uuid" UNION ALL SELECT "id" FROM "public"."records" WHERE ({expected_parent} AND "original_start_time" >= ${}::text::"timestamptz" AND "original_start_time" < ${end}::text::"timestamptz")))"#,
                    end - 1,
                )
            );
            assert_eq!(params, expected_params);
        }
    }

    #[test]
    fn the_key_in_gathers_only_in_branches_and_conjunctions() {
        let (w, p) = app_sql(
            "events",
            serde_json::json!({"or": [
                on_visible_record("record_id"),
                granted("id", "event_id", "u1"),
                {"and": [
                    {"col": "deleted_at", "isNull": true},
                    {"and": [granted("owner_id", "user_id", "u2"), {"col": "name", "op": "eq", "value": "a"}]},
                ]},
            ]}),
        );
        assert_eq!(
            w,
            format!(
                r#"("id" IN (SELECT "id" FROM "public"."events" WHERE "record_id" IN ({VISIBLE}) UNION ALL SELECT "event_id" FROM "public"."grants" WHERE "user_id" = $1::text::"uuid" UNION ALL SELECT "id" FROM "public"."events" WHERE ("deleted_at" IS NULL AND ("owner_id" IN (SELECT "user_id" FROM "public"."grants" WHERE "user_id" = $2::text::"uuid") AND "name" = $3::text::"text"))))"#
            )
        );
        assert_eq!(p, vec!["u1", "u2", "a"]);
    }

    /// A broad branch vetoes the entire key OR, including sibling/nested unions. A mere nested
    /// OR containing an IN, a NOT IN, and equalities on different columns are not positive IN lists.
    #[test]
    fn a_key_or_with_any_broad_branch_is_unchanged() {
        for (broad, printed) in [
            (serde_json::json!({"col": "deleted_at", "isNull": true}), r#""deleted_at" IS NULL"#.to_string()),
            (
                serde_json::json!({"col": "owner_id", "op": "eq", "value": "u2"}),
                r#""owner_id" = $2::text::"uuid""#.to_string(),
            ),
            (
                serde_json::json!({"col": "id", "negated": true, "in": {"table": "records", "project": "id"}}),
                r#""id" NOT IN (SELECT "id" FROM "public"."records")"#.to_string(),
            ),
            (
                serde_json::json!({"or": [on_visible_record("record_id"), on_visible_record("record_id")]}),
                format!(r#"("record_id" IN ({VISIBLE}) OR "record_id" IN ({VISIBLE}))"#),
            ),
            (
                serde_json::json!({"or": [{"col": "owner_id", "op": "eq", "value": "u2"}, {"col": "record_id", "op": "eq", "value": "r"}]}),
                r#"("owner_id" = $2::text::"uuid" OR "record_id" = $3::text::"uuid")"#.to_string(),
            ),
            (
                serde_json::json!({"and": [{"not": on_visible_record("record_id")}, {"col": "deleted_at", "isNull": true}]}),
                format!(r#"((NOT "record_id" IN ({VISIBLE})) AND "deleted_at" IS NULL)"#),
            ),
        ] {
            let (w, _) = app_sql(
                "events",
                serde_json::json!({"or": [
                    granted("id", "event_id", "u1"), on_visible_record("id"), broad,
                ]}),
            );
            assert_eq!(
                w,
                format!(
                    r#"("id" IN (SELECT "event_id" FROM "public"."grants" WHERE "user_id" = $1::text::"uuid") OR "id" IN ({VISIBLE}) OR {printed})"#
                )
            );
        }
    }

    /// Introspection carries no column collation. Do not assume matching type names mean matching
    /// collations: leave text (and unclassified/domain types) alone, even for the key rule.
    #[test]
    fn projections_without_known_collation_compatibility_are_left_alone() {
        for ty in ["text", "varchar", "bpchar", "name", "citext", "custom_domain"] {
            let mut schemas = HashMap::new();
            for table in ["outer", "a", "b"] {
                let ts = typed_schema(table, &[("id", ColumnType::Text, ty), ("value", ColumnType::Text, ty)]);
                schemas.insert(ts.table.clone(), ts);
            }
            for col in ["id", "value"] {
                let pj = serde_json::from_value(serde_json::json!({"or": [
                    {"col": col, "in": {"table": "a", "project": "id"}},
                    {"col": col, "in": {"table": "b", "project": "id"}},
                ]}))
                .unwrap();
                let (w, _) = predicate_json_to_sql(&pj, 1, &schemas, &"outer".into());
                assert_eq!(
                    w,
                    format!(
                        r#"("{col}" IN (SELECT "id" FROM "public"."a") OR "{col}" IN (SELECT "id" FROM "public"."b"))"#
                    )
                );
            }
        }
    }

    /// Gathering makes an UNKNOWN branch FALSE, which a `NOT` would tell apart: under a `NOT` the
    /// `OR` keeps its branches. The exact same-column union still applies there.
    #[test]
    fn the_key_union_is_not_made_under_not() {
        let (w, _) = app_sql(
            "events",
            serde_json::json!({"not": {"or": [granted("id", "event_id", "u1"), on_visible_record("record_id")]}}),
        );
        assert_eq!(
            w,
            format!(
                r#"(NOT ("id" IN (SELECT "event_id" FROM "public"."grants" WHERE "user_id" = $1::text::"uuid") OR "record_id" IN ({VISIBLE})))"#
            )
        );

        let (w, _) = app_sql(
            "events",
            serde_json::json!({"not": {"and": [{"or": [on_visible_record("record_id"), granted("record_id", "record_id", "u1")]}]}}),
        );
        assert_eq!(
            w,
            format!(
                r#"(NOT (("record_id" IN ({VISIBLE} UNION ALL SELECT "record_id" FROM "public"."grants" WHERE "user_id" = $1::text::"uuid"))))"#
            )
        );

        // A subquery's WHERE is a filter again, even under a NOT: the set it selects is the same.
        let (w, _) = app_sql(
            "events",
            serde_json::json!({"not": {"col": "record_id", "in": {"table": "records", "project": "id",
                "where": {"or": [granted("id", "record_id", "u1"), on_visible_record("owner_id")]}}}}),
        );
        assert_eq!(
            w,
            format!(
                r#"(NOT "record_id" IN (SELECT "id" FROM "public"."records" WHERE ("id" IN (SELECT "record_id" FROM "public"."grants" WHERE "user_id" = $1::text::"uuid" UNION ALL SELECT "id" FROM "public"."records" WHERE "owner_id" IN ({VISIBLE})))))"#
            )
        );
    }

    /// Only the table's key gathers: an `IN` on another column, a composite key, and a key
    /// subquery that projects another type all leave the `OR` as it was.
    #[test]
    fn only_a_single_column_key_of_the_projected_type_gathers() {
        let (w, _) = app_sql(
            "events",
            serde_json::json!({"or": [on_visible_record("record_id"), {"col": "name", "op": "eq", "value": "a"}]}),
        );
        assert_eq!(w, format!(r#"("record_id" IN ({VISIBLE}) OR "name" = $1::text::"text")"#));

        let (w, _) =
            app_sql("pairs", serde_json::json!({"or": [on_visible_record("a"), {"col": "c", "op": "eq", "value": 1}]}));
        assert_eq!(w, format!(r#"("a" IN ({VISIBLE}) OR "c" = 1)"#));

        let (w, _) = app_sql(
            "legacy",
            serde_json::json!({"or": [{"col": "id", "in": {"table": "legacy", "project": "big"}}, {"col": "record_id", "op": "eq", "value": "r"}]}),
        );
        assert_eq!(w, r#"("id" IN (SELECT "big" FROM "public"."legacy") OR "record_id" = $1::text::"text")"#);

        // One key `IN` alone has nothing to gather.
        let (w, _) = app_sql("events", serde_json::json!({"or": [granted("id", "event_id", "u1")]}));
        assert_eq!(w, r#"("id" IN (SELECT "event_id" FROM "public"."grants" WHERE "user_id" = $1::text::"uuid"))"#);
    }

    /// Both arms are positive INs, so the broad-branch veto cannot hide a missing key-type guard.
    /// The key is int4, while its subquery projects int8; these must remain separate branches.
    #[test]
    fn a_positive_key_or_requires_the_key_projection_type() {
        let (w, params) = app_sql(
            "legacy",
            serde_json::json!({"or": [
                {"col": "id", "in": {"table": "legacy", "project": "big"}},
                {"col": "big", "in": {"table": "legacy", "project": "big"}},
            ]}),
        );
        assert_eq!(
            w,
            r#"("id" IN (SELECT "big" FROM "public"."legacy") OR "big" IN (SELECT "big" FROM "public"."legacy"))"#
        );
        assert!(params.is_empty());
    }

    /// Both rules apply to every `OR` at every depth: under an `AND`, inside a subquery's `WHERE`
    /// (on the inner table's own key), and in a subquery nested in that one. Placeholders start
    /// where the caller says.
    #[test]
    fn the_rewrites_apply_at_every_depth() {
        let pj: PredicateJson = serde_json::from_value(serde_json::json!({"and": [
            {"col": "deleted_at", "isNull": true},
            {"col": "record_id", "in": {"table": "records", "project": "id", "where": {"or": [
                granted("owner_id", "user_id", "u1"),
                granted("id", "record_id", "u1"),
            ]}}},
        ]}))
        .unwrap();
        let (w, p) = predicate_json_to_sql(&pj, 3, &app_schemas(), &"events".into());
        assert_eq!(
            w,
            r#"("deleted_at" IS NULL AND "record_id" IN (SELECT "id" FROM "public"."records" WHERE ("id" IN (SELECT "id" FROM "public"."records" WHERE "owner_id" IN (SELECT "user_id" FROM "public"."grants" WHERE "user_id" = $3::text::"uuid") UNION ALL SELECT "record_id" FROM "public"."grants" WHERE "user_id" = $4::text::"uuid"))))"#
        );
        assert_eq!(p, vec!["u1".to_string(), "u1".to_string()]);
    }

    /// An `OR` that neither rule applies to is printed exactly as before the rewrites existed.
    #[test]
    fn an_or_neither_rule_applies_to_is_unchanged() {
        let (w, _) = app_sql(
            "events",
            serde_json::json!({"or": [{"col": "name", "op": "eq", "value": "a"}, {"col": "deleted_at", "isNull": true}]}),
        );
        assert_eq!(w, r#"("name" = $1::text::"text" OR "deleted_at" IS NULL)"#);
        let (w, _) = app_sql("events", serde_json::json!({"or": [on_visible_record("record_id")]}));
        assert_eq!(w, format!(r#"("record_id" IN ({VISIBLE}))"#));
    }
}
