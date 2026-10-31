//! The shared envelope codec: change-log envelope -> Z-set delta, and output delta ->
//! per-pk State-Protocol envelopes. Used by the sequencer and the subquery registry.

use super::*;

/// Turn a table change event into the resulting input Z-set delta, plus the originating txid and
/// commit LSN. The delta is computed entirely from the envelope's `value` (new row) and `old` (prior
/// row, carried by replication under `REPLICA IDENTITY FULL`) — no in-memory `table_state`.
pub(crate) fn apply_envelope(
    ts: &TableSchema,
    env: &Envelope,
) -> Result<(Vec<Tup2<Row, ZWeight>>, Option<String>, Option<String>)> {
    let txid = env.headers.txid.clone();
    let lsn = env.headers.lsn.clone();
    let to_row = |v: &serde_json::Value| -> Result<Row> {
        let obj = v.as_object().ok_or_else(|| anyhow::anyhow!("envelope row is not an object"))?;
        ts.row_from_json(obj)
    };
    let mut delta: Vec<Tup2<Row, ZWeight>> = Vec::new();
    match env.headers.operation.as_str() {
        // `insert` is folded by the SAME rule as `update`, because a before-image on an insert is
        // real: replication never produces one, but library mode's per-key view does (a client
        // retrying an insert for a key the engine already holds). Ignoring it here would add the
        // row a second time — idempotent for a row shape, a permanent double-count for an
        // aggregate. With no `old` this is exactly the old insert path.
        "insert" | "update" | "upsert" => {
            let new = to_row(env.value.as_ref().context("insert/update envelope missing value")?)?;
            match env.old.as_ref() {
                Some(old) => {
                    let old = to_row(old)?;
                    if old != new {
                        delta.push(Tup2(old, -1));
                        delta.push(Tup2(new, 1));
                    }
                }
                // No prior row available -> treat as an insert of the new row.
                None => delta.push(Tup2(new, 1)),
            }
        }
        "delete" => {
            // Replication carries the full old row (REPLICA IDENTITY FULL); retract it.
            if let Some(old) = env.old.as_ref() {
                delta.push(Tup2(to_row(old)?, -1));
            }
        }
        other => bail!("unknown operation '{other}'"),
    }
    Ok((delta, txid, lsn))
}

/// Does this envelope's membership decision have to be taken **absolutely** (per pk, from the row's
/// current value) rather than from the Z-set delta?
///
/// Yes exactly when it removes or replaces a row and carries **no before-image**. The delta then has
/// no `-1` half, so a delete produces no delta at all and an update looks like a bare insert —
/// nothing can leave a shape. Postgres mode never gets here (`REPLICA IDENTITY FULL` supplies the
/// old row, and a replica-identity regression retires the table's shapes instead); library mode does,
/// wherever the sequencer's per-key view cannot supply one: the first change to a key after a
/// restart, and every change replayed for a shape reactivating out of dormancy.
pub(crate) fn needs_absolute_emission(env: &Envelope) -> bool {
    env.old.is_none() && matches!(env.headers.operation.as_str(), "delete" | "update" | "upsert")
}

/// ONE absolute per-pk envelope for a shape: `upsert` when `row` is the value the shape holds now,
/// `delete` when the shape must not hold the key (the caller has already evaluated membership).
///
/// This is the emission rule the subquery registry uses for flip-driven query-backs, applied here
/// for the same reason: with no before-image the delta cannot express a move-out, so membership is
/// stated outright. A `delete` for a key the shape never held is a deliberate, tolerated no-op —
/// stream-db and every fold consumer drop a delete for an unknown key.
///
/// Returns `None` when the TEST-ONLY `drop_deletes` fault suppresses the delete, exactly as
/// [`translate_output`] and [`delete_envelopes`] do.
pub(crate) fn absolute_envelope(
    ts: &TableSchema,
    key: &str,
    row: Option<&Row>,
    txid: Option<String>,
    lsn: Option<String>,
    out_cols: Option<&[usize]>,
) -> Option<Envelope> {
    let (operation, value) = match row {
        Some(r) => ("upsert", Some(ts.row_to_json_cols(r, out_cols))),
        None if matches!(crate::fault::active(), crate::fault::Fault::DropDeletes) => return None,
        None => ("delete", None),
    };
    Some(Envelope {
        type_: ts.table.to_string(),
        key: key.to_string(),
        value,
        old: None,
        headers: EnvelopeHeaders {
            operation: operation.into(),
            txid,
            offset: None,
            lsn,
            seq: None,
            last: None,
            commit_ts: None,
        },
    })
}

/// Translate a shape circuit's output Z-set delta into State-Protocol envelopes. Grouped by pk:
/// any positive-weight row -> `upsert` (enter/update); otherwise `delete` (leave).
pub(crate) fn translate_output(
    ts: &TableSchema,
    out: Vec<(Row, ZWeight)>,
    txid: Option<String>,
    lsn: Option<String>,
    out_cols: Option<&[usize]>,
) -> Vec<Envelope> {
    let mut pos: HashMap<String, Row> = HashMap::new();
    let mut neg: HashSet<String> = HashSet::new();
    // First-appearance order per pk: HashMap iteration would shuffle the emission order per
    // process (observable in snapshot appends — readers see rows in a random order per boot).
    let mut order: Vec<String> = Vec::new();
    for (row, w) in out {
        let pk = match ts.key_string(&row) {
            Ok(pk) => pk,
            Err(e) => {
                tracing::warn!("translate_output: dropping row with unextractable pk on table {}: {e:#}", ts.table);
                continue;
            }
        };
        if !pos.contains_key(&pk) && !neg.contains(&pk) {
            order.push(pk.clone());
        }
        if w > 0 {
            pos.insert(pk, row);
        } else if w < 0 {
            neg.insert(pk);
        }
    }
    let mut envs = Vec::with_capacity(pos.len() + neg.len());
    for (pk, row) in order.iter().filter_map(|pk| pos.get_key_value(pk)) {
        envs.push(Envelope {
            type_: ts.table.to_string(),
            key: pk.clone(),
            value: Some(ts.row_to_json_cols(row, out_cols)),
            old: None,
            headers: EnvelopeHeaders {
                operation: "upsert".into(),
                txid: txid.clone(),
                offset: None,
                lsn: lsn.clone(),
                seq: None,
                last: None,
                commit_ts: None,
            },
        });
    }
    // TEST-ONLY: the `drop_deletes` fault suppresses "leave" envelopes so rows that exit a shape
    // linger in the client. No-op unless ELECTRIC_CIRCUITS_FAULT=drop_deletes (see `fault`).
    let drop_deletes = matches!(crate::fault::active(), crate::fault::Fault::DropDeletes);
    for pk in order.iter().filter(|pk| neg.contains(*pk)) {
        if pos.contains_key(pk) || drop_deletes {
            continue;
        }
        envs.push(Envelope {
            type_: ts.table.to_string(),
            key: pk.clone(),
            value: None,
            old: None,
            headers: EnvelopeHeaders {
                operation: "delete".into(),
                txid: txid.clone(),
                offset: None,
                lsn: lsn.clone(),
                seq: None,
                last: None,
                commit_ts: None,
            },
        });
    }
    envs
}

/// Key-only `delete` envelopes for pks the per-feed relation retracted. The feed relation's
/// retraction IS the delete decision (structural spurious-delete gating), so this needs no
/// row body — only the pk. Honors the TEST-ONLY `drop_deletes` fault exactly like
/// [`translate_output`].
pub(crate) fn delete_envelopes(ts: &TableSchema, pks: Vec<String>, txid: Option<String>) -> Vec<Envelope> {
    if matches!(crate::fault::active(), crate::fault::Fault::DropDeletes) {
        return Vec::new();
    }
    pks.into_iter()
        .map(|pk| Envelope {
            type_: ts.table.to_string(),
            key: pk,
            value: None,
            old: None,
            headers: EnvelopeHeaders {
                operation: "delete".into(),
                txid: txid.clone(),
                offset: None,
                lsn: None,
                seq: None,
                last: None,
                commit_ts: None,
            },
        })
        .collect()
}

/// The aggregate wire envelope — ONE `"agg"`-keyed row `{ value, n }`, upserted when the value
/// changes. Shared by the in-engine fold ([`super::executors::AggShape`]) and circuit-served
/// counts ([`super::executors::CircuitAgg`]) so the two aggregate tiers cannot drift apart on
/// the wire format.
pub(crate) fn agg_envelope(
    table: &crate::table_ref::TableRef,
    value: serde_json::Value,
    n: i64,
    txid: Option<String>,
    lsn: Option<String>,
) -> Envelope {
    Envelope {
        type_: table.to_string(),
        key: "agg".into(),
        value: Some(serde_json::json!({ "value": value, "n": n })),
        old: None,
        headers: EnvelopeHeaders {
            operation: "upsert".into(),
            txid,
            offset: None,
            lsn,
            seq: None,
            last: None,
            commit_ts: None,
        },
    }
}
