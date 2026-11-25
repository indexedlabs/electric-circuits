Result: variant S failed (strict pass line: some probes were lost, and `graph_edge` needs a share list)

S ends the stall. It still loses one or two drains per large deletion, and B all-plain loses them too.

**Cost** (engine `b42f70d`, plus `5b2cf3e` for one pair; 1 CPU; S = 11 runs, A = 4 same-runner controls)

| Workload | A (s) | S (s) | S backfills | S CPU | S drains / subsets / live |
| --- | --- | --- | --- | --- | --- |
| Import | 248–318 | 2.6–3.6 | 0 | 28–30% | 38/38, 38/38, 38/38 |
| Large deletion | 75–93 | 1.2–1.9 | 0 | 20–25% | **74/86**, 86/86, **73/86** |
| Small deletion | 127–162 | 0.4–0.6 | 0 | 19–24% | 17/17 each |
| Share deletion | 83–110 | 1.9–2.6 | 1,888 | 47–67% | **27/31**, 31/31, **30/31** |

- **Large deletion:** every S run lost a drain. Commit to empty takes 2.7–3.5 s; probes time out at 2 s. B alternated with S: drains 24/27. The earlier passing variant, re-run: 5/7.
- **Share deletion:** the backfills come from the kept `file`, `user` and `resource` filters.

**Parity** (account-row pairs, one snapshot, 20 accounts, unexplained: 0)

| Table | (a) only | (b) only |
| --- | ---: | ---: |
| Record family (6 tables), `calendar_event` | 0 | 0 |
| `graph_edge` | 556 | 0 |

All 556 are edges on directly shared Records. A `from_id` share list on `graph_edge` makes it 0 and costs the same. Planted rows: 0 surprises; drift check found 25/25.

**Planted rows show S also:**
- gives partners removed and deleted Records, and the grantor's generic edges;
- sends mirror copies;
- drops assignments with other variables, and edges to shared or archived-home Resources.

**"Keep today's filters" for chat fails:** with today's real chat filters the import takes 13–18 s and drains 4/31. S here uses B-cold's keyed chat.

**Head:** the commit holding this file. **Runs:** 45433484314, 45435340834, 45438866286.

**Not run:** re-subscribing after share or membership lists change; production data.

## Details

- Harness: `a228441` (all pairs and parity), `249b5e3` (S repeats), `807ac2c` (B against S).
  Variants and readings are in `tools/stall-repro/simple_keys.py`; parity is in `parity.py`;
  example ids and reasons are in `parity/REPORT.md`. Every run folder is beside this file.
- Times are first delivery to empty queue. Only same-runner ratios compare with earlier batches:
  A is about 40% faster here than earlier runs on the same CPU models, with an identical catalog
  (10,571 shapes, 1,406 nodes, 26,367 edges) and import backfills (15,420). The share seed adds
  contributors and feed entries. The cause of the speed difference is not known.
- Runners: Xeon 8370C, Xeon 8573C, EPYC 9V74 (two), EPYC 7763 (two).
- The share deletion removes account 17: 1,000 `record_share`, 1,000 `file_share` and 1,001
  `space_member` rows with its content, 4,895 changes in one commit.

### Same-runner summary

| Variant | Runs | Import | Large deletion | Small deletion | Share deletion | Drains lost (by workload, same order) |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| A | 4 | 248–318 s | 75–93 s | 127–162 s | 83–110 s | 1146/1146, 347/364, 590/594, 390/393 |
| S | 11 | 2.6–3.6 | 1.2–1.9 | 0.4–0.6 | 1.9–2.6 | 0/38, 12/86, 0/17, 4/31 |
| S-edge-shares | 2 | 3.0–3.5 | 1.4–1.9 | 0.4–0.6 | 2.1–2.6 | 0/7, 3/15, 0/3, 1/5 |
| S-rest-today | 2 | 13.3–17.8 | 4.7–5.7 | 4.7–6.6 | 5.4–7.5 | 27/31, 9/23, 9/14, 9/14 |
| B (all-plain) | 3 | 3.4–3.5 | 1.3–1.7 | 0.4 | 0.3 | 0/12, 3/27, 0/6, 0/3 |
| B-cold-key-edges | 1 | 3.0 | 1.9 | 0.6 | 4.4 | 0/3, 2/7, 0/2, 3/5 |

S's catalog: 860 shapes, 71 of them subquery shapes (`file`, `user`, `resource`, `resource_fact`),
84 nodes. A: 10,571 shapes, 4,287 subquery shapes.

### Readings

- **Chat and the other unchanged tables.** "Keep today's filters, exactly as in the passing B-cold
  run" names two different things. The passing run (`B-cold-key-edges`) keyed `message`,
  `suggestion` and `workflow_run` on audience columns and rewrote `resource` and `resource_fact`.
  S uses those shapes. `S-rest-today` uses today's real gateway predicates on every table S does
  not key, and gives the importing account its chat, Resource, File and User feeds. Its import
  makes 6,540 backfills: 2,100 each on `record`, `message` and `workflow`, and 120 each on
  `resource` and `resource_fact`.
- **Direct shares on edges.** The `graph_edge` row gives no feed for edges on directly shared
  Records, so S has none. `S-edge-shares` adds them by `from_id`/`to_id` id list.
- **"The account's Space list" on generic and `relates_to` edges.** S tests it with the grantor's
  Spaces included, because partners read the same feeds as members. Tested against member Spaces
  only, the partner differences on those edges go away (`parity/REPORT.md`, alternates).
- **Edge feeds.** One per Space: `from_space_id = S`, except bidirectional Resource edges
  (`to_space_id = S`). The other end is tested against the whole list where today's branch tests it.
- **Share lists** come from active `record_share` rows to the caller. They do not check the Record's
  type, deletion or removal.
- **Today's side of parity** uses each account's grantors from the database. The harness's
  `authority()` omits account 18's one grant.
- `assignment.variable` in the synthetic data was not a real value. It now cycles through
  `calendar`, `origin` and `driver`, for A and S alike. Today's predicate does not test `variable`.

### What the planted rows show

None of these rows exists in the base data. They are S's designed differences and the cases it
does not cover. The last line is what the brief expected.

- **Partners see more:** the grantor's removed Records with their index rows, assignments and
  edges; deleted Record rows; generic and `relates_to` edges in the grantor's Space; and edges of
  account-type Resources.
- **Calendar:** mirror copies reach the owner and partners. An event on a grantor's calendar whose
  `user_id` is someone else reaches the partner.
- **Direct shares see more:** hidden-type Records, and index rows of removed Records.
- **Rows lost:** generated edges to a directly shared Resource in another Space; edges to Resources
  in a grantor's archived home (the Resource rule keeps archived homes, the Record rule that fills
  the list does not); assignments with a variable outside the three.
- **Resource ends:** open edges on a superseded Resource, which the drift check reports.
- **Expected drift, all found:** children whose Space differs from their Record's, children and open
  edges on deleted Records, children and role edges on hidden-type Records, and edges whose Space
  differs from their end's.

### Not run

- Re-creating feeds when a grant, membership or share list changes. The share deletion only deletes
  rows; a real gateway would also rebuild each affected account's feeds.
- Parent moves (a Record changing Space), production data, and any client.
- `monitored_url_*` and `standing_grant_backfill_status`, which the synthetic catalog does not hold.


Follow-up variants S-assign-today and S-chat-narrow (Nov 25): `S-ASSIGN-CHAT.md` beside this file.
