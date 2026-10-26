# Expand optimisation report

Optimisation of the DB Manager's **Expand** operation (`raw_data.raw` JSONB → six relational tables via the
six challenge recipes), with checkpoint / resume and failure recovery. Every guarantee of the original is kept:
source and destination validation, function-safety checks, managed row counters, one atomic data commit,
the source manifest, and version registration with replay metadata.

## 1. Results

Measured with the application's own **Expansion Performance Report** (`MEASURED PROCESSING TOTAL`), driven
headlessly (`bench/headless_expand.py`, same service calls as the GUI). Datasets from the unmodified official
generator; every run starts from an identical database snapshot.

| Rows | Original (untouched code) | This solution | Speed-up | Output vs original |
|---:|---:|---:|---:|---|
| 100,000 | 6.45 s | **1.88 s** | 3.4× | byte-identical, including every generated identity value |
| 1,000,000 | 59.5 s | **9.9 s** | 6.0× | byte-identical, including every generated identity value |
| 10,000,000 | 10 min 12 s | **1 min 49 s** | 5.6× | identical rows (logical fingerprint); see §5 for identity values |
| 50,000,000 | 59 min 43 s | **9 min 32 s** | 6.3× | identical rows; `table2` (116M rows) and `table5` byte-identical incl. identity values |

The speed-up grows with size: the original slows down faster than linearly (its `table2` insert: 5 min at 10M,
26 min at 50M), while this solution stays close to linear (at 50M the 100 GB source no longer fits in the
server's 32 GB, so scans come from disk).

Output equality is checked with `bench/fingerprint.sql`: an *exact* hash of every table (all columns, surrogate
keys included) and a *logical* hash (surrogate keys replaced by natural keys), plus the row-counter table.

**Resources at 10M rows** (`bench/monitor.sh`, sampled every 0.5 s):

| Run | Peak process memory (anonymous) | Peak spill files (`pgsql_tmp`) | Peak staging tables | Peak WAL |
|---|---:|---:|---:|---:|
| original | 0.34 GB | 0.65 GB | – | 8.6 GB |
| this solution | 2.6 GB | 0.65 GB | 4.9 GB (≈ one extra copy of the output) | 6.4 GB |

At 50M rows this solution peaked at 2.2 GB process memory, 3.5 GB spill files, 24.7 GB staging and 9.1 GB WAL.
Fingerprints beyond 10M use `bench/fingerprint_large.sql` (bucketed: whole-table strings exceed the 1 GB limit).

Test server: PostgreSQL 16 in Docker (16 CPU quota, 32 GB, `shared_buffers=4GB`) on an ARM machine with 20 cores;
see `bench/server/`.

## 2. Where the original spends its time

Profiled with `EXPLAIN ANALYZE` at 1M rows (original total 59.5 s):

| Cost | Share | Why |
|---|---|---|
| JSONB extraction in the recipes | ~60 % | `table2` alone evaluates 56 `->`/`->>` lookups and 14 lateral rows per source row; single core |
| Per-row foreign-key checks | ~30 % | every inserted row fires an RI trigger (index probe) on 4 tables |
| Per-row index maintenance | ~10 % | unique and primary-key indexes updated row by row |
| Preflight round trips | 0.85 s | ~35 separate SSH commands, each with its own shell, psql start-up and SCRAM login |

PostgreSQL never parallelises `INSERT … SELECT`, so all of this runs on one core, inside one transaction.

## 3. Architecture

```
client (DB Manager)                         server (detached job, survives SSH loss)
───────────────────                         ───────────────────────────────────────────────
batched preflight ──1 SSH command──▶  control psql + data psql: every validation, counters, catalog
register job (outbox) + start runner ──▶  coordinator session: BEGIN … single COMMIT
wait on server (tail of job) ◀──────────    ├─ LOCK destinations (as original) + source SHARE
                                            ├─ source manifest (as original)
                                            ├─ wave 1: {table3, table4}     ─┐ parallel workers over
                                            ├─ wave 2: {table1}              ├ source block ranges,
                                            ├─ wave 3: {table5,table6,table2}┘ checkpointed staging
                                            ├─ inserts streamed in original order, ids pre-assigned
                                            ├─ indexes rebuilt / FKs validated set-based
                                            └─ COMMIT (data + job state, atomically)
version registration (exactly once) ◀───
```

**Coordinator** — one psql session, one transaction. It takes the original's destination locks
(`SHARE ROW EXCLUSIVE`) plus a `SHARE` lock on the source, so nothing can change either while the job runs;
it computes the original manifest; it is the only session that writes the destinations, and nothing becomes
visible before its single `COMMIT`.

**Statement analysis** (`services/fast_expansion.py`, offset-aware SQL lexer) classifies each recipe statement:
* *row-local* (no aggregates, window functions, `GROUP BY`, `ORDER BY`/`LIMIT`, set operations, `.*`, and the
  source read exactly once by the top-level `FROM`) → evaluated in parallel over source block ranges;
* `SELECT DISTINCT` → per-range partial `DISTINCT` + one final `DISTINCT`;
* anything else → runs inside the coordinator exactly as in the original (and so does everything after it).

**Workers** — psql sessions launched by the coordinator; each evaluates a statement over a TID range of the source
(`ctid >= '(lo,0)' AND ctid < '(hi,0)'`) and stores the rows, already converted to the destination column types
by the same assignment casts, in an `UNLOGGED` staging table. Later statements read earlier destinations through
views (`existing rows ∪ staged rows with their identity values pre-assigned`), so they see exactly what they
would have seen inside the original transaction. Statements that do not depend on each other form a **wave** and
share one pass over each source range (3 source scans instead of 6 for the challenge recipes). Ranges are capped
at ~400k source rows and their count is a multiple of the worker count.

**Identity values** — rows are staged in the original plan's order (source physical position, then emission
order), each range's offset is the row count of the earlier ranges, and the sequence is advanced with `setval` to
exactly where `nextval()` would have left it. For `SELECT DISTINCT` statements the planner's choice is read with
`EXPLAIN`: sort-based plans are reproduced by the parallel reduce, hash-based ones run as in the original.

**Bulk load** — for destinations that receive a bulk of rows, the coordinator drops their foreign keys,
unique/primary-key constraints and indexes inside its transaction and recreates them with identical names,
definitions and comments: set-based index builds (parallel maintenance workers) and one validation query per
foreign key instead of per-row RI triggers. Any violation still aborts the whole transaction, as in the original.
Only done when every table referencing the destination is itself managed by the expansion and owned by the role.

**Pipelining** — when every destination starts empty (re-checked under the lock), workers never touch the
destination tables, so the coordinator inserts finished ranges *while the wave is still computing*, rebuilds a
table's indexes as soon as its last statement is inserted, and validates each foreign key as soon as both ends are
complete. (A foreign key that references the source is dropped only after the last wave: dropping an FK locks the
referenced table `ACCESS EXCLUSIVE`.)

**Batched preflight** — every read-only validation of the original (source existence and columns, Raw version
references, function safety, destination existence, managed dependencies, row counters with the same reconcile
logic) in one SSH command with one psql session per database. Same checks, same order, same error messages.

**JSON path hoisting** — in worker statements every `alias.col -> 'p' ->> 'k'` / `alias.col ->> 'k'` on a json/jsonb
source column that is a complete operand (checked against PostgreSQL operator precedence on both sides) is
computed once per source row in narrow columns: the sub-object once, each key once, behind `OFFSET 0` fences, and
the wide JSON column is dropped from the rows when every use was hoisted. json/jsonb `->`/`->>` with text keys
are immutable and cannot fail, so this is equivalent to evaluating them at each use. (`table2` alone performs 56
lookups per source row; 1M: −14 %.)

**Plan-independent order** — rows of one source row are staged ordered by source position, then by their own
values in the destination types, then emission order, so identity values never depend on the join strategy a
worker's planner picks (a hash join may emit rows that share a key in reverse).

**Planner hints** — worker sessions and FK validation use `random_page_cost = 1.1` (staged data is cached / on SSD):
joins of a range against the staged `table1` become index lookups instead of a full re-hash per range
(at 10M this cut spill files from 20 GB to 0.65 GB). Cost hints only; results are unchanged.

## 4. Failure recovery and consistency

| Failure | Original | This solution |
|---|---|---|
| SSH / laptop / client lost mid-run | psql gets SIGHUP, transaction aborts, all work lost | the job is detached on the server and finishes; the next connection registers its versions |
| Client lost after the data commit, before version registration | data committed **without** version records | registered automatically on reconnect, exactly once |
| Server-side job killed | everything recomputed | rerunning the same expansion resumes from the committed checkpoints, same identity values |
| PostgreSQL crash restart | everything recomputed | crash-emptied `UNLOGGED` staging is detected and recomputed; never committed empty |
| Cancel | rollback | the job's process group is stopped; rollback; checkpoints kept for resume |

Mechanisms:
* **Detached runner** — the job runs under `setsid nohup` on the server; the client waits for it there. Losing
  SSH does not touch it. If the coordinator's own psql dies, its backend notices within a second
  (`client_connection_check_interval`) and rolls back; a coordinator waiting for ranges stops if the job's workers
  (tagged by `application_name`) vanish; worker sessions use `lock_timeout`, so a lock-order bug fails resumably.
* **Job registry (outbox)** — `app_control.expansion_jobs` in the control database records each job with everything
  needed to finish its registration. The data commit also marks the data-side job `committed` (same transaction as
  the data). `recover_pending_expansions()` runs on every connect and registers committed-but-unregistered jobs;
  the registry is flipped to `registered` inside the same control transaction as the version rows, so registration
  happens exactly once.
* **Checkpoints** — every (statement, source range) staging table is created and marked done in its own committed
  transaction (`pgdm_expansion.units`). A resume is allowed only for the same recipes and scope, and only if a change
  token (source write counters, destination row counters and update/delete counters) is unchanged; the coordinator
  additionally refuses to commit if the source manifest changed. Identity sequences are restored to their recorded
  start so a resumed job produces exactly the same identity values.
* **Never commit wrong data** — before using a range the coordinator checks the checkpoint is complete, its offsets
  are consistent and its staging table is physically non-empty when it recorded rows (crash recovery empties
  unlogged tables); streamed rows must equal the checkpointed totals.
* **Cleanup** — staging schemas are dropped after the commit; abandoned jobs (different recipes, changed data) are
  cleaned up before a new job starts. Job and staging schemas are hidden from the table list.

* **Abandoned jobs** — when the source or recipes changed, the old job's checkpoints are dropped and its identity
  reservations are given back (only if no committed value lies beyond them), so the fresh job numbers exactly as a
  first run would.
* **Concurrent reconnects** — the registry row is locked before the exactly-once check, so two clients recovering
  the same job at once register it once.

**Validation on the final code** (`bench/recovery_suite.sh`, 1M rows; each scenario is compared with the untouched
original's exact output fingerprint and version-registry fingerprint; log in `bench/evidence/`):

| # | Injected failure | Result |
|---|---|---|
| S0 | none | identical (9.6 s vs 58.2 s) |
| S1 | client killed mid-run | server committed; reconnect registered; identical |
| S2 | client lost after the commit; two clients reconnect at once | registered exactly once; identical |
| S3 | job killed at 1 s / 3 s / 5 s (52, 102, 122 of 122 checkpoints; 0 destination rows) | resumed; identical |
| S4 | Cancel at 3 s (98/122 checkpoints, 0 rows) | resumed; identical |
| S5 | PostgreSQL crash at 3 s | resumed; identical |
| S6 | job killed, then crash (112 checkpointed unlogged tables emptied) | recomputed; identical |
| S7 | job killed, then a new raw row ingested | old job abandoned; identical to the original on the new data |
| S8 | second client starts while the first runs | refused ("still running"); first finishes identical |
| S9 | expansion repeated on expanded tables | refused like the original; destinations and versions unchanged |

Also observed at 50M rows in a real failure (a worker-liveness check misfired, since fixed): the interrupted job
resumed from 711 of 842 checkpoints, stopped the previous attempt's leftover workers first, and committed.

## 5. Identity values at large sizes

Up to 1M rows the output matches the original byte for byte, surrogate keys included. At 10M two runs of the
original SQL itself (untouched code, and the same batch inside the job runner) produced the same rows with
different surrogate keys. Above a quarter of
`shared_buffers` PostgreSQL synchronises sequential scans: a new scan of the source starts where an earlier one
left off. The original's own `table1` insert therefore starts mid-table (observed at 10M: the first identity value
goes to `raw_id` 9,997,633) and its identity values depend on earlier scans, not on the data. Above that threshold
this solution produces deterministic identity values (source order) and identical rows; it detects the threshold
from the catalog and, below it, reproduces the original exactly (including hash-ordered `DISTINCT` statements).

## 6. Trade-offs and limitations

* Staging needs temporary space of roughly one copy of the output (unlogged, no WAL), released at commit.
* The source is `SHARE`-locked for the duration (writes to `raw_data` wait); the original only took a snapshot.
* The job schema `pgdm_expansion` (job and checkpoint tables) is added to the data database, like the original's
  row-counter table.
* Parallel execution needs PostgreSQL ≥ 14 (TID range scans) and `CREATE` on the database; otherwise, and for
  statements that are not provably row-local, the original serial batch runs (with the same recovery).
* Identity values follow the source's physical order (= `raw_id` order for generated and append-only raw data). If
  a `raw_data` row is updated in place it moves; the rows stay identical, but `table2` / `table5` surrogate keys
  can then differ from the original's, whose merge-join plan reads those inserts in `raw_id` order.
* A failed attempt keeps its staging schema (that is what makes the resume possible) until the next expansion of
  the database resumes or abandons it.
* Validating a foreign key that references the source reads the source heap unless its visibility map is set
  (autovacuum); about 10 s at 10M rows.
* Credentials no longer appear on server command lines for the new job (environment / stdin only).
* Tried and rejected: loading staged ranges with `COPY ... FROM PROGRAM` (batched multi-row inserts). In isolation
  it loads 23M rows 23 % faster than `INSERT ... SELECT`, but inside the running job (20 workers competing for CPU,
  I/O and WAL) it gave no gain at 10M and cost extra start-up at small sizes, so the simpler path stays.

## 7. Reproduce

```
bench/server/            PostgreSQL 16 + OpenSSH + psql server image used for testing (run with --init)
bench/headless_expand.py headless Expand with the GUI's report formatter; credentials via PDM_* env vars
bench/fingerprint.sql    exact + logical output fingerprints
bench/monitor.sh         peak memory / spill / staging / WAL sampler
bench/control_fingerprint.sql  version-registry fingerprint (versions + dependencies, volatile fields excluded)
bench/recovery_suite.sh  failure scenarios S0-S9 against the untouched original (evidence in bench/evidence/)
tests/test_fast_expansion.py  statement analysis, rewriting, planning and rendering tests
```

`PDM_FAST_EXPAND=0` forces the original serial batch; `PDM_BATCH_PREFLIGHT=0` restores the original preflight.
