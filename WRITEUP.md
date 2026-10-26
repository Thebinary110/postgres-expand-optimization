# Parallel, resumable Expand: 5.6–6.3× faster, same output, survives every failure we could inject

**Submission commit:** `c8c6294f4aef446d20bc602b48d2504be9489004` (code; later commits change documentation only). Full engineering report: [REPORT.md](REPORT.md).

## Summary

The DB Manager's Expand step turns `raw_data.raw` (JSONB) into six relational tables with the six challenge
recipes, in one transaction, on one CPU core. This submission keeps the recipes, the generator, the seed and every
guarantee of the original (validation, function-safety checks, managed row counters, one atomic data commit, the
source manifest, version registration) and changes *how* the work is executed:

* the recipes are analysed and every provably row-local statement runs in parallel over physical source ranges,
  on many PostgreSQL sessions, into unlogged staging tables;
* staged rows are streamed into the destinations inside **one** coordinator transaction, with constraints and
  indexes rebuilt once instead of maintained row by row;
* the job runs detached on the server with committed checkpoints, so a lost client, a killed job, a cancel or a
  PostgreSQL crash never loses finished work and never commits wrong data.

## Results (MEASURED PROCESSING TOTAL, application's own report)

| Rows | Original (untouched code) | This submission | Speed-up | Output vs original |
|---:|---:|---:|---:|---|
| 100,000 | 6.45 s | **1.88 s** | 3.4× | byte-identical, identity values included |
| 1,000,000 | 59.5 s | **9.9 s** | 6.0× | byte-identical, identity values included |
| 10,000,000 | 10 min 12 s | **1 min 49 s** | 5.6× | identical rows (see "Identity values") |
| 50,000,000 | 59 min 43 s | **9 min 32 s** | 6.3× | identical rows; `table2` (116M rows) and `table5` byte-identical |

Same server, same snapshot for every run (PostgreSQL 16 in Docker, 16 CPUs, 32 GB, `shared_buffers=4GB`; ARM,
20 cores). Output equality is checked with an exact hash of every table (all columns, surrogate keys included), a
logical hash (surrogate keys replaced by natural keys), the row-counter table, and a hash of the version registry.

**Resources at 10M rows:** peak process memory 2.6 GB (original 0.34 GB), spill files 0.65 GB (same as the
original), staging 4.9 GB (about one extra copy of the output, unlogged, released at commit), WAL 6.4 GB (original
8.6 GB). At 50M: 2.2 GB memory, 3.5 GB spill, 24.7 GB staging.

## What made it fast

1. **Parallel row-local statements.** An SQL lexer classifies each recipe statement. Statements whose output rows
   each depend on one source row run per TID range (`ctid` range scans) on N sessions; independent statements share
   one pass over the source ("waves"). `DISTINCT` and other global statements stay serial where order matters.
2. **JSON path hoisting.** Repeated `raw -> 'values' ->> 'x'` lookups are computed once per row into narrow
   columns behind an optimisation fence, which also cut spill files from 20 GB to 0.65 GB at 10M rows.
3. **Bulk constraint handling.** Foreign keys, primary keys and unique indexes are dropped inside the transaction
   and rebuilt (and validated) once, instead of firing an RI trigger and an index update per row.
4. **Pipelined coordinator.** Destinations are filled range by range while later ranges are still being
   computed; identity values are pre-assigned per range with offsets so they equal what the serial `nextval()`
   calls would have produced.
5. **Batched preflight.** About 35 SSH round trips (each a shell, psql start-up and SCRAM login) became one
   command with the control and data sessions running concurrently.

## Failure recovery (secondary goal)

| Failure | Original | This submission |
|---|---|---|
| Client / SSH lost mid-run | transaction aborted, all work lost | job keeps running on the server; next connection registers it |
| Client lost after the data commit | data committed **without** version records | registered on reconnect, exactly once |
| Server-side job killed | everything recomputed | rerun resumes from committed checkpoints, same identity values |
| Cancel | rollback | rollback; checkpoints kept, rerun resumes |
| PostgreSQL crash | everything recomputed | crash-emptied staging detected and recomputed, never committed empty |
| Source changes between attempts | – | checkpoints refused (change token), fresh job, reserved identity values given back |

**Validated on the submission commit** (`bench/recovery_suite.sh`, 1M rows, every scenario compared with the
untouched original's destinations *and* version registry):

| # | Injected failure | State right after the failure | Result after reconnect / rerun |
|---|---|---|---|
| S0 | none (clean run, 9.6 s vs 58.2 s) | – | identical |
| S1 | client killed mid-run | job keeps running on the server and commits | registered on reconnect; identical |
| S2 | client lost after the commit; two clients reconnect at once | data committed, versions pending | registered exactly once; identical |
| S3 | job killed 1 s / 3 s / 5 s in | 52, 102, 122 of 122 checkpoints; 0 destination rows | resumed; identical |
| S4 | Cancel pressed 3 s in | 98/122 checkpoints; 0 destination rows | resumed; identical |
| S5 | PostgreSQL crash 3 s in | 0 destination rows | resumed; identical |
| S6 | job killed, then PostgreSQL crash (empties 112 checkpointed unlogged tables) | 0 destination rows | emptied checkpoints recomputed; identical |
| S7 | job killed, then a new raw row is ingested | stale checkpoints | old job abandoned, fresh run; identical to the original on the new data |
| S8 | second client starts the same expansion | refused: "still running on the server" | first job finishes; identical |
| S9 | expansion run again on already expanded tables | original: duplicate key; this code: duplicate key (index rebuild) | both refuse; destinations and versions unchanged |

"Identical" means equal exact fingerprints of all six destination tables (identity values included), the row
counters, and the version registry (versions and dependencies). Log: `bench/evidence/recovery_1m_summary.txt`.

## Identity values

Up to 1M rows the output is byte-identical to the original, surrogate keys included. Above a quarter of
`shared_buffers`, PostgreSQL synchronises sequential scans, so the original's own identity values depend on where
an earlier scan stopped: two runs of the untouched code on the same data produced the same rows with different
keys. There this submission assigns keys deterministically in source order and produces identical rows.

## Trade-offs

* Temporary space of about one copy of the output (unlogged staging), released at commit.
* `raw_data` is `SHARE`-locked during the expansion (writes wait); the original only took a snapshot.
* Higher peak memory than the single-session original (parallel sessions).
* If a `raw_data` row is updated in place (raw tables are append-only in the application), its physical position
  no longer follows `raw_id`; the rows stay identical, but surrogate keys of `table2` / `table5` may then differ
  from the original's, whose plan reads those tables in `raw_id` order.

## Reproduce

`bench/server/` (test server image), `bench/headless_expand.py` (headless Expand with the GUI's report formatter),
`bench/fingerprint.sql` / `bench/control_fingerprint.sql` (output and registry fingerprints),
`bench/recovery_suite.sh` (failure scenarios), `bench/monitor.sh` (resource sampler). `PDM_FAST_EXPAND=0`
forces the original serial path.
