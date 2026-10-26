"""Parallel, checkpointed execution plan for the cross-table Expand operation.

The original Expand runs every recipe statement serially inside one PostgreSQL transaction
(PostgreSQL never parallelises INSERT ... SELECT), maintaining every index and checking every
foreign key row by row. This module builds an equivalent plan that keeps the same guarantees:

* Coordinator session (one transaction, READ COMMITTED): locks the destinations exactly like the
  original (SHARE ROW EXCLUSIVE) and additionally locks the source in SHARE mode, so neither can change
  while the job runs; builds the same source manifest; and is the only session that ever writes the
  destinations. Nothing becomes visible until its single COMMIT, so the expansion stays atomic.
* Workers (parallel psql sessions launched by the coordinator through psql's ``\\!``): evaluate each
  row-local recipe statement over a disjoint block range of the source (TID range scans) and store the
  rows, already converted to the destination column types, in UNLOGGED staging tables. ``SELECT
  DISTINCT`` statements are computed as per-range partial DISTINCTs plus one final DISTINCT. Later
  statements read earlier destinations through views (existing rows UNION ALL staged rows with their
  identity values pre-assigned), exactly as they would have seen them inside the original transaction.
* Every staging unit commits on its own and is recorded in ``pgdm_expansion.units``: these are the
  checkpoints that make an interrupted job resumable.
* The coordinator then moves the staged rows into the destinations in the original order, assigning
  the identity values the original would have assigned. For bulk loads it drops the destination's
  secondary indexes and foreign keys first and recreates them afterwards with their original
  definitions and names (set-based index builds and FK validation instead of per-row maintenance and
  per-row RI triggers); any violation still aborts the whole transaction.

Statements this module cannot prove row-local (aggregates, window functions, GROUP BY, ORDER BY/LIMIT,
set operations, more than one source reference, ...) and everything after them run serially inside the
coordinator transaction, i.e. exactly as before.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import shlex

STAGE_SCHEMA_PREFIX = "pgdm_xs_"
JOB_SCHEMA = "pgdm_expansion"
DISTINCT_SORT_SAFE = True


def qi(name: str) -> str:
    return '"' + str(name).replace('"', '""') + '"'


def ql(value) -> str:
    if value is None:
        return "NULL"
    return "'" + str(value).replace("'", "''") + "'"


# --------------------------------------------------------------------------- lexer with offsets
_WORD = re.compile(r"[A-Za-z_\u0080-￿][A-Za-z0-9_$\u0080-￿]*")
_NUMBER = re.compile(r"(?:\d+\.?\d*(?:[eE][+-]?\d+)?|\.\d+(?:[eE][+-]?\d+)?)")
_DOLLAR = re.compile(r"\$([A-Za-z_][A-Za-z0-9_]*)?\$")


def lex(sql: str) -> list[tuple[str, str, int, int, int]]:
    """-> [(kind, value, start, end, depth)]; kind in word/ident/string/number/symbol.
    word values are lower-cased; ident values are unescaped quoted identifiers."""
    out, i, n, depth = [], 0, len(sql), 0
    while i < n:
        c = sql[i]
        nxt = sql[i + 1] if i + 1 < n else ""
        if c.isspace():
            i += 1
        elif c == "-" and nxt == "-":
            j = sql.find("\n", i)
            i = n if j < 0 else j + 1
        elif c == "/" and nxt == "*":
            level, i = 1, i + 2
            while i < n and level:
                if sql.startswith("/*", i):
                    level, i = level + 1, i + 2
                elif sql.startswith("*/", i):
                    level, i = level - 1, i + 2
                else:
                    i += 1
            if level:
                raise ValueError("unterminated block comment")
        elif c == "'" or (c in "eEnN" and nxt == "'" and not (out and out[-1][3] == i and out[-1][0] == "word")):
            start = i
            backslash = c in "eE"
            i = i + 1 if c == "'" else i + 2
            while i < n:
                if backslash and sql[i] == "\\":
                    i += 2
                    continue
                if sql[i] == "'":
                    if i + 1 < n and sql[i + 1] == "'":
                        i += 2
                        continue
                    i += 1
                    break
                i += 1
            else:
                raise ValueError("unterminated string literal")
            out.append(("string", sql[start:i], start, i, depth))
        elif c == '"':
            start, i, buf = i, i + 1, []
            while i < n:
                if sql[i] == '"':
                    if i + 1 < n and sql[i + 1] == '"':
                        buf.append('"')
                        i += 2
                        continue
                    i += 1
                    break
                buf.append(sql[i])
                i += 1
            else:
                raise ValueError("unterminated quoted identifier")
            out.append(("ident", "".join(buf), start, i, depth))
        elif c == "$" and _DOLLAR.match(sql, i) and not (out and out[-1][3] == i and out[-1][0] in ("word", "number")):
            tag = _DOLLAR.match(sql, i).group(0)
            j = sql.find(tag, i + len(tag))
            if j < 0:
                raise ValueError("unterminated dollar-quoted string")
            out.append(("string", sql[i:j + len(tag)], i, j + len(tag), depth))
            i = j + len(tag)
        elif _WORD.match(sql, i):
            m = _WORD.match(sql, i)
            out.append(("word", m.group(0).lower(), i, m.end(), depth))
            i = m.end()
        elif c.isdigit() or (c == "." and nxt.isdigit()):
            m = _NUMBER.match(sql, i)
            out.append(("number", m.group(0), i, m.end(), depth))
            i = m.end()
        else:
            if c == "(":
                out.append(("symbol", c, i, i + 1, depth))
                depth += 1
            elif c == ")":
                depth -= 1
                out.append(("symbol", c, i, i + 1, depth))
            else:
                out.append(("symbol", c, i, i + 1, depth))
            i += 1
    return out


def _name(tok) -> str | None:
    if tok[0] == "word":
        return tok[1]
    if tok[0] == "ident":
        return tok[1]
    return None


# --------------------------------------------------------------------------- statement analysis
SOURCE_SENTINEL = "__pgdm_fast_source__"
NON_LOCAL_TOP_WORDS = {"group", "having", "window", "order", "limit", "offset", "fetch", "union",
                       "intersect", "except", "into", "for"}


def analyse_statement(statement: str, source_sql: str, aggregate_names: set[str]) -> dict:
    """Classify one prepared expansion statement (the source placeholder already replaced by
    ``source_sql``). -> {mode: parallel|distinct|serial, reason, query_start, target_span, ...}"""
    if statement.count(source_sql) != 1:
        return {"mode": "serial", "reason": "the source is referenced more than once"}
    probe = statement.replace(source_sql, SOURCE_SENTINEL)
    try:
        toks = lex(probe)
    except ValueError as exc:
        return {"mode": "serial", "reason": f"lexer: {exc}"}
    if len(toks) < 4 or toks[0][1] != "insert" or toks[1][1] != "into":
        return {"mode": "serial", "reason": "not INSERT INTO"}
    # target relation: name [. name]
    j = 2
    if j + 2 < len(toks) and toks[j + 1][1] == "." and toks[j + 1][0] == "symbol":
        j += 3
    else:
        j += 1
    target_span = (toks[2][2], toks[j - 1][3])
    insert_columns = []
    if j < len(toks) and toks[j][0] == "symbol" and toks[j][1] == "(":
        depth0 = toks[j][4]
        k = j + 1
        while k < len(toks) and not (toks[k][0] == "symbol" and toks[k][1] == ")" and toks[k][4] == depth0):
            if toks[k][0] in ("word", "ident"):
                insert_columns.append(toks[k][1])
            elif not (toks[k][0] == "symbol" and toks[k][1] == ","):
                return {"mode": "serial", "reason": "unsupported INSERT column list"}
            k += 1
        j = k + 1
    else:
        return {"mode": "serial", "reason": "INSERT without a column list"}
    if j >= len(toks):
        return {"mode": "serial", "reason": "no query"}
    query = toks[j:]
    if query[0][0] != "word" or query[0][1] != "select" or query[0][4] != 0:
        return {"mode": "serial", "reason": "query is not a plain top-level SELECT"}
    sentinel = [t for t in toks if t[0] == "word" and t[1] == SOURCE_SENTINEL]
    if len(sentinel) != 1 or sentinel[0][4] != 0:
        return {"mode": "serial", "reason": "the source is not read directly by the top-level FROM"}
    top_words = [t[1] for t in query if t[0] == "word" and t[4] == 0]
    if NON_LOCAL_TOP_WORDS.intersection(top_words):
        return {"mode": "serial", "reason": "uses " + ", ".join(sorted(NON_LOCAL_TOP_WORDS.intersection(top_words)))}
    if "over" in [t[1] for t in query if t[0] == "word"]:
        return {"mode": "serial", "reason": "uses a window function"}
    for idx, t in enumerate(query):
        if (t[0] == "word" and t[4] == 0 and t[1] in aggregate_names and idx + 1 < len(query)
                and query[idx + 1][1] == "(" and not (idx and query[idx - 1][1] == ".")):
            return {"mode": "serial", "reason": f"uses aggregate {t[1]}()"}
    mode = "parallel"
    if len(query) > 1 and query[1][0] == "word" and query[1][1] == "distinct":
        if len(query) > 2 and query[2][0] == "word" and query[2][1] == "on":
            return {"mode": "serial", "reason": "uses DISTINCT ON"}
        mode = "distinct"
    # end of the top-level select list, and the source's alias: row-local statements stage their rows
    # ordered by the source row's physical position, the order the original plans scan the source in
    from_tok = next((t for t in query if t[0] == "word" and t[1] == "from" and t[4] == 0), None)
    si = toks.index(sentinel[0])
    alias = None
    if si + 1 < len(toks):
        nxt = toks[si + 1]
        if nxt[0] == "word" and nxt[1] == "as" and si + 2 < len(toks) and toks[si + 2][0] in ("word", "ident"):
            alias = probe[toks[si + 2][2]:toks[si + 2][3]]
        elif nxt[0] == "ident" or (nxt[0] == "word" and nxt[1] not in _NOT_ALIAS):
            alias = probe[nxt[2]:nxt[3]]
    if mode == "parallel" and (from_tok is None or alias is None):
        return {"mode": "serial", "reason": "the source has no alias"}
    if mode == "parallel" and any(toks[k][1] == "*" and toks[k - 1][1] == "." for k in range(1, len(toks))):
        return {"mode": "serial", "reason": "expands a relation with .*"}
    return {"mode": mode, "reason": "", "query_start": query[0][2], "target_span": target_span,
            "from_pos": from_tok[2] if from_tok else None, "source_alias": alias, "insert_columns": insert_columns}


_NOT_ALIAS = {"where", "join", "inner", "left", "right", "full", "cross", "natural", "on", "using", "group",
              "order", "limit", "offset", "window", "union", "intersect", "except", "for", "fetch", "lateral"}


def rewrite_relations(sql: str, managed: dict[tuple[str, str], str]) -> str:
    """Replace schema-qualified references to managed relations (schema, table) -> new schema name.
    Unqualified references are redirected through search_path by the caller."""
    toks = lex(sql)
    edits = []
    for i in range(len(toks) - 2):
        a, dot, b = toks[i], toks[i + 1], toks[i + 2]
        if dot[0] != "symbol" or dot[1] != ".":
            continue
        sa, sb = _name(a), _name(b)
        if sa is None or sb is None:
            continue
        if i > 0 and toks[i - 1][0] == "symbol" and toks[i - 1][1] == ".":
            continue
        if i + 3 < len(toks) and toks[i + 3][0] == "symbol" and toks[i + 3][1] in (".", "("):
            continue
        key = (sa, sb)
        if key in managed:
            edits.append((a[2], a[3], qi(managed[key])))
    for start, end, text in sorted(edits, reverse=True):
        sql = sql[:start] + text + sql[end:]
    return sql


# --------------------------------------------------------------------------- job bookkeeping
JOB_DDL = f"""
CREATE SCHEMA IF NOT EXISTS {qi(JOB_SCHEMA)};
CREATE TABLE IF NOT EXISTS {qi(JOB_SCHEMA)}.jobs (
    job_id text PRIMARY KEY,
    signature text NOT NULL,
    state text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    attempts integer NOT NULL DEFAULT 0,
    payload jsonb NOT NULL DEFAULT '{{}}'::jsonb
);
CREATE TABLE IF NOT EXISTS {qi(JOB_SCHEMA)}.units (
    job_id text NOT NULL,
    unit_id text NOT NULL,
    segment text NOT NULL,
    state text NOT NULL DEFAULT 'pending',
    rows bigint,
    finished_at timestamptz,
    PRIMARY KEY (job_id, unit_id)
);
""".strip()


def job_setup_sql(job_id: str, signature_value: str, sequences: list[str], token_sql: str = "NULL") -> str:
    """Register (or re-open) the job; the identity sequences' state is recorded on the first attempt only,
    so every attempt can restore it and assign exactly the same identity values."""
    seq_json = (" || ".join(
        f"jsonb_build_object({ql(seq)}, (SELECT jsonb_build_array(last_value, is_called) FROM {seq}))"
        for seq in sequences) or "'{}'::jsonb")
    return (
        "\\set ON_ERROR_STOP 1\nSET client_min_messages = warning;\nBEGIN;\n" + JOB_DDL + "\n"
        f"INSERT INTO {qi(JOB_SCHEMA)}.jobs (job_id, signature, state, attempts, payload)\n"
        f"VALUES ({ql(job_id)}, {ql(signature_value)}, 'running', 1, "
        f"jsonb_build_object('sequences', {seq_json}, 'token', {token_sql}))\n"
        "ON CONFLICT (job_id) DO UPDATE SET state = 'running', attempts = jobs.attempts + 1, "
        "updated_at = clock_timestamp();\n"
        # staging tables are UNLOGGED: crash recovery empties them (and pg_postmaster_start_time() does not
        # move on a crash restart), so a finished checkpoint whose table is now missing or empty although it
        # recorded rows is recomputed - deterministically, so its identity offsets stay valid
        f"UPDATE {qi(JOB_SCHEMA)}.units u SET state = 'pending' WHERE u.job_id = {ql(job_id)} AND u.state = 'done' "
        f"AND u.rows > 0 AND COALESCE(pg_relation_size(to_regclass(format('%I.%I', "
        f"{ql(STAGE_SCHEMA_PREFIX + job_id)}, u.unit_id))), 0) = 0;\n"
        "COMMIT;\n")


def job_setup_step(job_dir: str, job_id: str, first_views: str | None) -> list[str]:
    """Coordinator lines (before its transaction) that register the job and, for parallel plans, create
    the checkpoint units, staging schema and first views. They commit on their own (workers must see
    them) and are timed so the setup is part of the measured total."""
    body = [f"\\! psql -X -q -v ON_ERROR_STOP=1 -f {shlex.quote(job_dir + '/setup.sql')}"]
    if first_views:
        body += [f"\\! psql -X -q -v ON_ERROR_STOP=1 -f {shlex.quote(job_dir + '/stage_setup.sql')}",
                 f"\\! psql -X -q -v ON_ERROR_STOP=1 -f {shlex.quote(job_dir + '/' + first_views)}",
                 f"SELECT 1 / (SELECT count(*)::int FROM {qi(JOB_SCHEMA)}.units WHERE job_id = {ql(job_id)}) "
                 "AS setup_check \\g /dev/null"]
    body.append(f"SELECT 1 / (SELECT count(*)::int FROM {qi(JOB_SCHEMA)}.jobs WHERE job_id = {ql(job_id)} "
                "AND state = 'running') AS job_check \\g /dev/null")
    t = _Timings()
    t.around(0, "Register job and prepare checkpoints", "\n".join(body), stage="prepare")
    return t.lines


def restore_sequences_sql(job_id: str, sequences: list[str]) -> str:
    return "\n".join(
        f"SELECT setval({ql(seq)}, (s.v->>0)::bigint, (s.v->>1)::boolean) FROM "
        f"(SELECT payload->'sequences'->{ql(seq)} AS v FROM {qi(JOB_SCHEMA)}.jobs WHERE job_id = {ql(job_id)}) s "
        "WHERE s.v IS NOT NULL \\g /dev/null" for seq in sequences)


def commit_marker_sql(job_id: str, manifest_sql: str) -> str:
    """Inside the expansion transaction: refuse to finish a resumed job whose source changed, store the
    manifest (what the version registration needs) and mark the job committed atomically with the data."""
    select = manifest_sql[manifest_sql.rindex("SELECT json_build_object("):].rstrip().rstrip(";").rstrip()
    if select.endswith("::text"):
        select = select[:-len("::text")]
    return (
        f"DO $pgdm_job_manifest$ BEGIN\n"
        f"  IF EXISTS (SELECT 1 FROM {qi(JOB_SCHEMA)}.jobs WHERE job_id = {ql(job_id)} AND payload ? 'manifest'\n"
        f"             AND payload->'manifest' <> ({select})::jsonb) THEN\n"
        f"    RAISE EXCEPTION 'The source changed since this expansion was checkpointed; it must be restarted.';\n"
        f"  END IF;\nEND $pgdm_job_manifest$;\n"
        f"UPDATE {qi(JOB_SCHEMA)}.jobs SET payload = payload || jsonb_build_object('manifest', ({select})::jsonb), "
        f"updated_at = clock_timestamp() WHERE job_id = {ql(job_id)};")


def runner_script(job_dir: str, files: dict, env: dict) -> str:
    """Materialise the job directory and run the coordinator detached from the SSH session: stdout and
    stderr go to files, the exit status to `status`. The client waits for the process and then reads them;
    if the SSH connection drops the job carries on and can be collected later."""
    lines = ["set -u", f"d={shlex.quote(job_dir)}", 'mkdir -p "$d"', 'chmod 700 "$d"', 'rm -f "$d/status"']
    for k, v in env.items():
        lines.append(f"export {k}={shlex.quote(str(v))}")
    for name, content in files.items():
        tag = "PGDM_FAST_EOF"
        while tag in content:
            tag += "_X"
        lines.append(f"cat > \"$d/{name}\" <<'{tag}'\n{content}{'' if content.endswith(chr(10)) else chr(10)}{tag}")
    lines.append('cat > "$d/run.sh" <<\'PGDM_RUN_EOF\'\n'
                 'd="$(cd "$(dirname "$0")" && pwd)"\n'
                 'psql -X -qAt -v ON_ERROR_STOP=1 -f "$d/coordinator.sql" >"$d/coordinator.out" 2>"$d/coordinator.err"\n'
                 'status=$?\n'
                 'if [ $status -ne 0 ] && [ -s "$d/workers.log" ]; then grep -m 20 -E "ERROR|FAILED" "$d/workers.log" >>"$d/coordinator.err"; fi\n'
                 'echo $status > "$d/status.tmp" && mv "$d/status.tmp" "$d/status"\n'
                 'PGDM_RUN_EOF')
    lines.append('setsid nohup bash "$d/run.sh" </dev/null >/dev/null 2>&1 & echo $! > "$d/pid"')
    lines.append('echo "PGDM_JOB_STARTED $(cat "$d/pid")"')
    return "\n".join(lines) + "\n"


# --------------------------------------------------------------------------- catalog inspection
def catalog_sql(source_schema: str, source_table: str, destinations: list[tuple[str, str]], function_names: list[str]) -> str:
    dest_values = ", ".join(f"({ql(s)}, {ql(t)})" for s, t in destinations) or "(NULL, NULL)"
    fn_values = ", ".join(f"({ql(f)})" for f in sorted(set(function_names))) or "(NULL)"
    return f"""
WITH dest(schema_name, table_name) AS (VALUES {dest_values}),
rel AS (
    SELECT d.schema_name, d.table_name, c.oid AS relid
    FROM dest d JOIN pg_namespace n ON n.nspname = d.schema_name
    JOIN pg_class c ON c.relnamespace = n.oid AND c.relname = d.table_name AND c.relkind = 'r'
)
SELECT json_build_object(
  'server_version_num', current_setting('server_version_num')::int,
  'max_connections', current_setting('max_connections')::int,
  'connections_in_use', (SELECT count(*) FROM pg_stat_activity),
  'can_create_schema', has_database_privilege(current_database(), 'CREATE'),
  'search_path', current_setting('search_path'),
  'source_blocks', (SELECT pg_relation_size(c.oid) / current_setting('block_size')::int
                    FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
                    WHERE n.nspname = {ql(source_schema)} AND c.relname = {ql(source_table)}),
  'source_rows_estimate', (SELECT c.reltuples::bigint FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
                           WHERE n.nspname = {ql(source_schema)} AND c.relname = {ql(source_table)}),
  'aggregate_names', (SELECT COALESCE(json_agg(DISTINCT p.proname), '[]'::json)
                      FROM pg_proc p WHERE p.prokind IN ('a', 'w')
                        AND p.proname IN (SELECT column1 FROM (VALUES {fn_values}) v)),
  'destinations', (SELECT COALESCE(json_agg(json_build_object(
      'schema_name', r.schema_name, 'table_name', r.table_name,
      'owned', pg_has_role(c.relowner, 'USAGE'),
      'rows_estimate', GREATEST(c.reltuples, 0)::bigint,
      'has_rules', c.relhasrules,
      'user_triggers', (SELECT count(*) FROM pg_trigger t WHERE t.tgrelid = r.relid AND NOT t.tgisinternal),
      'columns', (SELECT json_agg(json_build_object(
            'name', a.attname, 'type', format_type(a.atttypid, a.atttypmod), 'notnull', a.attnotnull,
            'identity', a.attidentity, 'generated', a.attgenerated,
            'default', pg_get_expr(ad.adbin, ad.adrelid),
            'sequence', seq.seqname,
            'seq_increment', sq.seqincrement, 'seq_cycle', sq.seqcycle)
            ORDER BY a.attnum)
          FROM pg_attribute a LEFT JOIN pg_attrdef ad ON ad.adrelid = a.attrelid AND ad.adnum = a.attnum
          LEFT JOIN LATERAL (SELECT pg_get_serial_sequence(quote_ident(r.schema_name) || '.' || quote_ident(r.table_name),
                                                           a.attname) AS seqname) seq ON true
          LEFT JOIN pg_sequence sq ON sq.seqrelid = to_regclass(seq.seqname)
          WHERE a.attrelid = r.relid AND a.attnum > 0 AND NOT a.attisdropped),
      'constraints', (SELECT COALESCE(json_agg(json_build_object(
            'name', con.conname, 'type', con.contype, 'def', pg_get_constraintdef(con.oid),
            'validated', con.convalidated,
            'can_reference', CASE WHEN con.contype = 'f' THEN has_table_privilege(con.confrelid, 'REFERENCES') END,
            'comment', obj_description(con.oid, 'pg_constraint')) ORDER BY con.conname), '[]'::json)
          FROM pg_constraint con WHERE con.conrelid = r.relid AND con.contype IN ('p', 'u', 'f', 'x')),
      'incoming_fks', (SELECT COALESCE(json_agg(json_build_object(
            'name', con.conname, 'schema_name', n2.nspname, 'table_name', c2.relname,
            'def', pg_get_constraintdef(con.oid), 'validated', con.convalidated,
            'owned', pg_has_role(c2.relowner, 'USAGE'),
            'can_reference', has_table_privilege(con.confrelid, 'REFERENCES'),
            'comment', obj_description(con.oid, 'pg_constraint'))), '[]'::json)
          FROM pg_constraint con JOIN pg_class c2 ON c2.oid = con.conrelid
          JOIN pg_namespace n2 ON n2.oid = c2.relnamespace
          WHERE con.confrelid = r.relid AND con.contype = 'f' AND con.conrelid <> r.relid),
      'indexes', (SELECT COALESCE(json_agg(json_build_object(
            'name', ic.relname, 'def', pg_get_indexdef(i.indexrelid),
            'constraint', (SELECT con.conname FROM pg_constraint con WHERE con.conindid = i.indexrelid
                           AND con.conrelid = r.relid AND con.contype IN ('p', 'u', 'x')),
            'comment', obj_description(i.indexrelid, 'pg_class'),
            'columns', (SELECT json_agg(a.attname ORDER BY k.ord) FROM unnest(i.indkey) WITH ORDINALITY k(attnum, ord)
                        JOIN pg_attribute a ON a.attrelid = r.relid AND a.attnum = k.attnum),
            'expression', i.indexprs IS NOT NULL OR i.indpred IS NOT NULL) ORDER BY ic.relname), '[]'::json)
          FROM pg_index i JOIN pg_class ic ON ic.oid = i.indexrelid WHERE i.indrelid = r.relid)
  )), '[]'::json) FROM rel r JOIN pg_class c ON c.oid = r.relid)
)::text;
""".strip()


# --------------------------------------------------------------------------- planning
class Plan:
    def __init__(self):
        self.segments = []          # one dict per prepared statement, in execution order
        self.workers = 1            # concurrent worker sessions
        self.units = 1              # source block ranges per parallel segment
        self.bulk = {}              # (schema, table) -> drop/recreate secondary structures
        self.reasons = []           # why the whole job is serial (if it is)

    @property
    def parallel(self) -> bool:
        return any(s["mode"] != "serial" for s in self.segments)


def distinct_statements(prepared: dict, source_sql: str, catalog: dict) -> list[tuple[int, str]]:
    """(segment index, statement) of every SELECT DISTINCT statement: the caller EXPLAINs them, because
    the identity values of their rows depend on the order the original plan emits them in."""
    aggregates = set(catalog.get("aggregate_names") or [])
    out, n = [], 0
    for dest in prepared["destinations"]:
        for stmt in dest["statements"]:
            if analyse_statement(stmt, source_sql, aggregates)["mode"] == "distinct":
                out.append((n, stmt))
            n += 1
    return out


def build_plan(prepared: dict, source_sql: str, catalog: dict, worker_slots: int,
               hashed_distinct: set[int] = frozenset(), min_rows_per_unit: int = 5_000) -> Plan:
    plan = Plan()
    if catalog.get("server_version_num", 0) < 140000:
        plan.reasons.append("PostgreSQL < 14 has no TID range scans")
    if not catalog.get("can_create_schema"):
        plan.reasons.append("the role cannot create the staging schema")
    dest_meta = {(d["schema_name"], d["table_name"]): d for d in catalog.get("destinations") or []}
    if len({k[1] for k in dest_meta}) != len(dest_meta):
        plan.reasons.append("two destinations share a table name")
    aggregates = set(catalog.get("aggregate_names") or [])
    parallel_seen = serial_tail = False
    for di, dest in enumerate(prepared["destinations"]):
        key = (dest["schema_name"], dest["pure_table_name"])
        meta = dest_meta.get(key)
        for si, stmt in enumerate(dest["statements"], start=1):
            info = analyse_statement(stmt, source_sql, aggregates)
            seg = {"dest_index": di, "statement_index": si, "table_name": dest["table_name"], "key": key,
                   "statement": stmt, **info}
            seg["columns"] = list(info.get("insert_columns") or dest.get("destination_columns") or [])
            if plan.reasons:
                seg.update(mode="serial", reason="; ".join(plan.reasons))
            elif meta is None:
                seg.update(mode="serial", reason="destination metadata unavailable")
            elif meta["user_triggers"] or meta["has_rules"]:
                seg.update(mode="serial", reason="destination has triggers or rules")
            else:
                problem = _column_problem(meta, seg["columns"])
                if problem:
                    seg.update(mode="serial", reason=problem)
            if seg["mode"] == "distinct" and len(plan.segments) in hashed_distinct:
                # The original plan hashes: its identity order is hash-table order, so the statement runs
                # exactly as in the original (inside the coordinator) and its rows are handed to workers.
                seg.update(mode="serial", reason="the original plan aggregates by hashing")
            if seg["mode"] == "serial" and not parallel_seen:
                seg["serial_kind"] = "prefix"
            elif seg["mode"] == "serial" or serial_tail:
                seg.update(mode="serial", serial_kind="tail",
                           reason=seg.get("reason") or "follows a serial statement")
                serial_tail = True
            parallel_seen = parallel_seen or seg["mode"] != "serial"
            seg["name"] = f"s{len(plan.segments)}"
            plan.segments.append(seg)
    for i, seg in enumerate(plan.segments):
        seg["export"] = (seg.get("serial_kind") == "prefix"
                         and any(s["mode"] != "serial" for s in plan.segments[i + 1:]))
    # a segment is "read later" if any later statement reads its destination
    reads = {}
    for dest in prepared["destinations"]:
        reads[(dest["schema_name"], dest["pure_table_name"])] = {tuple(r) for r in dest.get("read_relations") or []}
    for i, seg in enumerate(plan.segments):
        seg["read_later"] = any(seg["key"] in reads.get(later["key"], set()) or later["key"] == seg["key"]
                                for later in plan.segments[i + 1:])
    rows = int(catalog.get("source_rows_estimate") or 0)
    if rows <= 0:
        rows = int(catalog.get("source_blocks") or 0) * 4
    plan.workers = max(1, min(worker_slots, rows // min_rows_per_unit or 1))
    plan.units = plan.workers
    for key, meta in dest_meta.items():
        grows = any(s["key"] == key and s["mode"] != "serial" for s in plan.segments)
        plan.bulk[key] = (grows and bool(meta["owned"]) and _bulk_safe(meta, dest_meta)
                          and int(meta["rows_estimate"] or 0) <= 2 * max(rows, 1))
    return plan


def _column_problem(meta: dict, listed: list[str]) -> str | None:
    listed_set = set(listed)
    for col in meta["columns"]:
        if col["generated"]:
            return f"column {col['name']} is generated"
        if col["name"] in listed_set:
            continue
        if col["sequence"]:
            if col["seq_cycle"]:
                return f"sequence of {col['name']} cycles"
            if not (col["identity"] in ("a", "d") or (col["default"] or "").startswith("nextval(")):
                return f"column {col['name']} has an unexpected default"
            continue
        if col["default"] is not None:
            return f"column {col['name']} has a non-identity default"
    return None


def _bulk_safe(meta: dict, dest_meta: dict) -> bool:
    """Secondary structures of a destination may be dropped and recreated only if every FK that points
    at it comes from a destination we manage and own, and every FK involved can be re-added."""
    for fk in meta["incoming_fks"]:
        if (fk["schema_name"], fk["table_name"]) not in dest_meta or not fk["owned"] or not fk["can_reference"]:
            return False
    return all(c["can_reference"] for c in meta["constraints"] if c["type"] == "f")


# --------------------------------------------------------------------------- rendering
class _Timings:
    """Coordinator timing rows (same pg_temp table and JSON shape as the original batch)."""

    def __init__(self):
        self.lines = []

    def around(self, number, name, body, stage="execute", dest=None, stmt=None):
        self.lines.append(f"INSERT INTO pg_temp.pgdm_expansion_timings VALUES ({number}, {ql(stage)}, {ql(name)}, "
                          f"clock_timestamp(), NULL, NULL, {ql(dest)}, {stmt if stmt is not None else 'NULL'});")
        self.lines.append(body)
        self.lines.append(f"UPDATE pg_temp.pgdm_expansion_timings SET finished_at = clock_timestamp() "
                          f"WHERE sequence_number = {number};")


def render(plan: Plan, catalog: dict, source_schema: str, source_table: str, source_sql: str, job_id: str,
           job_dir: str, manifest_sql: str, lock_sql: str, timing_table_sql: str, timing_result_sql: str,
           row_counts_table: str, managed_dml, signature_value: str, token_sql: str = "NULL") -> dict:
    """-> bash runner. It writes the SQL files into job_dir, registers the job and its units (checkpoints),
    and runs the coordinator, whose stdout carries the manifest and timing JSON lines exactly like the
    original batch. Worker phases are launched by the coordinator with psql's \\! while it holds the lock."""
    xs = STAGE_SCHEMA_PREFIX + job_id
    dest_meta = {(d["schema_name"], d["table_name"]): d for d in catalog["destinations"]}
    blocks = int(catalog.get("source_blocks") or 0)
    step = max(1, -(-blocks // plan.units)) if blocks else 1
    ranges = [(u * step, None if u == plan.units - 1 else (u + 1) * step) for u in range(plan.units)]
    files = {}
    par = [s for s in plan.segments if s["mode"] != "serial"]

    def seq_col(seg):
        meta = dest_meta[seg["key"]]
        return next((c for c in meta["columns"] if c["sequence"] and c["name"] not in seg["columns"]), None)

    def unit_tables(seg):
        return [f"{seg['name']}_r0"] if seg["mode"] == "distinct" else [f"{seg['name']}_u{u}" for u in range(len(ranges))]

    def full_rows_sql(seg):
        """The segment's rows exactly as they will be stored in the destination (all columns, table order)."""
        meta = dest_meta[seg["key"]]
        parts = []
        for k, t in enumerate(unit_tables(seg)):
            cols = []
            for col in meta["columns"]:
                if col["name"] in seg["columns"]:
                    cols.append(f"u.{qi(col['name'])}")
                elif col["sequence"]:
                    cols.append(f"(o.first_value + (o.unit_offset + u.__rn - 1) * o.increment_by)::{col['type']} AS {qi(col['name'])}")
                else:
                    cols.append(f"NULL::{col['type']} AS {qi(col['name'])}")
            parts.append(f"SELECT {', '.join(cols)} FROM {qi(xs)}.{qi(t)} u JOIN {qi(xs)}.unit_offsets o "
                         f"ON o.segment = {ql(seg['name'])} AND o.unit = {k}")
        return "\nUNION ALL\n".join(parts)

    exported = [k for k in dest_meta if any(s.get("export") and s["key"] == k for s in plan.segments)]
    prefix_sequences = sorted({c["sequence"] for s in plan.segments if s.get("serial_kind") == "prefix"
                               for c in dest_meta[s["key"]]["columns"] if c["sequence"]})

    def serial_copy(key):
        return f"{qi(xs)}.{qi(key[1] + '__serial')}"

    def views_sql(upto):
        """Views named like each destination: committed rows + rows the coordinator inserted serially
        before the first parallel segment + rows staged by parallel segments < upto."""
        out = ["SET client_min_messages = warning;", "BEGIN;"]
        for key in dest_meta:
            parts = [f"SELECT * FROM {qi(key[0])}.{qi(key[1])}"]
            if key in exported:
                parts.append(f"SELECT * FROM {serial_copy(key)}")
            for s in plan.segments[:upto]:
                if s["key"] == key and s["mode"] != "serial":
                    parts.append(f"SELECT * FROM {qi(xs)}.{qi(s['name'] + '_c')}" if s["read_later"]
                                 else full_rows_sql(s))
            out.append(f"CREATE OR REPLACE VIEW {qi(xs)}.{qi(key[1])} AS\n" + "\nUNION ALL\n".join(parts) + ";")
        out.append("COMMIT;")
        return "\n".join(out) + "\n"

    managed_rel = {key: xs for key in dest_meta}
    for idx, seg in enumerate(par):
        i = plan.segments.index(seg)
        meta = dest_meta[seg["key"]]
        coltypes = {c["name"]: c["type"] for c in meta["columns"]}
        stage_cols = ", ".join(f"{qi(c)} {coltypes[c]}" for c in seg["columns"])
        q = seg["statement"]
        body, head = q[seg["query_start"]:], q[:seg["query_start"]]
        files[f"{seg['name']}_views.sql"] = views_sql(i)
        unit_files = []
        for u, (lo, hi) in enumerate(ranges):
            ranged = (source_sql.rstrip()[:-1].rstrip() + f"\n      AND ctid >= '({lo},0)'::tid"
                      + (f" AND ctid < '({hi},0)'::tid" if hi is not None else "") + "\n)")
            ranged_ctid = ranged.replace("SELECT *", "SELECT *, ctid AS __pgdm_ctid", 1)
            sel = rewrite_relations(body.replace(source_sql, ranged), managed_rel)
            table = f"{seg['name']}_u{u}"
            if seg["mode"] == "parallel":
                # rows are staged in the order the original plan produces them: by the source row's
                # physical position, then (several rows per source row) in emission order
                target = f"{qi(xs)}.{qi(table)}"
                cut = seg["from_pos"] - seg["query_start"]
                with_ctid = body[:cut].rstrip() + f",\n    {seg['source_alias']}.__pgdm_ctid\n" + body[cut:]
                sel_ctid = rewrite_relations(with_ctid.replace(source_sql, ranged_ctid), managed_rel)
                cols = [f"c{k + 1}" for k in range(len(seg["columns"]))]
                sql = (f"CREATE UNLOGGED TABLE {target} ({stage_cols}, __rn bigint GENERATED ALWAYS AS IDENTITY);\n"
                       f"INSERT INTO {target} ({', '.join(qi(c) for c in seg['columns'])})\n"
                       f"SELECT {', '.join(cols)} FROM (\n"
                       f"  SELECT x.*, row_number() OVER () AS __e FROM (\n{sel_ctid}\n  ) AS x({', '.join(cols)}, __src)\n"
                       f") AS y ORDER BY __src, __e;\n")
            else:
                aliases = ", ".join(f"c{k + 1}" for k in range(len(seg["columns"])))
                sql = f"CREATE UNLOGGED TABLE {qi(xs)}.{qi(table)} AS SELECT * FROM (\n{sel}\n) AS q({aliases});\n"
            unit_files.append(_unit_file(files, xs, job_id, table, sql, catalog["search_path"]))
        files[f"phase_{seg['name']}.list"] = "\n".join(unit_files) + "\n"
        if seg["mode"] == "distinct":
            aliases = ", ".join(f"c{k + 1}" for k in range(len(seg["columns"])))
            union = "\nUNION ALL\n".join(f"SELECT {aliases} FROM {qi(xs)}.{qi(seg['name'] + '_u' + str(u))}"
                                         for u in range(len(ranges)))
            target = f"{qi(xs)}.{qi(seg['name'] + '_r0')}"
            sql = (f"CREATE UNLOGGED TABLE {target} ({stage_cols}, __rn bigint GENERATED ALWAYS AS IDENTITY);\n"
                   f"{head[:seg['target_span'][0]]}{target}{head[seg['target_span'][1]:]}"
                   f"SELECT {aliases} FROM (SELECT DISTINCT {aliases} FROM (\n{union}\n) AS p) AS d "
                   f"ORDER BY {', '.join(str(k + 1) for k in range(len(seg['columns'])))};\n")
            files[f"phase_{seg['name']}.reduce"] = _unit_file(files, xs, job_id, f"{seg['name']}_r0", sql,
                                                               catalog["search_path"]) + "\n"
        files[f"phase_{seg['name']}.post"] = _post_phase_sql(xs, seg, unit_tables(seg), seq_col(seg), meta,
                                                              full_rows_sql(seg))
        files[f"phase_{seg['name']}.next"] = (f"{par[idx + 1]['name']}_views.sql\n" if idx + 1 < len(par) else "\n")

    # ---- coordinator ------------------------------------------------------------------------------
    t = _Timings()
    co = [timing_table_sql,
          "SET idle_in_transaction_session_timeout = 0;", "SET statement_timeout = 0;", "SET lock_timeout = 0;"]
    co += job_setup_step(job_dir, job_id, par[0]["name"] + "_views.sql" if par else None)
    co.append("BEGIN;")
    t.around(1, "Initialize database transaction",
             "SET LOCAL client_min_messages = warning;\nSET LOCAL synchronize_seqscans = off;")
    t.around(2, "Acquire destination table locks",
             lock_sql + f"\nLOCK TABLE {qi(source_schema)}.{qi(source_table)} IN SHARE MODE;")
    t.around(3, "Scan source and build dependency manifest", manifest_sql)
    if prefix_sequences:
        t.lines.append(restore_sequences_sql(job_id, prefix_sequences))
    # statements that must run exactly as in the original (before the first parallel one)
    for i, seg in enumerate(plan.segments):
        if seg.get("serial_kind") == "prefix":
            t.lines.append(managed_dml(seg["statement"], seg["key"][0], seg["key"][1], 10 + i,
                                       f"Insert into {seg['table_name']} (statement {seg['statement_index']})",
                                       seg["table_name"], seg["statement_index"]))
    if exported:
        # hand the rows this transaction inserted to the worker sessions (they cannot see uncommitted rows)
        body = []
        for key in exported:
            mine = (f"SELECT * FROM {qi(key[0])}.{qi(key[1])} "
                    "WHERE xmin::text::bigint = txid_current() % 4294967296")
            path = f"{job_dir}/{key[1]}__serial.copy"
            body.append(f"\\copy ({mine}) TO {ql(path)} WITH (FORMAT binary)")
            files[f"{key[1]}__serial_load.sql"] = (
                "\\set ON_ERROR_STOP 1\nBEGIN;\n"
                f"TRUNCATE {serial_copy(key)};\n"
                f"\\copy {serial_copy(key)} FROM {ql(path)} WITH (FORMAT binary)\n"
                "COMMIT;\n")
            body.append(f"\\! psql -X -q -v ON_ERROR_STOP=1 -f {shlex.quote(job_dir + '/' + key[1] + '__serial_load.sql')}")
            body.append(f"DO $pgdm_fast_copy$ BEGIN IF (SELECT count(*) FROM {serial_copy(key)}) <> ({mine.replace('SELECT *', 'SELECT count(*)', 1)}) "
                        f"THEN RAISE EXCEPTION 'Could not hand the rows of {key[0]}.{key[1]} to the parallel workers.'; "
                        "END IF; END $pgdm_fast_copy$;")
        body.append(f"\\! psql -X -q -v ON_ERROR_STOP=1 -f {shlex.quote(job_dir + '/' + par[0]['name'] + '_views.sql')}")
        t.around(9, "Hand serially inserted rows to the parallel workers", "\n".join(body))
    for idx, seg in enumerate(par):
        t.around(20 + idx, f"Parallel compute {seg['table_name']} (statement {seg['statement_index']}): "
                           f"{len(unit_tables(seg)) if seg['mode'] == 'parallel' else len(ranges)} ranges on {plan.workers} sessions",
                 f"\\! bash {shlex.quote(job_dir)}/phase.sh {seg['name']}", dest=seg["table_name"], stmt=seg["statement_index"])
    if par:
        t.around(59, "Verify parallel checkpoints", _verify_sql(xs, job_id, par, unit_tables))
    bulk_keys = [k for k in dest_meta if plan.bulk.get(k)]
    drop_sql, restore_sql = _bulk_ddl(bulk_keys, dest_meta)
    if drop_sql:
        t.around(60, "Defer secondary indexes and foreign keys (bulk load)", drop_sql)
    for i, seg in enumerate(plan.segments):
        if seg.get("serial_kind") == "tail":
            t.lines.append(managed_dml(seg["statement"], seg["key"][0], seg["key"][1], 100 + i,
                                       f"Insert into {seg['table_name']} (statement {seg['statement_index']})",
                                       seg["table_name"], seg["statement_index"]))
        elif seg["mode"] != "serial":
            t.lines.append(_final_insert_sql(xs, seg, dest_meta[seg["key"]], unit_tables(seg), row_counts_table, 100 + i))
    if restore_sql:
        t.around(200, "Rebuild indexes and validate foreign keys",
                 "SET LOCAL work_mem = '256MB';\nSET LOCAL maintenance_work_mem = '1GB';\n"
                 f"SET LOCAL max_parallel_maintenance_workers = {max(0, min(8, plan.workers - 1))};\n" + restore_sql)
    if par:
        t.around(201, "Advance identity sequences", _sequence_sql(xs, par, dest_meta, seq_col))
    t.lines.append(commit_marker_sql(job_id, manifest_sql))
    t.lines.append(f"UPDATE {qi(JOB_SCHEMA)}.jobs SET state = 'committed', updated_at = clock_timestamp() "
                   f"WHERE job_id = {ql(job_id)};")
    co += t.lines
    co += ["INSERT INTO pg_temp.pgdm_expansion_timings VALUES (9000, 'execute', 'Commit expansion transaction', "
           "clock_timestamp(), NULL, NULL, NULL, NULL);", "COMMIT;",
           "UPDATE pg_temp.pgdm_expansion_timings SET finished_at = clock_timestamp() WHERE sequence_number = 9000;"]
    if par:
        co += ["INSERT INTO pg_temp.pgdm_expansion_timings VALUES (9001, 'execute', 'Drop staging tables', "
               "clock_timestamp(), NULL, NULL, NULL, NULL);",
               f"DROP SCHEMA IF EXISTS {qi(xs)} CASCADE;",
               f"DELETE FROM {qi(JOB_SCHEMA)}.units WHERE job_id = {ql(job_id)};",
               "UPDATE pg_temp.pgdm_expansion_timings SET finished_at = clock_timestamp() WHERE sequence_number = 9001;"]
    co += [timing_result_sql, "DROP TABLE pg_temp.pgdm_expansion_timings;"]
    files["coordinator.sql"] = "\n".join(co) + "\n"
    all_sequences = sorted({c["sequence"] for m in dest_meta.values() for c in m["columns"] if c["sequence"]})
    files["setup.sql"] = job_setup_sql(job_id, signature_value, all_sequences, token_sql)
    files["stage_setup.sql"] = _setup_sql(xs, job_id, par, len(ranges),
                                          [(serial_copy(k), f"{qi(k[0])}.{qi(k[1])}") for k in exported])
    files["phase.sh"] = _phase_script(plan.workers)
    return files


def _unit_file(files, xs, job_id, table, sql, search_path) -> str:
    """One checkpointed unit: skipped if already done, otherwise rebuilt from scratch and marked done in
    the same transaction that creates its staging table."""
    name = f"{table}.sql"
    files[name] = (
        "\\set ON_ERROR_STOP 1\n"
        "SET client_min_messages = warning;\n"
        "SET max_parallel_workers_per_gather = 0;\n"
        "SET synchronize_seqscans = off;\n"
        "SET work_mem = '64MB';\n"
        f"SELECT set_config('search_path', {ql(qi(xs) + ', ' + search_path)}, false) AS search_path \\gset\n"
        f"SELECT EXISTS (SELECT 1 FROM {qi(JOB_SCHEMA)}.units WHERE job_id = {ql(job_id)} AND unit_id = {ql(table)} "
        "AND state = 'done') AS unit_done \\gset\n"
        "\\if :unit_done\n\\q\n\\endif\n"
        "BEGIN;\n"
        f"DROP TABLE IF EXISTS {qi(xs)}.{qi(table)};\n"
        f"{sql}"
        f"UPDATE {qi(JOB_SCHEMA)}.units SET state = 'done', rows = (SELECT count(*) FROM {qi(xs)}.{qi(table)}), "
        f"finished_at = clock_timestamp() WHERE job_id = {ql(job_id)} AND unit_id = {ql(table)};\n"
        "COMMIT;\n")
    return name


def _post_phase_sql(xs, seg, tables, seq_column, meta, full_rows) -> str:
    """Per-unit identity offsets, computed once from the sequence's current position (which already
    reflects every row inserted or staged before this segment) and immediately reserved with setval, so
    later statements (serial or parallel) continue numbering after this segment exactly as nextval would.
    Offsets are never recomputed on resume: staged rows keep their identity values. If a later statement
    reads this destination, a materialised copy of the staged rows gets the destination's own indexes."""
    if seq_column:
        first = (f"(SELECT CASE WHEN is_called THEN last_value + {seq_column['seq_increment']} ELSE last_value END "
                 f"FROM {seq_column['sequence']})")
        inc = str(seq_column["seq_increment"])
    else:
        first, inc = "0", "1"
    counts = ", ".join(f"({k}, (SELECT count(*) FROM {qi(xs)}.{qi(t)}))" for k, t in enumerate(tables))
    out = ["\\set ON_ERROR_STOP 1", "SET client_min_messages = warning;", "BEGIN;",
           f"LOCK TABLE {qi(xs)}.unit_offsets IN EXCLUSIVE MODE;",
           f"INSERT INTO {qi(xs)}.unit_offsets (segment, unit, unit_rows, unit_offset, first_value, increment_by)",
           f"SELECT {ql(seg['name'])}, k, n, COALESCE(sum(n) OVER (ORDER BY k ROWS BETWEEN UNBOUNDED PRECEDING "
           f"AND 1 PRECEDING), 0), {first}, {inc} FROM (VALUES {counts}) v(k, n)",
           f"WHERE NOT EXISTS (SELECT 1 FROM {qi(xs)}.unit_offsets WHERE segment = {ql(seg['name'])});"]
    if seq_column:
        out.append(f"SELECT setval({ql(seq_column['sequence'])}, max(first_value + (unit_offset + unit_rows - 1) * increment_by), true) "
                   f"FROM {qi(xs)}.unit_offsets WHERE segment = {ql(seg['name'])} AND unit_rows > 0 HAVING count(*) > 0 "
                   f"\\g /dev/null")
    if seg["read_later"]:
        c = f"{qi(xs)}.{qi(seg['name'] + '_c')}"
        out += [f"DROP TABLE IF EXISTS {c};", "SET LOCAL max_parallel_workers_per_gather = 4;",
                f"CREATE UNLOGGED TABLE {c} AS\n{full_rows};",
                "SET LOCAL maintenance_work_mem = '512MB';"]
        for idx in meta["indexes"]:
            if idx["expression"] or not idx["columns"]:
                continue
            unique = "UNIQUE " if idx["def"].startswith("CREATE UNIQUE") else ""
            out.append(f"CREATE {unique}INDEX ON {c} ({', '.join(qi(x) for x in idx['columns'])});")
        out.append(f"ANALYZE {c};")
    out.append("COMMIT;")
    return "\n".join(out) + "\n"


def _verify_sql(xs, job_id, par, unit_tables) -> str:
    """Abort unless every unit is done and every segment has complete, consistent offsets."""
    checks = []
    for seg in par:
        n = len(unit_tables(seg))
        checks.append(f"(SELECT count(*) = {n} AND COALESCE(sum(o.unit_rows), -1) = COALESCE((SELECT sum(rows) FROM "
                      f"{qi(JOB_SCHEMA)}.units WHERE job_id = {ql(job_id)} AND unit_id IN "
                      f"({', '.join(ql(t) for t in unit_tables(seg))})), -2) FROM {qi(xs)}.unit_offsets o "
                      f"WHERE o.segment = {ql(seg['name'])})")
    tables = [t for seg in par for t in unit_tables(seg)]
    checks.append(f"NOT EXISTS (SELECT 1 FROM {qi(JOB_SCHEMA)}.units u WHERE u.job_id = {ql(job_id)} AND u.rows > 0 "
                  f"AND u.unit_id IN ({', '.join(ql(t) for t in tables)}) AND COALESCE(pg_relation_size("
                  f"to_regclass(format('%I.%I', {ql(xs)}, u.unit_id))), 0) = 0)")
    return (f"DO $pgdm_fast_verify$ BEGIN\n"
            f"  IF EXISTS (SELECT 1 FROM {qi(JOB_SCHEMA)}.units WHERE job_id = {ql(job_id)} AND state <> 'done') THEN\n"
            f"    RAISE EXCEPTION 'Parallel expansion units did not complete: %', (SELECT string_agg(unit_id, ', ') "
            f"FROM {qi(JOB_SCHEMA)}.units WHERE job_id = {ql(job_id)} AND state <> 'done');\n  END IF;\n"
            f"  IF NOT ({' AND '.join(checks)}) THEN\n"
            f"    RAISE EXCEPTION 'Parallel expansion offsets are incomplete or inconsistent.';\n  END IF;\n"
            f"END $pgdm_fast_verify$;")


def _final_insert_sql(xs, seg, meta, tables, row_counts_table, timing_sequence) -> str:
    cols = [c for c in meta["columns"] if c["name"] in seg["columns"] or c["sequence"]]
    target_cols = ", ".join(qi(c["name"]) for c in cols)
    overriding = " OVERRIDING SYSTEM VALUE" if any(c["identity"] in ("a", "d") and c["name"] not in seg["columns"]
                                                    for c in cols) else ""
    stmts = []
    for k, t in enumerate(tables):
        exprs = [f"u.{qi(c['name'])}" if c["name"] in seg["columns"]
                 else f"(o.first_value + (o.unit_offset + u.__rn - 1) * o.increment_by)::{c['type']}" for c in cols]
        stmts.append(
            f"    INSERT INTO {qi(seg['key'][0])}.{qi(seg['key'][1])} ({target_cols}){overriding}\n"
            f"    SELECT {', '.join(exprs)} FROM {qi(xs)}.{qi(t)} u JOIN {qi(xs)}.unit_offsets o\n"
            f"      ON o.segment = {ql(seg['name'])} AND o.unit = {k};\n"
            f"    GET DIAGNOSTICS pgdm_part = ROW_COUNT;\n"
            f"    pgdm_affected_rows := pgdm_affected_rows + pgdm_part;")
    name = f"Insert into {seg['table_name']} (statement {seg['statement_index']})"
    return f"""DO $pgdm_fast_insert$
DECLARE
    pgdm_affected_rows bigint := 0;
    pgdm_part bigint;
    pgdm_timing_started_at timestamptz := clock_timestamp();
BEGIN
{chr(10).join(stmts)}
    UPDATE {row_counts_table}
    SET row_count = row_count + pgdm_affected_rows, updated_at = clock_timestamp()
    WHERE schema_name = {ql(seg['key'][0])} AND table_name = {ql(seg['key'][1])};
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Contador ausente para %.% durante insercao.', {ql(seg['key'][0])}, {ql(seg['key'][1])};
    END IF;
    INSERT INTO pg_temp.pgdm_expansion_timings (sequence_number, stage_key, item_name, started_at, finished_at,
        rows_affected, destination_name, statement_index)
    VALUES ({int(timing_sequence)}, 'execute', {ql(name)}, pgdm_timing_started_at, clock_timestamp(),
        pgdm_affected_rows, {ql(seg['table_name'])}, {int(seg['statement_index'])});
END
$pgdm_fast_insert$;"""


def _bulk_ddl(keys, dest_meta) -> tuple[str, str]:
    """Drop the FKs of / into the bulk destinations and their non-primary-key indexes and unique
    constraints; recreate them afterwards with identical names and definitions (indexes, then FKs)."""
    if not keys:
        return "", ""
    fks = {}
    for key in keys:
        meta = dest_meta[key]
        for con in meta["constraints"]:
            if con["type"] == "f":
                fks[(key, con["name"])] = con
        for fk in meta["incoming_fks"]:
            fks[((fk["schema_name"], fk["table_name"]), fk["name"])] = fk
    drop, restore_idx, restore_fk = [], [], []
    for (tbl, name), con in sorted(fks.items()):
        table = f"{qi(tbl[0])}.{qi(tbl[1])}"
        drop.append(f"ALTER TABLE {table} DROP CONSTRAINT {qi(name)};")
        restore_fk.append(f"ALTER TABLE {table} ADD CONSTRAINT {qi(name)} {con['def']}"
                          + ("" if con["validated"] else " NOT VALID") + ";")
        if con.get("comment"):
            restore_fk.append(f"COMMENT ON CONSTRAINT {qi(name)} ON {table} IS {ql(con['comment'])};")
    for key in keys:
        meta = dest_meta[key]
        table = f"{qi(key[0])}.{qi(key[1])}"
        cons = {c["name"]: c for c in meta["constraints"]}
        for idx in meta["indexes"]:
            con = cons.get(idx["constraint"]) if idx["constraint"] else None
            if con:
                drop.append(f"ALTER TABLE {table} DROP CONSTRAINT {qi(con['name'])};")
                restore_idx.append(f"ALTER TABLE {table} ADD CONSTRAINT {qi(con['name'])} {con['def']};")
                if con.get("comment"):
                    restore_idx.append(f"COMMENT ON CONSTRAINT {qi(con['name'])} ON {table} IS {ql(con['comment'])};")
            else:
                drop.append(f"DROP INDEX {qi(key[0])}.{qi(idx['name'])};")
                restore_idx.append(idx["def"] + ";")
                if idx.get("comment"):
                    restore_idx.append(f"COMMENT ON INDEX {qi(key[0])}.{qi(idx['name'])} IS {ql(idx['comment'])};")
    # primary keys first: FK validation needs the referenced unique indexes
    restore_idx.sort(key=lambda sql: 0 if " PRIMARY KEY " in sql else 1)
    return "\n".join(drop), "\n".join(restore_idx + restore_fk)


def _sequence_sql(xs, par, dest_meta, seq_col) -> str:
    """setval each identity sequence to the last value the original nextval() calls would have returned."""
    by_seq = {}
    for seg in par:
        col = seq_col(seg)
        if col:
            by_seq.setdefault(col["sequence"], []).append(seg["name"])
    out = []
    for seq, names in by_seq.items():
        out.append(f"SELECT setval({ql(seq)}, max(o.first_value + (o.unit_offset + o.unit_rows - 1) * o.increment_by), true)\n"
                   f"FROM {qi(xs)}.unit_offsets o WHERE o.segment IN ({', '.join(ql(n) for n in names)}) "
                   "AND o.unit_rows > 0 HAVING count(*) > 0;")
    return "\n".join(out) or "SELECT 1;"


def _setup_sql(xs, job_id, par, nranges, copies=()) -> str:
    units = []
    for s in par:
        units += [f"({ql(job_id)}, {ql(s['name'] + '_u' + str(u))}, {ql(s['name'])})" for u in range(nranges)]
        if s["mode"] == "distinct":
            units.append(f"({ql(job_id)}, {ql(s['name'] + '_r0')}, {ql(s['name'])})")
    return (
        "\\set ON_ERROR_STOP 1\nSET client_min_messages = warning;\nBEGIN;\n"
        f"CREATE SCHEMA IF NOT EXISTS {qi(xs)};\n"
        f"CREATE TABLE IF NOT EXISTS {qi(xs)}.unit_offsets (segment text NOT NULL, unit int NOT NULL, "
        "unit_rows bigint NOT NULL, unit_offset bigint NOT NULL, first_value bigint NOT NULL, "
        "increment_by bigint NOT NULL, PRIMARY KEY (segment, unit));\n"
        + "".join(f"CREATE UNLOGGED TABLE IF NOT EXISTS {c} (LIKE {src});\n" for c, src in copies)
        + (f"INSERT INTO {qi(JOB_SCHEMA)}.units (job_id, unit_id, segment) VALUES {', '.join(units)} "
           "ON CONFLICT DO NOTHING;\n" if units else "")
        + "COMMIT;\n")


def _phase_script(workers) -> str:
    return f"""#!/bin/bash
# One parallel phase: the segment's range units on {workers} concurrent psql sessions, the optional DISTINCT
# reduce, identity offsets (+ materialised copy when read later), then the views for the next segment.
# Failures are logged; the coordinator verifies every checkpoint before using any staged row.
set -u
d="$(cd "$(dirname "$0")" && pwd)"; seg="$1"
run() {{ psql -X -q -v ON_ERROR_STOP=1 -f "$d/$1" >>"$d/workers.log" 2>&1 || echo "FAILED $1" >>"$d/workers.log"; }}
export -f run; export d
xargs -a "$d/phase_$seg.list" -P {workers} -I{{}} bash -c 'run "$1"' _ {{}}
if [ -s "$d/phase_$seg.reduce" ]; then run "$(tr -d '\\n' < "$d/phase_$seg.reduce")"; fi
run "phase_$seg.post"
nv="$(tr -d '\\n' < "$d/phase_$seg.next")"
if [ -n "$nv" ]; then run "$nv"; fi
exit 0
"""
