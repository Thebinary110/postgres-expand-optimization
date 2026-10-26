import unittest
from pathlib import Path

from services import fast_expansion as fx
from services.postgres_service import PostgresAdminService


PROJECT_ROOT = Path(__file__).resolve().parents[1]
RECIPE_DIR = PROJECT_ROOT / "SQL files"
RECIPE_DESTINATIONS = (
    ("01_table3.sql", "public.table3"),
    ("02_table4.sql", "public.table4"),
    ("03_table1.sql", "public.table1"),
    ("04_table5.sql", "public.table5"),
    ("05_table6.sql", "public.table6"),
    ("06_table2.sql", "public.table2"),
)
SOURCE_SQL = (
    "(\n"
    "    SELECT *\n"
    '    FROM "public"."raw_data"\n'
    "    WHERE \"raw_schema\" IN ('group001')\n"
    ")"
)


def prepared_challenge():
    destinations = [
        {
            "table_name": table_name,
            "version_title": "Synthetic expansion",
            "sql": (RECIPE_DIR / file_name).read_text(encoding="utf-8"),
        }
        for file_name, table_name in RECIPE_DESTINATIONS
    ]
    return PostgresAdminService._prepare_cross_table_expansion("public.raw_data", ["group001"], destinations)


def statement(select_sql: str) -> str:
    return f"INSERT INTO public.t (a, b) {select_sql.replace('{{source}}', SOURCE_SQL)}"


def column(name, type_="integer", sequence=None, identity=""):
    return {"name": name, "type": type_, "notnull": False, "identity": identity, "generated": "",
            "default": None, "sequence": sequence, "seq_increment": 1 if sequence else None, "seq_cycle": False}


def destination(table, columns, constraints=(), incoming=(), indexes=(), owned=True):
    return {"schema_name": "public", "table_name": table, "owned": owned, "rows_estimate": 0, "has_rules": False,
            "user_triggers": 0, "columns": list(columns), "constraints": list(constraints),
            "incoming_fks": list(incoming), "indexes": list(indexes)}


class StatementAnalysisTests(unittest.TestCase):
    def test_challenge_recipes_are_classified(self):
        prepared = prepared_challenge()
        modes = {
            d["table_name"]: fx.analyse_statement(d["statements"][0], SOURCE_SQL, {"count", "sum"})["mode"]
            for d in prepared["destinations"]
        }
        self.assertEqual(modes, {
            "public.table3": "distinct",
            "public.table4": "distinct",
            "public.table1": "parallel",
            "public.table5": "parallel",
            "public.table6": "parallel",
            "public.table2": "parallel",
        })

    def test_row_local_statement_exposes_alias_and_columns(self):
        info = fx.analyse_statement(statement("SELECT s.x, s.y FROM {{source}} AS s WHERE s.x > 0"), SOURCE_SQL, set())
        self.assertEqual(info["mode"], "parallel")
        self.assertEqual(info["source_alias"], "s")
        self.assertEqual(info["insert_columns"], ["a", "b"])

    def test_non_row_local_statements_stay_serial(self):
        cases = {
            "aggregate": "SELECT count(*), 1 FROM {{source}} AS s",
            "group by": "SELECT s.x, 1 FROM {{source}} AS s GROUP BY s.x",
            "window": "SELECT s.x, row_number() OVER () FROM {{source}} AS s",
            "order/limit": "SELECT s.x, 1 FROM {{source}} AS s ORDER BY 1 LIMIT 5",
            "set operation": "SELECT s.x, 1 FROM {{source}} AS s UNION SELECT 1, 2",
            "distinct on": "SELECT DISTINCT ON (s.x) s.x, 1 FROM {{source}} AS s",
            "star expansion": "SELECT s.* FROM {{source}} AS s",
            "no alias": "SELECT x, y FROM {{source}}",
        }
        for label, sql in cases.items():
            with self.subTest(label):
                self.assertEqual(fx.analyse_statement(statement(sql), SOURCE_SQL, {"count"})["mode"], "serial")

    def test_source_referenced_twice_stays_serial(self):
        sql = statement("SELECT s.x, s2.x FROM {{source}} AS s JOIN {{source}} AS s2 ON s.x = s2.x")
        self.assertEqual(fx.analyse_statement(sql, SOURCE_SQL, set())["mode"], "serial")


class RewriteAndLexerTests(unittest.TestCase):
    def test_only_qualified_relation_references_are_rewritten(self):
        sql = ("SELECT main_row.id_column_1, public.table1.x, 'public.table1', public.table1_fn(1) "
               "FROM public.table1 AS main_row JOIN \"public\".\"table1\" t ON true -- public.table1")
        out = fx.rewrite_relations(sql, {("public", "table1"): "pgdm_xs_job"})
        self.assertIn('FROM "pgdm_xs_job".table1 AS main_row', out)
        self.assertIn('JOIN "pgdm_xs_job"."table1" t', out)
        self.assertIn("main_row.id_column_1", out)
        self.assertIn("public.table1.x", out)          # three-part column reference untouched
        self.assertIn("'public.table1'", out)          # string literal untouched
        self.assertIn("public.table1_fn(1)", out)      # different name untouched
        self.assertTrue(out.endswith("-- public.table1"))  # comment untouched

    def test_lexer_handles_quoting_forms(self):
        sql = "SELECT $tag$ a ) ( $tag$, E'it\\'s', 'a''b', \"q\"\"i\" /* x /* nested */ y */ FROM t"
        tokens = fx.lex(sql)
        self.assertEqual([t[0] for t in tokens if t[0] in ("string", "ident")], ["string", "string", "string", "ident"])
        self.assertTrue(all(t[4] == 0 for t in tokens))  # no parenthesis leaked out of the literals


class PlanningTests(unittest.TestCase):
    def catalog(self, owned_child=True):
        parent = destination("table3", [column("id", "bigint", "public.table3_id_seq", "d"), column("a"), column("b")],
                             constraints=[{"name": "table3_pkey", "type": "p", "def": "PRIMARY KEY (id)",
                                           "validated": True, "can_reference": None, "comment": None}],
                             incoming=[{"name": "child_fk", "schema_name": "public", "table_name": "outside",
                                        "def": "FOREIGN KEY (p) REFERENCES table3(id)", "validated": True,
                                        "owned": owned_child, "can_reference": True, "comment": None}])
        return {"server_version_num": 160000, "can_create_schema": True, "search_path": '"$user", public',
                "source_blocks": 1000, "source_rows_estimate": 1_000_000, "aggregate_names": [],
                "synchronize_seqscans": True, "source_bytes": 8_192_000, "syncscan_threshold_bytes": 1 << 30,
                "destinations": [parent]}

    def prepared(self, sql):
        return {"raw_schemas": ["group001"], "destinations": [{
            "table_name": "public.table3", "schema_name": "public", "pure_table_name": "table3",
            "statements": [statement(sql).replace("public.t (", "public.table3 (")],
            "destination_columns": ["a", "b"], "read_relations": []}]}

    def test_hashed_distinct_runs_as_original_prefix(self):
        prepared = self.prepared("SELECT DISTINCT s.x, s.y FROM {{source}} AS s")
        plan = fx.build_plan(prepared, SOURCE_SQL, self.catalog(), worker_slots=8, hashed_distinct={0})
        self.assertEqual(plan.segments[0]["mode"], "serial")
        self.assertEqual(plan.segments[0]["serial_kind"], "prefix")
        sorted_plan = fx.build_plan(prepared, SOURCE_SQL, self.catalog(), worker_slots=8)
        self.assertEqual(sorted_plan.segments[0]["mode"], "distinct")

    def test_hash_order_is_not_chased_when_the_original_is_not_reproducible(self):
        # above a quarter of shared_buffers the original's scans are synchronised: its identity values
        # depend on scan positions, so the DISTINCT statement runs in parallel even if it hashes
        prepared = self.prepared("SELECT DISTINCT s.x, s.y FROM {{source}} AS s")
        catalog = self.catalog()
        catalog["source_bytes"] = 2 << 30
        plan = fx.build_plan(prepared, SOURCE_SQL, catalog, worker_slots=8, hashed_distinct={0})
        self.assertFalse(plan.exact_ids)
        self.assertEqual(plan.segments[0]["mode"], "distinct")

    def test_bulk_ddl_only_when_every_referencing_table_is_managed(self):
        prepared = self.prepared("SELECT s.x, s.y FROM {{source}} AS s")
        plan = fx.build_plan(prepared, SOURCE_SQL, self.catalog(), worker_slots=8)
        self.assertFalse(plan.bulk[("public", "table3")])  # an unmanaged table references it

    def test_worker_count_follows_dataset_size(self):
        prepared = self.prepared("SELECT s.x, s.y FROM {{source}} AS s")
        catalog = self.catalog()
        catalog["source_rows_estimate"] = 12_000
        self.assertEqual(fx.build_plan(prepared, SOURCE_SQL, catalog, worker_slots=16).workers, 2)
        catalog["source_rows_estimate"] = 10_000_000
        self.assertEqual(fx.build_plan(prepared, SOURCE_SQL, catalog, worker_slots=16).workers, 16)


class RenderTests(unittest.TestCase):
    def test_render_produces_checkpointed_atomic_job(self):
        prepared = prepared_challenge()
        tables = {
            "table3": [column("id_column_1", "bigint", "public.table3_id_column_1_seq", "d")]
                      + [column(c, "character varying") for c in ("text_column_1", "text_column_2", "text_column_3",
                                                                   "text_column_4")] + [column("numeric_column_1", "numeric")],
            "table4": [column("id_column_1", "smallint"), column("text_column_1", "character varying"),
                       column("text_column_2", "character varying")],
        }
        catalog = {"server_version_num": 160000, "can_create_schema": True, "search_path": '"$user", public',
                   "source_blocks": 100, "source_rows_estimate": 100_000, "aggregate_names": [], "destinations": []}
        for d in prepared["destinations"]:
            name = d["pure_table_name"]
            cols = tables.get(name) or (
                [column("id_column_1", "bigint", f"public.{name}_id_column_1_seq", "d")]
                + [column(c) for c in fx.analyse_statement(d["statements"][0], SOURCE_SQL, set())["insert_columns"]])
            catalog["destinations"].append(destination(name, cols))
        plan = fx.build_plan(prepared, SOURCE_SQL, catalog, worker_slots=4)
        files = fx.render(plan, catalog, "public", "raw_data", SOURCE_SQL, "jobid", "dir/jobid", "SELECT json_build_object('a', 1)::text;",
                          "LOCK TABLE x IN SHARE ROW EXCLUSIVE MODE;", "CREATE TEMP TABLE pgdm_expansion_timings (x int);",
                          "SELECT 1;", '"public"."pgdm_table_row_counts"',
                          lambda *a: "-- serial", "sig")
        coordinator = files["coordinator.sql"]
        self.assertEqual(coordinator.count("\nCOMMIT;"), 1)                 # one atomic commit
        self.assertLess(coordinator.index("Verify parallel checkpoints"), coordinator.index("Insert into public.table1"))
        self.assertIn("LOCK TABLE \"public\".\"raw_data\" IN SHARE MODE", coordinator)
        unit = next(text for name, text in files.items()                    # table2, first block range
                    if name.startswith("w") and '"s5_u0"' in text)
        self.assertIn("ctid >= '(0,0)'::tid", unit)
        self.assertIn('"pgdm_xs_jobid".table1', unit)                       # reads the staged view of table1
        self.assertIn("ORDER BY __src, __e", unit)                          # original emission order
        reduce = files["s0_r0.sql"]                                         # DISTINCT reduce
        self.assertIn("SELECT DISTINCT c1, c2, c3, c4, c5", reduce)
        self.assertIn("ORDER BY 1, 2, 3, 4, 5", reduce)
        # one pass per source range for independent statements: {table3, table4}, {table1},
        # {table5, table6, table2}
        waves = sorted(n for n in files if n.startswith("phase_w") and n.endswith(".list"))
        self.assertEqual(waves, ["phase_w0.list", "phase_w1.list", "phase_w2.list"])
        w2 = next(text for name, text in files.items() if name.startswith("w2_u0"))
        for segment in ("s3_u0", "s4_u0", "s5_u0"):
            self.assertIn(f'"{segment}"', w2)


if __name__ == "__main__":
    unittest.main()
