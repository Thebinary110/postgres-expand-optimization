"""Headless driver for the DB Manager's Expand operation (development benchmark).

Performs the same service calls as ``main.App._execute_cross_table_expansion_thread`` (connect, control
setup, timing report, execute_cross_table_expansion, register_cross_table_expansion_versions, refresh)
without the Tk GUI, then prints the Expansion Performance Report with the GUI's own formatter
(ui.dialogs.ExpansionTimingReportDialog.build_report_text). The GUI-only refresh step "Rebuild table
navigation controls" is not executed here, so totals can differ from the GUI by that step.

Credentials come from the environment (never from arguments):
  PDM_SSH_HOST, PDM_SSH_PORT, PDM_SSH_USER, PDM_SSH_PASSWORD, PDM_PG_PORT, PDM_PG_USER, PDM_PG_PASSWORD

  python bench/headless_expand.py --database kaggle_challenge --report out.txt
"""
import argparse
import json
import os
import socket
import sys
import types
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1] if globals().get("__file__", "").endswith(".py") else Path.cwd()
sys.path.insert(0, str(ROOT))

RECIPES = [
    ("public.table3", "01_table3.sql"),
    ("public.table4", "02_table4.sql"),
    ("public.table1", "03_table1.sql"),
    ("public.table5", "04_table5.sql"),
    ("public.table6", "05_table6.sql"),
    ("public.table2", "06_table2.sql"),
]


def _stub_gui_modules():
    """ui.dialogs imports tkinter/customtkinter at module level; only its pure classmethods are used here."""
    class _Any(type):
        def __getattr__(cls, name):
            return _Any(name, (object,), {})

    def module(name):
        m = types.ModuleType(name)
        m.__getattr__ = lambda attr: _Any(attr, (object,), {})
        return m
    for name in ("tkinter", "tkinter.ttk", "tkinter.font", "tkinter.filedialog", "tkinter.messagebox",
                 "customtkinter"):
        sys.modules.setdefault(name, module(name))
    sys.modules["tkinter"].ttk = sys.modules["tkinter.ttk"]
    sys.modules["tkinter"].font = sys.modules["tkinter.font"]
    sys.modules["tkinter"].filedialog = sys.modules["tkinter.filedialog"]
    sys.modules["tkinter"].messagebox = sys.modules["tkinter.messagebox"]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--database", default="kaggle_challenge")
    ap.add_argument("--source", default="public.raw_data")
    ap.add_argument("--raw-schema", default="group001")
    ap.add_argument("--author", default="bench")
    ap.add_argument("--title", default="headless benchmark")
    ap.add_argument("--report", default=None, help="write the report text here")
    ap.add_argument("--json", default=None, help="write the raw timing report dict here")
    ap.add_argument("--skip-register", action="store_true",
                    help="stop right after the data commit (simulates a client lost before version registration)")
    ap.add_argument("--recover-only", action="store_true", help="only finish pending registrations, then exit")
    args = ap.parse_args()

    _stub_gui_modules()
    from services.postgres_service import PostgresAdminService as PostgresService
    from ui.dialogs import ExpansionTimingReportDialog

    env = os.environ
    service = PostgresService()
    service.connect(host=env["PDM_SSH_HOST"], ssh_port=int(env["PDM_SSH_PORT"]), ssh_username=env["PDM_SSH_USER"],
                    ssh_password=env["PDM_SSH_PASSWORD"], postgres_port=int(env.get("PDM_PG_PORT", "5432")),
                    sql_username=env["PDM_PG_USER"], sql_password=env["PDM_PG_PASSWORD"])
    service.ensure_control_database()
    service.ensure_remote_admin_dirs()
    service.ensure_admin_schema()
    for item in service.recover_pending_expansions():
        print(f"recovered pending expansion {item['job_id']} in {item['database_name']}", flush=True)
    if args.recover_only:
        service.close()
        return

    raw_schemas = service.resolve_raw_schema_interval(
        service.list_table_raw_schemas(args.database, args.source), args.raw_schema, args.raw_schema)
    destinations = [dict(table_name=t, version_title=args.title,
                         sql=(ROOT / "SQL files" / f).read_text(encoding="utf-8")) for t, f in RECIPES]

    report = service.create_cross_table_expansion_timing_report(
        args.database, args.source, raw_schemas, [d["table_name"] for d in destinations])
    with service.expansion_timing_scope(report, "prepare", "Ensure control database metadata"):
        service.ensure_admin_schema(cancel_event=None)
    result = service.execute_cross_table_expansion(
        database_name=args.database, source_full_table_name=args.source, raw_schemas=raw_schemas,
        destinations=destinations, cancel_event=None,
        job_context={"requested_by": args.author, "workstation_name": socket.gethostname()},
        progress_callback=lambda stage, pct, msg: print(f"[{stage} {pct:>3}%] {msg}", flush=True),
        post_commit_callback=lambda *a, **k: None, timing_report=report)
    if args.skip_register:
        print(f"data committed by job {result.get('job_id')}; registration skipped", flush=True)
        service.close()
        return
    versions = service.register_cross_table_expansion_versions(
        expansion_result=result, requested_by=args.author, workstation_name=socket.gethostname(),
        cancel_event=None, progress_callback=lambda stage, pct, msg: print(f"[{stage} {pct:>3}%] {msg}", flush=True),
        timing_report=report)
    with service.expansion_timing_scope(report, "refresh", "Invalidate destination filter caches"):
        pass  # GUI-side sqlite facet cache; no cache exists in a headless run
    with service.expansion_timing_scope(report, "refresh", "Reload database table catalog"):
        service.list_tables(args.database)
    with service.expansion_timing_scope(report, "refresh", "Build completion summary"):
        ", ".join(f"{v['table_name']}={v['version_code']}" for v in versions)
    report["completed_at"] = datetime.now().astimezone().isoformat(timespec="seconds")

    text = ExpansionTimingReportDialog.build_report_text(report)
    print(text)
    if args.report:
        Path(args.report).write_text(text, encoding="utf-8")
    if args.json:
        Path(args.json).write_text(json.dumps(report, default=str, indent=1), encoding="utf-8")
    service.close()


if __name__ == "__main__":
    main()
