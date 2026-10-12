#!/usr/bin/env python3
"""
Backout (rollback) the MedEnroll834 Databricks deployment.

Reverses, in order:
  1. databricks_olap_backout.sql   - GOLD star schema + bronze/silver/gold schemas
  2. databricks_clean_backout.sql  - EDI medallion tables + landing volume + edi schema
  3. Deletes the medallion job and notebook from the workspace (optional)

Does NOT drop the med834enrl catalog itself - re-running the deploy scripts
recreates everything cleanly. Safe to re-run (idempotent, IF EXISTS).

Usage:
    python 07_scripts/02_deployment/backout_databricks.py [--profile <name>] [--keep-job]
"""
import argparse
import os
import re
import sys
import time
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState

REPO_ROOT = Path(__file__).resolve().parents[2]
DEPLOY_DIR = REPO_ROOT / "07_scripts" / "02_deployment"
OLAP_BACKOUT = DEPLOY_DIR / "databricks_olap_backout.sql"
EDI_BACKOUT = DEPLOY_DIR / "databricks_clean_backout.sql"

JOB_ID = 950623271437212
NOTEBOOK_PATH = "/Shared/med834enrl/med834_medallion_job"

_DEFAULT_CLI = (
    Path(os.environ.get("LOCALAPPDATA", ""))
    / "Microsoft" / "WinGet" / "Packages"
    / "Databricks.DatabricksCLI_Microsoft.Winget.Source_8wekyb3d8bbwe"
    / "databricks.exe"
)
if "DATABRICKS_CLI_PATH" not in os.environ and _DEFAULT_CLI.exists():
    os.environ["DATABRICKS_CLI_PATH"] = str(_DEFAULT_CLI)


def split_statements(sql_text: str) -> list[str]:
    no_comments = re.sub(r"--[^\n]*", "", sql_text)
    return [s.strip() for s in no_comments.split(";") if s.strip()]


def get_warehouse_id(w: WorkspaceClient, override):
    if override:
        return override
    warehouses = list(w.warehouses.list())
    if not warehouses:
        raise RuntimeError("No SQL warehouse found. Pass --warehouse-id.")
    for wh in warehouses:
        if getattr(wh.state, "value", str(wh.state)) == "RUNNING":
            return wh.id
    return warehouses[0].id


def run_sql(w, warehouse_id, statement):
    resp = w.statement_execution.execute_statement(
        warehouse_id=warehouse_id, statement=statement, wait_timeout="50s"
    )
    statement_id = resp.statement_id
    state = resp.status.state
    while state in (StatementState.PENDING, StatementState.RUNNING):
        time.sleep(2)
        resp = w.statement_execution.get_statement(statement_id)
        state = resp.status.state
    if state != StatementState.SUCCEEDED:
        msg = resp.status.error.message if resp.status.error else "unknown error"
        raise RuntimeError(f"Statement failed: {msg}\n  SQL: {statement[:120]}...")


def execute_file(w, warehouse_id, path):
    statements = split_statements(path.read_text(encoding="utf-8"))
    print(f"\nExecuting {len(statements)} statements from {path.name}...")
    for i, stmt in enumerate(statements, 1):
        first_line = stmt.splitlines()[0][:70]
        run_sql(w, warehouse_id, stmt)
        print(f"  [{i}/{len(statements)}] OK: {first_line}")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", default="dbc-645321b4-3ef9")
    parser.add_argument("--warehouse-id", default=None)
    parser.add_argument("--keep-job", action="store_true",
                         help="Do not delete the Databricks job/notebook")
    args = parser.parse_args()

    print(f"Connecting to Databricks (profile={args.profile})...")
    w = WorkspaceClient(profile=args.profile)
    print(f"  Authenticated as: {w.current_user.me().user_name}")

    warehouse_id = get_warehouse_id(w, args.warehouse_id)

    # Order matters: OLAP references nothing in edi, so either order is safe,
    # but OLAP first keeps the pattern "most-derived objects first".
    execute_file(w, warehouse_id, OLAP_BACKOUT)
    execute_file(w, warehouse_id, EDI_BACKOUT)

    if not args.keep_job:
        print("\nRemoving Databricks job and notebook...")
        try:
            w.jobs.delete(job_id=JOB_ID)
            print(f"  Deleted job {JOB_ID}")
        except Exception as exc:  # noqa: BLE001
            print(f"  (job delete skipped: {exc})")
        try:
            w.workspace.delete(NOTEBOOK_PATH)
            print(f"  Deleted notebook {NOTEBOOK_PATH}")
        except Exception as exc:  # noqa: BLE001
            print(f"  (notebook delete skipped: {exc})")

    print("\nDatabricks backout complete. Catalog med834enrl itself was kept.")
    print("Re-run deploy_databricks.py + deploy_databricks_olap.py to redeploy.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
