"""
Validate the populated pipeline: edi.bronze_edi_segments -> edi.silver_members
-> edi.gold_enrollments -> gold.fact_enrollment (+ dims). Read-only, no writes.
"""
import os, time
from pathlib import Path
for line in Path(".env").read_text(encoding="utf-8").splitlines():
    line = line.strip()
    if line and not line.startswith("#") and "=" in line:
        k, v = line.split("=", 1); os.environ.setdefault(k.strip(), v.strip())
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState
w = WorkspaceClient(host=os.environ["DATABRICKS_HOST"], token=os.environ["DATABRICKS_TOKEN"])
wid = os.environ["DATABRICKS_WAREHOUSE_ID"]

def q(sql):
    r = w.statement_execution.execute_statement(warehouse_id=wid, statement=sql, wait_timeout="50s")
    while r.status.state in (StatementState.PENDING, StatementState.RUNNING):
        time.sleep(2); r = w.statement_execution.get_statement(r.statement_id)
    if r.status.state != StatementState.SUCCEEDED:
        return [("ERROR", r.status.error.message if r.status.error else "?")]
    return r.result.data_array if r.result else []

def section(title):
    print("\n" + "=" * 70)
    print(title)
    print("=" * 70)

section("1. Source files in bronze (distinct file_name)")
print(q("SELECT file_name, COUNT(*) AS segments FROM med834enrl.edi.bronze_edi_segments GROUP BY file_name"))

section("2. Bronze segment_id distribution (sanity check on parsing)")
print(q("SELECT segment_id, COUNT(*) AS c FROM med834enrl.edi.bronze_edi_segments GROUP BY segment_id ORDER BY c DESC"))

section("3. NM1 count per file (expected member count: subscribers + dependents)")
print(q("SELECT file_name, COUNT(*) AS nm1_count FROM med834enrl.edi.bronze_edi_segments WHERE segment_id='NM1' GROUP BY file_name"))

section("4. Silver: total, valid, invalid, distinct member_id, duplicate member_id check")
print("totals:", q("SELECT COUNT(*) AS total FROM med834enrl.edi.silver_members"))
print("distinct member_id:", q("SELECT COUNT(DISTINCT member_id) FROM med834enrl.edi.silver_members"))
print("duplicate member_id rows (should be 0 if PK-clean):",
      q("SELECT member_id, COUNT(*) c FROM med834enrl.edi.silver_members GROUP BY member_id HAVING COUNT(*) > 1"))

section("5. Quarantine (invalid records) - should explain bronze segment rows that did not become members")
print(q("SELECT COUNT(*) FROM med834enrl.edi.quarantine_invalid_members"))

section("6. Silver -> Gold: row count parity + dedup check")
print("silver total:", q("SELECT COUNT(*) FROM med834enrl.edi.silver_members"))
print("gold total:", q("SELECT COUNT(*) FROM med834enrl.edi.gold_enrollments"))
print("gold distinct (file_name, member_id):",
      q("SELECT COUNT(*) FROM (SELECT DISTINCT file_name, member_id FROM med834enrl.edi.gold_enrollments)"))

section("7. Gold -> FACT_ENROLLMENT: row count parity")
print("gold_enrollments:", q("SELECT COUNT(*) FROM med834enrl.edi.gold_enrollments"))
print("fact_enrollment:", q("SELECT COUNT(*) FROM med834enrl.gold.fact_enrollment"))

section("8. FACT_ENROLLMENT: any NULL surrogate keys (would mean a failed dimension join)")
print(q("""
    SELECT
      SUM(CASE WHEN MEME_KEY IS NULL THEN 1 ELSE 0 END) AS null_meme,
      SUM(CASE WHEN PLPL_KEY IS NULL THEN 1 ELSE 0 END) AS null_plpl,
      SUM(CASE WHEN GRGR_KEY IS NULL THEN 1 ELSE 0 END) AS null_grgr,
      SUM(CASE WHEN TXN_SRC_KEY IS NULL THEN 1 ELSE 0 END) AS null_txn,
      SUM(CASE WHEN COVERAGE_START_DATE_KEY IS NULL THEN 1 ELSE 0 END) AS null_start_date,
      COUNT(*) AS total_rows
    FROM med834enrl.gold.fact_enrollment
"""))

section("9. FACT_ENROLLMENT FK resolution: every key actually exists in its dimension")
print("MEME_KEY orphans:", q(
    "SELECT COUNT(*) FROM med834enrl.gold.fact_enrollment f "
    "LEFT JOIN med834enrl.gold.dim_member m ON f.MEME_KEY = m.MEME_KEY "
    "WHERE m.MEME_KEY IS NULL"))
print("PLPL_KEY orphans:", q(
    "SELECT COUNT(*) FROM med834enrl.gold.fact_enrollment f "
    "LEFT JOIN med834enrl.gold.dim_plan p ON f.PLPL_KEY = p.PLPL_KEY "
    "WHERE p.PLPL_KEY IS NULL"))
print("GRGR_KEY orphans:", q(
    "SELECT COUNT(*) FROM med834enrl.gold.fact_enrollment f "
    "LEFT JOIN med834enrl.gold.dim_group gr ON f.GRGR_KEY = gr.GRGR_KEY "
    "WHERE gr.GRGR_KEY IS NULL"))
print("TXN_SRC_KEY orphans:", q(
    "SELECT COUNT(*) FROM med834enrl.gold.fact_enrollment f "
    "LEFT JOIN med834enrl.gold.dim_transaction_source t ON f.TXN_SRC_KEY = t.TXN_SRC_KEY "
    "WHERE t.TXN_SRC_KEY IS NULL"))
print("COVERAGE_START_DATE_KEY orphans:", q(
    "SELECT COUNT(*) FROM med834enrl.gold.fact_enrollment f "
    "LEFT JOIN med834enrl.gold.dim_date d ON f.COVERAGE_START_DATE_KEY = d.DATE_KEY "
    "WHERE d.DATE_KEY IS NULL"))

section("10. Grain check: FACT_ENROLLMENT should be one row per member (no fan-out duplication)")
print("distinct MEME_KEY in fact:", q("SELECT COUNT(DISTINCT MEME_KEY) FROM med834enrl.gold.fact_enrollment"))
print("total fact rows:", q("SELECT COUNT(*) FROM med834enrl.gold.fact_enrollment"))
print("duplicate MEME_KEY rows (should be empty):",
      q("SELECT MEME_KEY, COUNT(*) c FROM med834enrl.gold.fact_enrollment GROUP BY MEME_KEY HAVING COUNT(*) > 1"))

section("11. Expected count sanity (subscribers x (1 self + N dependents), summed per file)")
print("files processed:", q("SELECT COUNT(DISTINCT file_name) FROM med834enrl.edi.gold_enrollments"))
print("members per file:", q("SELECT file_name, COUNT(*) FROM med834enrl.edi.gold_enrollments GROUP BY file_name"))

print("\nDONE - read-only validation complete, no data modified.")

