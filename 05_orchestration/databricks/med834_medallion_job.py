# Databricks notebook source
# MedEnroll834 medallion pipeline - SINGLE CELL (reliable on serverless jobs).
import json
import time
import traceback
from datetime import timedelta, datetime, timezone
from pyspark.sql import functions as F
from pyspark.sql.window import Window

LANDING = "/Volumes/med834enrl/edi/landing/834/"
CATALOG = "med834enrl"
EDI = f"{CATALOG}.edi"
GOLD = f"{CATALOG}.gold"
spark.sql(f"USE CATALOG {CATALOG}")
result = {}

# airflow_run_id is passed as a job_parameter by the Airflow DAG's
# run_databricks_job task (WorkspaceClient().jobs.run_now_and_wait(...,
# job_parameters={"airflow_run_id": ...})) so this notebook's own log/print
# output can be correlated back to the triggering Airflow DAG run. Falls back
# to "manual" for ad-hoc runs triggered directly from the Databricks UI.
dbutils.widgets.text("airflow_run_id", "manual")
RUN_ID = dbutils.widgets.get("airflow_run_id")


def _log(stage: str, message: str) -> None:
    """Lightweight structured print -- Databricks captures stdout into the
    job run's log output, and run=RUN_ID lets it be grepped/correlated with
    the Airflow task log for the same pipeline run."""
    ts = datetime.now(timezone.utc).isoformat()
    print(f"{ts} [INFO] medallion | run={RUN_ID} - {stage} | {message}")


def _log_error(stage: str, exc: Exception) -> None:
    ts = datetime.now(timezone.utc).isoformat()
    print(f"{ts} [ERROR] medallion | run={RUN_ID} - {stage} FAILED | {exc}")
    traceback.print_exc()


_log("PIPELINE", f"starting medallion job | landing={LANDING}")
_pipeline_start = time.monotonic()

# ---------- Bronze ----------
_stage_start = time.monotonic()
raw = spark.sql(
    f"SELECT value, element_at(split(_metadata.file_path, '/'), -1) AS file_name "
    f"FROM read_files('{LANDING}', format => 'text', wholeText => true)"
)
bronze = (
    raw.withColumn("segment", F.explode(F.split(F.regexp_replace("value", r"[\r\n]+", "~"), "~")))
    .filter(F.trim("segment") != "")
    .withColumn("elements", F.split(F.col("segment"), r"\*"))
    .withColumn("segment_id", F.element_at("elements", 1))
    .withColumn("seg_order", F.monotonically_increasing_id())
    .withColumn("raw_segment", F.col("segment"))
    .withColumn("ingested_at", F.current_timestamp())
    .select("file_name", "segment_id", "elements", "raw_segment", "seg_order", "ingested_at")
)
bronze.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{EDI}.bronze_edi_segments")
result["bronze"] = spark.table(f"{EDI}.bronze_edi_segments").count()
_log("BRONZE", f"segments={result['bronze']} duration_s={time.monotonic() - _stage_start:.2f}")

# ---------- Silver (carry-forward current NM1 member) ----------
_stage_start = time.monotonic()
b = spark.table(f"{EDI}.bronze_edi_segments")
mem_win = Window.partitionBy("file_name").orderBy("seg_order").rowsBetween(Window.unboundedPreceding, Window.currentRow)
tagged = (
    b.withColumn("cur_member", F.last(F.when(F.col("segment_id") == "NM1", F.element_at("elements", 9)), ignorenulls=True).over(mem_win))
     .withColumn("cur_first", F.last(F.when(F.col("segment_id") == "NM1", F.element_at("elements", 4)), ignorenulls=True).over(mem_win))
     .withColumn("cur_last", F.last(F.when(F.col("segment_id") == "NM1", F.element_at("elements", 3)), ignorenulls=True).over(mem_win))
     .filter(F.col("cur_member").isNotNull())
)
members = tagged.groupBy("file_name", "cur_member", "cur_first", "cur_last").agg(
    F.max(F.when((F.col("segment_id") == "DTP") & (F.element_at("elements", 2) == "291"), F.element_at("elements", 4))).alias("dob"),
    F.max(F.when((F.col("segment_id") == "DTP") & (F.element_at("elements", 2).isin("348", "472")), F.element_at("elements", 4))).alias("cov_start"),
    F.max(F.when((F.col("segment_id") == "DTP") & (F.element_at("elements", 2) == "349"), F.element_at("elements", 4))).alias("cov_end"),
    F.max(F.when(F.col("segment_id") == "HD", F.element_at("elements", 4))).alias("plan_id"),
    F.max(F.when(F.col("segment_id") == "N3", F.element_at("elements", 2))).alias("street_1"),
    F.max(F.when(F.col("segment_id") == "N4", F.element_at("elements", 2))).alias("city"),
    F.max(F.when(F.col("segment_id") == "N4", F.element_at("elements", 3))).alias("state"),
    F.max(F.when(F.col("segment_id") == "N4", F.element_at("elements", 4))).alias("zip_code"),
    F.max(F.when(F.col("segment_id") == "INS", F.element_at("elements", 3))).alias("rel_code"),
    # REF*1L*<group number> - X12 834 standard Group or Policy Number reference.
    F.max(F.when((F.col("segment_id") == "REF") & (F.element_at("elements", 2) == "1L"), F.element_at("elements", 3))).alias("group_id"),
).withColumnRenamed("cur_member", "member_id").withColumnRenamed("cur_first", "first_name").withColumnRenamed("cur_last", "last_name")

# Plan codes are plain numeric identifiers (e.g. "100"); PLAN_TIER_NAME maps
# each code to a readable tier label used for PLAN_NAME synthesis.
VALID_PLANS = ["100", "101", "102", "103", "104", "105", "106", "107"]
PLAN_TIER_NAME = F.create_map(
    F.lit("100"), F.lit("GOLD"), F.lit("101"), F.lit("SILVER"),
    F.lit("102"), F.lit("BRONZE"), F.lit("103"), F.lit("PLATINUM"),
    F.lit("104"), F.lit("HDHP"), F.lit("105"), F.lit("EPO"),
    F.lit("106"), F.lit("PPO"), F.lit("107"), F.lit("HMO"),
)
rel_map = F.create_map(F.lit("18"), F.lit("self"), F.lit("01"), F.lit("spouse"), F.lit("03"), F.lit("child"), F.lit("19"), F.lit("domestic_partner"))
silver = (members
    .withColumn("relationship", F.coalesce(rel_map[F.col("rel_code")], F.lit("self")))
    .withColumn("is_subscriber", F.col("relationship") == "self")
    .withColumn("gender", F.lit(None).cast("string"))
    .withColumn("group_id", F.coalesce(F.col("group_id"), F.lit("UNKNOWN")))
    .withColumn("group_name", F.concat(F.lit("Group "), F.col("group_id")))
    .withColumn("street_2", F.lit(None).cast("string"))
    .withColumn("date_of_birth", F.col("dob"))
    .withColumn("coverage_start_date", F.col("cov_start")).withColumn("coverage_end_date", F.col("cov_end"))
    .withColumn("validation_errors", F.array().cast("array<string>"))
    .withColumn("is_valid", F.col("member_id").isNotNull() & F.col("plan_id").isin(VALID_PLANS))
    .withColumn("ingested_at", F.current_timestamp())
    .select("file_name", "member_id", "first_name", "last_name", "date_of_birth", "gender",
            "group_id", "group_name", "plan_id", "coverage_start_date", "coverage_end_date",
            "street_1", "street_2", "city", "state", "zip_code", "relationship", "is_subscriber",
            "validation_errors", "is_valid"))
silver.filter("is_valid").write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{EDI}.silver_members")
silver.filter("NOT is_valid").withColumn("quarantine_id", F.expr("uuid()")).withColumn("quarantined_at", F.current_timestamp()) \
    .write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{EDI}.quarantine_invalid_members")
result["silver"] = spark.table(f"{EDI}.silver_members").count()
result["quarantine"] = spark.table(f"{EDI}.quarantine_invalid_members").count()
_log("SILVER", f"total={result['silver']} quarantined={result['quarantine']} duration_s={time.monotonic() - _stage_start:.2f}")

# ---------- Gold enrollments ----------
_stage_start = time.monotonic()
gold = (spark.table(f"{EDI}.silver_members")
    .dropDuplicates(["file_name", "member_id"])
    .withColumn("full_name", F.concat_ws(" ", "first_name", "last_name"))
    .withColumn("age_years", F.floor(F.datediff(F.current_date(), F.to_date("date_of_birth", "yyyyMMdd")) / 365.25).cast("int"))
    .withColumn("ingested_at", F.current_timestamp())
    .select("file_name", "member_id", "first_name", "last_name", "date_of_birth", "gender",
            "group_id", "group_name", "plan_id", "coverage_start_date", "coverage_end_date",
            "street_1", "street_2", "city", "state", "zip_code", "relationship", "is_subscriber",
            "full_name", "age_years", "ingested_at"))
gold.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{EDI}.gold_enrollments")
result["gold_enrollments"] = spark.table(f"{EDI}.gold_enrollments").count()
_log("GOLD", f"records={result['gold_enrollments']} duration_s={time.monotonic() - _stage_start:.2f}")

# ---------- OLAP star-schema dimensions ----------
_stage_start = time.monotonic()
g = spark.table(f"{EDI}.gold_enrollments")

(g.select(F.col("plan_id").alias("PLPL_ID"), F.col("plan_id").alias("PLAN_CODE"),
          F.concat(F.coalesce(PLAN_TIER_NAME[F.col("plan_id")], F.col("plan_id")), F.lit(" Plan")).alias("PLAN_NAME"))
  .withColumn("LINE_OF_BUSINESS_CODE", F.lit("COMMERCIAL")).dropDuplicates(["PLPL_ID"])
  .withColumn("EFFECTIVE_DATE", F.current_date()).withColumn("END_DATE", F.to_date(F.lit("9999-12-31")))
  .withColumn("CURRENT_FLAG", F.lit(True))
  .write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(f"{GOLD}.dim_plan"))

(g.select(F.col("group_id").alias("GRGR_ID"), F.col("group_id").alias("GROUP_NUMBER"),
          F.col("group_name").alias("GROUP_NAME"), F.col("state").alias("STATE_CODE")).dropDuplicates(["GRGR_ID"])
  .withColumn("EFFECTIVE_DATE", F.current_date()).withColumn("END_DATE", F.to_date(F.lit("9999-12-31")))
  .withColumn("CURRENT_FLAG", F.lit(True))
  .write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(f"{GOLD}.dim_group"))

(g.select(F.col("member_id").alias("MEME_ID"), F.col("member_id").alias("MESB_ID"),
          F.col("first_name").alias("FIRST_NAME"), F.col("last_name").alias("LAST_NAME"),
          F.to_date("date_of_birth", "yyyyMMdd").alias("DATE_OF_BIRTH"), F.col("gender").alias("GENDER_CODE"),
          F.lit("18").alias("RELATIONSHIP_CODE")).dropDuplicates(["MEME_ID"])
  .withColumn("EFFECTIVE_DATE", F.current_date()).withColumn("END_DATE", F.to_date(F.lit("9999-12-31")))
  .withColumn("CURRENT_FLAG", F.lit(True))
  .write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(f"{GOLD}.dim_member"))

# DIM_DATE - spans the min/max coverage dates seen in Gold (+/- 1 year pad),
# plus a fixed 9999-12-31 sentinel row representing "open ended" coverage
# (used by FACT_ENROLLMENT.COVERAGE_END_DATE_KEY instead of NULL).
date_bounds = g.select(
    F.to_date("coverage_start_date", "yyyyMMdd").alias("d1"),
    F.to_date("coverage_end_date", "yyyyMMdd").alias("d2"),
).agg(F.min("d1").alias("min_d"), F.max(F.greatest("d1", "d2")).alias("max_d")).first()
min_d = (date_bounds["min_d"] or spark.sql("SELECT current_date()").first()[0]) - timedelta(days=365)
max_d = (date_bounds["max_d"] or spark.sql("SELECT current_date()").first()[0]) + timedelta(days=365)
dim_date = (
    spark.sql(f"SELECT explode(sequence(to_date('{min_d}'), to_date('{max_d}'), interval 1 day)) AS FULL_DATE")
    .withColumn("DATE_KEY", F.date_format("FULL_DATE", "yyyyMMdd").cast("int"))
    .withColumn("YEAR_NUMBER", F.year("FULL_DATE"))
    .withColumn("QUARTER_NUMBER", F.quarter("FULL_DATE"))
    .withColumn("MONTH_NUMBER", F.month("FULL_DATE"))
    .withColumn("DAY_NUMBER", F.dayofmonth("FULL_DATE"))
    .withColumn("DAY_OF_WEEK_NAME", F.date_format("FULL_DATE", "EEEE"))
    .select("DATE_KEY", "FULL_DATE", "YEAR_NUMBER", "QUARTER_NUMBER", "MONTH_NUMBER", "DAY_NUMBER", "DAY_OF_WEEK_NAME")
)
open_ended_row = spark.createDataFrame(
    [(99991231, "9999-12-31", 9999, 4, 12, 31, "Open Ended")],
    "DATE_KEY int, FULL_DATE string, YEAR_NUMBER int, QUARTER_NUMBER int, MONTH_NUMBER int, DAY_NUMBER int, DAY_OF_WEEK_NAME string",
).withColumn("FULL_DATE", F.to_date("FULL_DATE"))
dim_date = dim_date.unionByName(open_ended_row)
dim_date.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{GOLD}.dim_date")

# DIM_TRANSACTION_SOURCE - one row per source file (TXN_ID derived from file_name hash)
dim_txn_src = (
    g.select("file_name").dropDuplicates()
    .withColumn("TXN_ID", F.abs(F.xxhash64("file_name")))
    .withColumn("FILE_NAME", F.col("file_name"))
    .withColumn("SENDER_ID", F.lit("SENDERID"))
    .withColumn("RECEIVED_TIMESTAMP", F.current_timestamp())
    .select("TXN_ID", "FILE_NAME", "SENDER_ID", "RECEIVED_TIMESTAMP")
)
dim_txn_src.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(f"{GOLD}.dim_transaction_source")

result["dim_member"] = spark.table(f"{GOLD}.dim_member").count()
result["dim_plan"] = spark.table(f"{GOLD}.dim_plan").count()
result["dim_group"] = spark.table(f"{GOLD}.dim_group").count()
result["dim_date"] = spark.table(f"{GOLD}.dim_date").count()
result["dim_transaction_source"] = spark.table(f"{GOLD}.dim_transaction_source").count()
_log("OLAP_DIMS", f"member={result['dim_member']} plan={result['dim_plan']} group={result['dim_group']} "
                   f"date={result['dim_date']} txn_src={result['dim_transaction_source']} "
                   f"duration_s={time.monotonic() - _stage_start:.2f}")

# ---------- FACT_ENROLLMENT - resolve surrogate keys from each dimension ----------
_stage_start = time.monotonic()
dim_member_cur = spark.table(f"{GOLD}.dim_member").filter("CURRENT_FLAG = true").select(
    F.col("MEME_KEY"), F.col("MEME_ID").alias("jk_meme_id"))
dim_plan_cur = spark.table(f"{GOLD}.dim_plan").filter("CURRENT_FLAG = true").select(
    F.col("PLPL_KEY"), F.col("PLPL_ID").alias("jk_plpl_id"))
dim_group_cur = spark.table(f"{GOLD}.dim_group").filter("CURRENT_FLAG = true").select(
    F.col("GRGR_KEY"), F.col("GRGR_ID").alias("jk_grgr_id"))
dim_txn_tbl = spark.table(f"{GOLD}.dim_transaction_source").select(
    F.col("TXN_SRC_KEY"), F.col("FILE_NAME").alias("jk_file_name"))
dim_date_start = spark.table(f"{GOLD}.dim_date").select(F.col("DATE_KEY").alias("start_date_key"))
dim_date_end = spark.table(f"{GOLD}.dim_date").select(F.col("DATE_KEY").alias("end_date_key"))

fact = (
    g.withColumn("jk_cov_start_key", F.date_format(F.to_date("coverage_start_date", "yyyyMMdd"), "yyyyMMdd").cast("int"))
     # Open-ended coverage (no end date) resolves to the DIM_DATE 9999-12-31
     # sentinel row (DATE_KEY=99991231) instead of a NULL foreign key.
     .withColumn("jk_cov_end_key", F.coalesce(
         F.date_format(F.to_date("coverage_end_date", "yyyyMMdd"), "yyyyMMdd").cast("int"),
         F.lit(99991231),
     ))
     .join(dim_member_cur, F.col("member_id") == F.col("jk_meme_id"), "left").drop("jk_meme_id")
     .join(dim_plan_cur, F.col("plan_id") == F.col("jk_plpl_id"), "left").drop("jk_plpl_id")
     .join(dim_group_cur, F.col("group_id") == F.col("jk_grgr_id"), "left").drop("jk_grgr_id")
     .join(dim_txn_tbl, F.col("file_name") == F.col("jk_file_name"), "left").drop("jk_file_name")
     .join(dim_date_start, F.col("jk_cov_start_key") == F.col("start_date_key"), "left")
     .join(dim_date_end, F.col("jk_cov_end_key") == F.col("end_date_key"), "left")
     .withColumn("MAINTENANCE_TYPE_CODE", F.lit("021"))
     .withColumn("ENROLLMENT_STATUS", F.when(F.col("coverage_end_date").isNull() | (F.col("coverage_end_date") == ""), F.lit("ACTIVE")).otherwise(F.lit("TERMED")))
     .withColumn("ENROLLMENT_COUNT", F.lit(1))
     .withColumn("LOAD_TIMESTAMP", F.current_timestamp())
     .select(
        F.col("MEME_KEY"), F.col("PLPL_KEY"), F.col("GRGR_KEY"), F.col("TXN_SRC_KEY"),
        F.col("start_date_key").alias("COVERAGE_START_DATE_KEY"),
        F.col("end_date_key").alias("COVERAGE_END_DATE_KEY"),
        "MAINTENANCE_TYPE_CODE", "ENROLLMENT_STATUS", "ENROLLMENT_COUNT", "LOAD_TIMESTAMP",
     )
     .filter(F.col("MEME_KEY").isNotNull() & F.col("PLPL_KEY").isNotNull()
             & F.col("GRGR_KEY").isNotNull() & F.col("TXN_SRC_KEY").isNotNull()
             & F.col("COVERAGE_START_DATE_KEY").isNotNull())
)
fact.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{GOLD}.fact_enrollment")
result["fact_enrollment"] = spark.table(f"{GOLD}.fact_enrollment").count()
_log("FACT_ENROLLMENT", f"records={result['fact_enrollment']} duration_s={time.monotonic() - _stage_start:.2f}")

result["airflow_run_id"] = RUN_ID
_log("PIPELINE", f"completed | total_duration_s={time.monotonic() - _pipeline_start:.2f} | {json.dumps(result)}")
print("MEDALLION RESULT:", json.dumps(result, indent=2))
dbutils.notebook.exit(json.dumps(result))


