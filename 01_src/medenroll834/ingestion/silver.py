"""
src/ingestion/silver.py
Silver layer: Structured member records from Bronze segments.
"""
from typing import Dict

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    StringType,
    StructField,
    StructType,
)
from pyspark.sql.window import Window

from src.medenroll834.common.logging import log_pipeline_end, log_pipeline_start, setup_logging


SILVER_SCHEMA = StructType([
    StructField("file_name", StringType(), True),
    StructField("member_id", StringType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("date_of_birth", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("group_id", StringType(), True),
    StructField("group_name", StringType(), True),
    StructField("plan_id", StringType(), True),
    StructField("coverage_start_date", StringType(), True),
    StructField("coverage_end_date", StringType(), True),
    StructField("street_1", StringType(), True),
    StructField("street_2", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("zip_code", StringType(), True),
    StructField("relationship", StringType(), True),
    StructField("is_subscriber", BooleanType(), True),
    StructField("hl_id", StringType(), True),
    StructField("hl_parent_id", StringType(), True),
    StructField("subscriber_id", StringType(), True),
    StructField("address_member_id", StringType(), True),
    StructField("validation_errors", ArrayType(StringType()), True),
    StructField("is_valid", BooleanType(), True),
])


class SilverLayer:
    """Silver layer transforms raw segments into validated member records."""

    def __init__(self, spark: SparkSession, config: Dict):
        self.spark = spark
        self.config = config
        self.rel_map = F.create_map(
            *[F.lit(x) for pair in {"18": "self", "01": "spouse", "03": "child", "19": "domestic_partner"}.items() for x in pair]
        )

    def transform(self, bronze_df: DataFrame, write: bool = True) -> DataFrame:
        self.logger = setup_logging("silver")
        log_pipeline_start(self.logger, "SILVER", "transforming bronze to silver")

        ordered = bronze_df.withColumn(
            "seg_row",
            F.row_number().over(Window.partitionBy("file_name").orderBy(F.col("seg_order")))
        )

        # HL ids are only unique *within a transaction set* (each ST*834.../SE
        # pair restarts numbering at 1), not within the file. A file with 100
        # enrolments contains 100 HL rows with hl_id=1, so resolving dependents
        # against subscribers on hl_id alone matches every subscriber in the
        # file and fans out 100x.
        #
        # Derive a transaction-scoped key by carrying the *most recent* ST row
        # forward. An earlier version range-joined against every ST row instead,
        # which matched a segment to all preceding transaction sets and
        # multiplied the member rows ~5x; last() over the transaction-start
        # window yields exactly one key per segment.
        file_order = Window.partitionBy("file_name").orderBy(F.col("seg_row"))
        ordered = ordered.withColumn(
            "txn_row",
            F.last(
                F.when(F.col("segment_id") == "ST", F.col("seg_row")),
                ignorenulls=True,
            ).over(file_order.rowsBetween(Window.unboundedPreceding, Window.currentRow)),
        )
        ordered = ordered.withColumn(
            "hl_key",
            F.concat_ws("-", F.col("file_name"), F.col("txn_row").cast("string")),
        )

        # HL structure: HL*<id>*<parent_id>*<level>*<child_count>
        # e.g. HL*1**20*3  -> id=1, parent=None, level=20 (subscriber)
        #      HL*2*1*21*  -> id=2, parent=1,    level=21 (dependent)
        # Capturing id/parent lets a dependent be tied to its own subscriber
        # through the X12 hierarchy instead of guessing by position.
        hl_events = ordered.filter(F.col("segment_id") == "HL").select(
            F.col("file_name"),
            F.col("hl_key"),
            F.col("seg_row"),
            F.col("elements")[1].alias("hl_id"),
            F.col("elements")[2].alias("hl_parent_id"),
            F.col("elements")[3].alias("hl_level"),
        )

        hl_window = Window.partitionBy("hl_key").orderBy(F.col("seg_row"))
        hl_with_next = hl_events.withColumn(
            "next_hl_row",
            F.lead("seg_row").over(hl_window)
        ).fillna(999999999, subset=["next_hl_row"])

        member_ranges = hl_with_next.select(
            F.col("file_name"),
            F.col("hl_key"),
            F.col("seg_row").alias("seg_start"),
            F.col("next_hl_row").alias("seg_end"),
            F.col("hl_id"),
            F.col("hl_parent_id"),
            F.col("hl_level"),
        )

        segment_events = ordered.filter(F.col("segment_id").isNotNull())

        members_df = segment_events.alias("seg").join(
            member_ranges.alias("rng"),
            (F.col("seg.file_name") == F.col("rng.file_name")) &
            (F.col("seg.hl_key") == F.col("rng.hl_key")) &
            (F.col("seg.seg_row") >= F.col("rng.seg_start")) &
            (F.col("seg.seg_row") < F.col("rng.seg_end")),
            how="inner"
        ).drop(F.col("rng.file_name"))

        base = member_ranges
        base = base.join(self._extract_nm1_data(members_df), ["file_name", "seg_start"], "left")
        base = base.join(self._extract_ins_data(members_df), ["file_name", "seg_start"], "left")
        base = base.join(self._extract_dtp_data(members_df), ["file_name", "seg_start"], "left")
        base = base.withColumn("date_of_birth", self._format_date("dob_raw"))
        base = base.withColumn("coverage_start_date", self._format_date("cov_start_raw"))
        base = base.withColumn("coverage_end_date", self._format_date("cov_end_raw"))
        base = base.join(self._extract_hd_data(members_df), ["file_name", "seg_start"], "left")
        base = base.join(self._extract_eb_data(members_df), ["file_name", "seg_start"], "left")
        base = base.withColumn(
            "coverage_start_date",
            F.coalesce(F.col("coverage_start_date"), self._format_date("eb_start"))
        )
        base = base.withColumn(
            "coverage_end_date",
            F.coalesce(F.col("coverage_end_date"), self._format_date("eb_end"))
        )

        # HL level 20 = subscriber, 21 = dependent. Gold depends on this flag to
        # roll dependents up to their subscriber, so it must be materialised
        # here; leaving it null silently produces null group_id in Gold.
        base = base.withColumn(
            "is_subscriber",
            F.when(F.col("hl_level") == "20", F.lit(True)).otherwise(F.lit(False)),
        )

        # Resolve each dependent to its subscriber via the HL hierarchy: a
        # dependent (level 21) points at its subscriber through hl_parent_id,
        # so the subscriber is the level-20 row with hl_id equal to this row's
        # hl_parent_id within the same transaction set.
        #
        # Implemented with a broadcast map rather than a self-join. A join here
        # duplicated every dependent against every candidate subscriber and
        # blew the executor heap; a broadcast lookup is O(1) per row and cannot
        # fan out because the key set is collapsed with first().
        subscriber_map = {
            f"{r['hl_key']}|{r['hl_id']}": r["member_id"]
            for r in base.filter(F.col("is_subscriber"))
            .select("hl_key", "hl_id", "member_id")
            .dropDuplicates(["hl_key", "hl_id"])
            .collect()
        }
        subscriber_lookup = F.create_map(
            *[
                item
                for (k, v) in subscriber_map.items()
                for item in (F.lit(k), F.lit(v))
            ]
        )
        base = base.withColumn(
            "subscriber_id",
            F.when(
                F.col("is_subscriber"),
                F.col("member_id"),
            ).otherwise(
                F.element_at(
                    subscriber_lookup,
                    F.concat_ws("|", F.col("hl_key"), F.col("hl_parent_id")),
                )
            ),
        )

        base = self._extract_address_data(base, segment_events)
        base = self._apply_validation(base)

        base = base.withColumn("ingested_at", F.current_timestamp().cast("string"))

        if write:
            table_path = self.config["layers"]["silver"]["path"]
            self._write(base, table_path)

        valid_count = base.filter(F.col("is_valid")).count()
        invalid_count = base.filter(~F.col("is_valid")).count()
        log_pipeline_end(self.logger, "SILVER", f"total={base.count()} valid={valid_count} invalid={invalid_count}")

        return base

    def _write(self, df: DataFrame, path: str) -> None:
        try:
            df.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(path)
        except Exception as exc:
            self.logger.warning(f"Delta write failed, falling back to parquet: {exc}")
            df.write.format("parquet").mode("overwrite").save(path)

    def _extract_nm1_data(self, members_df):
        nm1_seg = members_df.filter(F.col("segment_id") == "NM1")
        return nm1_seg.groupBy("file_name", "seg_start", "hl_level").agg(
            F.max(F.when(F.col("elements")[1] == "IL", F.col("elements")[2])).alias("last_name"),
            F.max(F.when(F.col("elements")[1] == "IL", F.col("elements")[3])).alias("first_name"),
            F.max(F.when(F.col("elements")[1] == "IL", F.col("elements")[8])).alias("member_id"),
        ).select("file_name", "seg_start", "last_name", "first_name", "member_id")

    def _extract_ins_data(self, members_df):
        ins_seg = members_df.filter(F.col("segment_id") == "INS")
        return ins_seg.groupBy("file_name", "seg_start", "hl_level").agg(
            F.max(F.col("elements")[2]).alias("ins_rel_code"),
        ).withColumn(
            "relationship",
            F.when(F.col("hl_level") == "20", F.lit("self"))
            .otherwise(F.element_at(self.rel_map, F.col("ins_rel_code")))
        ).select("file_name", "seg_start", "relationship")

    def _extract_dtp_data(self, members_df):
        dtp_seg = members_df.filter(F.col("segment_id") == "DTP")
        return dtp_seg.groupBy("file_name", "seg_start").agg(
            F.max(F.when(F.col("elements")[1] == "291", F.col("elements")[3])).alias("dob_raw"),
            F.max(F.when(F.col("elements")[1].isin(["472", "348"]), F.col("elements")[3])).alias("cov_start_raw"),
            F.max(F.when(F.col("elements")[1] == "349", F.col("elements")[3])).alias("cov_end_raw"),
        )

    def _extract_hd_data(self, members_df):
        hd_seg = members_df.filter(F.col("segment_id") == "HD")
        return hd_seg.groupBy("file_name", "seg_start").agg(
            F.max(F.col("elements")[3]).alias("plan_code"),
        ).withColumn("plan_id", F.col("plan_code")).select("file_name", "seg_start", "plan_id")

    def _extract_eb_data(self, members_df):
        eb_seg = members_df.filter(F.col("segment_id") == "EB")
        return eb_seg.groupBy("file_name", "seg_start").agg(
            F.max(F.col("elements")[2]).alias("eb_start"),
            F.max(F.col("elements")[3]).alias("eb_end"),
        )

    def _extract_address_data(self, df, segment_events):
        n3_seg = segment_events.filter(F.col("segment_id") == "N3").select(
            F.col("file_name"),
            F.col("seg_row").alias("n3_row"),
            F.col("elements")[1].alias("addr_street_1"),
            F.col("elements")[2].alias("addr_street_2"),
        )
        n4_seg = segment_events.filter(F.col("segment_id") == "N4").select(
            F.col("file_name"),
            F.col("seg_row").alias("n4_row"),
            F.col("elements")[1].alias("addr_city"),
            F.col("elements")[2].alias("addr_state"),
            F.col("elements")[3].alias("addr_zip"),
        )
        ref_seg = segment_events.filter(
            (F.col("segment_id") == "REF") & (F.col("elements")[1] == "2L")
        ).select(
            F.col("file_name"),
            F.col("seg_row").alias("ref_row"),
            F.col("elements")[2].alias("ref_member_id"),
        )

        ref_with_n3 = ref_seg.alias("r").join(
            n3_seg.alias("n3"),
            (F.col("r.file_name") == F.col("n3.file_name")) & (F.col("n3.n3_row") < F.col("r.ref_row")),
            how="left"
        ).withColumn(
            "n3_rank",
            F.row_number().over(
                Window.partitionBy("r.ref_row").orderBy(F.col("n3.n3_row").desc())
            )
        ).filter(F.col("n3_rank") == 1).select(
            F.col("r.file_name"),
            F.col("r.ref_row"),
            F.col("r.ref_member_id").alias("member_id"),
            F.col("n3.addr_street_1"),
            F.col("n3.addr_street_2"),
        )

        ref_with_addr = ref_with_n3.alias("r2").join(
            n4_seg.alias("n4"),
            (F.col("r2.file_name") == F.col("n4.file_name")) & (F.col("n4.n4_row") < F.col("r2.ref_row")),
            how="left"
        ).withColumn(
            "n4_rank",
            F.row_number().over(
                Window.partitionBy("r2.member_id").orderBy(F.col("n4.n4_row").desc())
            )
        ).filter(F.col("n4_rank") == 1).select(
            F.col("r2.file_name"),
            F.col("r2.member_id"),
            F.col("r2.addr_street_1"),
            F.col("r2.addr_street_2"),
            F.col("n4.addr_city"),
            F.col("n4.addr_state"),
            F.col("n4.addr_zip"),
        )

        ref_with_group = ref_with_addr

        result = df.join(
            ref_with_group,
            (df.file_name == ref_with_group.file_name) & (df.member_id == ref_with_group.member_id),
            how="left"
        ).drop(ref_with_group.member_id).drop(ref_with_group.file_name)

        return (
            result.withColumn("street_1", F.col("addr_street_1"))
            .withColumn("street_2", F.col("addr_street_2"))
            .withColumn("city", F.col("addr_city"))
            .withColumn("state", F.col("addr_state"))
            .withColumn("zip_code", F.col("addr_zip"))
            # REF*2L carries the *member* id that the address block belongs to
            # (verified 1:1 against NM1 ids), not a group. Modelling it as
            # group_id silently set group_id = member_id for matched rows and
            # null for the rest, so expose it under an honest name instead.
            .withColumn("address_member_id", F.col("member_id"))
            .drop("addr_street_1", "addr_street_2", "addr_city", "addr_state", "addr_zip")
        )

    def _format_date(self, col_name: str):
        return F.when(
            (F.length(F.col(col_name)) == 8) & F.col(col_name).rlike(r"^\d{8}$"),
            F.concat(
                F.substring(F.col(col_name), 1, 4), F.lit("-"),
                F.substring(F.col(col_name), 5, 2), F.lit("-"),
                F.substring(F.col(col_name), 7, 2),
            ),
        ).otherwise(F.col(col_name))

    def _apply_validation(self, df: DataFrame) -> DataFrame:
        errors = F.filter(
            F.array(
                F.when(F.col("member_id") == "", F.lit("Member ID is required")),
                F.when(F.col("first_name") == "", F.lit("First name is required")),
                F.when(F.col("last_name") == "", F.lit("Last name is required")),
                F.when(F.col("plan_id") == "", F.lit("Plan ID is required")),
                F.when(
                    (F.col("coverage_start_date").isNotNull()) & (F.col("coverage_end_date").isNotNull()),
                    F.when(
                        F.try_to_date(F.col("coverage_start_date")) > F.try_to_date(F.col("coverage_end_date")),
                        F.lit("Coverage start after end"),
                    ),
                ),
                F.when(
                    (F.col("date_of_birth").isNotNull()) & (F.col("coverage_start_date").isNotNull()),
                    F.when(
                        F.try_to_date(F.col("coverage_start_date")) < F.try_to_date(F.col("date_of_birth")),
                        F.lit("Coverage before DOB"),
                    ),
                ),
                F.when(
                    ~F.col("plan_id").isin(list({"GOLD", "SILVER", "BRONZE", "PLATINUM", "HDHP", "EPO", "PPO", "HMO"})),
                    F.concat(F.lit("Unknown plan: "), F.col("plan_id")),
                ),
            ),
            lambda x: x.isNotNull(),
        )
        return df.withColumn("validation_errors", errors).withColumn("is_valid", F.size(F.col("validation_errors")) == 0)
