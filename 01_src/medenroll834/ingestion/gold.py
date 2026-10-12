"""
src/ingestion/gold.py
Gold layer: Analytics-ready deduplicated enrollments.
"""
from typing import Dict

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.medenroll834.common.logging import log_pipeline_end, log_pipeline_start, setup_logging


class GoldLayer:
    """Gold layer produces deduplicated, analytics-ready enrollment records."""

    def __init__(self, spark: SparkSession, config: Dict):
        self.spark = spark
        self.config = config

    def transform(self, silver_df: DataFrame) -> DataFrame:
        self.logger = setup_logging("gold")
        log_pipeline_start(self.logger, "GOLD", "transforming silver to gold")

        # subscriber_id is resolved in Silver from the HL hierarchy (a
        # dependent's hl_parent_id points at its subscriber's hl_id), so Gold
        # does no re-derivation and no positional guesswork. The earlier version
        # joined on group_id, which Silver had populated from the REF*2L
        # address-association key -- that is a *member* id, so the join both
        # failed to resolve and fanned out rows.
        gold_df = (
            silver_df.withColumn(
                "subscriber_id",
                F.when(F.col("is_subscriber"), F.col("member_id")).otherwise(
                    F.col("subscriber_id")
                ),
            )
            .filter(F.col("is_valid") == True)
            .dropDuplicates(["file_name", "member_id"])
            .withColumn("full_name", F.concat_ws(" ", F.col("first_name"), F.col("last_name")))
            .withColumn(
                "age_years",
                F.floor(
                    F.datediff(F.current_date(), F.to_date(F.col("date_of_birth"), "yyyy-MM-dd")) / 365.25
                ),
            )
            .withColumn("ingested_at", F.current_timestamp().cast("string"))
        )

        table_path = self.config["layers"]["gold"]["path"]
        (
            gold_df.write.format("delta")
            .mode("overwrite")
            .option("mergeSchema", "true")
            .option("delta.enableChangeDataFeed", "true")
            .save(table_path)
        )

        log_pipeline_end(self.logger, "GOLD", f"records={gold_df.count()}")
        return gold_df
