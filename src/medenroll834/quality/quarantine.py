"""
src/quality/quarantine.py
Quarantine manager using Delta Lake for invalid records.
"""
from typing import Dict

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.medenroll834.common.logging import log_pipeline_end, log_pipeline_start, setup_logging


class DeltaQuarantineManager:
    """Writes invalid member records to a Delta quarantine table."""

    def __init__(self, spark: SparkSession, config: Dict):
        self.spark = spark
        self.config = config

    def write_invalid(self, invalid_df: DataFrame) -> None:
        self.logger = setup_logging("quarantine")
        log_pipeline_start(self.logger, "QUARANTINE", "writing invalid records")

        quarantine_df = invalid_df.withColumn(
            "quarantined_at", F.current_timestamp().cast("string")
        ).withColumn("quarantine_id", F.expr("uuid()"))

        table_path = self.config["quarantine"]["path"]
        (
            quarantine_df.write.format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .save(table_path)
        )

        log_pipeline_end(self.logger, "QUARANTINE", f"records={quarantine_df.count()}")
