"""
src/quality/quarantine.py
Quarantine manager using Delta Lake for invalid records.
"""
from typing import Dict

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.medenroll834.common.logging import (
    log_pipeline_end,
    log_pipeline_error,
    log_pipeline_start,
    setup_logging,
)


class DeltaQuarantineManager:
    """Writes invalid member records to a Delta quarantine table."""

    def __init__(self, spark: SparkSession, config: Dict):
        self.spark = spark
        self.config = config

    def write_invalid(self, invalid_df: DataFrame) -> None:
        self.logger = setup_logging("quarantine")
        start_time = log_pipeline_start(self.logger, "QUARANTINE", "writing invalid records")

        try:
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
            record_count = quarantine_df.count()
        except Exception:
            log_pipeline_error(self.logger, "QUARANTINE", "writing invalid records", start_time)
            raise

        if record_count:
            self.logger.warning("QUARANTINE | %d record(s) quarantined this run", record_count)
        log_pipeline_end(self.logger, "QUARANTINE", f"records={record_count}", start_time)
