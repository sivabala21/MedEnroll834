"""
src/ingestion/pipeline.py
Databricks Professional Delta Lake medallion pipeline orchestrator.
Bronze → Silver → Gold with Unity Catalog support.
"""
import uuid
from pathlib import Path
from typing import Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.medenroll834.ingestion.bronze import BronzeLayer
from src.medenroll834.ingestion.gold import GoldLayer
from src.medenroll834.ingestion.silver import SilverLayer
from src.medenroll834.quality.quarantine import DeltaQuarantineManager
from src.medenroll834.common.config import load_config
from src.medenroll834.common.logging import (
    log_pipeline_end,
    log_pipeline_error,
    log_pipeline_start,
    run_context,
    setup_logging,
)


class DeltaMedallionPipeline:
    """End-to-end medallion pipeline for 834 EDI ingestion."""

    def __init__(self, spark: SparkSession, config_path: str = "03_config/pipeline_config.yaml"):
        self.spark = spark
        self.config = load_config(config_path)
        self.logger = setup_logging("pipeline")
        self.bronze = BronzeLayer(spark, self.config)
        self.silver = SilverLayer(spark, self.config)
        self.gold = GoldLayer(spark, self.config)
        self.quarantine = DeltaQuarantineManager(spark, self.config)

    def run(self, landing_path: str, run_id: Optional[str] = None) -> Dict:
        # run_id correlates every log line emitted by every layer for this
        # pipeline invocation (Bronze/Silver/Gold/Quarantine all call
        # setup_logging() independently, so a contextvar is how they share
        # it without the caller threading run_id through each constructor).
        run_id = run_id or uuid.uuid4().hex[:12]
        with run_context(run_id=run_id):
            start_time = log_pipeline_start(self.logger, "PIPELINE", f"landing_path={landing_path}")
            try:
                bronze_df = self.bronze.ingest(landing_path)
                silver_df = self.silver.transform(bronze_df)
                gold_df = self.gold.transform(silver_df)

                valid_df = silver_df.filter(F.col("is_valid") == True)
                invalid_df = silver_df.filter(F.col("is_valid") == False)
                self.quarantine.write_invalid(invalid_df)

                summary = {
                    "run_id": run_id,
                    "bronze_segments": bronze_df.count(),
                    "silver_total": silver_df.count(),
                    "silver_valid": valid_df.count(),
                    "silver_invalid": invalid_df.count(),
                    "gold_records": gold_df.count(),
                }
            except Exception:
                log_pipeline_error(self.logger, "PIPELINE", f"landing_path={landing_path}", start_time)
                raise

            log_pipeline_end(self.logger, "PIPELINE", str(summary), start_time)
            return summary


def run_pipeline(
    spark: SparkSession,
    landing_path: str = "landing/",
    config_path: str = "03_config/pipeline_config.yaml",
    run_id: Optional[str] = None,
) -> Dict:
    pipeline = DeltaMedallionPipeline(spark=spark, config_path=config_path)
    return pipeline.run(landing_path, run_id=run_id)
