"""
src/ingestion/pipeline.py
Databricks Professional Delta Lake medallion pipeline orchestrator.
Bronze → Silver → Gold with Unity Catalog support.
"""
from pathlib import Path
from typing import Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.medenroll834.ingestion.bronze import BronzeLayer
from src.medenroll834.ingestion.gold import GoldLayer
from src.medenroll834.ingestion.silver import SilverLayer
from src.medenroll834.quality.quarantine import DeltaQuarantineManager
from src.medenroll834.common.config import load_config
from src.medenroll834.common.logging import setup_logging


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

    def run(self, landing_path: str) -> Dict:
        self.logger.info("Pipeline started | landing_path=%s", landing_path)
        bronze_df = self.bronze.ingest(landing_path)
        silver_df = self.silver.transform(bronze_df)
        gold_df = self.gold.transform(silver_df)

        valid_df = silver_df.filter(F.col("is_valid") == True)
        invalid_df = silver_df.filter(F.col("is_valid") == False)
        self.quarantine.write_invalid(invalid_df)

        summary = {
            "bronze_segments": bronze_df.count(),
            "silver_total": silver_df.count(),
            "silver_valid": valid_df.count(),
            "silver_invalid": invalid_df.count(),
            "gold_records": gold_df.count(),
        }
        self.logger.info("Pipeline completed | %s", summary)
        return summary


def run_pipeline(
    spark: SparkSession,
    landing_path: str = "landing/",
    config_path: str = "03_config/pipeline_config.yaml",
) -> Dict:
    pipeline = DeltaMedallionPipeline(spark=spark, config_path=config_path)
    return pipeline.run(landing_path)
