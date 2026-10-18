"""
src/ingestion/bronze.py
Bronze layer: Raw 834 segments into Delta table.
"""
from pathlib import Path
from typing import Dict, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from src.medenroll834.common.logging import log_pipeline_end, log_pipeline_error, log_pipeline_start

def split_segments(content: str) -> list:
    """Split an X12 834 payload into individual segments.

    Handles both segment conventions found in real 834 files:

    * terminator-delimited ("ISA*...~GS*...~") where "~" is the only record
      delimiter and physical newlines are cosmetic;
    * line-delimited ("ISA*...\nGS*...\n") which some upstream systems emit,
      where the newline is the de-facto terminator and "~" may be absent.

    The two are frequently mixed within a single file, so neither delimiter can
    be assumed on its own. Normalise every newline to "~" then split on "~" to
    get exactly one record per segment whatever the producer used.
    """
    normalised = content.replace("\r\n", "~").replace("\r", "~").replace("\n", "~")
    return [seg.strip() for seg in normalised.split("~") if seg.strip()]


BRONZE_SCHEMA = StructType([
    StructField("file_name", StringType(), True),
    StructField("segment_id", StringType(), True),
    StructField("elements", ArrayType(StringType()), True),
    StructField("raw_segment", StringType(), True),
    StructField("seg_order", LongType(), True),
    StructField("ingested_at", StringType(), True),
])


class BronzeLayer:
    """Bronze layer ingestion for raw 834 segments."""

    def __init__(self, spark: SparkSession, config: Dict):
        self.spark = spark
        self.config = config
        self.logger = None

    def ingest(self, landing_path: str) -> DataFrame:
        from src.medenroll834.common.logging import setup_logging
        self.logger = setup_logging("bronze")
        start_time = log_pipeline_start(self.logger, "BRONZE", f"landing_path={landing_path}")

        path = Path(landing_path)
        files = sorted(path.glob("*.834"))
        self.logger.info("BRONZE | discovered %d file(s) under %s", len(files), landing_path)
        rows = []
        try:
            for file in files:
                content = file.read_text(encoding="utf-8")
                file_name = file.name
                segments = split_segments(content)
                for seg_order, line in enumerate(segments):
                    elements = line.split("*")
                    rows.append({
                        "file_name": file_name,
                        "segment_id": elements[0],
                        "elements": elements,
                        "raw_segment": line,
                        "seg_order": seg_order,
                    })
                self.logger.debug("BRONZE | file=%s segments=%d", file_name, len(segments))

            parsed = self.spark.createDataFrame(rows, schema=BRONZE_SCHEMA)
            parsed = parsed.withColumn("ingested_at", F.current_timestamp().cast("string"))

            table_path = self.config["layers"]["bronze"]["path"]
            (
                parsed.write.format("delta")
                .mode("overwrite")
                .option("mergeSchema", "true")
                .option("delta.autoOptimize.optimizeWrite", "true")
                .save(table_path)
            )
        except Exception:
            log_pipeline_error(
                self.logger, "BRONZE", f"landing_path={landing_path} files={len(files)}", start_time
            )
            raise

        result_count = parsed.count()
        log_pipeline_end(
            self.logger, "BRONZE", f"files={len(files)} segments_loaded={result_count}", start_time
        )
        return parsed
