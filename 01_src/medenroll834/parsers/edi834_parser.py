"""
src/parser/edi834_parser.py
PySpark-native X12 834 EDI parser using DataFrame operations.
Databricks Professional Standard: Delta-ready, Unity Catalog compatible.
"""
from pathlib import Path
from typing import Dict, List

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from src.medenroll834.ingestion.bronze import split_segments
BRONZE_SCHEMA = StructType([
    StructField("file_name", StringType(), True),
    StructField("segment_id", StringType(), True),
    StructField("elements", ArrayType(StringType()), True),
    StructField("raw_segment", StringType(), True),
    StructField("seg_order", LongType(), True),
])


MEMBER_SCHEMA = StructType([
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
    StructField("is_subscriber", StringType(), True),
    StructField("validation_errors", ArrayType(StringType()), True),
    StructField("is_valid", StringType(), True),
])


class EDI834Parser:
    """Parses X12 834 EDI files into Bronze → Silver DataFrames."""

    RELATIONSHIP_MAP = {
        "18": "self",
        "01": "spouse",
        "03": "child",
        "19": "domestic_partner",
    }

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.rel_map = F.create_map(
            *[F.lit(x) for pair in self.RELATIONSHIP_MAP.items() for x in pair]
        )

    def parse_files(self, landing_path: str) -> DataFrame:
        path = Path(landing_path)
        files = sorted(path.glob("*.834"))
        rows = []
        for file in files:
            content = file.read_text(encoding="utf-8")
            file_name = file.name
            for seg_order, line in enumerate(split_segments(content)):
                elements = line.split("*")
                rows.append({
                    "file_name": file_name,
                    "segment_id": elements[0],
                    "elements": elements,
                    "raw_segment": line,
                    "seg_order": seg_order,
                })
        return self.spark.createDataFrame(rows, schema=BRONZE_SCHEMA)

    def parse_dataframe(self, df: DataFrame, file_name_col: str = "raw") -> DataFrame:
        rows = []
        for row in df.collect():
            content = row[file_name_col]
            file_name = row.get("file_name", "unknown")
            for seg_order, line in enumerate(split_segments(content)):
                elements = line.split("*")
                rows.append({
                    "file_name": file_name,
                    "segment_id": elements[0],
                    "elements": elements,
                    "raw_segment": line,
                    "seg_order": seg_order,
                })
        return self.spark.createDataFrame(rows, schema=BRONZE_SCHEMA)
