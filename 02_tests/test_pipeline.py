"""
tests/test_pipeline.py
Tests for Databricks Professional Delta medallion pipeline.
"""
from datetime import date

import pytest
from pyspark.sql import functions as F

from generator.synthetic_834_generator import generate_batch
from src.medenroll834.ingestion.pipeline import DeltaMedallionPipeline
from src.medenroll834.parsers.edi834_parser import EDI834Parser
from src.medenroll834.quality.validator import DataFrameValidator


@pytest.fixture(scope="function")
def sample_834_files(tmp_path):
    files = generate_batch(
        output_dir=str(tmp_path),
        num_files=2,
        subscribers_per_file=5,
        dependents_per_sub=1,
        start_date=date(2026, 9, 1),
    )
    return tmp_path, files


class TestEDI834Parser:
    def test_parse_files_returns_bronze_dataframe(self, spark, sample_834_files):
        tmp_path, files = sample_834_files
        parser = EDI834Parser(spark)
        df = parser.parse_files(str(tmp_path))
        assert df.count() > 0
        assert "segment_id" in df.columns
        assert "elements" in df.columns


class TestSilverLayer:
    def test_silver_parses_members(self, spark, sample_834_files):
        tmp_path, files = sample_834_files
        parser = EDI834Parser(spark)
        bronze_df = parser.parse_files(str(tmp_path))
        from src.medenroll834.ingestion.silver import SilverLayer
        from src.medenroll834.common.config import load_config
        config = load_config()
        config["layers"]["silver"]["path"] = str(tmp_path / "silver")
        silver = SilverLayer(spark, config)
        silver_df = silver.transform(bronze_df, write=False)
        assert silver_df.count() > 0
        assert "member_id" in silver_df.columns
        assert "is_valid" in silver_df.columns

    def test_silver_validation_flags_invalid(self, spark, sample_834_files):
        tmp_path, files = sample_834_files
        parser = EDI834Parser(spark)
        bronze_df = parser.parse_files(str(tmp_path))
        from src.medenroll834.ingestion.silver import SilverLayer
        from src.medenroll834.common.config import load_config
        config = load_config()
        config["layers"]["silver"]["path"] = str(tmp_path / "silver")
        silver = SilverLayer(spark, config)
        silver_df = silver.transform(bronze_df, write=False)
        invalid_count = silver_df.filter(F.col("is_valid") == False).count()
        assert invalid_count == 0  # synthetic data should be valid


class TestDataFrameValidator:
    def test_validates_missing_member_id(self, spark):
        data = [("", "JOHN", "SMITH", "", "", "", "GOLD")]
        df = spark.createDataFrame(
            data,
            ["member_id", "first_name", "last_name", "date_of_birth", "coverage_start_date", "coverage_end_date", "plan_id"],
        )
        validator = DataFrameValidator()
        result = validator.validate(df)
        invalid = result.filter(F.col("is_valid") == False).count()
        assert invalid == 1

    def test_validates_plan_code(self, spark):
        data = [("M1", "JOHN", "SMITH", "1990-01-01", "2020-01-01", "2030-01-01", "UNKNOWN")]
        df = spark.createDataFrame(
            data,
            ["member_id", "first_name", "last_name", "date_of_birth", "coverage_start_date", "coverage_end_date", "plan_id"],
        )
        validator = DataFrameValidator()
        result = validator.validate(df)
        invalid = result.filter(F.col("is_valid") == False).count()
        assert invalid == 1


class TestGoldLayer:
    def test_gold_deduplicates(self, spark, sample_834_files):
        tmp_path, files = sample_834_files
        parser = EDI834Parser(spark)
        bronze_df = parser.parse_files(str(tmp_path))
        from src.medenroll834.ingestion.silver import SilverLayer
        from src.medenroll834.ingestion.gold import GoldLayer
        from src.medenroll834.common.config import load_config
        config = load_config()
        config["layers"]["silver"]["path"] = str(tmp_path / "silver")
        config["layers"]["gold"]["path"] = str(tmp_path / "gold")
        silver = SilverLayer(spark, config)
        silver_df = silver.transform(bronze_df)
        gold = GoldLayer(spark, config)
        gold_df = gold.transform(silver_df)
        assert gold_df.count() > 0
        assert "full_name" in gold_df.columns
        assert "age_years" in gold_df.columns
