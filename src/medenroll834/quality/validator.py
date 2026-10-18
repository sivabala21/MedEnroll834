"""
src/quality/validator.py
DataFrame-native validation rules for 834 members.
"""
from typing import List

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.medenroll834.common.logging import setup_logging


class DataFrameValidator:
    """Validates member DataFrames using Spark SQL expressions."""

    VALID_PLANS = {"100", "101", "102", "103", "104", "105", "106", "107"}

    def __init__(self):
        self.logger = setup_logging("validator")

    def validate(self, df: DataFrame) -> DataFrame:
        self.logger.debug("VALIDATOR | applying business rules to member DataFrame")
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
                    ~F.col("plan_id").isin(list(self.VALID_PLANS)),
                    F.concat(F.lit("Unknown plan: "), F.col("plan_id")),
                ),
            ),
            lambda x: x.isNotNull(),
        )
        return df.withColumn("validation_errors", errors).withColumn("is_valid", F.size(F.col("validation_errors")) == 0)
