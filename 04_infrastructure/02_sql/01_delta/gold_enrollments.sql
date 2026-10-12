CREATE TABLE IF NOT EXISTS catalog.schema.gold_enrollments (
    file_name STRING,
    member_id STRING,
    first_name STRING,
    last_name STRING,
    date_of_birth STRING,
    gender STRING,
    group_id STRING,
    group_name STRING,
    plan_id STRING,
    coverage_start_date STRING,
    coverage_end_date STRING,
    street_1 STRING,
    street_2 STRING,
    city STRING,
    state STRING,
    zip_code STRING,
    relationship STRING,
    is_subscriber BOOLEAN,
    full_name STRING,
    age_years INT,
    ingested_at TIMESTAMP
) USING DELTA
TBLPROPERTIES (delta.enableChangeDataFeed = true, delta.autoOptimize.optimizeWrite = true)
