-- =============================================================================
-- MedEnroll834 - Databricks Unity Catalog CLEAN DEPLOY
-- =============================================================================
-- WARNING: This DROPS the entire 'med834enrl' catalog (all schemas + tables)
-- and recreates it from scratch. Destructive and irreversible.
--
-- Target: catalog = med834enrl, schema = edi
-- Run on a SQL Warehouse (serverless or pro) via the Databricks CLI.
-- =============================================================================

-- 1. Drop the whole catalog (removes every schema, table, and volume in it)
DROP CATALOG IF EXISTS med834enrl CASCADE;

-- 2. Recreate the catalog
CREATE CATALOG IF NOT EXISTS med834enrl;

-- 3. Create the working schema
CREATE SCHEMA IF NOT EXISTS med834enrl.edi;

-- 4. Managed volume for raw 834 landing files
CREATE VOLUME IF NOT EXISTS med834enrl.edi.landing;

-- 5. Bronze - raw 834 segments
CREATE TABLE IF NOT EXISTS med834enrl.edi.bronze_edi_segments (
    file_name STRING,
    segment_id STRING,
    elements ARRAY<STRING>,
    raw_segment STRING,
    seg_order BIGINT,
    ingested_at TIMESTAMP
) USING DELTA
TBLPROPERTIES (delta.enableChangeDataFeed = true, delta.autoOptimize.optimizeWrite = true);

-- 6. Silver - validated member records
CREATE TABLE IF NOT EXISTS med834enrl.edi.silver_members (
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
    validation_errors ARRAY<STRING>,
    is_valid BOOLEAN,
    ingested_at TIMESTAMP
) USING DELTA
TBLPROPERTIES (delta.deletedFileRetentionDuration = 'interval 7 days');

-- 7. Gold - analytics-ready enrollments
CREATE TABLE IF NOT EXISTS med834enrl.edi.gold_enrollments (
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
TBLPROPERTIES (delta.enableChangeDataFeed = true, delta.autoOptimize.optimizeWrite = true);

-- 8. Quarantine - invalid records
CREATE TABLE IF NOT EXISTS med834enrl.edi.quarantine_invalid_members (
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
    validation_errors ARRAY<STRING>,
    is_valid BOOLEAN,
    quarantine_id STRING,
    quarantined_at TIMESTAMP
) USING DELTA;
