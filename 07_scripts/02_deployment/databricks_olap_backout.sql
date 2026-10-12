-- =============================================================================
-- MedEnroll834 - Databricks OLAP (Medallion + Star Schema) BACKOUT
-- =============================================================================
-- Reverses 07_scripts/02_deployment/databricks_olap_deploy.sql and
-- databricks_olap_constraints.sql.
--
-- Drops the GOLD star-schema fact + dimension tables (PK/FK constraints drop
-- automatically with their tables) and the bronze / silver / gold OLAP
-- schemas.
--
-- Idempotent (IF EXISTS). Re-run databricks_olap_deploy.py to recreate.
-- =============================================================================

DROP TABLE IF EXISTS med834enrl.gold.FACT_ENROLLMENT;
DROP TABLE IF EXISTS med834enrl.gold.DIM_TRANSACTION_SOURCE;
DROP TABLE IF EXISTS med834enrl.gold.DIM_GROUP;
DROP TABLE IF EXISTS med834enrl.gold.DIM_PLAN;
DROP TABLE IF EXISTS med834enrl.gold.DIM_MEMBER;
DROP TABLE IF EXISTS med834enrl.gold.DIM_DATE;

DROP SCHEMA IF EXISTS med834enrl.gold CASCADE;
DROP SCHEMA IF EXISTS med834enrl.silver CASCADE;
DROP SCHEMA IF EXISTS med834enrl.bronze CASCADE;
