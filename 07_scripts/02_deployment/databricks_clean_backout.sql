-- =============================================================================
-- MedEnroll834 - Databricks EDI pipeline BACKOUT
-- =============================================================================
-- Reverses 07_scripts/02_deployment/databricks_clean_deploy.sql
--
-- Drops the 4 medallion Delta tables, the landing volume, and the edi schema.
-- Does NOT drop the med834enrl catalog itself, so OLAP objects in
-- med834enrl.gold / silver / bronze are unaffected. Run
-- databricks_olap_backout.sql separately to remove those too.
--
-- Idempotent (IF EXISTS). Re-run databricks_clean_deploy.sql to recreate.
-- =============================================================================

DROP TABLE IF EXISTS med834enrl.edi.bronze_edi_segments;
DROP TABLE IF EXISTS med834enrl.edi.silver_members;
DROP TABLE IF EXISTS med834enrl.edi.gold_enrollments;
DROP TABLE IF EXISTS med834enrl.edi.quarantine_invalid_members;

DROP VOLUME IF EXISTS med834enrl.edi.landing;

DROP SCHEMA IF EXISTS med834enrl.edi CASCADE;
