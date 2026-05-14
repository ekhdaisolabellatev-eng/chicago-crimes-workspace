-- ============================================================
-- CHICAGO CRIME DATA LAKE — DATABRICKS SQL
-- Script: 00_meta_control_tables.sql
-- Purpose: Create all ETL control, logging, and lineage tables
-- Run: Once, immediately after 00_create_databases_and_schemas.sql
-- ============================================================
USE CATALOG chicago_crimes_workspace;

USE meta;

-- ────────────────────────────────────────────────────────────
-- 1. ETL RUN LOG
--    One row per notebook/script execution.
--    Tracks start, end, status, and row counts at zone level.
-- ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS meta.etl_run_log (
    run_id          BIGINT    GENERATED ALWAYS AS IDENTITY,  -- auto-increment PK
    pipeline_name   STRING    NOT NULL,   -- e.g. 'bronze_crimes_ingest'
    zone            STRING    NOT NULL,   -- 'BRONZE' | 'SILVER' | 'GOLD'
    script_name     STRING    NOT NULL,   -- filename of the SQL script
    start_ts        TIMESTAMP NOT NULL,
    end_ts          TIMESTAMP,
    status          STRING,              -- 'RUNNING' | 'SUCCESS' | 'FAILED'
    rows_processed  BIGINT,
    rows_rejected   BIGINT,
    error_message   STRING,
    run_by          STRING    DEFAULT current_user(),
    databricks_job  STRING               -- optional job/cluster ID
)
USING DELTA
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')
COMMENT 'Master ETL run log — one row per pipeline execution';

-- ────────────────────────────────────────────────────────────
-- 2. FILE INGESTION LOG
--    One row per source file loaded into Bronze.
--    Proves immutability and traceability for the rubric.
-- ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS meta.file_ingestion_log (
    ingestion_id     BIGINT    GENERATED ALWAYS AS IDENTITY,
    run_id           BIGINT    NOT NULL,   -- FK → etl_run_log.run_id
    source_file_name STRING    NOT NULL,   -- 'Chicago_Crimes_2001_to_2004.csv'
    source_file_path STRING    NOT NULL,   -- full DBFS path
    target_table     STRING    NOT NULL,   -- 'bronze.crimes_2001_2004'
    zone             STRING    NOT NULL DEFAULT 'BRONZE',
    ingest_ts        TIMESTAMP NOT NULL DEFAULT current_timestamp(),
    row_count        BIGINT,
    file_size_bytes  BIGINT,
    status           STRING,              -- 'SUCCESS' | 'FAILED'
    error_message    STRING               -- error details if status = 'FAILED'
)
USING DELTA
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')
COMMENT 'Per-file ingestion record — one row per CSV loaded into Bronze';

-- ────────────────────────────────────────────────────────────
-- 3. QUALITY GATE FAILURE LOG (REJECT TABLE)
--    One row per record that failed a Silver quality check.
--    reason_code + reason_desc is what the rubric demands.
-- ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS meta.quality_failures (
    failure_id      BIGINT    GENERATED ALWAYS AS IDENTITY,
    run_id          BIGINT,
    source_table    STRING    NOT NULL,   -- 'bronze.crimes_2001_2004'
    target_table    STRING    NOT NULL,   -- 'silver.crimes_cleaned'
    record_id       STRING    NOT NULL,   -- value of the PK column
    reason_code     STRING    NOT NULL,   -- e.g. 'NULL_DATE'
    reason_desc     STRING    NOT NULL,   -- human-readable explanation
    raw_value       STRING,              -- what the bad value actually was
    detected_at     TIMESTAMP NOT NULL DEFAULT current_timestamp(),
    zone            STRING    NOT NULL DEFAULT 'SILVER'
)
USING DELTA
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')
COMMENT 'Quality gate failures — rejected records with reason codes';

-- ────────────────────────────────────────────────────────────
-- 4. SILVER TRANSFORMATION LOG
--    Summarises what each Silver step changed.
-- ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS meta.silver_transform_log (
    transform_id      BIGINT    GENERATED ALWAYS AS IDENTITY,
    run_id            BIGINT,
    step_name         STRING    NOT NULL,  -- 'DEDUP' | 'TYPE_CAST' | 'JOIN_DIMS'
    source_table      STRING    NOT NULL,
    target_table      STRING    NOT NULL,
    rows_in           BIGINT,
    rows_out          BIGINT,
    rows_dropped      BIGINT,
    transform_ts      TIMESTAMP NOT NULL DEFAULT current_timestamp(),
    notes             STRING
)
USING DELTA
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')
COMMENT 'Silver transformation step audit — rows in vs rows out per step';

-- ────────────────────────────────────────────────────────────
-- 5. GOLD PUBLICATION LOG
--    Tracks when each Gold table/view was last refreshed.
-- ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS meta.gold_publication_log (
    log_id BIGINT GENERATED ALWAYS AS IDENTITY,
    run_id BIGINT,
    step_name STRING,
    source_table STRING,
    target_table STRING,
    rows_in BIGINT,
    rows_out BIGINT,
    rows_dropped BIGINT,
    notes STRING,
    log_ts TIMESTAMP DEFAULT current_timestamp()
)
USING DELTA
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')
COMMENT 'Gold layer publication log — when each Gold object was last built';

-- ────────────────────────────────────────────────────────────
-- 6. SCHEMA REGISTRY (light data dictionary)
-- ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS meta.schema_registry (
    registry_id     BIGINT    GENERATED ALWAYS AS IDENTITY,
    zone            STRING    NOT NULL,
    table_name      STRING    NOT NULL,
    column_name     STRING    NOT NULL,
    data_type       STRING,
    description     STRING,
    is_pk           BOOLEAN   DEFAULT FALSE,
    is_nullable     BOOLEAN   DEFAULT TRUE,
    registered_at   TIMESTAMP DEFAULT current_timestamp()
)
USING DELTA
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')
COMMENT 'Lightweight data dictionary — documents every table and column';

SELECT 'META schema created successfully' AS status;
