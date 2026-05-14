-- ============================================================
-- CHICAGO CRIME DATA LAKE — DATABRICKS SQL
-- Script: 04_silver_transformations.sql
-- Zone:   SILVER
-- Purpose: Execute all Silver transformation steps in order.
--          Every decision is backed by Bronze profiling data.
--
-- EXECUTION ORDER:
--   STEP 1  — UNION all 4 Bronze crime tables
--   STEP 2  — Remove injected header rows
--   STEP 3  — Clean dimension tables
--   STEP 4  — Type casting, standardisation, computed columns
--   STEP 5  — Quality gates → meta.quality_failures
--   STEP 6  — Deduplication (within-file + cross-file)
--   STEP 7  — Dimension joins → crimes_conformed
--
-- Run: AFTER 03_silver_create_tables.sql
-- ============================================================

USE CATALOG chicago_crimes_workspace;

-- ── Open ETL run ─────────────────────────────────────────────
INSERT INTO meta.etl_run_log
    (pipeline_name, zone, script_name, start_ts, status)
VALUES
    ('silver_transform', 'SILVER',
     '04_silver_transformations.sql',
     current_timestamp(), 'RUNNING');

-- ════════════════════════════════════════════════════════════
-- STEP 1: UNION ALL 4 BRONZE CRIME TABLES → crimes_unioned
-- Justification: 4 separate CSVs split by year range must be
-- consolidated into one table for all downstream analysis.
-- Total expected: ~6,301,149 rows before any filtering.
-- ════════════════════════════════════════════════════════════

INSERT INTO silver.crimes_unioned
SELECT
    id, case_number, date, block, iucr, primary_type,
    description, location_description, arrest, domestic,
    beat, district, ward, community_area, fbi_code,
    x_coordinate, y_coordinate, year, updated_on,
    latitude, longitude, location,
    _source_file, _source_path, _ingest_ts,
    'STEP1_UNION' AS _silver_step
FROM bronze.crimes_2001_2004

UNION ALL

SELECT
    id, case_number, date, block, iucr, primary_type,
    description, location_description, arrest, domestic,
    beat, district, ward, community_area, fbi_code,
    x_coordinate, y_coordinate, year, updated_on,
    latitude, longitude, location,
    _source_file, _source_path, _ingest_ts,
    'STEP1_UNION'
FROM bronze.crimes_2005_2007

UNION ALL

SELECT
    id, case_number, date, block, iucr, primary_type,
    description, location_description, arrest, domestic,
    beat, district, ward, community_area, fbi_code,
    x_coordinate, y_coordinate, year, updated_on,
    latitude, longitude, location,
    _source_file, _source_path, _ingest_ts,
    'STEP1_UNION'
FROM bronze.crimes_2008_2011

UNION ALL

SELECT
    id, case_number, date, block, iucr, primary_type,
    description, location_description, arrest, domestic,
    beat, district, ward, community_area, fbi_code,
    x_coordinate, y_coordinate, year, updated_on,
    latitude, longitude, location,
    _source_file, _source_path, _ingest_ts,
    'STEP1_UNION'
FROM bronze.crimes_2012_2017;

-- Log Step 1
INSERT INTO meta.silver_transform_log
    (run_id, step_name, source_table, target_table,
     rows_in, rows_out, rows_dropped, notes)
SELECT
    (SELECT MAX(run_id) FROM meta.etl_run_log
     WHERE pipeline_name = 'silver_transform'),
    'STEP1_UNION_4_FILES',
    'bronze.crimes_2001_2004 + 2005_2007 + 2008_2011 + 2012_2017',
    'silver.crimes_unioned',
    (SELECT COUNT(*) FROM bronze.crimes_2001_2004) +
    (SELECT COUNT(*) FROM bronze.crimes_2005_2007) +
    (SELECT COUNT(*) FROM bronze.crimes_2008_2011) +
    (SELECT COUNT(*) FROM bronze.crimes_2012_2017),
    (SELECT COUNT(*) FROM silver.crimes_unioned),
    0,
    'UNION ALL 4 year-range files into one table. No filtering at this step.';

-- ════════════════════════════════════════════════════════════
-- STEP 2: REMOVE HEADER ROWS → crimes_header_removed
--
-- PROBLEM FOUND IN PROFILING:
--   null_id = 48,576 — CSV loader injected column header
--   strings as actual data rows. Evidence:
--     • district field contains "Beat" (a column name)
--     • domestic field contains "Arrest" (a column name)
--     • community_area field contains "Ward"
--     • year field contains "Y Coordinate" and "41.789..."
--     • fbi_code field contains "Community Area"
--     • primary_type contains "IUCR"
--   These ~48K rows must be removed BEFORE type casting,
--   otherwise TRY_CAST on id will produce NULLs for headers.
--
-- FILTER LOGIC:
--   Remove rows where id is NULL OR cannot be cast to BIGINT
--   (header rows have id = "ID" which is not numeric)
-- ════════════════════════════════════════════════════════════

INSERT INTO silver.crimes_header_removed
SELECT
    id, case_number, date, block, iucr, primary_type,
    description, location_description, arrest, domestic,
    beat, district, ward, community_area, fbi_code,
    x_coordinate, y_coordinate, year, updated_on,
    latitude, longitude, location,
    _source_file, _source_path, _ingest_ts,
    'STEP2_HEADER_REMOVED' AS _silver_step
FROM silver.crimes_unioned
WHERE
    -- Keep only rows where id is a real numeric value
    id IS NOT NULL
    AND TRIM(id) != ''
    AND TRY_CAST(TRIM(id) AS BIGINT) IS NOT NULL;

-- Log Step 2
INSERT INTO meta.silver_transform_log
    (run_id, step_name, source_table, target_table,
     rows_in, rows_out, rows_dropped, notes)
SELECT
    (SELECT MAX(run_id) FROM meta.etl_run_log
     WHERE pipeline_name = 'silver_transform'),
    'STEP2_REMOVE_HEADER_ROWS',
    'silver.crimes_unioned',
    'silver.crimes_header_removed',
    (SELECT COUNT(*) FROM silver.crimes_unioned),
    (SELECT COUNT(*) FROM silver.crimes_header_removed),
    (SELECT COUNT(*) FROM silver.crimes_unioned) -
    (SELECT COUNT(*) FROM silver.crimes_header_removed),
    'Removed injected CSV header rows: id=NULL or non-numeric (~48K rows per profiling finding null_id=48576)';

-- ════════════════════════════════════════════════════════════
-- STEP 3: CLEAN DIMENSION TABLES
-- Drop the_geom (geometry blob) and cast types
-- ════════════════════════════════════════════════════════════

-- 3A: dim_wards
-- Dropped: the_geom — raw MULTIPOLYGON string (no SQL value)
INSERT INTO silver.dim_wards
SELECT
    TRY_CAST(TRIM(ward) AS INT)           AS ward_num,
    CONCAT('Ward ', TRIM(ward))           AS ward_label,
    TRY_CAST(TRIM(shape_area) AS DOUBLE)  AS shape_area,
    TRY_CAST(TRIM(shape_leng) AS DOUBLE)  AS shape_leng,
    _source_file,
    current_timestamp()                   AS _transform_ts
    -- the_geom intentionally excluded (raw geometry blob)
FROM bronze.dim_wards_raw
WHERE TRIM(ward) IS NOT NULL
  AND TRIM(ward) != ''
  AND TRY_CAST(TRIM(ward) AS INT) IS NOT NULL;

-- 3B: dim_districts
-- Dropped: the_geom
-- Note: 13 and 21 absent from source — not a cleaning issue
INSERT INTO silver.dim_districts
SELECT
    TRY_CAST(TRIM(dist_num) AS INT)       AS district_num,
    UPPER(TRIM(dist_label))               AS district_label,
    _source_file,
    current_timestamp()                   AS _transform_ts
    -- the_geom intentionally excluded
FROM bronze.dim_districts_raw
WHERE TRIM(dist_num) IS NOT NULL
  AND TRIM(dist_num) != ''
  AND TRY_CAST(TRIM(dist_num) AS INT) IS NOT NULL;

-- 3C: dim_community_areas
-- Dropped: the_geom AND area_num_1 (duplicate column of area_numbe)
INSERT INTO silver.dim_community_areas
SELECT
    TRY_CAST(TRIM(area_numbe) AS INT)     AS area_num,
    UPPER(TRIM(community))                AS community_name,
    TRY_CAST(TRIM(shape_area) AS DOUBLE)  AS shape_area,
    TRY_CAST(TRIM(shape_len)  AS DOUBLE)  AS shape_len,
    _source_file,
    current_timestamp()                   AS _transform_ts
    -- the_geom intentionally excluded
    -- area_num_1 intentionally excluded (duplicate of area_numbe)
FROM bronze.dim_community_areas_raw
WHERE TRIM(area_numbe) IS NOT NULL
  AND TRIM(area_numbe) != ''
  AND TRY_CAST(TRIM(area_numbe) AS INT) IS NOT NULL;

-- Log Step 3
INSERT INTO meta.silver_transform_log
    (run_id, step_name, source_table, target_table,
     rows_in, rows_out, rows_dropped, notes)
SELECT
    (SELECT MAX(run_id) FROM meta.etl_run_log
     WHERE pipeline_name = 'silver_transform'),
    'STEP3_CLEAN_DIMS',
    'bronze.dim_wards_raw + dim_districts_raw + dim_community_areas_raw',
    'silver.dim_wards + dim_districts + dim_community_areas',
    (SELECT COUNT(*) FROM bronze.dim_wards_raw) +
    (SELECT COUNT(*) FROM bronze.dim_districts_raw) +
    (SELECT COUNT(*) FROM bronze.dim_community_areas_raw),
    (SELECT COUNT(*) FROM silver.dim_wards) +
    (SELECT COUNT(*) FROM silver.dim_districts) +
    (SELECT COUNT(*) FROM silver.dim_community_areas),
    ((SELECT COUNT(*) FROM bronze.dim_wards_raw) +
    (SELECT COUNT(*) FROM bronze.dim_districts_raw) +
    (SELECT COUNT(*) FROM bronze.dim_community_areas_raw))-
    ((SELECT COUNT(*) FROM silver.dim_wards) +
    (SELECT COUNT(*) FROM silver.dim_districts) +
    (SELECT COUNT(*) FROM silver.dim_community_areas)),
    'Dropped the_geom from all dims. Dropped area_num_1 (duplicate). Cast keys to INT. UPPER TRIM names.';

-- ════════════════════════════════════════════════════════════
-- STEP 4: TYPE CASTING, STANDARDISATION, COMPUTED COLUMNS
--         → crimes_typed
--
-- Rules applied (each backed by profiling finding):
--
-- 1. crime_id: TRY_CAST(id AS BIGINT)
--    Profiling: id_not_numeric = 0, so all remaining ids
--    after header removal are safely numeric
--
-- 2. incident_datetime: TWO FORMAT PATHS
--    Profiling finding: format1_success=5,252,574 (MM/dd/yyyy hh:mm:ss a)
--    format1_fail=1,000,000 (different format — TRY second pattern)
--    COALESCE both attempts
--
-- 3. ward, community_area: Cast via DOUBLE first
--    Profiling: ward_not_int=4,552,414 (stored as "1.0" not "1")
--    Fix: TRY_CAST(TRY_CAST(ward AS DOUBLE) AS INT)
--
-- 4. district: LPAD to 3 chars
--    Profiling: values like "4", "04", "004" — standardise to "004"
--
-- 5. primary_type, fbi_code, location_description: UPPER TRIM
--    Profiling: all values were already uppercase but TRIM is needed
--    for any hidden whitespace. No mixed casing found.
--
-- 6. is_arrest, is_domestic: CASE on LOWER(TRIM())
--    Profiling: arrest_invalid_value=1 (the remaining header row
--    that had "Location Description" — removed in step 2)
--    After step 2 removal, values will be TRUE/FALSE only
--
-- 7. Coordinate recovery from location string:
--    Profiling: 139,741 rows have NULL lat/lon
--    location string "(41.8, -87.6)" present for some of those
--    Use regexp_extract to recover coordinates
--
-- 8. Dropped columns:
--    x_coordinate, y_coordinate — State Plane, redundant
--    location — used for recovery, then excluded
--    updated_on — admin audit field
--    year — redundant (derivable from date); profiling showed
--            year column had "Y Coordinate" and "41.789..." as
--            values proving it's unreliable as a source
-- ════════════════════════════════════════════════════════════

INSERT INTO silver.crimes_typed
SELECT
    -- ── Primary key ──────────────────────────────────────────
    TRY_CAST(TRIM(id) AS BIGINT)                            AS crime_id,

    TRIM(case_number)                                       AS case_number,

    -- ── Date parsing — TWO FORMAT PATHS ──────────────────────
    -- Profiling found two distinct formats in the data.
    -- Format A: MM/dd/yyyy hh:mm:ss a  (5.25M rows)
    -- Format B: M/d/yyyy H:mm          (remaining ~1M rows)
    -- COALESCE tries A first, falls back to B
    COALESCE(
        try_to_timestamp(TRIM(date), 'MM/dd/yyyy hh:mm:ss a'),
        try_to_timestamp(TRIM(date), 'M/d/yyyy H:mm')
    )                                                       AS incident_datetime,

    -- Computed date parts — derived from incident_datetime
    YEAR(COALESCE(
        try_to_timestamp(TRIM(date), 'MM/dd/yyyy hh:mm:ss a'),
        try_to_timestamp(TRIM(date), 'M/d/yyyy H:mm')
    ))                                                      AS incident_year,

    MONTH(COALESCE(
        try_to_timestamp(TRIM(date), 'MM/dd/yyyy hh:mm:ss a'),
        try_to_timestamp(TRIM(date), 'M/d/yyyy H:mm')
    ))                                                      AS incident_month,

    DAY(COALESCE(
        try_to_timestamp(TRIM(date), 'MM/dd/yyyy hh:mm:ss a'),
        try_to_timestamp(TRIM(date), 'M/d/yyyy H:mm')
    ))                                                      AS incident_day,

    HOUR(COALESCE(
        try_to_timestamp(TRIM(date), 'MM/dd/yyyy hh:mm:ss a'),
        try_to_timestamp(TRIM(date), 'M/d/yyyy H:mm')
    ))                                                      AS incident_hour,

    DAYOFWEEK(COALESCE(
        try_to_timestamp(TRIM(date), 'MM/dd/yyyy hh:mm:ss a'),
        try_to_timestamp(TRIM(date), 'M/d/yyyy H:mm')
    ))                                                      AS incident_dayofweek,

    -- ── Crime classification ──────────────────────────────────
    UPPER(TRIM(block))                                      AS block,
    UPPER(TRIM(iucr))                                       AS iucr,
    UPPER(TRIM(primary_type))                               AS primary_type,
    UPPER(TRIM(description))                                AS crime_description,
    UPPER(TRIM(location_description))                       AS location_description,
    UPPER(TRIM(fbi_code))                                   AS fbi_code,

    -- ── Boolean flags ─────────────────────────────────────────
    -- Header rows removed in Step 2, so only TRUE/FALSE remain
    CASE WHEN LOWER(TRIM(arrest))   = 'true'  THEN TRUE
         WHEN LOWER(TRIM(arrest))   = 'false' THEN FALSE
         ELSE NULL
    END                                                     AS is_arrest,

    CASE WHEN LOWER(TRIM(domestic)) = 'true'  THEN TRUE
         WHEN LOWER(TRIM(domestic)) = 'false' THEN FALSE
         ELSE NULL
    END                                                     AS is_domestic,

    -- ── Geographic codes ──────────────────────────────────────
    UPPER(TRIM(beat))                                       AS beat,

    -- district: standardise casting
    TRY_CAST(
    TRY_CAST(TRIM(district) AS DOUBLE) AS INT
    )                                                       AS district,

    -- ward: stored as DOUBLE string ("1.0") — cast via DOUBLE
    -- Profiling: ward_not_int = 4,552,414 when cast directly
    TRY_CAST(
        TRY_CAST(TRIM(ward) AS DOUBLE) AS INT
    )                                                       AS ward,

    -- community_area: same DOUBLE string issue as ward
    -- Value 0 exists (~small count) — kept but flagged in quality gate
    TRY_CAST(
        TRY_CAST(TRIM(community_area) AS DOUBLE) AS INT
    )                                                       AS community_area,

    -- ── Coordinates ───────────────────────────────────────────
    -- Direct lat/lon (NULL for ~139K rows per profiling)
    TRY_CAST(TRIM(latitude)  AS DOUBLE)                     AS latitude,
    TRY_CAST(TRIM(longitude) AS DOUBLE)                     AS longitude,

    -- Semi-structured parsing: recover from location string
    -- location format: "(41.817229156, -87.637328162)"
    -- regexp_extract group 1 = content before the comma
    -- regexp_extract group 1 variant = content after comma before )
    TRY_CAST(
        regexp_extract(TRIM(location), '\\(([^,]+),', 1)
    AS DOUBLE)                                              AS lat_recovered,

    TRY_CAST(
        regexp_extract(TRIM(location), ',\\s*([^)]+)\\)', 1)
    AS DOUBLE)                                              AS lon_recovered,

    -- Best available coordinate (direct preferred, parsed as fallback)
    COALESCE(
        TRY_CAST(TRIM(latitude) AS DOUBLE),
        TRY_CAST(regexp_extract(TRIM(location), '\\(([^,]+),', 1) AS DOUBLE)
    )                                                       AS lat_final,

    COALESCE(
        TRY_CAST(TRIM(longitude) AS DOUBLE),
        TRY_CAST(regexp_extract(TRIM(location), ',\\s*([^)]+)\\)', 1) AS DOUBLE)
    )                                                       AS lon_final,

    -- Track the source of the final coordinate
    CASE
        WHEN TRY_CAST(TRIM(latitude) AS DOUBLE) IS NOT NULL
             THEN 'DIRECT'
        WHEN TRY_CAST(
                 regexp_extract(TRIM(location), '\\(([^,]+),', 1)
             AS DOUBLE) IS NOT NULL
             THEN 'RECOVERED_FROM_STR'
        ELSE 'NULL'
    END                                                     AS coord_source,

    -- ── File provenance ───────────────────────────────────────
    CASE
        WHEN _source_file LIKE '%2001%' THEN '2001_2004'
        WHEN _source_file LIKE '%2005%' THEN '2005_2007'
        WHEN _source_file LIKE '%2008%' THEN '2008_2011'
        WHEN _source_file LIKE '%2012%' THEN '2012_2017'
        ELSE 'UNKNOWN'
    END                                                     AS source_year_range,

    _source_file,
    'STEP4_TYPED'                                           AS _silver_step,
    current_timestamp()                                     AS _transform_ts

    -- DROPPED (not selected):
    -- x_coordinate  — redundant (State Plane NAD83 projection)
    -- y_coordinate  — redundant
    -- location      — compound string used for coord recovery above
    -- updated_on    — admin audit field, no analytical value
    -- year          — redundant AND unreliable (had "Y Coordinate" as value)

FROM silver.crimes_header_removed;

-- Log Step 4
INSERT INTO meta.silver_transform_log
    (run_id, step_name, source_table, target_table,
     rows_in, rows_out, rows_dropped, notes)
SELECT
    (SELECT MAX(run_id) FROM meta.etl_run_log
     WHERE pipeline_name = 'silver_transform'),
    'STEP4_TYPE_CAST_STANDARDISE',
    'silver.crimes_header_removed',
    'silver.crimes_typed',
    (SELECT COUNT(*) FROM silver.crimes_header_removed),
    (SELECT COUNT(*) FROM silver.crimes_typed),
    ((SELECT COUNT(*) FROM silver.crimes_header_removed)-
    (SELECT COUNT(*) FROM silver.crimes_typed)),
    'date: COALESCE 2 formats. ward/community_area: via DOUBLE. district: LPAD 3. UPPER TRIM all text. Coords recovered from location string. Dropped: x_coord, y_coord, location, updated_on, year.';

