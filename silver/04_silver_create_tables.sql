-- ============================================================
-- CHICAGO CRIME DATA LAKE — DATABRICKS SQL
-- Script: 03_silver_create_tables.sql
-- Zone:   SILVER
-- Purpose: Create all Silver tables based on ACTUAL Bronze
--          profiling findings. Every schema decision is
--          justified by a specific profiling observation.
--
-- KEY DECISIONS FROM BRONZE PROFILING:
-- ┌─────────────────────────────────────────────────────────┐
-- │ DROPPED COLUMNS (all_crimes):                           │
-- │  • x_coordinate, y_coordinate — redundant (lat/lon     │
-- │    exists); State Plane projection not needed           │
-- │  • location (string) — redundant after parsing lat/lon  │
-- │  • updated_on — admin audit field, no analytical value  │
-- │  • year — derivable from date (redundant column)        │
-- │                                                         │
-- │ DROPPED COLUMNS (boundary dims):                        │
-- │  • the_geom — raw MULTIPOLYGON blob, no SQL analytical  │
-- │    use; makes tables unreadable                         │
-- │  • area_num_1 — duplicate of area_numbe in comm areas  │
-- │                                                         │
-- │ SPECIAL HANDLING REQUIRED:                              │
-- │  • ~48,575 header rows injected per file (CSV loader    │
-- │    bug — headers appeared as data rows)                 │
-- │  • 1M rows with different date format than majority     │
-- │  • ward/community_area stored as DOUBLE strings         │
-- │    (e.g. "1.0" not "1") — cast via DOUBLE→INT          │
-- │  • community_area = 0 → no match in dim (flag only)    │
-- │  • district 13 & 21 absent from crimes AND dim          │
-- │  • Cross-file duplicates: 1 crime_id in 2 files        │
-- │  • Within-file dups: 2001_2004=89K, 2005_2007=533K     │
-- │  • No lat/lon bbox rejection (project decision)         │
-- └─────────────────────────────────────────────────────────┘
-- ============================================================

USE CATALOG chicago_crimes_workspace;
USE silver;

-- ────────────────────────────────────────────────────────────
-- STEP 1: crimes_unioned
-- Raw UNION ALL of 4 Bronze crime tables
-- Still all STRING — proves the union step for the rubric
-- ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS silver.crimes_unioned (
    id                    STRING,
    case_number           STRING,
    date                  STRING,
    block                 STRING,
    iucr                  STRING,
    primary_type          STRING,
    description           STRING,
    location_description  STRING,
    arrest                STRING,
    domestic              STRING,
    beat                  STRING,
    district              STRING,
    ward                  STRING,
    community_area        STRING,
    fbi_code              STRING,
    x_coordinate          STRING,   -- carried through to step 2 then dropped
    y_coordinate          STRING,   -- carried through to step 2 then dropped
    year                  STRING,   -- carried through to step 2 then dropped
    updated_on            STRING,   -- carried through to step 2 then dropped
    latitude              STRING,
    longitude             STRING,
    location              STRING,   -- carried through for parsing then dropped
    _source_file          STRING,
    _source_path          STRING,
    _ingest_ts            TIMESTAMP,
    _silver_step          STRING
)
USING DELTA
COMMENT 'Silver Step 1 — UNION ALL of 4 Bronze crime files. All STRING. No transforms yet.';

-- ────────────────────────────────────────────────────────────
-- STEP 2: crimes_header_removed
-- Removes ~48,575 rows per the null profiling result:
-- null_id=48,576 means CSV loader injected column name
-- strings as data rows (e.g. id="ID", arrest="Arrest")
-- These are NOT real crimes — must be removed before typing
-- ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS silver.crimes_header_removed (
    id                    STRING,
    case_number           STRING,
    date                  STRING,
    block                 STRING,
    iucr                  STRING,
    primary_type          STRING,
    description           STRING,
    location_description  STRING,
    arrest                STRING,
    domestic              STRING,
    beat                  STRING,
    district              STRING,
    ward                  STRING,
    community_area        STRING,
    fbi_code              STRING,
    x_coordinate          STRING,
    y_coordinate          STRING,
    year                  STRING,
    updated_on            STRING,
    latitude              STRING,
    longitude             STRING,
    location              STRING,
    _source_file          STRING,
    _source_path          STRING,
    _ingest_ts            TIMESTAMP,
    _silver_step          STRING
)
USING DELTA
COMMENT 'Silver Step 2 — ~48K injected header rows removed. Filter: id IS NULL OR TRY_CAST(id AS BIGINT) IS NULL';

-- ────────────────────────────────────────────────────────────
-- STEP 3: crimes_typed
-- Type casting + standardisation + computed columns
-- Redundant columns DROPPED at this step
-- ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS silver.crimes_typed (

    -- Primary key
    crime_id              BIGINT
                          COMMENT 'PK — TRY_CAST(id AS BIGINT)',

    case_number           STRING,

    -- ── Date/time ────────────────────────────────────────────
    -- Two formats found in profiling:
    --   Format A (5.25M rows): MM/dd/yyyy hh:mm:ss a → e.g. 01/15/2003 08:30:00 AM
    --   Format B (~1M rows):   unknown — handled with TRY multiple patterns
    incident_datetime     TIMESTAMP
                          COMMENT 'Parsed from date — COALESCE of 2 format attempts',
    incident_year         INT,
    incident_month        INT,
    incident_day          INT,
    incident_hour         INT
                          COMMENT 'Used for police shift analysis in Gold',
    incident_dayofweek    INT
                          COMMENT '1=Sunday, 7=Saturday (Spark convention)',

    -- ── Crime classification ──────────────────────────────────
    block                 STRING,
    iucr                  STRING
                          COMMENT 'UPPER TRIM applied',
    primary_type          STRING
                          COMMENT 'UPPER TRIM — all 36 distinct values confirmed OK in profiling',
    crime_description     STRING
                          COMMENT 'Renamed from description — UPPER TRIM',
    location_description  STRING
                          COMMENT 'UPPER TRIM',
    fbi_code              STRING
                          COMMENT 'UPPER TRIM',

    -- ── Boolean flags ─────────────────────────────────────────
    -- Profiling found: arrest field had "Location Description" as value
    -- (header injection) — these are caught in crimes_header_removed step
    is_arrest             BOOLEAN
                          COMMENT 'CASE LOWER(TRIM(arrest)) = true THEN TRUE ELSE FALSE',
    is_domestic           BOOLEAN,

    -- ── Geographic codes ──────────────────────────────────────
    beat                  STRING,

    -- district: profiling found missing 13, 21 — LEFT JOIN will produce NULL
    -- crimes also lack 23,26,27,28,29,30 but those don't exist in dim either
    district              STRING
                          COMMENT 'LPAD to 3 chars: 4→004. Missing 13,21 noted.',

    -- ward: profiling found 4,552,414 values not castable as INT directly
    -- because stored as DOUBLE string e.g. "1.0" — cast via DOUBLE first
    -- also found 50 distinct wards (valid range 1–50)
    ward                  INT
                          COMMENT 'TRY_CAST(TRY_CAST(ward AS DOUBLE) AS INT) — handles "1.0" format',

    -- community_area: same DOUBLE string issue + value 0 exists but not in dim
    community_area        INT
                          COMMENT 'TRY_CAST via DOUBLE. Value 0 kept but flagged in quality gate.',

    -- ── Coordinates ───────────────────────────────────────────
    -- No bounding box rejection per project decision
    -- lat/lon null for ~139K rows — recovered via location string for some
    latitude              DOUBLE
                          COMMENT 'Direct from source — NULL for ~139,741 rows',
    longitude             DOUBLE
                          COMMENT 'Direct from source — NULL for ~139,742 rows',

    -- Semi-structured parsing: the location column "(41.8, -87.6)"
    -- Justification: 139K rows have NULL lat/lon but some have location string
    lat_recovered         DOUBLE
                          COMMENT 'Parsed from location string when latitude was NULL',
    lon_recovered         DOUBLE
                          COMMENT 'Parsed from location string when longitude was NULL',

    -- Best available coordinate
    lat_final             DOUBLE
                          COMMENT 'COALESCE(latitude, lat_recovered)',
    lon_final             DOUBLE
                          COMMENT 'COALESCE(longitude, lon_recovered)',
    coord_source          STRING
                          COMMENT 'DIRECT | RECOVERED_FROM_STR | NULL',

    -- ── Lineage ───────────────────────────────────────────────
    source_year_range     STRING
                          COMMENT '2001_2004 | 2005_2007 | 2008_2011 | 2012_2017',
    _source_file          STRING,
    _silver_step          STRING,
    _transform_ts         TIMESTAMP

    -- DROPPED from Bronze:
    -- x_coordinate  — State Plane projection, redundant given lat/lon
    -- y_coordinate  — same
    -- location      — compound string, used for recovery then dropped
    -- updated_on    — admin audit field, no analytical value
    -- year          — derivable from incident_datetime (was also inconsistent:
    --                 profiling found "Y Coordinate" and "41.789..." as year values)
)
USING DELTA
COMMENT 'Silver Step 3 — typed, standardised, computed, columns dropped. Pre-dedup.';

-- ────────────────────────────────────────────────────────────
-- STEP 4: crimes_deduped
-- Removes within-file and cross-file duplicates
-- Strategy: ROW_NUMBER() OVER (PARTITION BY crime_id
--           ORDER BY _ingest_ts DESC) — keep latest
-- ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS silver.crimes_deduped (
    crime_id              BIGINT,
    case_number           STRING,
    incident_datetime     TIMESTAMP,
    incident_year         INT,
    incident_month        INT,
    incident_day          INT,
    incident_hour         INT,
    incident_dayofweek    INT,
    block                 STRING,
    iucr                  STRING,
    primary_type          STRING,
    crime_description     STRING,
    location_description  STRING,
    fbi_code              STRING,
    is_arrest             BOOLEAN,
    is_domestic           BOOLEAN,
    beat                  STRING,
    district              STRING,
    ward                  INT,
    community_area        INT,
    latitude              DOUBLE,
    longitude             DOUBLE,
    lat_final             DOUBLE,
    lon_final             DOUBLE,
    coord_source          STRING,
    source_year_range     STRING,
    _source_file          STRING,
    _silver_step          STRING,
    _transform_ts         TIMESTAMP
)
USING DELTA
COMMENT 'Silver Step 4 — deduplicated. Within-file: 2001_2004=89K, 2005_2007=533K, 2008_2011=48K. Cross-file: 1.';

-- ────────────────────────────────────────────────────────────
-- STEP 5: crimes_conformed  ← MAIN SILVER FACT TABLE
-- After LEFT JOIN to 3 boundary dimensions
-- LEFT JOIN preserves all crime records even where dim has
-- no match (district 13/21, community_area=0)
-- ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS silver.crimes_conformed (
    crime_id              BIGINT,
    case_number           STRING,
    incident_datetime     TIMESTAMP,
    incident_year         INT,
    incident_month        INT,
    incident_day          INT,
    incident_hour         INT,
    incident_dayofweek    INT,
    block                 STRING,
    iucr                  STRING,
    primary_type          STRING,
    crime_description     STRING,
    location_description  STRING,
    fbi_code              STRING,
    is_arrest             BOOLEAN,
    is_domestic           BOOLEAN,
    beat                  STRING,
    -- Raw codes (kept for filtering/joining downstream)
    district_code         STRING,
    ward_num              INT,
    community_area_num    INT,
    -- Enriched names from dimension joins
    district_name         STRING
                          COMMENT 'From dim_districts. NULL for district 13 and 21 (absent in source)',
    ward_label            STRING
                          COMMENT 'From dim_wards. NULL if ward not in dim.',
    community_name        STRING
                          COMMENT 'From dim_community_areas. NULL where community_area=0',
    -- Coordinates
    lat_final             DOUBLE,
    lon_final             DOUBLE,
    coord_source          STRING,
    -- Lineage
    source_year_range     STRING,
    _source_file          STRING,
    _silver_step          STRING,
    _transform_ts         TIMESTAMP
)
USING DELTA
COMMENT 'Silver Step 5 — MAIN SILVER TABLE. Conformed with boundary dims via LEFT JOIN.';

-- ────────────────────────────────────────────────────────────
-- DIMENSION: dim_wards
-- DROPPED: the_geom (raw MULTIPOLYGON string — no SQL value)
-- Source has 50 distinct wards (profiling confirmed)
-- ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS silver.dim_wards (
    ward_num      INT
                  COMMENT 'PK — TRY_CAST(ward AS INT). Range 1–50.',
    ward_label    STRING
                  COMMENT 'Constructed label: CONCAT("Ward ", ward_num)',
    shape_area    DOUBLE,
    shape_leng    DOUBLE,
    _source_file  STRING,
    _transform_ts TIMESTAMP
    -- DROPPED: the_geom — raw MULTIPOLYGON polygon string
    --          No analytical use in SQL. Would bloat table storage.
)
USING DELTA
COMMENT 'Silver dim — Wards (2015). Dropped: the_geom. 50 distinct wards.';

-- ────────────────────────────────────────────────────────────
-- DIMENSION: dim_districts
-- DROPPED: the_geom
-- Note: dist 13 and 21 are ABSENT from both crimes data
-- AND the dim source — not a data quality issue, just noted
-- districts 23,26-30 absent from crimes data but don't exist in dim
-- ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS silver.dim_districts (
    district_num   INT
                   COMMENT 'PK — TRY_CAST(dist_num AS INT)',
    district_label STRING
                   COMMENT 'UPPER TRIM of dist_label e.g. "1ST"',
    _source_file   STRING,
    _transform_ts  TIMESTAMP
    -- DROPPED: the_geom
)
USING DELTA
COMMENT 'Silver dim — Police Districts (Dec 2012). Dropped: the_geom. Note: 13, 21 absent in source.';

-- ────────────────────────────────────────────────────────────
-- DIMENSION: dim_community_areas
-- DROPPED: the_geom, area_num_1 (duplicate of area_numbe)
-- Note: community_area=0 in crimes has no match here
-- ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS silver.dim_community_areas (
    area_num       INT
                   COMMENT 'PK — TRY_CAST(area_numbe AS INT). Range 1–77.',
    community_name STRING
                   COMMENT 'UPPER TRIM of community e.g. "ROGERS PARK"',
    shape_area     DOUBLE,
    shape_len      DOUBLE,
    _source_file   STRING,
    _transform_ts  TIMESTAMP
    -- DROPPED: the_geom — raw MULTIPOLYGON blob
    -- DROPPED: area_num_1 — exact duplicate of area_numbe (profiling confirmed)
)
USING DELTA
COMMENT 'Silver dim — Community Areas (77 areas). Dropped: the_geom, area_num_1.';

SELECT 'Silver tables created successfully' AS status;
