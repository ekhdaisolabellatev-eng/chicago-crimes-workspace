-- ============================================================
-- CHICAGO CRIME DATA LAKE — DATABRICKS SQL
-- Script: 09_gold_create_tables.sql
-- Zone:   GOLD
-- Purpose: Create the complete Gold layer star schema.
--
-- STAR SCHEMA DESIGN:
--   ┌──────────────────────────────────────────────┐
--   │  fact_crimes (central fact table)            │
--   │  One row per unique, clean crime event       │
--   │  Sourced from: silver.crimes_conformed       │
--   └──────────────────────────────────────────────┘
--
--   5 DIMENSION TABLES:
--   • dim_date          — temporal (year/month/quarter/weekday)
--   • dim_crime_type    — crime classification (type/FBI/category)
--   • dim_location      — geographic hierarchy (beat→district→ward→area)
--   • dim_shift         — police shift pattern (hour→shift name)
--   • dim_location_type — where crime occurred (street/residence/etc)
--
--   10 ANALYTICAL VIEWS (one per business question):
--   • v_temporal_monthly_trend
--   • v_temporal_yoy_change
--   • v_temporal_shift_analysis
--   • v_temporal_weekday_pattern
--   • v_geo_community_profile
--   • v_geo_district_performance
--   • v_crime_type_breakdown
--   • v_crime_type_by_area
--   • v_arrest_effectiveness
--   • v_domestic_crime_analysis
--
--   2 ML FEATURE TABLES:
--   • ml_community_features  — K-Means input (community level)
--   • ml_district_features   — K-Means input (district level)
-- ============================================================

USE CATALOG chicago_crimes_workspace;
USE gold;

-- ════════════════════════════════════════════════════════════
-- DIMENSION TABLES
-- ════════════════════════════════════════════════════════════

-- ────────────────────────────────────────────────────────────
-- dim_date
-- Temporal grain for all time-series analysis
-- Populated from distinct dates in silver.crimes_conformed
-- ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS gold.dim_date (
    date_key          INT        COMMENT 'PK — surrogate key: YYYYMMDD integer',
    full_date         DATE       COMMENT 'Calendar date',
    year              INT,
    month             INT,
    month_name        STRING     COMMENT 'January … December',
    quarter           STRING     COMMENT 'Q1 | Q2 | Q3 | Q4',
    day_of_month      INT,
    day_of_week_num   INT        COMMENT '1=Sunday … 7=Saturday (Spark DAYOFWEEK)',
    day_name          STRING     COMMENT 'Sunday … Saturday',
    weekday_type      STRING     COMMENT 'Weekday | Weekend',
    hour_of_day       INT        COMMENT 'Repeated per hour (0–23) for join with fact',
    is_holiday        BOOLEAN    COMMENT 'FALSE — placeholder for future enrichment'
)
USING DELTA
COMMENT 'Gold dim — date attributes. One row per distinct incident date in silver.';

-- ────────────────────────────────────────────────────────────
-- dim_crime_type
-- Crime classification hierarchy
-- primary_type → fbi_code → crime_category (VIOLENT/PROPERTY/OTHER)
-- ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS gold.dim_crime_type (
    crime_type_key    INT        COMMENT 'PK — surrogate key via DENSE_RANK',
    primary_type      STRING     COMMENT 'e.g. THEFT, BATTERY, NARCOTICS',
    fbi_code          STRING     COMMENT 'e.g. 06, 04A, 16',
    fbi_description   STRING     COMMENT 'Full FBI classification label',
    crime_category    STRING     COMMENT 'VIOLENT | PROPERTY | OTHER',
    is_violent        BOOLEAN    COMMENT 'TRUE when category = VIOLENT'
)
USING DELTA
COMMENT 'Gold dim — crime type classifications. 36 distinct primary types from profiling.';

-- ────────────────────────────────────────────────────────────
-- dim_location
-- Geographic hierarchy: beat → district → ward → community area
-- One row per unique combination found in silver data
-- ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS gold.dim_location (
    location_key       INT       COMMENT 'PK — surrogate key via DENSE_RANK',
    beat               STRING,
    district_code      STRING    COMMENT 'e.g. 004',
    district_name      STRING    COMMENT 'e.g. 4TH — from dim_districts join',
    ward_num           INT,
    ward_label         STRING    COMMENT 'e.g. Ward 1',
    community_area_num INT,
    community_name     STRING    COMMENT 'e.g. ROGERS PARK'
)
USING DELTA
COMMENT 'Gold dim — geographic hierarchy. Unique beat+district+ward+community combinations.';

-- ────────────────────────────────────────────────────────────
-- dim_shift
-- Maps hour of day (0–23) to named police shift
-- Enables direct shift-level aggregation in Gold
-- ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS gold.dim_shift (
    shift_key         INT        COMMENT 'PK — same as hour_of_day (0–23)',
    hour_of_day       INT        COMMENT 'Hour component (0=midnight, 13=1pm)',
    shift_name        STRING     COMMENT 'Morning | Evening | Night',
    shift_hours       STRING     COMMENT 'e.g. 06:00–14:00',
    shift_type        STRING     COMMENT 'DAY | EVENING | NIGHT'
)
USING DELTA
COMMENT 'Gold dim — police shift mapping. 24 rows, one per hour.';

-- ────────────────────────────────────────────────────────────
-- dim_location_type
-- Where the crime physically occurred
-- e.g. STREET, RESIDENCE, APARTMENT, PARKING LOT
-- ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS gold.dim_location_type (
    location_type_key  INT       COMMENT 'PK — surrogate key via DENSE_RANK',
    location_description STRING  COMMENT 'Raw value from crimes e.g. STREET',
    location_category  STRING    COMMENT 'OUTDOOR | RESIDENTIAL | COMMERCIAL | TRANSPORT | OTHER'
)
USING DELTA
COMMENT 'Gold dim — location type where crime occurred. Derived from location_description.';

-- ════════════════════════════════════════════════════════════
-- FACT TABLE
-- ════════════════════════════════════════════════════════════

-- ────────────────────────────────────────────────────────────
-- fact_crimes
-- Central fact table — one row per unique crime event
-- Surrogate FK keys to all 5 dimensions
-- Denormalised frequently-used attributes to avoid joins
-- in dashboard queries
-- ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS gold.fact_crimes (

    -- ── Natural key ──────────────────────────────────────────
    crime_id              BIGINT    COMMENT 'NK — from silver.crimes_conformed',
    case_number           STRING,

    -- ── Dimension foreign keys ────────────────────────────────
    date_key              INT       COMMENT 'FK → gold.dim_date.date_key',
    crime_type_key        INT       COMMENT 'FK → gold.dim_crime_type.crime_type_key',
    location_key          INT       COMMENT 'FK → gold.dim_location.location_key',
    shift_key             INT       COMMENT 'FK → gold.dim_shift.shift_key (= incident_hour)',
    location_type_key     INT       COMMENT 'FK → gold.dim_location_type.location_type_key',

    -- ── Measures ──────────────────────────────────────────────
    is_arrest             BOOLEAN   COMMENT 'Measure — 1 if arrest made',
    is_domestic           BOOLEAN   COMMENT 'Measure — 1 if domestic incident',
    lat_final             DOUBLE    COMMENT 'Spatial measure — latitude',
    lon_final             DOUBLE    COMMENT 'Spatial measure — longitude',

    -- ── Denormalised attributes  ───
    -- Date
    incident_year         INT,
    incident_month        INT,
    incident_hour         INT,
    incident_dayofweek    INT,
    -- Crime
    primary_type          STRING,
    fbi_code              STRING,
    crime_category        STRING,   -- VIOLENT | PROPERTY | OTHER
    is_violent            BOOLEAN,
    -- Geography
    community_area_num    INT,
    community_name        STRING,
    district_code         STRING,
    district_name         STRING,
    ward_num              INT,
    -- Location type
    location_description  STRING,
    -- Lineage
    source_year_range     STRING,
    _source_file          STRING,
    _gold_ts              TIMESTAMP COMMENT 'When this row was written to Gold'
)
USING DELTA
COMMENT 'Gold central fact table — one row per crime. Star schema with 5 dimension FKs.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact'   = 'true'
);

SELECT 'Gold tables created successfully' AS status;
