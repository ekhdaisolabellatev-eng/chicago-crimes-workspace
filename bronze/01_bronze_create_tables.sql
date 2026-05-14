-- ============================================================
-- CHICAGO CRIME DATA LAKE — DATABRICKS SQL
-- Script: 01_bronze_create_tables.sql
-- Zone:   BRONZE
-- Purpose: Create all Bronze landing tables.
--          ALL columns are STRING — zero transformation,
--          zero casting. Raw data preserved exactly as-is.
--          Extra metadata columns added for traceability.
-- Run: Once (or DROP + recreate if reloading from scratch)
-- ============================================================
USE CATALOG chicago_crimes_workspace;

USE bronze;

-- ────────────────────────────────────────────────────────────
-- CRIMES — 2001 to 2004
-- ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS bronze.crimes_2001_2004 (

    -- ── Original 22 columns — all STRING (raw, no casting) ──
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

    -- ── Bronze metadata columns ──────────────────────────────
    _source_file          STRING  COMMENT 'CSV filename this row came from',
    _source_path          STRING  COMMENT 'Full DBFS path of source file',
    _ingest_ts            TIMESTAMP COMMENT 'UTC timestamp when row was loaded',
    _bronze_table         STRING  COMMENT 'This table name for lineage'
)
USING DELTA
COMMENT 'Bronze landing — Chicago Crimes 2001 to 2004, raw, all columns STRING'
TBLPROPERTIES (
    'delta.appendOnly' = 'true',
    'bronze.source'    = 'Kaggle / City of Chicago',
    'bronze.zone'      = 'BRONZE'
);

-- ────────────────────────────────────────────────────────────
-- CRIMES — 2005 to 2007
-- ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS bronze.crimes_2005_2007 (
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
    _bronze_table         STRING
)
USING DELTA
COMMENT 'Bronze landing — Chicago Crimes 2005 to 2007, raw, all columns STRING'
TBLPROPERTIES ('delta.appendOnly' = 'true', 'bronze.zone' = 'BRONZE');

-- ────────────────────────────────────────────────────────────
-- CRIMES — 2008 to 2011
-- ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS bronze.crimes_2008_2011 (
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
    _bronze_table         STRING
)
USING DELTA
COMMENT 'Bronze landing — Chicago Crimes 2008 to 2011, raw, all columns STRING'
TBLPROPERTIES ('delta.appendOnly' = 'true', 'bronze.zone' = 'BRONZE');

-- ────────────────────────────────────────────────────────────
-- CRIMES — 2012 to 2017
-- ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS bronze.crimes_2012_2017 (
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
    _bronze_table         STRING
)
USING DELTA
COMMENT 'Bronze landing — Chicago Crimes 2012 to 2017, raw, all columns STRING'
TBLPROPERTIES ('delta.appendOnly' = 'true', 'bronze.zone' = 'BRONZE');

-- ────────────────────────────────────────────────────────────
-- BOUNDARY — WARDS 2015
-- Only 3 real columns + geometry; ward number is the key
-- ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS bronze.dim_wards_raw (
    the_geom      STRING   COMMENT 'Raw MULTIPOLYGON geometry string',
    ward          STRING   COMMENT 'Ward number (join key to crimes)',
    shape_leng    STRING,
    shape_area    STRING,
    _source_file  STRING,
    _source_path  STRING,
    _ingest_ts    TIMESTAMP,
    _bronze_table STRING
)
USING DELTA
COMMENT 'Bronze landing — Ward boundaries 2015, raw'
TBLPROPERTIES ('delta.appendOnly' = 'true', 'bronze.zone' = 'BRONZE');

-- ────────────────────────────────────────────────────────────
-- BOUNDARY — POLICE DISTRICTS (Dec 2012)
-- ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS bronze.dim_districts_raw (
    the_geom      STRING   COMMENT 'Raw MULTIPOLYGON geometry string',
    dist_label    STRING   COMMENT 'District label e.g. "1ST"',
    dist_num      STRING   COMMENT 'District number (join key to crimes.district)',
    _source_file  STRING,
    _source_path  STRING,
    _ingest_ts    TIMESTAMP,
    _bronze_table STRING
)
USING DELTA
COMMENT 'Bronze landing — Police Districts Dec 2012, raw'
TBLPROPERTIES ('delta.appendOnly' = 'true', 'bronze.zone' = 'BRONZE');

-- ────────────────────────────────────────────────────────────
-- BOUNDARY — COMMUNITY AREAS
-- ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS bronze.dim_community_areas_raw (
    the_geom      STRING   COMMENT 'Raw MULTIPOLYGON geometry string',
    area_numbe    STRING   COMMENT 'Area number (join key to crimes.community_area)',
    community     STRING   COMMENT 'Community area name e.g. "ROGERS PARK"',
    area_num_1    STRING,
    shape_area    STRING,
    shape_len     STRING,
    _source_file  STRING,
    _source_path  STRING,
    _ingest_ts    TIMESTAMP,
    _bronze_table STRING
)
USING DELTA
COMMENT 'Bronze landing — Community Area boundaries, raw'
TBLPROPERTIES ('delta.appendOnly' = 'true', 'bronze.zone' = 'BRONZE');

SELECT 'Bronze tables created successfully' AS status;
