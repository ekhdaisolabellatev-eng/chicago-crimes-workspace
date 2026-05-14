-- ============================================================
-- CHICAGO CRIME DATA LAKE — DATABRICKS SQL
-- Script: 00_bronze_profiling_analysis.sql
-- Zone:   PRE-SILVER (Bronze Analysis)
-- Purpose: Profile ALL Bronze tables BEFORE any transformation.
--          Results from this script directly justify every
--          Silver cleaning rule in the project report.
-- Run: BEFORE 03_silver_create_tables.sql
-- Catalog: chicago_crimes_workspace
-- ============================================================

USE CATALOG chicago_crimes_workspace;

-- ============================================================
-- SECTION 1: ROW COUNT VERIFICATION
-- Confirm all 7 Bronze tables loaded correctly
-- ============================================================

SELECT '=== SECTION 1: BRONZE ROW COUNTS ===' AS section;

SELECT 'crimes_2001_2004' AS table_name, COUNT(*) AS row_count FROM bronze.crimes_2001_2004
UNION ALL SELECT 'crimes_2005_2007', COUNT(*) FROM bronze.crimes_2005_2007
UNION ALL SELECT 'crimes_2008_2011', COUNT(*) FROM bronze.crimes_2008_2011
UNION ALL SELECT 'crimes_2012_2017', COUNT(*) FROM bronze.crimes_2012_2017
UNION ALL SELECT 'dim_wards_raw',    COUNT(*) FROM bronze.dim_wards_raw
UNION ALL SELECT 'dim_districts_raw',COUNT(*) FROM bronze.dim_districts_raw
UNION ALL SELECT 'dim_community_areas_raw', COUNT(*) FROM bronze.dim_community_areas_raw
UNION ALL SELECT '--- TOTAL CRIMES ---',
    (SELECT COUNT(*) FROM bronze.crimes_2001_2004) +
    (SELECT COUNT(*) FROM bronze.crimes_2005_2007) +
    (SELECT COUNT(*) FROM bronze.crimes_2008_2011) +
    (SELECT COUNT(*) FROM bronze.crimes_2012_2017);

-- ============================================================
-- SECTION 2: RAW DATA PREVIEW — 5 rows per table
-- See exactly what came in from each CSV
-- ============================================================

SELECT '=== SECTION 2A: CRIMES 2001-2004 SAMPLE ===' AS section;
SELECT * FROM bronze.crimes_2001_2004 LIMIT 5;

SELECT '=== SECTION 2B: CRIMES 2005-2007 SAMPLE ===' AS section;
SELECT * FROM bronze.crimes_2005_2007 LIMIT 5;

SELECT '=== SECTION 2C: CRIMES 2008-2011 SAMPLE ===' AS section;
SELECT * FROM bronze.crimes_2008_2011 LIMIT 5;

SELECT '=== SECTION 2D: CRIMES 2012-2017 SAMPLE ===' AS section;
SELECT * FROM bronze.crimes_2012_2017 LIMIT 5;

SELECT '=== SECTION 2E: DIM WARDS SAMPLE ===' AS section;
SELECT ward, shape_leng, shape_area, _source_file FROM bronze.dim_wards_raw LIMIT 5;

SELECT '=== SECTION 2F: DIM DISTRICTS SAMPLE ===' AS section;
SELECT dist_label, dist_num, _source_file FROM bronze.dim_districts_raw LIMIT 5;

SELECT '=== SECTION 2G: DIM COMMUNITY AREAS SAMPLE ===' AS section;
SELECT area_numbe, community, area_num_1, shape_area, _source_file
FROM bronze.dim_community_areas_raw LIMIT 10;

-- ============================================================
-- SECTION 3: NULL / MISSING VALUE ANALYSIS — CRIMES
-- Count NULLs and empty strings per column across all 4 files
-- This directly tells you which quality gates to build
-- ============================================================

SELECT '=== SECTION 3: NULL ANALYSIS — UNIONED CRIMES ===' AS section;

-- Union all 4 first for null analysis
WITH all_crimes AS (
    SELECT *, '2001_2004' AS yr_range FROM bronze.crimes_2001_2004
    UNION ALL
    SELECT *, '2005_2007' FROM bronze.crimes_2005_2007
    UNION ALL
    SELECT *, '2008_2011' FROM bronze.crimes_2008_2011
    UNION ALL
    SELECT *, '2012_2017' FROM bronze.crimes_2012_2017
)

SELECT
    COUNT(*) AS total_rows,

    -- Core identifier
    SUM(CASE WHEN id IS NULL THEN 1 ELSE 0 END) AS null_id,
    SUM(CASE WHEN case_number IS NULL OR TRIM(case_number) = '' THEN 1 ELSE 0 END) AS null_case_number,

    -- Date
    SUM(CASE WHEN date IS NULL OR TRIM(date) = '' THEN 1 ELSE 0 END) AS null_date,

    -- Crime classification
    SUM(CASE WHEN iucr IS NULL OR TRIM(iucr) = '' THEN 1 ELSE 0 END) AS null_iucr,
    SUM(CASE WHEN primary_type IS NULL OR TRIM(primary_type) = '' THEN 1 ELSE 0 END) AS null_primary_type,
    SUM(CASE WHEN description IS NULL OR TRIM(description) = '' THEN 1 ELSE 0 END) AS null_description,
    SUM(CASE WHEN fbi_code IS NULL OR TRIM(fbi_code) = '' THEN 1 ELSE 0 END) AS null_fbi_code,

    -- Location text
    SUM(CASE WHEN block IS NULL OR TRIM(block) = '' THEN 1 ELSE 0 END) AS null_block,
    SUM(CASE WHEN location_description IS NULL OR TRIM(location_description) = '' THEN 1 ELSE 0 END) AS null_location_desc,

    -- Geographic codes (cast to STRING)
    SUM(CASE WHEN beat IS NULL OR TRIM(CAST(beat AS STRING)) = '' THEN 1 ELSE 0 END) AS null_beat,
    SUM(CASE WHEN district IS NULL OR TRIM(CAST(district AS STRING)) = '' THEN 1 ELSE 0 END) AS null_district,
    SUM(CASE WHEN ward IS NULL OR TRIM(CAST(ward AS STRING)) = '' THEN 1 ELSE 0 END) AS null_ward,
    SUM(CASE WHEN community_area IS NULL OR TRIM(CAST(community_area AS STRING)) = '' THEN 1 ELSE 0 END) AS null_community_area,

    -- Flags (cast if boolean/int)
    SUM(CASE WHEN arrest IS NULL OR TRIM(CAST(arrest AS STRING)) = '' THEN 1 ELSE 0 END) AS null_arrest,
    SUM(CASE WHEN domestic IS NULL OR TRIM(CAST(domestic AS STRING)) = '' THEN 1 ELSE 0 END) AS null_domestic,

    -- Coordinates (cast to STRING)
    SUM(CASE WHEN latitude IS NULL OR TRIM(CAST(latitude AS STRING)) = '' THEN 1 ELSE 0 END) AS null_latitude,
    SUM(CASE WHEN longitude IS NULL OR TRIM(CAST(longitude AS STRING)) = '' THEN 1 ELSE 0 END) AS null_longitude,
    SUM(CASE WHEN x_coordinate IS NULL OR TRIM(CAST(x_coordinate AS STRING)) = '' THEN 1 ELSE 0 END) AS null_x_coord,
    SUM(CASE WHEN y_coordinate IS NULL OR TRIM(CAST(y_coordinate AS STRING)) = '' THEN 1 ELSE 0 END) AS null_y_coord,
    SUM(CASE WHEN location IS NULL OR TRIM(location) = '' THEN 1 ELSE 0 END) AS null_location_str,

    -- Dropped cols check
    SUM(CASE WHEN year IS NULL OR TRIM(CAST(year AS STRING)) = '' THEN 1 ELSE 0 END) AS null_year,
    SUM(CASE WHEN updated_on IS NULL OR TRIM(updated_on) = '' THEN 1 ELSE 0 END) AS null_updated_on

FROM all_crimes;


-- ============================================================
-- SECTION 3B: NULL ANALYSIS PER FILE
-- ============================================================

SELECT '=== SECTION 3B: NULLS BY SOURCE FILE ===' AS section;

WITH all_crimes AS (
    SELECT *, '2001_2004' AS yr_range FROM bronze.crimes_2001_2004
    UNION ALL
    SELECT *, '2005_2007' FROM bronze.crimes_2005_2007
    UNION ALL
    SELECT *, '2008_2011' FROM bronze.crimes_2008_2011
    UNION ALL
    SELECT *, '2012_2017' FROM bronze.crimes_2012_2017
)

SELECT
    yr_range,
    COUNT(*) AS total_rows,

    SUM(CASE WHEN latitude IS NULL OR TRIM(CAST(latitude AS STRING)) = '' THEN 1 ELSE 0 END) AS null_lat,
    SUM(CASE WHEN longitude IS NULL OR TRIM(CAST(longitude AS STRING)) = '' THEN 1 ELSE 0 END) AS null_lon,
    SUM(CASE WHEN location IS NULL OR TRIM(location) = '' THEN 1 ELSE 0 END) AS null_location,
    SUM(CASE WHEN community_area IS NULL OR TRIM(CAST(community_area AS STRING)) = '' THEN 1 ELSE 0 END) AS null_comm_area,
    SUM(CASE WHEN ward IS NULL OR TRIM(CAST(ward AS STRING)) = '' THEN 1 ELSE 0 END) AS null_ward,
    SUM(CASE WHEN district IS NULL OR TRIM(CAST(district AS STRING)) = '' THEN 1 ELSE 0 END) AS null_district

FROM all_crimes
GROUP BY yr_range
ORDER BY yr_range;


-- ============================================================
-- SECTION 4: DUPLICATE ANALYSIS
-- Check for duplicate crime_id WITHIN and ACROSS files
-- ============================================================

SELECT '=== SECTION 4A: DUPLICATE IDs WITHIN EACH FILE ===' AS section;

SELECT '2001_2004' AS file_range,
    COUNT(*) AS total_rows,
    COUNT(DISTINCT `id`) AS distinct_ids,
    COUNT(*) - COUNT(DISTINCT `id`) AS duplicate_count
FROM bronze.crimes_2001_2004
UNION ALL
SELECT '2005_2007', COUNT(*), COUNT(DISTINCT `id`), COUNT(*) - COUNT(DISTINCT `id`)
FROM bronze.crimes_2005_2007
UNION ALL
SELECT '2008_2011', COUNT(*), COUNT(DISTINCT `id`), COUNT(*) - COUNT(DISTINCT `id`)
FROM bronze.crimes_2008_2011
UNION ALL
SELECT '2012_2017', COUNT(*), COUNT(DISTINCT `id`), COUNT(*) - COUNT(DISTINCT `id`)
FROM bronze.crimes_2012_2017;

SELECT '=== SECTION 4B: IDs APPEARING IN MULTIPLE FILES (CROSS-FILE DUPS) ===' AS section;

WITH all_ids AS (
    SELECT `id`, '2001_2004' AS yr_range FROM bronze.crimes_2001_2004
    UNION ALL SELECT `id`, '2005_2007' FROM bronze.crimes_2005_2007
    UNION ALL SELECT `id`, '2008_2011' FROM bronze.crimes_2008_2011
    UNION ALL SELECT `id`, '2012_2017' FROM bronze.crimes_2012_2017
),
id_counts AS (
    SELECT `id`, COUNT(*) AS appearances, COUNT(DISTINCT yr_range) AS in_n_files
    FROM all_ids
    GROUP BY `id`
    HAVING COUNT(*) > 1
)
SELECT
    in_n_files AS appears_in_n_files,
    COUNT(*) AS id_count,
    MIN(`id`) AS sample_id
FROM id_counts
GROUP BY in_n_files
ORDER BY in_n_files DESC;

-- ── Show actual duplicate rows ───────────────────────────────
SELECT '=== SECTION 4C: SAMPLE DUPLICATE RECORDS ===' AS section;

WITH all_crimes AS (
    SELECT `id`, `date`, primary_type, district, _source_file, '2001_2004' AS yr_range FROM bronze.crimes_2001_2004
    UNION ALL SELECT `id`, `date`, primary_type, district, _source_file, '2005_2007' FROM bronze.crimes_2005_2007
    UNION ALL SELECT `id`, `date`, primary_type, district, _source_file, '2008_2011' FROM bronze.crimes_2008_2011
    UNION ALL SELECT `id`, `date`, primary_type, district, _source_file, '2012_2017' FROM bronze.crimes_2012_2017
),
dup_ids AS (
    SELECT `id` FROM all_crimes GROUP BY `id` HAVING COUNT(*) > 1 LIMIT 5
)
SELECT ac.*
FROM all_crimes ac
INNER JOIN dup_ids d ON ac.`id` = d.`id`
ORDER BY ac.`id`;

-- ============================================================
-- SECTION 5: DATA TYPE VALIDITY CHECKS
-- Validate that STRING columns can be safely cast
-- ============================================================

SELECT '=== SECTION 5: TYPE VALIDITY CHECKS ===' AS section;

WITH all_crimes AS (
    SELECT * FROM bronze.crimes_2001_2004
    UNION ALL SELECT * FROM bronze.crimes_2005_2007
    UNION ALL SELECT * FROM bronze.crimes_2008_2011
    UNION ALL SELECT * FROM bronze.crimes_2012_2017
)
SELECT
    -- id should be castable to BIGINT
    SUM(CASE WHEN TRY_CAST(`id` AS BIGINT) IS NULL
             AND `id` IS NOT NULL AND TRIM(`id`) != ''
             THEN 1 ELSE 0 END)           AS id_not_numeric,

    -- ward should be castable to INT
    SUM(CASE WHEN TRY_CAST(ward AS INT) IS NULL
             AND ward IS NOT NULL AND TRIM(ward) != ''
             THEN 1 ELSE 0 END)           AS ward_not_int,

    -- community_area should be castable to INT
    SUM(CASE WHEN TRY_CAST(community_area AS INT) IS NULL
             AND community_area IS NOT NULL AND TRIM(community_area) != ''
             THEN 1 ELSE 0 END)           AS community_area_not_int,

    -- year should be castable to INT
    SUM(CASE WHEN TRY_CAST(`year` AS INT) IS NULL
             AND `year` IS NOT NULL AND TRIM(`year`) != ''
             THEN 1 ELSE 0 END)           AS year_not_int,

    -- latitude should be castable to DOUBLE
    SUM(CASE WHEN TRY_CAST(latitude AS DOUBLE) IS NULL
             AND latitude IS NOT NULL AND TRIM(latitude) != ''
             THEN 1 ELSE 0 END)           AS lat_not_double,

    -- longitude should be castable to DOUBLE
    SUM(CASE WHEN TRY_CAST(longitude AS DOUBLE) IS NULL
             AND longitude IS NOT NULL AND TRIM(longitude) != ''
             THEN 1 ELSE 0 END)           AS lon_not_double,

    -- x_coordinate should be castable to DOUBLE
    SUM(CASE WHEN TRY_CAST(x_coordinate AS DOUBLE) IS NULL
             AND x_coordinate IS NOT NULL AND TRIM(x_coordinate) != ''
             THEN 1 ELSE 0 END)           AS x_coord_not_double,

    -- arrest should only be TRUE or FALSE
    SUM(CASE WHEN LOWER(TRIM(arrest)) NOT IN ('true','false')
             AND arrest IS NOT NULL AND TRIM(arrest) != ''
             THEN 1 ELSE 0 END)           AS arrest_invalid_value,

    -- domestic should only be TRUE or FALSE
    SUM(CASE WHEN LOWER(TRIM(domestic)) NOT IN ('true','false')
             AND domestic IS NOT NULL AND TRIM(domestic) != ''
             THEN 1 ELSE 0 END)           AS domestic_invalid_value

FROM all_crimes;

-- ============================================================
-- SECTION 6: DATE FORMAT ANALYSIS
-- Check what date formats exist and if they parse correctly
-- ============================================================

SELECT '=== SECTION 6A: DATE FORMAT SAMPLES ===' AS section;

WITH all_crimes AS (
    SELECT `date` FROM bronze.crimes_2001_2004
    UNION ALL SELECT `date` FROM bronze.crimes_2005_2007
    UNION ALL SELECT `date` FROM bronze.crimes_2008_2011
    UNION ALL SELECT `date` FROM bronze.crimes_2012_2017
)
SELECT
    `date` AS raw_date_value,
    try_to_timestamp(TRIM(`date`), 'MM/dd/yyyy hh:mm:ss a') AS parsed_date_attempt
    
FROM all_crimes
LIMIT 20;

SELECT '=== SECTION 6B: HOW MANY DATES FAIL TO PARSE ===' AS section;

WITH all_crimes AS (
    SELECT `date` FROM bronze.crimes_2001_2004
    UNION ALL SELECT `date` FROM bronze.crimes_2005_2007
    UNION ALL SELECT `date` FROM bronze.crimes_2008_2011
    UNION ALL SELECT `date` FROM bronze.crimes_2012_2017
)
SELECT
    COUNT(*) AS total,
    SUM(CASE WHEN try_to_timestamp(TRIM(`date`), 'MM/dd/yyyy hh:mm:ss a') IS NOT NULL THEN 1 ELSE 0 END) AS format1_success,
    SUM(CASE WHEN try_to_timestamp(TRIM(`date`), 'MM/dd/yyyy hh:mm:ss a') IS NULL
             AND `date` IS NOT NULL AND TRIM(`date`) != ''           THEN 1 ELSE 0 END) AS format1_fail,
    SUM(CASE WHEN `date` IS NULL OR TRIM(`date`) = ''                THEN 1 ELSE 0 END) AS null_dates
FROM all_crimes;

-- ── Future dates check ───────────────────────────────────────
SELECT '=== SECTION 6C: FUTURE DATE CHECK ===' AS section;

WITH all_crimes AS (
    SELECT `id`, `date`, `year` FROM bronze.crimes_2001_2004
    UNION ALL SELECT `id`, `date`, `year` FROM bronze.crimes_2005_2007
    UNION ALL SELECT `id`, `date`, `year` FROM bronze.crimes_2008_2011
    UNION ALL SELECT `id`, `date`, `year` FROM bronze.crimes_2012_2017
)
SELECT
    SUM(CASE WHEN try_to_timestamp(TRIM(`date`), 'MM/dd/yyyy hh:mm:ss a') > current_timestamp() THEN 1 ELSE 0 END) AS future_dates,
    SUM(CASE WHEN TRY_CAST(`year` AS INT) < 2001 THEN 1 ELSE 0 END) AS year_before_2001,
    SUM(CASE WHEN TRY_CAST(`year` AS INT) > 2017 THEN 1 ELSE 0 END) AS year_after_2017,
    MIN(`year`) AS min_year_raw,
    MAX(`year`) AS max_year_raw
FROM all_crimes;

-- ============================================================
-- SECTION 7: VALUE DISTRIBUTION ANALYSIS
-- Understand what's in key categorical columns
-- ============================================================

SELECT '=== SECTION 7A: DISTINCT PRIMARY TYPES ===' AS section;

WITH all_crimes AS (
    SELECT primary_type FROM bronze.crimes_2001_2004
    UNION ALL SELECT primary_type FROM bronze.crimes_2005_2007
    UNION ALL SELECT primary_type FROM bronze.crimes_2008_2011
    UNION ALL SELECT primary_type FROM bronze.crimes_2012_2017
)
SELECT
    primary_type         AS raw_value,
    UPPER(TRIM(primary_type)) AS cleaned_value,
    COUNT(*)             AS frequency,
    -- Flag if cleaned differs from raw (casing/spacing issues)
    CASE WHEN primary_type != UPPER(TRIM(primary_type)) THEN 'NEEDS_CLEAN' ELSE 'OK' END AS needs_cleaning
FROM all_crimes
GROUP BY primary_type
ORDER BY frequency DESC;

SELECT '=== SECTION 7B: ARREST FIELD DISTINCT VALUES ===' AS section;

WITH all_crimes AS (
    SELECT arrest FROM bronze.crimes_2001_2004
    UNION ALL SELECT arrest FROM bronze.crimes_2005_2007
    UNION ALL SELECT arrest FROM bronze.crimes_2008_2011
    UNION ALL SELECT arrest FROM bronze.crimes_2012_2017
)
SELECT
    arrest AS raw_value,
    LOWER(TRIM(arrest)) AS cleaned_value,
    COUNT(*) AS frequency
FROM all_crimes
GROUP BY arrest
ORDER BY frequency DESC;

SELECT '=== SECTION 7C: DOMESTIC FIELD DISTINCT VALUES ===' AS section;

WITH all_crimes AS (
    SELECT domestic FROM bronze.crimes_2001_2004
    UNION ALL SELECT domestic FROM bronze.crimes_2005_2007
    UNION ALL SELECT domestic FROM bronze.crimes_2008_2011
    UNION ALL SELECT domestic FROM bronze.crimes_2012_2017
)
SELECT domestic AS raw_value, LOWER(TRIM(domestic)) AS cleaned_value, COUNT(*) AS frequency
FROM all_crimes
GROUP BY domestic
ORDER BY frequency DESC;

SELECT '=== SECTION 7D: DISTRICT DISTINCT VALUES ===' AS section;

WITH all_crimes AS (
    SELECT district FROM bronze.crimes_2001_2004
    UNION ALL SELECT district FROM bronze.crimes_2005_2007
    UNION ALL SELECT district FROM bronze.crimes_2008_2011
    UNION ALL SELECT district FROM bronze.crimes_2012_2017
)
SELECT district AS raw_district, COUNT(*) AS frequency
FROM all_crimes
GROUP BY district
ORDER BY TRY_CAST(district AS INT);

SELECT '=== SECTION 7E: COMMUNITY AREA DISTINCT VALUES ===' AS section;

WITH all_crimes AS (
    SELECT community_area FROM bronze.crimes_2001_2004
    UNION ALL SELECT community_area FROM bronze.crimes_2005_2007
    UNION ALL SELECT community_area FROM bronze.crimes_2008_2011
    UNION ALL SELECT community_area FROM bronze.crimes_2012_2017
)
SELECT community_area AS raw_value, COUNT(*) AS frequency
FROM all_crimes
GROUP BY community_area
ORDER BY TRY_CAST(community_area AS INT);

SELECT '=== SECTION 7F: FBI CODE DISTINCT VALUES ===' AS section;

WITH all_crimes AS (
    SELECT fbi_code FROM bronze.crimes_2001_2004
    UNION ALL SELECT fbi_code FROM bronze.crimes_2005_2007
    UNION ALL SELECT fbi_code FROM bronze.crimes_2008_2011
    UNION ALL SELECT fbi_code FROM bronze.crimes_2012_2017
)
SELECT fbi_code AS raw_value, COUNT(*) AS frequency
FROM all_crimes
GROUP BY fbi_code
ORDER BY frequency DESC;

-- ============================================================
-- SECTION 8: COORDINATE / LOCATION STRING ANALYSIS
-- Many rows have NULL lat/lon — check if location string
-- can recover them (the semi-structured parsing justification)
-- ============================================================

SELECT '=== SECTION 8: COORDINATE RECOVERY ANALYSIS ===' AS section;

WITH all_crimes AS (
    SELECT `id`, latitude, longitude, location FROM bronze.crimes_2001_2004
    UNION ALL SELECT `id`, latitude, longitude, location FROM bronze.crimes_2005_2007
    UNION ALL SELECT `id`, latitude, longitude, location FROM bronze.crimes_2008_2011
    UNION ALL SELECT `id`, latitude, longitude, location FROM bronze.crimes_2012_2017
)
SELECT
    -- How many have direct lat/lon
    SUM(CASE WHEN latitude  IS NOT NULL AND TRIM(latitude)  != '' THEN 1 ELSE 0 END) AS has_direct_lat,
    SUM(CASE WHEN longitude IS NOT NULL AND TRIM(longitude) != '' THEN 1 ELSE 0 END) AS has_direct_lon,
    -- How many have the location string
    SUM(CASE WHEN location IS NOT NULL AND TRIM(location) != '' THEN 1 ELSE 0 END) AS has_location_str,
    -- How many have NEITHER lat/lon NOR location string
    SUM(CASE WHEN (latitude IS NULL OR TRIM(latitude)='')
              AND (longitude IS NULL OR TRIM(longitude)='')
              AND (location IS NULL OR TRIM(location)='') THEN 1 ELSE 0 END) AS has_nothing,
    -- How many can be recovered via location string parsing
    SUM(CASE WHEN (latitude IS NULL OR TRIM(latitude)='')
              AND location IS NOT NULL AND TRIM(location) != ''
              THEN 1 ELSE 0 END) AS recoverable_via_location_str,

    COUNT(*) AS total_rows
FROM all_crimes;

-- Sample what the location string looks like
SELECT '=== SECTION 8B: LOCATION STRING PARSE TEST ===' AS section;

SELECT
    location                                                             AS raw_location,
    latitude                                                             AS raw_lat,
    regexp_extract(TRIM(location), '\\(([^,]+),', 1)                    AS extracted_lat,
    regexp_extract(TRIM(location), ',\\s*([^)]+)\\)', 1)                AS extracted_lon,
    -- Compare parsed vs direct
    CASE WHEN latitude IS NULL OR TRIM(latitude) = '' THEN 'NEEDS_PARSE'
         ELSE 'HAS_DIRECT' END                                           AS lat_source
FROM bronze.crimes_2001_2004
WHERE location IS NOT NULL AND TRIM(location) != ''
LIMIT 15;

-- ============================================================
-- SECTION 9: COLUMNS TO DROP ANALYSIS
-- Identify columns that are redundant or low-value
-- ============================================================

SELECT '=== SECTION 9: COLUMNS TO DROP JUSTIFICATION ===' AS section;

WITH all_crimes AS (
    SELECT
        `id`, `year`, `date`,
        updated_on,
        x_coordinate, y_coordinate,
        latitude, longitude, location
    FROM bronze.crimes_2001_2004
    UNION ALL SELECT `id`, `year`, `date`, updated_on, x_coordinate, y_coordinate, latitude, longitude, location FROM bronze.crimes_2005_2007
    UNION ALL SELECT `id`, `year`, `date`, updated_on, x_coordinate, y_coordinate, latitude, longitude, location FROM bronze.crimes_2008_2011
    UNION ALL SELECT `id`, `year`, `date`, updated_on, x_coordinate, y_coordinate, latitude, longitude, location FROM bronze.crimes_2012_2017
)
SELECT
    -- YEAR column: already derivable from date — is it always consistent?
    SUM(CASE WHEN `year` != CAST(YEAR(try_to_timestamp(TRIM(`date`), 'MM/dd/yyyy hh:mm:ss a')) AS STRING) 
              THEN 1 ELSE 0 END) AS year_inconsistent_with_date,
    -- updated_on: audit column, not analytically useful
    COUNT(DISTINCT updated_on) AS distinct_updated_on_values,
    -- x_coordinate / y_coordinate: State Plane projection — redundant if lat/lon exists
    SUM(CASE WHEN x_coordinate IS NOT NULL AND latitude IS NOT NULL THEN 1 ELSE 0 END) AS both_coord_systems_present,
    -- location string: redundant if lat/lon exists after parsing
    SUM(CASE WHEN location IS NOT NULL AND latitude IS NOT NULL THEN 1 ELSE 0 END) AS has_both_location_and_latlon
FROM all_crimes;

-- ============================================================
-- SECTION 10: DIMENSION TABLE PROFILING
-- ============================================================

SELECT '=== SECTION 10A: DIM WARDS PROFILING ===' AS section;

SELECT
    COUNT(*) AS total_rows,
    COUNT(DISTINCT ward) AS distinct_wards,
    SUM(CASE WHEN ward IS NULL OR TRIM(ward)='' THEN 1 ELSE 0 END) AS null_ward,
    SUM(CASE WHEN TRY_CAST(ward AS INT) IS NULL
             AND ward IS NOT NULL THEN 1 ELSE 0 END) AS ward_not_castable_to_int,
    MIN(TRY_CAST(ward AS INT)) AS min_ward,
    MAX(TRY_CAST(ward AS INT)) AS max_ward
FROM bronze.dim_wards_raw;

SELECT '=== SECTION 10B: DIM DISTRICTS PROFILING ===' AS section;

SELECT
    COUNT(*) AS total_rows,
    COUNT(DISTINCT dist_num) AS distinct_districts,
    SUM(CASE WHEN dist_num IS NULL OR TRIM(dist_num)='' THEN 1 ELSE 0 END) AS null_dist_num,
    SUM(CASE WHEN dist_label IS NULL OR TRIM(dist_label)='' THEN 1 ELSE 0 END) AS null_dist_label,
    MIN(TRY_CAST(dist_num AS INT)) AS min_dist,
    MAX(TRY_CAST(dist_num AS INT)) AS max_dist
FROM bronze.dim_districts_raw;

SELECT '=== SECTION 10C: DIM COMMUNITY AREAS PROFILING ===' AS section;

SELECT
    COUNT(*) AS total_rows,
    COUNT(DISTINCT area_numbe) AS distinct_areas,
    SUM(CASE WHEN area_numbe IS NULL OR TRIM(area_numbe)='' THEN 1 ELSE 0 END) AS null_area_num,
    SUM(CASE WHEN community IS NULL OR TRIM(community)='' THEN 1 ELSE 0 END) AS null_community_name,
    MIN(TRY_CAST(area_numbe AS INT)) AS min_area,
    MAX(TRY_CAST(area_numbe AS INT)) AS max_area
FROM bronze.dim_community_areas_raw;

SELECT 'ALL COMMUNITY AREA NAMES:' AS info;
SELECT area_numbe, community FROM bronze.dim_community_areas_raw
ORDER BY TRY_CAST(area_numbe AS INT);

-- ============================================================
-- SECTION 11: JOIN KEY VALIDATION
-- Check that crimes.district, ward, community_area values
-- actually exist in the dimension tables
-- ============================================================

WITH all_crimes AS (
    SELECT district, ward, community_area FROM bronze.crimes_2001_2004
    UNION ALL 
    SELECT district, ward, community_area FROM bronze.crimes_2005_2007
    UNION ALL 
    SELECT district, ward, community_area FROM bronze.crimes_2008_2011
    UNION ALL 
    SELECT district, ward, community_area FROM bronze.crimes_2012_2017
)

SELECT
    '=== SECTION 11: JOIN KEY VALIDATION ===' AS section,

    -- Districts not matching dimension
    (
        SELECT COUNT(DISTINCT CAST(CAST(ac.district AS DOUBLE) AS INT))
        FROM all_crimes ac
        WHERE ac.district IS NOT NULL
          AND TRIM(CAST(ac.district AS STRING)) != ''
          AND LOWER(TRIM(CAST(ac.district AS STRING))) != 'beat'
          AND NOT EXISTS (
              SELECT 1 
              FROM bronze.dim_districts_raw d
              WHERE TRY_CAST(TRIM(d.dist_num) AS INT) =
                    CAST(CAST(ac.district AS DOUBLE) AS INT)
          )
    ) AS district_codes_not_in_dim,

    -- Wards not matching dimension
    (
        SELECT COUNT(DISTINCT CAST(CAST(ac.ward AS DOUBLE) AS INT))
        FROM all_crimes ac
        WHERE ac.ward IS NOT NULL
          AND TRIM(CAST(ac.ward AS STRING)) != ''
          AND LOWER(TRIM(CAST(ac.ward AS STRING))) != 'district'
          AND NOT EXISTS (
              SELECT 1 
              FROM bronze.dim_wards_raw w
              WHERE TRY_CAST(TRIM(w.ward) AS INT) =
                    CAST(CAST(ac.ward AS DOUBLE) AS INT)
          )
    ) AS ward_codes_not_in_dim,

    -- Community areas not matching dimension
    (
        SELECT COUNT(DISTINCT CAST(CAST(ac.community_area AS DOUBLE) AS INT))
        FROM all_crimes ac
        WHERE ac.community_area IS NOT NULL
          AND TRIM(CAST(ac.community_area AS STRING)) != ''
          AND LOWER(TRIM(CAST(ac.community_area AS STRING))) != 'ward'
          AND NOT EXISTS (
              SELECT 1 
              FROM bronze.dim_community_areas_raw ca
              WHERE TRY_CAST(TRIM(ca.area_numbe) AS INT) =
                    CAST(CAST(ac.community_area AS DOUBLE) AS INT)
          )
    ) AS community_area_codes_not_in_dim;

-- ============================================================
-- SECTION 12: YEAR DISTRIBUTION — understand data spread
-- ============================================================

SELECT '=== SECTION 12: CRIMES BY YEAR ===' AS section;

WITH all_crimes AS (
    SELECT `year` FROM bronze.crimes_2001_2004
    UNION ALL SELECT `year` FROM bronze.crimes_2005_2007
    UNION ALL SELECT `year` FROM bronze.crimes_2008_2011
    UNION ALL SELECT `year` FROM bronze.crimes_2012_2017
)
SELECT
    `year`,
    COUNT(*) AS crime_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct_of_total
FROM all_crimes
GROUP BY `year`
ORDER BY TRY_CAST(`year` AS INT);

-- ============================================================
-- SECTION 13: SUMMARY — SILVER STRATEGY TABLE
-- This is what you paste into your report Section 5
-- ============================================================

SELECT '=== SECTION 13: SILVER CLEANING STRATEGY SUMMARY ===' AS section;

SELECT * FROM (VALUES
    ('1', 'UNION 4 FILES',       'crimes_unioned',   'All 4 crime CSVs are separate due to size — must be unified into one table for analysis'),
    ('2', 'DROP COLUMNS',        'year, updated_on', 'year is derivable from date (redundant); updated_on is an admin audit field with no analytical value'),
    ('3', 'DROP COLUMNS',        'x_coordinate, y_coordinate', 'State Plane NAD83 projection — redundant given lat/lon in WGS84. Inconsistently populated.'),
    ('4', 'TYPE CAST: id',       'STRING → BIGINT',  'id is a numeric identifier stored as string in Bronze — must be INT for dedup and joins'),
    ('5', 'TYPE CAST: date',     'STRING → TIMESTAMP','date is stored as M/d/yyyy H:mm string — must be TIMESTAMP to extract year/month/hour for analysis'),
    ('6', 'TYPE CAST: arrest',   'STRING → BOOLEAN', 'arrest stored as TRUE/FALSE string — cast to BOOLEAN for numeric aggregation'),
    ('7', 'TYPE CAST: domestic', 'STRING → BOOLEAN', 'domestic stored as TRUE/FALSE string — cast to BOOLEAN for numeric aggregation'),
    ('8', 'TYPE CAST: ward',     'STRING → INT',     'ward is a numeric code stored as string — must be INT to join to dim_wards'),
    ('9', 'TYPE CAST: community_area','STRING → INT', 'community_area is numeric stored as string — must be INT to join dim_community_areas'),
    ('10','STANDARDIZE: primary_type','UPPER+TRIM',   'Mixed casing observed (e.g. Theft vs THEFT) — UPPER+TRIM enforces consistent classification'),
    ('11','STANDARDIZE: district','LPAD 3 chars',     'district values like 4 and 004 must be standardized for joins'),
    ('12','NULL HANDLING: lat/lon','COALESCE from location', 'Many rows have NULL lat/lon but a valid location string — parse string to recover coordinates'),
    ('13','NULL HANDLING: coords','Keep NULLs as-is','Records with no lat/lon AND no location string — kept but flagged, not dropped'),
    ('14','DEDUPLICATION',       'ROW_NUMBER on crime_id', 'Same crime_id can appear in multiple year-range files — keep one row, flag rest as rejects'),
    ('15','QUALITY GATE: date',  'NULL incident_datetime', 'Records where date cannot be parsed — rejected with reason code NULL_DATE'),
    ('16','QUALITY GATE: coords','Outside Chicago bbox',   'lat < 41.6 or > 42.1, lon < -88.0 or > -87.5 — rejected as INVALID_COORDS'),
    ('17','QUALITY GATE: year',  'year < 2001 or > 2017',  'Dataset scope is 2001-2017 — out-of-range years rejected as INVALID_YEAR'),
    ('18','DIM JOIN',            'LEFT JOIN on 3 dims',    'Join district/ward/community_area codes to their named dimension tables for Gold readability')
) AS t(step_num, rule_type, columns_affected, justification)
ORDER BY step_num;

WITH all_crimes AS (
    SELECT district, ward, community_area FROM bronze.crimes_2001_2004
    UNION ALL SELECT district, ward, community_area FROM bronze.crimes_2005_2007
    UNION ALL SELECT district, ward, community_area FROM bronze.crimes_2008_2011
    UNION ALL SELECT district, ward, community_area FROM bronze.crimes_2012_2017
)
SELECT DISTINCT ac.district
     FROM all_crimes ac
     WHERE ac.district IS NOT NULL
       AND TRIM(ac.community_area) != '';
       
SELECT DISTINCT dist_num
FROM bronze.dim_districts_raw ;

SELECT DISTINCT area_num_1, area_numbe, community
FROM bronze.dim_community_areas_raw;
