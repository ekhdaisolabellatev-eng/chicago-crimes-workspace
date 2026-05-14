-- ════════════════════════════════════════════════════════════
-- STEP 6: DEDUPLICATION → crimes_deduped
--
-- TWO LEVELS of duplication found in profiling:
--
-- WITHIN-FILE:
--   2001_2004: 89,260 duplicates
--   2005_2007: 533,718 duplicates  ← largest problem
--   2008_2011: 48,575 duplicates
--   2012_2017: 0 duplicates
--
-- CROSS-FILE:
--   1 crime_id appeared in 2 different files
--
-- STRATEGY:
--   ROW_NUMBER() OVER (PARTITION BY crime_id ORDER BY _ingest_ts DESC)
--   Keep rn = 1 (most recently ingested version)
--
-- EXCLUSIONS from deduped table:
--   Exclude rows where crime_id IS NULL (QG-01 — already logged)
--   Exclude rows where incident_datetime IS NULL (QG-02/03)
--   These are logged as quality failures, not carried forward
-- ════════════════════════════════════════════════════════════
USE CATALOG chicago_crimes_workspace;

INSERT INTO silver.crimes_deduped
SELECT
    crime_id,
    case_number,
    incident_datetime,
    incident_year,
    incident_month,
    incident_day,
    incident_hour,
    incident_dayofweek,
    block,
    iucr,
    primary_type,
    crime_description,
    location_description,
    fbi_code,
    is_arrest,
    is_domestic,
    beat,
    district,
    ward,
    community_area,
    latitude,
    longitude,
    lat_final,
    lon_final,
    coord_source,
    source_year_range,
    _source_file,
    'STEP6_DEDUPED'       AS _silver_step,
    current_timestamp()   AS _transform_ts
FROM (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY crime_id
            -- For cross-file dups: keep most recently ingested row
            -- For within-file dups: same effect
            ORDER BY _transform_ts DESC, source_year_range DESC
        ) AS _rn
    FROM silver.crimes_typed
    WHERE
        -- Exclude records flagged by critical quality gates
        crime_id IS NOT NULL            -- QG-01
        AND incident_datetime IS NOT NULL  -- QG-02 and QG-03
) ranked
WHERE _rn = 1;   -- keep exactly one row per crime_id

-- Log Step 6
INSERT INTO meta.silver_transform_log
    (run_id, step_name, source_table, target_table,
     rows_in, rows_out, rows_dropped, notes)
SELECT
    (SELECT MAX(run_id) FROM meta.etl_run_log
     WHERE pipeline_name = 'silver_transform'),
    'STEP6_DEDUPLICATION',
    'silver.crimes_typed',
    'silver.crimes_deduped',
    (SELECT COUNT(*) FROM silver.crimes_typed),
    (SELECT COUNT(*) FROM silver.crimes_deduped),
    (SELECT COUNT(*) FROM silver.crimes_typed) -
    (SELECT COUNT(*) FROM silver.crimes_deduped),
    'ROW_NUMBER PARTITION BY crime_id. Removed within-file dups (2001_2004=89K, 2005_2007=533K, 2008_2011=48K) and 1 cross-file dup. Excluded NULL crime_id and NULL datetime rows.';

-- ════════════════════════════════════════════════════════════
-- STEP 7: DIMENSION JOINS → crimes_conformed (MAIN SILVER TABLE)
--
-- JOIN LOGIC:
--   LEFT JOIN on all 3 dims — preserves ALL valid crime records
--   even where no dim match exists (no INNER JOIN data loss)
--
-- JOIN KEYS:
--   crimes.district (STRING "004") → dim_districts.district_num (INT 4)
--   crimes.ward     (INT 1)        → dim_wards.ward_num          (INT 1)
--   crimes.community_area (INT 25) → dim_community_areas.area_num (INT 25)
--
-- KNOWN MISMATCHES (documented in quality gates):
--   • district 13 and 21 → district_name will be NULL
--   • community_area = 0 → community_name will be NULL
--   • ~749K rows with NULL community_area → community_name NULL
--   • ~748K rows with NULL ward → ward_label NULL
-- ════════════════════════════════════════════════════════════

INSERT INTO silver.crimes_conformed
SELECT
    c.crime_id,
    c.case_number,
    c.incident_datetime,
    c.incident_year,
    c.incident_month,
    c.incident_day,
    c.incident_hour,
    c.incident_dayofweek,
    c.block,
    c.iucr,
    c.primary_type,
    c.crime_description,
    c.location_description,
    c.fbi_code,
    c.is_arrest,
    c.is_domestic,
    c.beat,
    -- Raw geographic codes (kept for filtering)
    c.district                           AS district_code,
    c.ward                               AS ward_num,
    c.community_area                     AS community_area_num,
    -- Enriched names from dimension joins
    -- district: join INT
    d.district_label                     AS district_name,
    -- ward: direct INT to INT join
    w.ward_label                         AS ward_label,
    -- community: direct INT to INT join
    ca.community_name                    AS community_name,
    -- Coordinates
    c.lat_final,
    c.lon_final,
    c.coord_source,
    -- Lineage
    c.source_year_range,
    c._source_file,
    'STEP7_CONFORMED'                    AS _silver_step,
    current_timestamp()                  AS _transform_ts

FROM silver.crimes_deduped c

-- Join district: crimes.district is "004" → cast to INT = 4
LEFT JOIN silver.dim_districts d
    ON TRY_CAST(c.district AS INT) = d.district_num

-- Join ward: both are INT after Silver typing
LEFT JOIN silver.dim_wards w
    ON c.ward = w.ward_num

-- Join community area: both are INT after Silver typing
LEFT JOIN silver.dim_community_areas ca
    ON c.community_area = ca.area_num;

-- Log Step 7
INSERT INTO meta.silver_transform_log
    (run_id, step_name, source_table, target_table,
     rows_in, rows_out, rows_dropped, notes)
SELECT
    (SELECT MAX(run_id) FROM meta.etl_run_log
     WHERE pipeline_name = 'silver_transform'),
    'STEP7_DIMENSION_JOIN',
    'silver.crimes_deduped + silver.dim_districts + silver.dim_wards + silver.dim_community_areas',
    'silver.crimes_conformed',
    (SELECT COUNT(*) FROM silver.crimes_deduped),
    (SELECT COUNT(*) FROM silver.crimes_conformed),
    0,
    'LEFT JOIN on district/ward/community_area. No row loss. NULL district_name where district=13 or 21. NULL community_name where area=0 or NULL.';

-- ── Close ETL run ─────────────────────────────────────────────
UPDATE meta.etl_run_log
SET
    end_ts         = current_timestamp(),
    status         = 'SUCCESS',
    rows_processed = (SELECT COUNT(*) FROM silver.crimes_conformed),
    rows_rejected  = (SELECT COUNT(*) FROM meta.quality_failures
                      WHERE source_table = 'silver.crimes_typed')
WHERE run_id = (SELECT MAX(run_id) FROM meta.etl_run_log
                WHERE pipeline_name = 'silver_transform');

-- ════════════════════════════════════════════════════════════
-- FINAL VERIFICATION QUERIES
-- Run these after transformation to confirm results
-- ════════════════════════════════════════════════════════════

SELECT '=== SILVER PIPELINE SUMMARY ===' AS section;

SELECT step_name, rows_in, rows_out, rows_dropped, notes
FROM meta.silver_transform_log
ORDER BY transform_id;

SELECT '=== QUALITY FAILURES BY REASON CODE ===' AS section;

SELECT
    reason_code,
    COUNT(*)  AS record_count,
    ROUND(COUNT(*) * 100.0 /
        (SELECT COUNT(*) FROM silver.crimes_typed), 4) AS pct_of_typed
FROM meta.quality_failures
WHERE source_table = 'silver.crimes_typed'
GROUP BY reason_code
ORDER BY record_count DESC;

SELECT '=== SILVER TABLE ROW COUNTS ===' AS section;

SELECT 'crimes_unioned'        AS table_name, COUNT(*) AS rows FROM silver.crimes_unioned
UNION ALL SELECT 'crimes_header_removed', COUNT(*) FROM silver.crimes_header_removed
UNION ALL SELECT 'crimes_typed',          COUNT(*) FROM silver.crimes_typed
UNION ALL SELECT 'crimes_deduped',        COUNT(*) FROM silver.crimes_deduped
UNION ALL SELECT 'crimes_conformed',      COUNT(*) FROM silver.crimes_conformed
UNION ALL SELECT 'dim_wards',             COUNT(*) FROM silver.dim_wards
UNION ALL SELECT 'dim_districts',         COUNT(*) FROM silver.dim_districts
UNION ALL SELECT 'dim_community_areas',   COUNT(*) FROM silver.dim_community_areas;

SELECT '=== COORDINATE RECOVERY SUMMARY ===' AS section;

SELECT
    coord_source,
    COUNT(*) AS record_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct_of_total
FROM silver.crimes_conformed
GROUP BY coord_source
ORDER BY record_count DESC;

SELECT '=== CRIMES WITH NULL DISTRICT_NAME (dim mismatch) ===' AS section;

SELECT district_code, COUNT(*) AS crime_count
FROM silver.crimes_conformed
WHERE district_name IS NULL AND district_code IS NOT NULL
GROUP BY district_code
ORDER BY crime_count DESC;

SELECT '=== CONFORMED TABLE PREVIEW ===' AS section;

SELECT
    crime_id, case_number, incident_datetime, incident_year,
    incident_hour, primary_type, is_arrest, is_domestic,
    district_code, district_name,
    ward_num, ward_label,
    community_area_num, community_name,
    lat_final, lon_final, coord_source,
    source_year_range
FROM silver.crimes_conformed
LIMIT 10;
