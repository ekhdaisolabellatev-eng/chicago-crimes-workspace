-- ════════════════════════════════════════════════════════════
-- STEP 8: ML FEATURE TABLES
-- ════════════════════════════════════════════════════════════
USE CATALOG chicago_crimes_workspace;
-- ── ML Table 1: Community-level features (K-Means input) ────
-- One row per community area — 77 rows
-- Ready to import into Python/sklearn for clustering
CREATE OR REPLACE TABLE gold.ml_community_features
USING DELTA
COMMENT 'ML input — community-level features for K-Means clustering. 77 rows.'
AS
SELECT
    cp.community_area_num,
    cp.community_name,
    -- Raw features for clustering
    cp.total_crimes,
    cp.arrest_rate_pct,
    cp.domestic_pct,
    cp.violent_pct,
    cp.peak_crime_hour,
    cp.yoy_pct_2017_vs_2016                       AS yoy_change_pct,
    -- Derived normalised features
    ROUND(cp.total_crimes / 17.0, 0)              AS avg_crimes_per_year,
    ROUND(cp.violent_crimes * 100.0 /
          NULLIF(cp.total_crimes, 0), 2)          AS violent_share_pct,
    ROUND(cp.property_crimes * 100.0 /
          NULLIF(cp.total_crimes, 0), 2)          AS property_share_pct,
    -- Binary risk flags (for report commentary)
    CASE WHEN cp.total_crimes >
        (SELECT PERCENTILE_CONT(0.5)
             WITHIN GROUP (ORDER BY total_crimes)
         FROM gold.v_geo_community_profile)
    THEN TRUE ELSE FALSE END                      AS is_high_volume_area,
    CASE WHEN cp.arrest_rate_pct <
        (SELECT PERCENTILE_CONT(0.5)
             WITHIN GROUP (ORDER BY arrest_rate_pct)
         FROM gold.v_geo_community_profile)
    THEN TRUE ELSE FALSE END                      AS is_low_arrest_area,
    CASE WHEN cp.violent_pct >
        (SELECT PERCENTILE_CONT(0.75)
             WITHIN GROUP (ORDER BY violent_pct)
         FROM gold.v_geo_community_profile)
    THEN TRUE ELSE FALSE END                      AS is_high_violent_area
FROM gold.v_geo_community_profile cp
WHERE cp.community_area_num IS NOT NULL;

-- ── ML Table 2: District-level features ─────────────────────
CREATE OR REPLACE TABLE gold.ml_district_features
USING DELTA
COMMENT 'ML input — district-level features. One row per district per year.'
AS
SELECT
    district_code,
    district_name,
    incident_year,
    total_crimes,
    arrest_rate_pct,
    violent_crime_pct,
    crime_volume_rank,
    arrest_rate_rank,
    -- Effectiveness score: higher arrest rate + lower crime volume = better
    ROUND(
        arrest_rate_pct -
        (crime_volume_rank * 1.0 / MAX(crime_volume_rank) OVER (PARTITION BY incident_year) * 100),
        2
    )                                             AS effectiveness_score
FROM gold.v_geo_district_performance
WHERE district_name IS NOT NULL;

-- ════════════════════════════════════════════════════════════
-- STEP 9: LOG ALL GOLD PUBLICATIONS
-- ════════════════════════════════════════════════════════════

INSERT INTO meta.gold_publication_log
    (run_id, step_name, source_table, target_table, rows_in, rows_out, rows_dropped, notes)
SELECT
    (SELECT MAX(run_id) FROM meta.etl_run_log WHERE pipeline_name='gold_build'),
    'GOLD_STEP9_FINAL_PUBLICATION',
    'N/A', -- Source varies by view/table, marked as N/A for this aggregate step
    obj,
    rc,    -- Using final row count for rows_in
    rc,    -- Using final row count for rows_out
    0,     -- No rows dropped during a direct publication check
    CONCAT('Object Type: ', typ, ' - Published and verified successfully.')
FROM (
    SELECT 'gold.dim_date'                    AS obj, 'TABLE' AS typ, COUNT(*) AS rc FROM gold.dim_date
    UNION ALL SELECT 'gold.dim_crime_type',   'TABLE', COUNT(*) FROM gold.dim_crime_type
    UNION ALL SELECT 'gold.dim_location',     'TABLE', COUNT(*) FROM gold.dim_location
    UNION ALL SELECT 'gold.dim_shift',        'TABLE', COUNT(*) FROM gold.dim_shift
    UNION ALL SELECT 'gold.dim_location_type','TABLE', COUNT(*) FROM gold.dim_location_type
    UNION ALL SELECT 'gold.fact_crimes',      'TABLE', COUNT(*) FROM gold.fact_crimes
    UNION ALL SELECT 'gold.v_temporal_monthly_trend',    'VIEW', COUNT(*) FROM gold.v_temporal_monthly_trend
    UNION ALL SELECT 'gold.v_temporal_yoy_change',       'VIEW', COUNT(*) FROM gold.v_temporal_yoy_change
    UNION ALL SELECT 'gold.v_temporal_shift_analysis',   'VIEW', COUNT(*) FROM gold.v_temporal_shift_analysis
    UNION ALL SELECT 'gold.v_temporal_weekday_pattern',  'VIEW', COUNT(*) FROM gold.v_temporal_weekday_pattern
    UNION ALL SELECT 'gold.v_geo_community_profile',     'VIEW', COUNT(*) FROM gold.v_geo_community_profile
    UNION ALL SELECT 'gold.v_geo_district_performance',  'VIEW', COUNT(*) FROM gold.v_geo_district_performance
    UNION ALL SELECT 'gold.v_crime_type_breakdown',      'VIEW', COUNT(*) FROM gold.v_crime_type_breakdown
    UNION ALL SELECT 'gold.v_crime_type_by_area',        'VIEW', COUNT(*) FROM gold.v_crime_type_by_area
    UNION ALL SELECT 'gold.v_arrest_effectiveness',      'VIEW', COUNT(*) FROM gold.v_arrest_effectiveness
    UNION ALL SELECT 'gold.v_domestic_crime_analysis',   'VIEW', COUNT(*) FROM gold.v_domestic_crime_analysis
    UNION ALL SELECT 'gold.v_city_kpi_summary',          'VIEW', COUNT(*) FROM gold.v_city_kpi_summary
    UNION ALL SELECT 'gold.ml_community_features',       'TABLE', COUNT(*) FROM gold.ml_community_features
    UNION ALL SELECT 'gold.ml_district_features',        'TABLE', COUNT(*) FROM gold.ml_district_features
);

-- ── Close ETL run ─────────────────────────────────────────────
UPDATE meta.etl_run_log
SET
    end_ts         = current_timestamp(),
    status         = 'SUCCESS',
    rows_processed = (SELECT COUNT(*) FROM gold.fact_crimes)
WHERE run_id = (SELECT MAX(run_id) FROM meta.etl_run_log
                WHERE pipeline_name = 'gold_build');

-- ════════════════════════════════════════════════════════════
-- FINAL VERIFICATION
-- ════════════════════════════════════════════════════════════

SELECT '=== GOLD TABLE ROW COUNTS ===' AS section;
SELECT 'dim_date'           AS gold_object, COUNT(*) AS rows FROM gold.dim_date
UNION ALL SELECT 'dim_crime_type',    COUNT(*) FROM gold.dim_crime_type
UNION ALL SELECT 'dim_location',      COUNT(*) FROM gold.dim_location
UNION ALL SELECT 'dim_shift',         COUNT(*) FROM gold.dim_shift
UNION ALL SELECT 'dim_location_type', COUNT(*) FROM gold.dim_location_type
UNION ALL SELECT 'fact_crimes',       COUNT(*) FROM gold.fact_crimes
UNION ALL SELECT 'ml_community_features', COUNT(*) FROM gold.ml_community_features
UNION ALL SELECT 'ml_district_features',  COUNT(*) FROM gold.ml_district_features;

SELECT '=== CITY KPI DASHBOARD ===' AS section;
SELECT * FROM gold.v_city_kpi_summary;

SELECT '=== TOP 10 COMMUNITIES BY CRIME ===' AS section;
SELECT community_name, total_crimes, arrest_rate_pct,
       violent_pct, domestic_pct, dominant_crime_type
FROM gold.v_geo_community_profile
LIMIT 10;

SELECT '=== SHIFT ANALYSIS ===' AS section;
SELECT shift_name, shift_hours,
       SUM(total_crimes)    AS total_crimes,
       SUM(total_arrests)   AS total_arrests,
       ROUND(AVG(arrest_rate_pct), 2) AS avg_arrest_rate_pct
FROM gold.v_temporal_shift_analysis
GROUP BY shift_name, shift_hours
ORDER BY total_crimes DESC;

SELECT '=== CRIME TYPE BREAKDOWN (all years) ===' AS section;
SELECT primary_type, crime_category,
       SUM(total_crimes) AS total, ROUND(AVG(arrest_rate_pct),2) AS avg_arrest_rate
FROM gold.v_crime_type_breakdown
GROUP BY primary_type, crime_category
ORDER BY total DESC
LIMIT 15;

SELECT '=== ML COMMUNITY FEATURES PREVIEW ===' AS section;
SELECT community_name, total_crimes, arrest_rate_pct,
       violent_pct, domestic_pct, is_high_volume_area, is_low_arrest_area
FROM gold.ml_community_features
ORDER BY total_crimes DESC
LIMIT 10;
