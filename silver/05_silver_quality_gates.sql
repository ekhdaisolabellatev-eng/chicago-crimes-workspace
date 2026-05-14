USE CATALOG chicago_crimes_workspace;
-- ════════════════════════════════════════════════════════════
-- STEP 5: QUALITY GATES → meta.quality_failures
--
-- Gates defined based on ACTUAL profiling findings:
--
-- QG-01: NULL_CRIME_ID       — should be 0 after header removal
-- QG-02: NULL_DATE           — ~48,575 rows per profiling
-- QG-03: UNPARSEABLE_DATE    — rows where both format attempts fail
-- QG-04: FUTURE_DATE         — date parsed but > today
-- QG-05: INVALID_YEAR        — year outside 2001–2017
-- QG-06: NULL_PRIMARY_TYPE   — ~48,575 per profiling
-- QG-07: INVALID_PRIMARY_TYPE— "IUCR" header value (post step2 safety)
-- QG-08: NULL_ARREST_FLAG    — is_arrest still NULL after cast
-- QG-09: NULL_COMMUNITY_AREA — ~749,767 per profiling
-- QG-10: COMMUNITY_AREA_ZERO — community_area = 0 (no dim match)
-- QG-11: NULL_DISTRICT       — ~48,624 per profiling
-- QG-12: UNMATCHED_DISTRICT  — district 13, 21 (no dim match)
-- QG-13: NULL_WARD           — ~748,750 per profiling
-- QG-14: NO_COORDINATES      — lat_final AND lon_final both NULL
-- ════════════════════════════════════════════════════════════

-- QG-01: NULL crime_id after header removal (sanity check)
INSERT INTO meta.quality_failures
    (run_id, source_table, target_table,
     record_id, reason_code, reason_desc, raw_value)
SELECT
    (SELECT MAX(run_id) FROM meta.etl_run_log
     WHERE pipeline_name = 'silver_transform'),
    'silver.crimes_typed',
    'silver.crimes_deduped',
    COALESCE(case_number, 'UNKNOWN'),
    'NULL_CRIME_ID',
    'crime_id is NULL after TRY_CAST — row survived header filter but id is non-numeric',
    NULL
FROM silver.crimes_typed
WHERE crime_id IS NULL;

-- QG-02: NULL incident_datetime — date field was NULL in source
INSERT INTO meta.quality_failures
    (run_id, source_table, target_table,
     record_id, reason_code, reason_desc, raw_value)
SELECT
    (SELECT MAX(run_id) FROM meta.etl_run_log
     WHERE pipeline_name = 'silver_transform'),
    'silver.crimes_typed',
    'silver.crimes_deduped',
    CAST(crime_id AS STRING),
    'NULL_DATE',
    'date field was NULL or empty in source — incident_datetime cannot be determined',
    NULL
FROM silver.crimes_typed
WHERE crime_id IS NOT NULL
  AND incident_datetime IS NULL
  -- Distinguish from unparseable (this gate: date itself was null)
  AND NOT EXISTS (
      SELECT 1 FROM silver.crimes_header_removed hr
      WHERE TRY_CAST(TRIM(hr.id) AS BIGINT) = crimes_typed.crime_id
        AND (hr.date IS NOT NULL AND TRIM(hr.date) != '')
  );

-- QG-03: UNPARSEABLE_DATE — date was present but neither format worked
-- Profiling: format1_fail = 1,000,000 rows — some recovered by format B
-- This gate catches the ones BOTH formats failed on
INSERT INTO meta.quality_failures
    (run_id, source_table, target_table,
     record_id, reason_code, reason_desc, raw_value)
SELECT
    (SELECT MAX(run_id) FROM meta.etl_run_log
     WHERE pipeline_name = 'silver_transform'),
    'silver.crimes_typed',
    'silver.crimes_deduped',
    CAST(crime_id AS STRING),
    'UNPARSEABLE_DATE',
    'date field was present but could not be parsed by either known format (MM/dd/yyyy hh:mm:ss a OR M/d/yyyy H:mm)',
    -- Retrieve original raw date value from header_removed table
    (SELECT hr.date FROM silver.crimes_header_removed hr
     WHERE TRY_CAST(TRIM(hr.id) AS BIGINT) = ct.crime_id LIMIT 1)
FROM silver.crimes_typed ct
WHERE crime_id IS NOT NULL
  AND incident_datetime IS NULL
  AND EXISTS (
      SELECT 1 FROM silver.crimes_header_removed hr
      WHERE TRY_CAST(TRIM(hr.id) AS BIGINT) = ct.crime_id
        AND hr.date IS NOT NULL AND TRIM(hr.date) != ''
  );

-- QG-04: FUTURE_DATE — incident_datetime parsed but is after today
INSERT INTO meta.quality_failures
    (run_id, source_table, target_table,
     record_id, reason_code, reason_desc, raw_value)
SELECT
    (SELECT MAX(run_id) FROM meta.etl_run_log
     WHERE pipeline_name = 'silver_transform'),
    'silver.crimes_typed',
    'silver.crimes_deduped',
    CAST(crime_id AS STRING),
    'FUTURE_DATE',
    'incident_datetime is after current date — likely a data entry error',
    CAST(incident_datetime AS STRING)
FROM silver.crimes_typed
WHERE crime_id IS NOT NULL
  AND incident_datetime > current_timestamp();

-- QG-05: INVALID_YEAR — outside the dataset scope of 2001–2017
-- Profiling showed "Y Coordinate" and "41.789..." as year values
-- (header rows). After step 2 those are gone but out-of-range
-- real years may still exist.
INSERT INTO meta.quality_failures
    (run_id, source_table, target_table,
     record_id, reason_code, reason_desc, raw_value)
SELECT
    (SELECT MAX(run_id) FROM meta.etl_run_log
     WHERE pipeline_name = 'silver_transform'),
    'silver.crimes_typed',
    'silver.crimes_deduped',
    CAST(crime_id AS STRING),
    'INVALID_YEAR',
    CONCAT('incident_year=', CAST(incident_year AS STRING),
           ' is outside expected dataset range 2001–2017'),
    CAST(incident_year AS STRING)
FROM silver.crimes_typed
WHERE crime_id IS NOT NULL
  AND incident_datetime IS NOT NULL
  AND (incident_year < 2001 OR incident_year > 2017);

-- QG-06: NULL_PRIMARY_TYPE
-- Profiling: null_primary_type = 48,575 (the header rows)
-- After step 2 cleanup, remaining NULLs are genuine data gaps
INSERT INTO meta.quality_failures
    (run_id, source_table, target_table,
     record_id, reason_code, reason_desc, raw_value)
SELECT
    (SELECT MAX(run_id) FROM meta.etl_run_log
     WHERE pipeline_name = 'silver_transform'),
    'silver.crimes_typed',
    'silver.crimes_deduped',
    CAST(crime_id AS STRING),
    'NULL_PRIMARY_TYPE',
    'primary_type is NULL or empty — crime cannot be classified',
    NULL
FROM silver.crimes_typed
WHERE crime_id IS NOT NULL
  AND (primary_type IS NULL OR TRIM(primary_type) = '');

-- QG-07: INVALID_PRIMARY_TYPE
-- Catch any surviving header values like "IUCR" in primary_type
-- (profiling found 1 row with primary_type = "IUCR")
INSERT INTO meta.quality_failures
    (run_id, source_table, target_table,
     record_id, reason_code, reason_desc, raw_value)
SELECT
    (SELECT MAX(run_id) FROM meta.etl_run_log
     WHERE pipeline_name = 'silver_transform'),
    'silver.crimes_typed',
    'silver.crimes_deduped',
    CAST(crime_id AS STRING),
    'INVALID_PRIMARY_TYPE',
    CONCAT('primary_type="', primary_type,
           '" is a column header name injected as data — not a valid crime type'),
    primary_type
FROM silver.crimes_typed
WHERE crime_id IS NOT NULL
  AND primary_type IN ('IUCR', 'PRIMARY TYPE', 'DESCRIPTION',
                       'LOCATION DESCRIPTION', 'FBI CODE');

-- QG-08: NULL_ARREST_FLAG
-- Should only happen if arrest value was something other than
-- TRUE/FALSE after header removal
INSERT INTO meta.quality_failures
    (run_id, source_table, target_table,
     record_id, reason_code, reason_desc, raw_value)
SELECT
    (SELECT MAX(run_id) FROM meta.etl_run_log
     WHERE pipeline_name = 'silver_transform'),
    'silver.crimes_typed',
    'silver.crimes_deduped',
    CAST(crime_id AS STRING),
    'NULL_ARREST_FLAG',
    'is_arrest is NULL — arrest field value was not TRUE or FALSE',
    NULL
FROM silver.crimes_typed
WHERE crime_id IS NOT NULL
  AND is_arrest IS NULL;

-- QG-09: NULL_COMMUNITY_AREA
-- Profiling: null_community_area = 749,767 (large — mostly 2001_2004 file)
-- These rows cannot join to dim_community_areas — flagged
INSERT INTO meta.quality_failures
    (run_id, source_table, target_table,
     record_id, reason_code, reason_desc, raw_value)
SELECT
    (SELECT MAX(run_id) FROM meta.etl_run_log
     WHERE pipeline_name = 'silver_transform'),
    'silver.crimes_typed',
    'silver.crimes_deduped',
    CAST(crime_id AS STRING),
    'NULL_COMMUNITY_AREA',
    'community_area is NULL — cannot join to dim_community_areas for geographic enrichment',
    NULL
FROM silver.crimes_typed
WHERE crime_id IS NOT NULL
  AND community_area IS NULL;

-- QG-10: COMMUNITY_AREA_ZERO
-- Profiling: community_area = 0 exists in crimes data
-- but dim_community_areas has no area_num = 0 (range is 1–77)
-- Kept in dataset but flagged — the LEFT JOIN will produce NULL name
INSERT INTO meta.quality_failures
    (run_id, source_table, target_table,
     record_id, reason_code, reason_desc, raw_value)
SELECT
    (SELECT MAX(run_id) FROM meta.etl_run_log
     WHERE pipeline_name = 'silver_transform'),
    'silver.crimes_typed',
    'silver.crimes_deduped',
    CAST(crime_id AS STRING),
    'COMMUNITY_AREA_ZERO',
    'community_area = 0 has no matching entry in dim_community_areas (valid range 1–77). community_name will be NULL after join.',
    '0'
FROM silver.crimes_typed
WHERE crime_id IS NOT NULL
  AND community_area = 0;

-- QG-11: NULL_DISTRICT
-- Profiling: null_district = 48,624
INSERT INTO meta.quality_failures
    (run_id, source_table, target_table,
     record_id, reason_code, reason_desc, raw_value)
SELECT
    (SELECT MAX(run_id) FROM meta.etl_run_log
     WHERE pipeline_name = 'silver_transform'),
    'silver.crimes_typed',
    'silver.crimes_deduped',
    CAST(crime_id AS STRING),
    'NULL_DISTRICT',
    'district is NULL — cannot join to dim_districts',
    NULL
FROM silver.crimes_typed
WHERE crime_id IS NOT NULL
  AND district IS NULL;

-- QG-12: UNMATCHED_DISTRICT
-- Profiling: crimes data lacks district 13 and 21
-- dim_districts also lacks 13 and 21 — consistent absence
-- Flag for documentation but row is NOT rejected
INSERT INTO meta.quality_failures
    (run_id, source_table, target_table,
     record_id, reason_code, reason_desc, raw_value)
SELECT
    (SELECT MAX(run_id) FROM meta.etl_run_log
     WHERE pipeline_name = 'silver_transform'),
    'silver.crimes_typed',
    'silver.crimes_deduped',
    CAST(crime_id AS STRING),
    'UNMATCHED_DISTRICT',
    CONCAT('district="', district,
           '" has no matching entry in dim_districts. district_name will be NULL after join.'),
    district
FROM silver.crimes_typed
WHERE crime_id IS NOT NULL
  AND district IS NOT NULL
  AND NOT EXISTS (
      SELECT 1 FROM silver.dim_districts d
      WHERE d.district_num = TRY_CAST(district AS INT)
  );

-- QG-13: NULL_WARD
-- Profiling: null_ward = 748,750 (large — mostly 2001_2004 file)
INSERT INTO meta.quality_failures
    (run_id, source_table, target_table,
     record_id, reason_code, reason_desc, raw_value)
SELECT
    (SELECT MAX(run_id) FROM meta.etl_run_log
     WHERE pipeline_name = 'silver_transform'),
    'silver.crimes_typed',
    'silver.crimes_deduped',
    CAST(crime_id AS STRING),
    'NULL_WARD',
    'ward is NULL — cannot join to dim_wards for geographic enrichment',
    NULL
FROM silver.crimes_typed
WHERE crime_id IS NOT NULL
  AND ward IS NULL;

-- QG-14: NO_COORDINATES
-- Records where BOTH direct and recovered coordinates are NULL
-- These rows have absolutely no spatial information
INSERT INTO meta.quality_failures
    (run_id, source_table, target_table,
     record_id, reason_code, reason_desc, raw_value)
SELECT
    (SELECT MAX(run_id) FROM meta.etl_run_log
     WHERE pipeline_name = 'silver_transform'),
    'silver.crimes_typed',
    'silver.crimes_deduped',
    CAST(crime_id AS STRING),
    'NO_COORDINATES',
    'Both lat_final and lon_final are NULL — no spatial data available from either direct or recovered source',
    NULL
FROM silver.crimes_typed
WHERE crime_id IS NOT NULL
  AND lat_final IS NULL
  AND lon_final IS NULL;

-- ── Quality gate summary ───────────
SELECT '=== QUALITY GATE SUMMARY ===' AS section;
SELECT
    reason_code,
    COUNT(*)                                            AS failed_records,
    ROUND(COUNT(*) * 100.0 /
        (SELECT COUNT(*) FROM silver.crimes_typed), 4) AS pct_of_total
FROM meta.quality_failures
WHERE source_table = 'silver.crimes_typed'
GROUP BY reason_code
ORDER BY failed_records DESC;

-----------------------------------------------------------------------------------
-- Log Step 5
INSERT INTO meta.silver_transform_log
    (run_id, step_name, source_table, target_table,
     rows_in, rows_out, rows_dropped, notes)
SELECT
    (SELECT MAX(run_id) FROM meta.etl_run_log
     WHERE pipeline_name = 'silver_transform'),
    'STEP5_QUALITY_GATES',
    'silver.crimes_typed',
    'meta.quality_failures',
    (SELECT COUNT(*) FROM silver.crimes_typed),
    (SELECT COUNT(*) FROM meta.quality_failures
     WHERE source_table = 'silver.crimes_typed'),
    0,
    '14 quality gates applied based on Bronze profiling. Records flagged (not deleted). See meta.quality_failures for details.';
