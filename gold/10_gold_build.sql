-- ============================================================
-- CHICAGO CRIME DATA LAKE — DATABRICKS SQL
-- Script: 10_gold_build.sql
-- Zone:   GOLD
-- Purpose: Populate all Gold dimension tables, fact table,
--          and create all 10 analytical views + 2 ML tables.
--
-- EXECUTION ORDER:
--   STEP 1  — Populate dim_date
--   STEP 2  — Populate dim_crime_type
--   STEP 3  — Populate dim_location
--   STEP 4  — Populate dim_shift
--   STEP 5  — Populate dim_location_type
--   STEP 6  — Populate fact_crimes
--   STEP 7  — Create 10 analytical views
--   STEP 8  — Create 2 ML feature tables
--   STEP 9  — Log to meta.gold_publication_log
-- ============================================================

USE CATALOG chicago_crimes_workspace;

-- ── Open ETL run ─────────────────────────────────────────────
INSERT INTO meta.etl_run_log
    (pipeline_name, zone, script_name, start_ts, status)
VALUES
    ('gold_build', 'GOLD', '06_gold_build.sql',
     current_timestamp(), 'RUNNING');

-- ════════════════════════════════════════════════════════════
-- STEP 1: POPULATE dim_date
-- ════════════════════════════════════════════════════════════

INSERT INTO gold.dim_date
SELECT DISTINCT
    CAST(DATE_FORMAT(CAST(incident_datetime AS DATE), 'yyyyMMdd') AS INT) AS date_key,
    CAST(incident_datetime AS DATE)              AS full_date,
    incident_year                                AS year,
    incident_month                               AS month,
    CASE incident_month
        WHEN 1  THEN 'January'   WHEN 2  THEN 'February'
        WHEN 3  THEN 'March'     WHEN 4  THEN 'April'
        WHEN 5  THEN 'May'       WHEN 6  THEN 'June'
        WHEN 7  THEN 'July'      WHEN 8  THEN 'August'
        WHEN 9  THEN 'September' WHEN 10 THEN 'October'
        WHEN 11 THEN 'November'  WHEN 12 THEN 'December'
    END                                          AS month_name,
    CASE
        WHEN incident_month IN (1,2,3)    THEN 'Q1'
        WHEN incident_month IN (4,5,6)    THEN 'Q2'
        WHEN incident_month IN (7,8,9)    THEN 'Q3'
        WHEN incident_month IN (10,11,12) THEN 'Q4'
    END                                          AS quarter,
    incident_day                                 AS day_of_month,
    incident_dayofweek                           AS day_of_week_num,
    CASE incident_dayofweek
        WHEN 1 THEN 'Sunday'   WHEN 2 THEN 'Monday'
        WHEN 3 THEN 'Tuesday'  WHEN 4 THEN 'Wednesday'
        WHEN 5 THEN 'Thursday' WHEN 6 THEN 'Friday'
        WHEN 7 THEN 'Saturday'
    END                                          AS day_name,
    CASE
        WHEN incident_dayofweek IN (1,7) THEN 'Weekend'
        ELSE 'Weekday'
    END                                          AS weekday_type,
    incident_hour                                AS hour_of_day,
    FALSE                                        AS is_holiday
FROM silver.crimes_conformed
WHERE incident_datetime IS NOT NULL;

-- Logging: meta.gold_publication_log
INSERT INTO meta.gold_publication_log
    (run_id, step_name, source_table, target_table, rows_in, rows_out, rows_dropped, notes)
SELECT
    (SELECT MAX(run_id) FROM meta.etl_run_log WHERE pipeline_name='gold_build'),
    'GOLD_STEP1_DIM_DATE', 'silver.crimes_conformed', 'gold.dim_date',
    (SELECT COUNT(*) FROM silver.crimes_conformed),
    (SELECT COUNT(*) FROM gold.dim_date), 0,
    'Distinct dates from silver. year/month/quarter/weekday attributes computed.';

-- ════════════════════════════════════════════════════════════
-- STEP 2: POPULATE dim_crime_type
-- ════════════════════════════════════════════════════════════

INSERT INTO gold.dim_crime_type
SELECT
    DENSE_RANK() OVER (ORDER BY primary_type, fbi_code) AS crime_type_key,
    primary_type,
    fbi_code,
    -- FBI code → description lookup
    CASE UPPER(TRIM(fbi_code))
        WHEN '01A' THEN 'Homicide — Murder & Non-Negligent Manslaughter'
        WHEN '01B' THEN 'Homicide — Manslaughter by Negligence'
        WHEN '02'  THEN 'Criminal Sexual Assault'
        WHEN '03'  THEN 'Robbery'
        WHEN '04A' THEN 'Aggravated Assault'
        WHEN '04B' THEN 'Aggravated Battery'
        WHEN '05'  THEN 'Burglary'
        WHEN '06'  THEN 'Larceny / Theft'
        WHEN '07'  THEN 'Motor Vehicle Theft'
        WHEN '08A' THEN 'Simple Assault'
        WHEN '08B' THEN 'Simple Battery'
        WHEN '09'  THEN 'Arson'
        WHEN '10'  THEN 'Forgery & Counterfeiting'
        WHEN '11'  THEN 'Fraud'
        WHEN '12'  THEN 'Embezzlement'
        WHEN '14'  THEN 'Weapons Violation'
        WHEN '15'  THEN 'Prostitution & Vice'
        WHEN '16'  THEN 'Drug Abuse Violations'
        WHEN '17'  THEN 'Gambling'
        WHEN '18'  THEN 'Offenses Against Family & Children'
        WHEN '19'  THEN 'Driving Under the Influence'
        WHEN '20'  THEN 'Liquor Laws'
        WHEN '24'  THEN 'Disorderly Conduct'
        WHEN '26'  THEN 'Miscellaneous Non-Index Offenses'
        WHEN '20'  THEN 'Liquor Laws'
        ELSE CONCAT('FBI Code ', fbi_code)
    END                                                  AS fbi_description,
    -- Crime category — VIOLENT / PROPERTY / OTHER
    CASE
        WHEN primary_type IN (
            'HOMICIDE', 'CRIM SEXUAL ASSAULT', 'ROBBERY',
            'ASSAULT', 'BATTERY', 'KIDNAPPING',
            'HUMAN TRAFFICKING', 'STALKING', 'INTIMIDATION')
        THEN 'VIOLENT'
        WHEN primary_type IN (
            'THEFT', 'BURGLARY', 'MOTOR VEHICLE THEFT',
            'ARSON', 'CRIMINAL DAMAGE', 'CRIMINAL TRESPASS',
            'DECEPTIVE PRACTICE', 'ROBBERY')
        THEN 'PROPERTY'
        ELSE 'OTHER'
    END                                                  AS crime_category,
    CASE
        WHEN primary_type IN (
            'HOMICIDE', 'CRIM SEXUAL ASSAULT', 'ROBBERY',
            'ASSAULT', 'BATTERY', 'KIDNAPPING',
            'HUMAN TRAFFICKING', 'STALKING', 'INTIMIDATION')
        THEN TRUE ELSE FALSE
    END                                                  AS is_violent
FROM (
    SELECT DISTINCT primary_type, fbi_code
    FROM silver.crimes_conformed
    WHERE primary_type IS NOT NULL AND fbi_code IS NOT NULL
) t;

-- ════════════════════════════════════════════════════════════
-- STEP 3: POPULATE dim_location
-- ════════════════════════════════════════════════════════════

INSERT INTO gold.dim_location
SELECT
    DENSE_RANK() OVER (
        ORDER BY community_area_num, ward_num, district_code, beat
    )                         AS location_key,
    beat,
    district_code,
    district_name,
    ward_num,
    ward_label,
    community_area_num,
    community_name
FROM (
    SELECT DISTINCT
        beat,
        district_code,
        district_name,
        ward_num,
        ward_label,
        community_area_num,
        community_name
    FROM silver.crimes_conformed
    WHERE beat IS NOT NULL
      AND district_code IS NOT NULL
) t;

-- ════════════════════════════════════════════════════════════
-- STEP 4: POPULATE dim_shift
-- 24 rows — one per hour. Police shift assignment:
--   Morning (Day)  Shift: 06:00 – 13:59
--   Evening        Shift: 14:00 – 21:59
--   Night          Shift: 22:00 – 05:59
-- ════════════════════════════════════════════════════════════

INSERT INTO gold.dim_shift
SELECT
    hour                                  AS shift_key,
    hour                                  AS hour_of_day,
    CASE
        WHEN hour BETWEEN 6  AND 13 THEN 'Morning Shift'
        WHEN hour BETWEEN 14 AND 21 THEN 'Evening Shift'
        ELSE 'Night Shift'
    END                                   AS shift_name,
    CASE
        WHEN hour BETWEEN 6  AND 13 THEN '06:00 – 14:00'
        WHEN hour BETWEEN 14 AND 21 THEN '14:00 – 22:00'
        ELSE '22:00 – 06:00'
    END                                   AS shift_hours,
    CASE
        WHEN hour BETWEEN 6  AND 13 THEN 'DAY'
        WHEN hour BETWEEN 14 AND 21 THEN 'EVENING'
        ELSE 'NIGHT'
    END                                   AS shift_type
FROM (
    SELECT explode(sequence(0, 23)) AS hour
);

-- ════════════════════════════════════════════════════════════
-- STEP 5: POPULATE dim_location_type
-- ════════════════════════════════════════════════════════════

INSERT INTO gold.dim_location_type
SELECT
    DENSE_RANK() OVER (ORDER BY location_description) AS location_type_key,
    location_description,
    CASE
        WHEN location_description IN (
            'STREET', 'SIDEWALK', 'ALLEY', 'PARKING LOT / GARAGE (NON RESIDENTIAL)',
            'PARK PROPERTY', 'FOREST PRESERVE', 'HIGHWAY / EXPRESSWAY',
            'BRIDGE', 'RIVER BANK', 'LAKEFRONT / WATERFRONT / RIVERBANK',
            'DRIVEWAY RESIDENTIAL', 'VEHICLE NON-COMMERCIAL', 'VEHICLE-COMMERCIAL',
            'TAXICAB', 'OTHER VEHICLE', 'CTA BUS', 'CTA TRAIN', 'CTA STATION',
            'CTA BUS STOP', 'PARKING LOT / GARAGE (NON RESIDENTIAL)',
            'AIRCRAFT', 'BOAT / WATERCRAFT', 'ATHLETIC CLUB',
            'EXPRESSWAY EMBANKMENT', 'YARD')
        THEN 'OUTDOOR / TRANSPORT'
        WHEN location_description IN (
            'RESIDENCE', 'APARTMENT', 'RESIDENCE - PORCH / HALLWAY',
            'RESIDENCE - YARD (FRONT / BACK)', 'HOUSE', 'DRIVEWAY RESIDENTIAL',
            'GARAGE', 'BASEMENT', 'VESTIBULE', 'STAIRWELL',
            'RESIDENCE - GARAGE', 'NURSING HOME / RETIREMENT HOME',
            'COLLEGE / UNIVERSITY - GROUNDS', 'COLLEGE / UNIVERSITY - RESIDENCE HALL')
        THEN 'RESIDENTIAL'
        WHEN location_description IN (
            'SMALL RETAIL STORE', 'DEPARTMENT STORE', 'GROCERY FOOD STORE',
            'RESTAURANT', 'BAR OR TAVERN', 'CONVENIENCE STORE', 'DRUG STORE',
            'BANK', 'COMMERCIAL / BUSINESS OFFICE', 'HOTEL / MOTEL',
            'CURRENCY EXCHANGE', 'GAS STATION', 'BOWLING ALLEY',
            'SPORTS ARENA / STADIUM', 'THEATER', 'HOSPITAL BUILDING / GROUNDS',
            'MEDICAL / DENTAL OFFICE', 'CAR WASH', 'AUTO / BOAT / RV DEALERSHIP',
            'TAVERN / LIQUOR STORE', 'LAUNDRY ROOM', 'CLEANERS / LAUNDROMAT',
            'MOVIE HOUSE / THEATER')
        THEN 'COMMERCIAL'
        WHEN location_description IN (
            'SCHOOL - PUBLIC GROUNDS', 'SCHOOL - PUBLIC BUILDING',
            'SCHOOL - PRIVATE BUILDING', 'SCHOOL - PRIVATE GROUNDS',
            'CHURCH / SYNAGOGUE / PLACE OF WORSHIP', 'GOVERNMENT BUILDING / PROPERTY',
            'POLICE FACILITY / VEH PARKING LOT', 'LIBRARY',
            'HOSPITAL BUILDING / GROUNDS', 'FIRE STATION')
        THEN 'INSTITUTIONAL'
        ELSE 'OTHER'
    END                                               AS location_category
FROM (
    SELECT DISTINCT location_description
    FROM silver.crimes_conformed
    WHERE location_description IS NOT NULL
      AND TRIM(location_description) != ''
) t;

-- ════════════════════════════════════════════════════════════
-- STEP 6: POPULATE fact_crimes
-- Central fact table — join silver to all 5 dimension keys
-- ════════════════════════════════════════════════════════════

INSERT INTO gold.fact_crimes
SELECT
    c.crime_id,
    c.case_number,
    -- Dimension FKs
    CAST(DATE_FORMAT(CAST(c.incident_datetime AS DATE), 'yyyyMMdd') AS INT) AS date_key,
    ct.crime_type_key,
    dl.location_key,
    c.incident_hour                                   AS shift_key,
    lt.location_type_key,
    -- Measures
    c.is_arrest,
    c.is_domestic,
    c.lat_final,
    c.lon_final,
    -- Denormalised date attributes
    c.incident_year,
    c.incident_month,
    c.incident_hour,
    c.incident_dayofweek,
    -- Denormalised crime attributes
    c.primary_type,
    c.fbi_code,
    ct.crime_category,
    ct.is_violent,
    -- Denormalised geography
    c.community_area_num,
    c.community_name,
    c.district_code,
    c.district_name,
    c.ward_num,
    -- Denormalised location type
    c.location_description,
    -- Lineage
    c.source_year_range,
    c._source_file,
    current_timestamp()                               AS _gold_ts

FROM silver.crimes_conformed c

-- Join dim_crime_type on primary_type + fbi_code
LEFT JOIN gold.dim_crime_type ct
    ON  c.primary_type = ct.primary_type
    AND c.fbi_code     = ct.fbi_code

-- Join dim_location on full geographic combination
LEFT JOIN gold.dim_location dl
    ON  c.beat               = dl.beat
    AND c.district_code      = dl.district_code
    AND c.community_area_num = dl.community_area_num

-- Join dim_location_type on location_description
LEFT JOIN gold.dim_location_type lt
    ON c.location_description = lt.location_description;

-- Logging: meta.gold_publication_log
INSERT INTO meta.gold_publication_log
    (run_id, step_name, source_table, target_table, rows_in, rows_out, rows_dropped, notes)
SELECT
    (SELECT MAX(run_id) FROM meta.etl_run_log WHERE pipeline_name='gold_build'),
    'GOLD_STEP6_FACT_CRIMES', 'silver.crimes_conformed', 'gold.fact_crimes',
    (SELECT COUNT(*) FROM silver.crimes_conformed),
    (SELECT COUNT(*) FROM gold.fact_crimes), 0,
    'Star schema joins: dim_crime_type + dim_location + dim_location_type. shift_key = incident_hour directly.';