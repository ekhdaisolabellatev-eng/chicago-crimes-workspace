-- ════════════════════════════════════════════════════════════
-- STEP 7: ANALYTICAL VIEWS
-- ════════════════════════════════════════════════════════════
USE CATALOG chicago_crimes_workspace;
-- ── VIEW 1: Monthly crime trend (temporal) ───────────────────
-- Business question: Is crime seasonal? What is the long-term trend?
CREATE OR REPLACE VIEW gold.v_temporal_monthly_trend AS
SELECT
    f.incident_year,
    f.incident_month,
    d.month_name,
    d.quarter,
    COUNT(*)                                        AS total_crimes,
    SUM(CASE WHEN f.is_arrest   THEN 1 ELSE 0 END) AS total_arrests,
    SUM(CASE WHEN f.is_domestic THEN 1 ELSE 0 END) AS domestic_crimes,
    SUM(CASE WHEN f.is_violent  THEN 1 ELSE 0 END) AS violent_crimes,
    ROUND(
        100.0 * SUM(CASE WHEN f.is_arrest THEN 1 ELSE 0 END) / COUNT(*), 2
    )                                               AS arrest_rate_pct,
    -- Rolling 3-month average for smoothing
    ROUND(AVG(COUNT(*)) OVER (
        ORDER BY f.incident_year, f.incident_month
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ), 0)                                           AS rolling_3m_avg
FROM gold.fact_crimes f
LEFT JOIN gold.dim_date d
    ON f.date_key = d.date_key
GROUP BY f.incident_year, f.incident_month, d.month_name, d.quarter
ORDER BY f.incident_year, f.incident_month;

-- ── VIEW 2: Year-over-year change per district (temporal) ────
-- Business question: Are crime rates improving over time per district?
CREATE OR REPLACE VIEW gold.v_temporal_yoy_change AS
WITH base AS (
    SELECT
        district_name,
        incident_year,
        COUNT(*)                                        AS total_crimes,
        SUM(CASE WHEN is_arrest   THEN 1 ELSE 0 END)   AS total_arrests,
        SUM(CASE WHEN is_domestic THEN 1 ELSE 0 END)   AS domestic_crimes
    FROM gold.fact_crimes
    WHERE district_name IS NOT NULL
    GROUP BY district_name, incident_year
)
SELECT
    curr.district_name,
    curr.incident_year,
    curr.total_crimes,
    curr.total_arrests,
    ROUND(100.0 * curr.total_arrests / curr.total_crimes, 2)    AS arrest_rate_pct,
    prev.total_crimes                                           AS prev_year_crimes,
    curr.total_crimes - prev.total_crimes                       AS yoy_change_abs,
    ROUND(
        100.0 * (curr.total_crimes - prev.total_crimes) /
        NULLIF(prev.total_crimes, 0), 2
    )                                                           AS yoy_change_pct,
    CASE
        WHEN curr.total_crimes > prev.total_crimes THEN 'INCREASING'
        WHEN curr.total_crimes < prev.total_crimes THEN 'DECREASING'
        ELSE 'STABLE'
    END                                                         AS trend_direction
FROM base curr
LEFT JOIN base prev
    ON  curr.district_name = prev.district_name
    AND curr.incident_year = prev.incident_year + 1
ORDER BY curr.district_name, curr.incident_year;

-- ── VIEW 3: Police shift effectiveness (temporal) ────────────
-- Business question: Which shift has the highest crime volume?
-- Are night shifts effective at making arrests?
CREATE OR REPLACE VIEW gold.v_temporal_shift_analysis AS
SELECT
    s.shift_name,
    s.shift_hours,
    s.shift_type,
    f.incident_hour,
    COUNT(*)                                        AS total_crimes,
    SUM(CASE WHEN f.is_arrest   THEN 1 ELSE 0 END) AS total_arrests,
    SUM(CASE WHEN f.is_domestic THEN 1 ELSE 0 END) AS domestic_crimes,
    SUM(CASE WHEN f.is_violent  THEN 1 ELSE 0 END) AS violent_crimes,
    ROUND(
        100.0 * SUM(CASE WHEN f.is_arrest THEN 1 ELSE 0 END) / COUNT(*), 2
    )                                               AS arrest_rate_pct,
    ROUND(
        100.0 * SUM(CASE WHEN f.is_violent THEN 1 ELSE 0 END) / COUNT(*), 2
    )                                               AS violent_crime_pct
FROM gold.fact_crimes f
LEFT JOIN gold.dim_shift s
    ON f.shift_key = s.shift_key
GROUP BY s.shift_name, s.shift_hours, s.shift_type, f.incident_hour
ORDER BY f.incident_hour;

-- ── VIEW 4: Weekday vs weekend crime pattern (temporal) ──────
-- Business question: Does crime concentrate on weekends?
CREATE OR REPLACE VIEW gold.v_temporal_weekday_pattern AS
SELECT
    d.day_name,
    d.day_of_week_num,
    d.weekday_type,
    COUNT(*)                                        AS total_crimes,
    SUM(CASE WHEN f.is_arrest   THEN 1 ELSE 0 END) AS total_arrests,
    SUM(CASE WHEN f.is_violent  THEN 1 ELSE 0 END) AS violent_crimes,
    SUM(CASE WHEN f.is_domestic THEN 1 ELSE 0 END) AS domestic_crimes,
    ROUND(
        100.0 * SUM(CASE WHEN f.is_arrest THEN 1 ELSE 0 END) / COUNT(*), 2
    )                                               AS arrest_rate_pct,
    ROUND(
        100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2
    )                                               AS pct_of_all_crimes
FROM gold.fact_crimes f
LEFT JOIN gold.dim_date d
    ON f.date_key = d.date_key
WHERE d.day_name IS NOT NULL
GROUP BY d.day_name, d.day_of_week_num, d.weekday_type
ORDER BY d.day_of_week_num;

-- ── VIEW 5: Community area crime profile (geographic) ────────
-- Business question: Which areas are highest risk?
-- Key output for ML clustering and dashboard map
CREATE OR REPLACE VIEW gold.v_geo_community_profile AS
SELECT
    f.community_area_num,
    f.community_name,
    -- Volume
    COUNT(*)                                        AS total_crimes,
    -- Arrest effectiveness
    SUM(CASE WHEN f.is_arrest   THEN 1 ELSE 0 END) AS total_arrests,
    ROUND(
        100.0 * SUM(CASE WHEN f.is_arrest THEN 1 ELSE 0 END) / COUNT(*), 2
    )                                               AS arrest_rate_pct,
    -- Crime composition
    SUM(CASE WHEN f.is_violent  THEN 1 ELSE 0 END) AS violent_crimes,
    SUM(CASE WHEN f.is_domestic THEN 1 ELSE 0 END) AS domestic_crimes,
    SUM(CASE WHEN ct.crime_category = 'PROPERTY' THEN 1 ELSE 0 END) AS property_crimes,
    ROUND(
        100.0 * SUM(CASE WHEN f.is_violent THEN 1 ELSE 0 END) / COUNT(*), 2
    )                                               AS violent_pct,
    ROUND(
        100.0 * SUM(CASE WHEN f.is_domestic THEN 1 ELSE 0 END) / COUNT(*), 2
    )                                               AS domestic_pct,
    -- Peak patterns
    MODE(f.incident_hour)                           AS peak_crime_hour,
    MODE(f.primary_type)                            AS dominant_crime_type,
    -- Year-over-year (most recent two years in dataset)
    SUM(CASE WHEN f.incident_year = 2017 THEN 1 ELSE 0 END) AS crimes_2017,
    SUM(CASE WHEN f.incident_year = 2016 THEN 1 ELSE 0 END) AS crimes_2016,
    ROUND(
        100.0 * (
            SUM(CASE WHEN f.incident_year = 2017 THEN 1 ELSE 0 END) -
            SUM(CASE WHEN f.incident_year = 2016 THEN 1 ELSE 0 END)
        ) / NULLIF(SUM(CASE WHEN f.incident_year = 2016 THEN 1 ELSE 0 END), 0), 2
    )                                               AS yoy_pct_2017_vs_2016
FROM gold.fact_crimes f
LEFT JOIN gold.dim_crime_type ct
    ON f.crime_type_key = ct.crime_type_key
WHERE f.community_name IS NOT NULL
GROUP BY f.community_area_num, f.community_name
ORDER BY total_crimes DESC;

-- ── VIEW 6: District performance summary (geographic) ────────
-- Business question: Which districts are failing vs succeeding?
CREATE OR REPLACE VIEW gold.v_geo_district_performance AS
SELECT
    f.district_code,
    f.district_name,
    f.incident_year,
    COUNT(*)                                         AS total_crimes,
    SUM(CASE WHEN f.is_arrest   THEN 1 ELSE 0 END)  AS total_arrests,
    SUM(CASE WHEN f.is_violent  THEN 1 ELSE 0 END)  AS violent_crimes,
    SUM(CASE WHEN f.is_domestic THEN 1 ELSE 0 END)  AS domestic_crimes,
    ROUND(
        100.0 * SUM(CASE WHEN f.is_arrest THEN 1 ELSE 0 END) / COUNT(*), 2
    )                                                AS arrest_rate_pct,
    ROUND(
        100.0 * SUM(CASE WHEN f.is_violent THEN 1 ELSE 0 END) / COUNT(*), 2
    )                                                AS violent_crime_pct,
    -- Rank districts by total crimes per year
    RANK() OVER (
        PARTITION BY f.incident_year
        ORDER BY COUNT(*) DESC
    )                                                AS crime_volume_rank,
    -- Rank districts by arrest rate per year
    RANK() OVER (
        PARTITION BY f.incident_year
        ORDER BY ROUND(100.0 * SUM(CASE WHEN f.is_arrest THEN 1 ELSE 0 END) / COUNT(*), 2) DESC
    )                                                AS arrest_rate_rank
FROM gold.fact_crimes f
WHERE f.district_name IS NOT NULL
GROUP BY f.district_code, f.district_name, f.incident_year
ORDER BY f.incident_year, total_crimes DESC;

-- ── VIEW 7: Crime type breakdown (crime-type analysis) ───────
-- Business question: What types of crime dominate? How have
-- distributions shifted over time?
CREATE OR REPLACE VIEW gold.v_crime_type_breakdown AS
SELECT
    f.primary_type,
    f.fbi_code,
    ct.fbi_description,
    ct.crime_category,
    f.incident_year,
    COUNT(*)                                        AS total_crimes,
    SUM(CASE WHEN f.is_arrest THEN 1 ELSE 0 END)   AS total_arrests,
    ROUND(
        100.0 * SUM(CASE WHEN f.is_arrest THEN 1 ELSE 0 END) / COUNT(*), 2
    )                                               AS arrest_rate_pct,
    ROUND(
        100.0 * COUNT(*) /
        SUM(COUNT(*)) OVER (PARTITION BY f.incident_year), 2
    )                                               AS pct_of_year_total,
    -- Rank by volume per year
    RANK() OVER (
        PARTITION BY f.incident_year
        ORDER BY COUNT(*) DESC
    )                                               AS rank_in_year
FROM gold.fact_crimes f
LEFT JOIN gold.dim_crime_type ct ON f.crime_type_key = ct.crime_type_key
GROUP BY f.primary_type, f.fbi_code, ct.fbi_description, ct.crime_category, f.incident_year
ORDER BY f.incident_year, total_crimes DESC;

-- ── VIEW 8: Crime type by community area (crime-type analysis)
-- Business question: Does crime composition differ by neighbourhood?
CREATE OR REPLACE VIEW gold.v_crime_type_by_area AS
SELECT
    f.community_name,
    f.primary_type,
    ct.crime_category,
    COUNT(*)                                        AS crime_count,
    ROUND(
        100.0 * COUNT(*) /
        SUM(COUNT(*)) OVER (PARTITION BY f.community_name), 2
    )                                               AS pct_of_community,
    DENSE_RANK() OVER (
        PARTITION BY f.community_name
        ORDER BY COUNT(*) DESC
    )                                               AS rank_in_community
FROM gold.fact_crimes f
LEFT JOIN gold.dim_crime_type ct ON f.crime_type_key = ct.crime_type_key
WHERE f.community_name IS NOT NULL
GROUP BY f.community_name, f.primary_type, ct.crime_category
ORDER BY f.community_name, rank_in_community;

-- ── VIEW 9: Arrest effectiveness (arrest analysis) ───────────
-- Business question: Where are arrests highest and lowest?
-- Which crime types have the lowest arrest rates?
CREATE OR REPLACE VIEW gold.v_arrest_effectiveness AS
SELECT
    f.primary_type,
    ct.crime_category,
    f.district_name,
    f.community_name,
    f.incident_year,
    COUNT(*)                                        AS total_crimes,
    SUM(CASE WHEN f.is_arrest THEN 1 ELSE 0 END)   AS total_arrests,
    ROUND(
        100.0 * SUM(CASE WHEN f.is_arrest THEN 1 ELSE 0 END) / COUNT(*), 2
    )                                               AS arrest_rate_pct,
    -- City-wide average arrest rate for this crime type (for comparison)
    ROUND(
        100.0 * SUM(SUM(CASE WHEN f.is_arrest THEN 1 ELSE 0 END))
                    OVER (PARTITION BY f.primary_type) /
                SUM(COUNT(*)) OVER (PARTITION BY f.primary_type), 2
    )                                               AS citywide_avg_arrest_rate,
    -- Flag areas below the citywide average
    CASE
        WHEN ROUND(100.0 * SUM(CASE WHEN f.is_arrest THEN 1 ELSE 0 END) / COUNT(*), 2)
             < ROUND(
                   100.0 * SUM(SUM(CASE WHEN f.is_arrest THEN 1 ELSE 0 END))
                               OVER (PARTITION BY f.primary_type) /
                           SUM(COUNT(*)) OVER (PARTITION BY f.primary_type), 2)
        THEN TRUE ELSE FALSE
    END                                             AS below_citywide_avg
FROM gold.fact_crimes f
LEFT JOIN gold.dim_crime_type ct ON f.crime_type_key = ct.crime_type_key
WHERE f.district_name   IS NOT NULL
  AND f.community_name  IS NOT NULL
GROUP BY f.primary_type, ct.crime_category,
         f.district_name, f.community_name, f.incident_year;

-- ── VIEW 10: Domestic crime analysis (arrest + crime analysis)
-- Business question: Where does domestic violence concentrate?
-- Has it improved or worsened over time?
CREATE OR REPLACE VIEW gold.v_domestic_crime_analysis AS
SELECT
    f.community_name,
    f.district_name,
    f.incident_year,
    COUNT(*)                                         AS total_crimes,
    SUM(CASE WHEN f.is_domestic THEN 1 ELSE 0 END)  AS domestic_count,
    SUM(CASE WHEN NOT f.is_domestic THEN 1 ELSE 0 END) AS non_domestic_count,
    ROUND(
        100.0 * SUM(CASE WHEN f.is_domestic THEN 1 ELSE 0 END) / COUNT(*), 2
    )                                                AS domestic_pct,
    -- Arrests specifically for domestic crimes
    SUM(CASE WHEN f.is_domestic AND f.is_arrest THEN 1 ELSE 0 END) AS domestic_arrests,
    ROUND(
        100.0 * SUM(CASE WHEN f.is_domestic AND f.is_arrest THEN 1 ELSE 0 END) /
        NULLIF(SUM(CASE WHEN f.is_domestic THEN 1 ELSE 0 END), 0), 2
    )                                                AS domestic_arrest_rate_pct,
    -- Rank communities by domestic crime rate per year
    RANK() OVER (
        PARTITION BY f.incident_year
        ORDER BY
            ROUND(100.0 * SUM(CASE WHEN f.is_domestic THEN 1 ELSE 0 END) / COUNT(*), 2)
        DESC
    )                                                AS domestic_rate_rank
FROM gold.fact_crimes f
WHERE f.community_name IS NOT NULL
GROUP BY f.community_name, f.district_name, f.incident_year
ORDER BY f.incident_year, domestic_pct DESC;

-- ── VIEW 11: City-wide KPI summary (dashboard home card) ─────
CREATE OR REPLACE VIEW gold.v_city_kpi_summary AS
SELECT
    COUNT(*)                                        AS total_crimes,
    COUNT(DISTINCT incident_year)                   AS years_covered,
    MIN(incident_year)                              AS first_year,
    MAX(incident_year)                              AS last_year,
    COUNT(DISTINCT community_area_num)              AS communities_with_crime,
    COUNT(DISTINCT district_code)                   AS districts_covered,
    ROUND(
        100.0 * SUM(CASE WHEN is_arrest   THEN 1 ELSE 0 END) / COUNT(*), 2
    )                                               AS overall_arrest_rate_pct,
    ROUND(
        100.0 * SUM(CASE WHEN is_domestic THEN 1 ELSE 0 END) / COUNT(*), 2
    )                                               AS domestic_crime_pct,
    ROUND(
        100.0 * SUM(CASE WHEN is_violent  THEN 1 ELSE 0 END) / COUNT(*), 2
    )                                               AS violent_crime_pct,
    MODE(primary_type)                              AS most_common_crime,
    MODE(community_name)                            AS highest_crime_community,
    MODE(incident_hour)                             AS peak_crime_hour,
    MODE(location_description)                      AS most_common_location_type
FROM gold.fact_crimes;
