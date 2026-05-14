-- ============================================================
-- CHICAGO CRIME DATA LAKE — DATABRICKS SQL
-- Script: 02_bronze_bulk_insert_v2.sql
-- Zone:   BRONZE
-- Purpose: Load all CSVs from Unity Catalog Volumes into Bronze landing tables.
--          Log every file to meta.file_ingestion_log.
--          Wrapped in stored procedure with error handling.
-- 
-- Run: CALL chicago_crimes_workspace.meta.load_bronze_data();
-- ============================================================

USE CATALOG chicago_crimes_workspace;

-- ============================================================
-- DROP AND CREATE STORED PROCEDURE
-- ============================================================
DROP PROCEDURE IF EXISTS meta.load_bronze_data;

CREATE PROCEDURE meta.load_bronze_data()
LANGUAGE SQL
SQL SECURITY INVOKER
BEGIN
    -- Declare variables for error tracking
    DECLARE v_run_id BIGINT;
    DECLARE v_row_count BIGINT;
    DECLARE v_total_rows BIGINT DEFAULT 0;
    DECLARE v_error_count INT DEFAULT 0;
    
    -- ────────────────────────────────────────────────────────────
    -- Step 0: Open an ETL run log entry
    -- ────────────────────────────────────────────────────────────
    INSERT INTO meta.etl_run_log
        (pipeline_name, zone, script_name, start_ts, status)
    VALUES
        ('bronze_bulk_ingest', 'BRONZE', '02_bronze_bulk_insert_v2.sql',
         current_timestamp(), 'RUNNING');
    
    -- Get the run_id we just created
    SET v_run_id = (SELECT MAX(run_id) FROM meta.etl_run_log 
                    WHERE pipeline_name = 'bronze_bulk_ingest');

    -- ────────────────────────────────────────────────────────────
    -- LOAD: Chicago Crimes 2001–2004
    -- ────────────────────────────────────────────────────────────
    BEGIN
        DECLARE EXIT HANDLER FOR SQLEXCEPTION
        BEGIN
            INSERT INTO meta.file_ingestion_log
                (run_id, source_file_name, source_file_path, target_table, zone, row_count, status, error_message)
            VALUES
                (v_run_id,
                 'Chicago_Crimes_2001_to_2004.csv',
                 '/Volumes/chicago_crimes_workspace/bronze/chicago_data/Chicago_Crimes_2001_to_2004.csv',
                 'bronze.crimes_2001_2004',
                 'BRONZE',
                 0,
                 'FAILED',
                 'Error loading file - check file path and format');
            SET v_error_count = v_error_count + 1;
        END;
        
        INSERT INTO bronze.crimes_2001_2004
        SELECT
            `ID`                   AS id,
            `Case Number`          AS case_number,
            `Date`                 AS date,
            `Block`                AS block,
            `IUCR`                 AS iucr,
            `Primary Type`         AS primary_type,
            `Description`          AS description,
            `Location Description` AS location_description,
            `Arrest`               AS arrest,
            `Domestic`             AS domestic,
            `Beat`                 AS beat,
            `District`             AS district,
            `Ward`                 AS ward,
            `Community Area`       AS community_area,
            `FBI Code`             AS fbi_code,
            `X Coordinate`         AS x_coordinate,
            `Y Coordinate`         AS y_coordinate,
            `Year`                 AS year,
            `Updated On`           AS updated_on,
            `Latitude`             AS latitude,
            `Longitude`            AS longitude,
            `Location`             AS location,
            -- Bronze metadata
            'Chicago_Crimes_2001_to_2004.csv'                          AS _source_file,
            '/Volumes/chicago_crimes_workspace/bronze/chicago_data/Chicago_Crimes_2001_to_2004.csv' AS _source_path,
            current_timestamp()                                        AS _ingest_ts,
            'bronze.crimes_2001_2004'                                  AS _bronze_table
        FROM read_files(
            '/Volumes/chicago_crimes_workspace/bronze/chicago_data/Chicago_Crimes_2001_to_2004.csv',
            format => 'csv',
            header => true,
            mode => 'PERMISSIVE'
        );
        
        -- Get actual row count from table
        SET v_row_count = (SELECT COUNT(*) FROM bronze.crimes_2001_2004);
        SET v_total_rows = v_total_rows + v_row_count;
        
        -- Log success
        INSERT INTO meta.file_ingestion_log
            (run_id, source_file_name, source_file_path, target_table, zone, row_count, status)
        VALUES
            (v_run_id,
             'Chicago_Crimes_2001_to_2004.csv',
             '/Volumes/chicago_crimes_workspace/bronze/chicago_data/Chicago_Crimes_2001_to_2004.csv',
             'bronze.crimes_2001_2004',
             'BRONZE',
             v_row_count,
             'SUCCESS');
    END;

    -- ────────────────────────────────────────────────────────────
    -- LOAD: Chicago Crimes 2005–2007
    -- ────────────────────────────────────────────────────────────
    BEGIN
        DECLARE EXIT HANDLER FOR SQLEXCEPTION
        BEGIN
            INSERT INTO meta.file_ingestion_log
                (run_id, source_file_name, source_file_path, target_table, zone, row_count, status, error_message)
            VALUES
                (v_run_id, 'Chicago_Crimes_2005_to_2007.csv',
                 '/Volumes/chicago_crimes_workspace/bronze/chicago_data/Chicago_Crimes_2005_to_2007.csv',
                 'bronze.crimes_2005_2007', 'BRONZE', 0, 'FAILED', 'Error loading file - check file path and format');
            SET v_error_count = v_error_count + 1;
        END;
        
        INSERT INTO bronze.crimes_2005_2007
        SELECT
            `ID`                   AS id,
            `Case Number`          AS case_number,
            `Date`                 AS date,
            `Block`                AS block,
            `IUCR`                 AS iucr,
            `Primary Type`         AS primary_type,
            `Description`          AS description,
            `Location Description` AS location_description,
            `Arrest`               AS arrest,
            `Domestic`             AS domestic,
            `Beat`                 AS beat,
            `District`             AS district,
            `Ward`                 AS ward,
            `Community Area`       AS community_area,
            `FBI Code`             AS fbi_code,
            `X Coordinate`         AS x_coordinate,
            `Y Coordinate`         AS y_coordinate,
            `Year`                 AS year,
            `Updated On`           AS updated_on,
            `Latitude`             AS latitude,
            `Longitude`            AS longitude,
            `Location`             AS location,
            'Chicago_Crimes_2005_to_2007.csv'                          AS _source_file,
            '/Volumes/chicago_crimes_workspace/bronze/chicago_data/Chicago_Crimes_2005_to_2007.csv' AS _source_path,
            current_timestamp()                                        AS _ingest_ts,
            'bronze.crimes_2005_2007'                                  AS _bronze_table
        FROM read_files(
            '/Volumes/chicago_crimes_workspace/bronze/chicago_data/Chicago_Crimes_2005_to_2007.csv',
            format => 'csv',
            header => true,
            mode => 'PERMISSIVE'
        );
        
        SET v_row_count = (SELECT COUNT(*) FROM bronze.crimes_2005_2007);
        SET v_total_rows = v_total_rows + v_row_count;
        
        INSERT INTO meta.file_ingestion_log
            (run_id, source_file_name, source_file_path, target_table, zone, row_count, status)
        VALUES
            (v_run_id, 'Chicago_Crimes_2005_to_2007.csv',
             '/Volumes/chicago_crimes_workspace/bronze/chicago_data/Chicago_Crimes_2005_to_2007.csv',
             'bronze.crimes_2005_2007', 'BRONZE', v_row_count, 'SUCCESS');
    END;

    -- ────────────────────────────────────────────────────────────
    -- LOAD: Chicago Crimes 2008–2011
    -- ────────────────────────────────────────────────────────────
    BEGIN
        DECLARE EXIT HANDLER FOR SQLEXCEPTION
        BEGIN
            INSERT INTO meta.file_ingestion_log
                (run_id, source_file_name, source_file_path, target_table, zone, row_count, status, error_message)
            VALUES
                (v_run_id, 'Chicago_Crimes_2008_to_2011.csv',
                 '/Volumes/chicago_crimes_workspace/bronze/chicago_data/Chicago_Crimes_2008_to_2011.csv',
                 'bronze.crimes_2008_2011', 'BRONZE', 0, 'FAILED', 'Error loading file - check file path and format');
            SET v_error_count = v_error_count + 1;
        END;
        
        INSERT INTO bronze.crimes_2008_2011
        SELECT
            `ID`                   AS id,
            `Case Number`          AS case_number,
            `Date`                 AS date,
            `Block`                AS block,
            `IUCR`                 AS iucr,
            `Primary Type`         AS primary_type,
            `Description`          AS description,
            `Location Description` AS location_description,
            `Arrest`               AS arrest,
            `Domestic`             AS domestic,
            `Beat`                 AS beat,
            `District`             AS district,
            `Ward`                 AS ward,
            `Community Area`       AS community_area,
            `FBI Code`             AS fbi_code,
            `X Coordinate`         AS x_coordinate,
            `Y Coordinate`         AS y_coordinate,
            `Year`                 AS year,
            `Updated On`           AS updated_on,
            `Latitude`             AS latitude,
            `Longitude`            AS longitude,
            `Location`             AS location,
            'Chicago_Crimes_2008_to_2011.csv'                          AS _source_file,
            '/Volumes/chicago_crimes_workspace/bronze/chicago_data/Chicago_Crimes_2008_to_2011.csv' AS _source_path,
            current_timestamp()                                        AS _ingest_ts,
            'bronze.crimes_2008_2011'                                  AS _bronze_table
        FROM read_files(
            '/Volumes/chicago_crimes_workspace/bronze/chicago_data/Chicago_Crimes_2008_to_2011.csv',
            format => 'csv',
            header => true,
            mode => 'PERMISSIVE'
        );
        
        SET v_row_count = (SELECT COUNT(*) FROM bronze.crimes_2008_2011);
        SET v_total_rows = v_total_rows + v_row_count;
        
        INSERT INTO meta.file_ingestion_log
            (run_id, source_file_name, source_file_path, target_table, zone, row_count, status)
        VALUES
            (v_run_id, 'Chicago_Crimes_2008_to_2011.csv',
             '/Volumes/chicago_crimes_workspace/bronze/chicago_data/Chicago_Crimes_2008_to_2011.csv',
             'bronze.crimes_2008_2011', 'BRONZE', v_row_count, 'SUCCESS');
    END;

    -- ────────────────────────────────────────────────────────────
    -- LOAD: Chicago Crimes 2012–2017
    -- ────────────────────────────────────────────────────────────
    BEGIN
        DECLARE EXIT HANDLER FOR SQLEXCEPTION
        BEGIN
            INSERT INTO meta.file_ingestion_log
                (run_id, source_file_name, source_file_path, target_table, zone, row_count, status, error_message)
            VALUES
                (v_run_id, 'Chicago_Crimes_2012_to_2017.csv',
                 '/Volumes/chicago_crimes_workspace/bronze/chicago_data/Chicago_Crimes_2012_to_2017.csv',
                 'bronze.crimes_2012_2017', 'BRONZE', 0, 'FAILED', 'Error loading file - check file path and format');
            SET v_error_count = v_error_count + 1;
        END;
        
        INSERT INTO bronze.crimes_2012_2017
        SELECT
            `ID`                   AS id,
            `Case Number`          AS case_number,
            `Date`                 AS date,
            `Block`                AS block,
            `IUCR`                 AS iucr,
            `Primary Type`         AS primary_type,
            `Description`          AS description,
            `Location Description` AS location_description,
            `Arrest`               AS arrest,
            `Domestic`             AS domestic,
            `Beat`                 AS beat,
            `District`             AS district,
            `Ward`                 AS ward,
            `Community Area`       AS community_area,
            `FBI Code`             AS fbi_code,
            `X Coordinate`         AS x_coordinate,
            `Y Coordinate`         AS y_coordinate,
            `Year`                 AS year,
            `Updated On`           AS updated_on,
            `Latitude`             AS latitude,
            `Longitude`            AS longitude,
            `Location`             AS location,
            'Chicago_Crimes_2012_to_2017.csv'                          AS _source_file,
            '/Volumes/chicago_crimes_workspace/bronze/chicago_data/Chicago_Crimes_2012_to_2017.csv' AS _source_path,
            current_timestamp()                                        AS _ingest_ts,
            'bronze.crimes_2012_2017'                                  AS _bronze_table
        FROM read_files(
            '/Volumes/chicago_crimes_workspace/bronze/chicago_data/Chicago_Crimes_2012_to_2017.csv',
            format => 'csv',
            header => true,
            mode => 'PERMISSIVE'
        );
        
        SET v_row_count = (SELECT COUNT(*) FROM bronze.crimes_2012_2017);
        SET v_total_rows = v_total_rows + v_row_count;
        
        INSERT INTO meta.file_ingestion_log
            (run_id, source_file_name, source_file_path, target_table, zone, row_count, status)
        VALUES
            (v_run_id, 'Chicago_Crimes_2012_to_2017.csv',
             '/Volumes/chicago_crimes_workspace/bronze/chicago_data/Chicago_Crimes_2012_to_2017.csv',
             'bronze.crimes_2012_2017', 'BRONZE', v_row_count, 'SUCCESS');
    END;

    -- ────────────────────────────────────────────────────────────
    -- LOAD: Ward Boundaries
    -- ────────────────────────────────────────────────────────────
    BEGIN
        DECLARE EXIT HANDLER FOR SQLEXCEPTION
        BEGIN
            INSERT INTO meta.file_ingestion_log
                (run_id, source_file_name, source_file_path, target_table, zone, row_count, status, error_message)
            VALUES
                (v_run_id, 'Wards.csv',
                 '/Volumes/chicago_crimes_workspace/bronze/chicago_data/Wards.csv',
                 'bronze.dim_wards_raw', 'BRONZE', 0, 'FAILED', 'Error loading file - check file path and format');
            SET v_error_count = v_error_count + 1;
        END;
        
        INSERT INTO bronze.dim_wards_raw
        SELECT
            `the_geom`    AS the_geom,
            `WARD`        AS ward,
            `SHAPE_Leng`  AS shape_leng,
            `SHAPE_Area`  AS shape_area,
            'Wards.csv'                                              AS _source_file,
            '/Volumes/chicago_crimes_workspace/bronze/chicago_data/Wards.csv' AS _source_path,
            current_timestamp()                                           AS _ingest_ts,
            'bronze.dim_wards_raw'                                        AS _bronze_table
        FROM read_files(
            '/Volumes/chicago_crimes_workspace/bronze/chicago_data/Wards.csv',
            format => 'csv',
            header => true,
            mode => 'PERMISSIVE'
        );
        
        SET v_row_count = (SELECT COUNT(*) FROM bronze.dim_wards_raw);
        SET v_total_rows = v_total_rows + v_row_count;
        
        INSERT INTO meta.file_ingestion_log
            (run_id, source_file_name, source_file_path, target_table, zone, row_count, status)
        VALUES
            (v_run_id, 'Wards.csv',
             '/Volumes/chicago_crimes_workspace/bronze/chicago_data/Wards.csv',
             'bronze.dim_wards_raw', 'BRONZE', v_row_count, 'SUCCESS');
    END;

    -- ────────────────────────────────────────────────────────────
    -- LOAD: Police Districts
    -- ────────────────────────────────────────────────────────────
    BEGIN
        DECLARE EXIT HANDLER FOR SQLEXCEPTION
        BEGIN
            INSERT INTO meta.file_ingestion_log
                (run_id, source_file_name, source_file_path, target_table, zone, row_count, status, error_message)
            VALUES
                (v_run_id, 'Police_Districts.csv',
                 '/Volumes/chicago_crimes_workspace/bronze/chicago_data/Police_Districts.csv',
                 'bronze.dim_districts_raw', 'BRONZE', 0, 'FAILED', 'Error loading file - check file path and format');
            SET v_error_count = v_error_count + 1;
        END;
        
        INSERT INTO bronze.dim_districts_raw
        SELECT
            `the_geom`    AS the_geom,
            `DIST_LABEL`  AS dist_label,
            `DIST_NUM`    AS dist_num,
            'Police_Districts.csv'                                         AS _source_file,
            '/Volumes/chicago_crimes_workspace/bronze/chicago_data/Police_Districts.csv' AS _source_path,
            current_timestamp()                                                 AS _ingest_ts,
            'bronze.dim_districts_raw'                                          AS _bronze_table
        FROM read_files(
            '/Volumes/chicago_crimes_workspace/bronze/chicago_data/Police_Districts.csv',
            format => 'csv',
            header => true,
            mode => 'PERMISSIVE'
        );
        
        SET v_row_count = (SELECT COUNT(*) FROM bronze.dim_districts_raw);
        SET v_total_rows = v_total_rows + v_row_count;
        
        INSERT INTO meta.file_ingestion_log
            (run_id, source_file_name, source_file_path, target_table, zone, row_count, status)
        VALUES
            (v_run_id, 'Police_Districts.csv',
             '/Volumes/chicago_crimes_workspace/bronze/chicago_data/Police_Districts.csv',
             'bronze.dim_districts_raw', 'BRONZE', v_row_count, 'SUCCESS');
    END;

    -- ────────────────────────────────────────────────────────────
    -- LOAD: Community Areas
    -- ────────────────────────────────────────────────────────────
    BEGIN
        DECLARE EXIT HANDLER FOR SQLEXCEPTION
        BEGIN
            INSERT INTO meta.file_ingestion_log
                (run_id, source_file_name, source_file_path, target_table, zone, row_count, status, error_message)
            VALUES
                (v_run_id, 'Community_Areas.csv',
                 '/Volumes/chicago_crimes_workspace/bronze/chicago_data/Community_Areas.csv',
                 'bronze.dim_community_areas_raw', 'BRONZE', 0, 'FAILED', 'Error loading file - check file path and format');
            SET v_error_count = v_error_count + 1;
        END;
        
        INSERT INTO bronze.dim_community_areas_raw
        SELECT
            `the_geom`    AS the_geom,
            `AREA_NUMBE`  AS area_numbe,
            `COMMUNITY`   AS community,
            `AREA_NUM_1`  AS area_num_1,
            `SHAPE_AREA`  AS shape_area,
            `SHAPE_LEN`   AS shape_len,
            'Community_Areas.csv'                                           AS _source_file,
            '/Volumes/chicago_crimes_workspace/bronze/chicago_data/Community_Areas.csv' AS _source_path,
            current_timestamp()                                                        AS _ingest_ts,
            'bronze.dim_community_areas_raw'                                           AS _bronze_table
        FROM read_files(
            '/Volumes/chicago_crimes_workspace/bronze/chicago_data/Community_Areas.csv',
            format => 'csv',
            header => true,
            mode => 'PERMISSIVE'
        );
        
        SET v_row_count = (SELECT COUNT(*) FROM bronze.dim_community_areas_raw);
        SET v_total_rows = v_total_rows + v_row_count;
        
        INSERT INTO meta.file_ingestion_log
            (run_id, source_file_name, source_file_path, target_table, zone, row_count, status)
        VALUES
            (v_run_id, 'Community_Areas.csv',
             '/Volumes/chicago_crimes_workspace/bronze/chicago_data/Community_Areas.csv',
             'bronze.dim_community_areas_raw', 'BRONZE', v_row_count, 'SUCCESS');
    END;

    -- ────────────────────────────────────────────────────────────
    -- Step Final: Close the ETL run log
    -- ────────────────────────────────────────────────────────────
    UPDATE meta.etl_run_log
    SET
        end_ts         = current_timestamp(),
        status         = CASE WHEN v_error_count = 0 THEN 'SUCCESS' ELSE 'PARTIAL_SUCCESS' END,
        rows_processed = v_total_rows,
        error_message  = CASE WHEN v_error_count > 0 
                             THEN CONCAT(CAST(v_error_count AS STRING), ' file(s) failed to load')
                             ELSE NULL END
    WHERE run_id = v_run_id;

END;

-- ============================================================
-- VERIFICATION QUERIES (Run these AFTER calling the procedure)
-- ============================================================

-- To execute the procedure:
CALL chicago_crimes_workspace.meta.load_bronze_data();

-- Then verify results:
SELECT 'BRONZE ROW COUNTS' AS check_name;

SELECT 'crimes_2001_2004' AS tbl, COUNT(*) AS rows FROM bronze.crimes_2001_2004
UNION ALL
SELECT 'crimes_2005_2007',        COUNT(*) FROM bronze.crimes_2005_2007
UNION ALL
SELECT 'crimes_2008_2011',        COUNT(*) FROM bronze.crimes_2008_2011
UNION ALL
SELECT 'crimes_2012_2017',        COUNT(*) FROM bronze.crimes_2012_2017
UNION ALL
SELECT 'dim_wards_raw',           COUNT(*) FROM bronze.dim_wards_raw
UNION ALL
SELECT 'dim_districts_raw',       COUNT(*) FROM bronze.dim_districts_raw
UNION ALL
SELECT 'dim_community_areas_raw', COUNT(*) FROM bronze.dim_community_areas_raw;

SELECT 'FILE INGESTION LOG' AS check_name;

SELECT *
FROM meta.file_ingestion_log
ORDER BY ingestion_id DESC
LIMIT 20;

SELECT 'ETL RUN LOG' AS check_name;

SELECT *
FROM meta.etl_run_log
ORDER BY run_id DESC
LIMIT 10;


