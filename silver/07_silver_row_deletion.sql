USE CATALOG chicago_crimes_workspace;

-- Databricks uses DECLARE VARIABLE for session-level scripting
DECLARE VARIABLE rows_in BIGINT;
DECLARE VARIABLE rows_out BIGINT;

-- Step 1: Count BEFORE delete
SET VAR rows_in = (SELECT COUNT(*) FROM silver.crimes_typed);

-- Step 2: Delete invalid rows
DELETE FROM silver.crimes_typed
WHERE 
    community_area = 0 
    OR TRY_CAST(district AS INT) IN (13, 21);

-- Step 3: Count AFTER delete
SET VAR rows_out = (SELECT COUNT(*) FROM silver.crimes_typed);

-- Step 4: Log transformation
INSERT INTO meta.silver_transform_log 
    (run_id, step_name, source_table, target_table, 
     rows_in, rows_out, rows_dropped, notes)
SELECT 
    (SELECT MAX(run_id) FROM meta.etl_run_log 
     WHERE pipeline_name = 'silver_transform'),

    'STEP5_GEOGRAPHY_FILTER_DELETE',

    'silver.crimes_typed',
    'silver.crimes_typed',

    rows_in,
    rows_out,
    (rows_in - rows_out),

    'Deleted rows where community_area = 0 OR district IN (13,21). District cast using TRY_CAST from STRING.';



 