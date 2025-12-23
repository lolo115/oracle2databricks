-- ============================================================================
-- File: 25_complete_etl_package.sql
-- Description: Complete ETL package example with all components
-- ============================================================================

-- =============================================================================
-- SUPPORTING TABLES
-- =============================================================================

-- ETL Configuration Table
CREATE TABLE etl_config (
    config_key VARCHAR2(100) PRIMARY KEY,
    config_value VARCHAR2(4000),
    description VARCHAR2(500),
    created_date DATE DEFAULT SYSDATE,
    modified_date DATE
);

-- ETL Job Log Table
CREATE TABLE etl_job_log (
    job_id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    job_name VARCHAR2(100) NOT NULL,
    start_time TIMESTAMP DEFAULT SYSTIMESTAMP,
    end_time TIMESTAMP,
    status VARCHAR2(20) DEFAULT 'RUNNING',
    rows_processed NUMBER DEFAULT 0,
    rows_inserted NUMBER DEFAULT 0,
    rows_updated NUMBER DEFAULT 0,
    rows_rejected NUMBER DEFAULT 0,
    error_message VARCHAR2(4000),
    parameters CLOB
);

-- ETL Step Log Table
CREATE TABLE etl_step_log (
    step_id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    job_id NUMBER REFERENCES etl_job_log(job_id),
    step_name VARCHAR2(100),
    step_order NUMBER,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    status VARCHAR2(20),
    rows_affected NUMBER,
    error_message VARCHAR2(4000)
);

-- ETL Error Table
CREATE TABLE etl_errors (
    error_id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    job_id NUMBER,
    step_id NUMBER,
    source_table VARCHAR2(100),
    source_key VARCHAR2(500),
    error_code NUMBER,
    error_message VARCHAR2(4000),
    error_timestamp TIMESTAMP DEFAULT SYSTIMESTAMP,
    source_data CLOB
);

-- Data Quality Rules Table
CREATE TABLE etl_dq_rules (
    rule_id NUMBER PRIMARY KEY,
    rule_name VARCHAR2(100),
    rule_type VARCHAR2(50),  -- NULL_CHECK, RANGE_CHECK, LOOKUP_CHECK, etc.
    table_name VARCHAR2(100),
    column_name VARCHAR2(100),
    rule_sql VARCHAR2(4000),
    severity VARCHAR2(20),  -- ERROR, WARNING, INFO
    is_active CHAR(1) DEFAULT 'Y'
);

-- =============================================================================
-- ETL PACKAGE SPECIFICATION
-- =============================================================================

CREATE OR REPLACE PACKAGE etl_framework AS
    
    -- =========================================================================
    -- Constants
    -- =========================================================================
    c_status_running   CONSTANT VARCHAR2(20) := 'RUNNING';
    c_status_completed CONSTANT VARCHAR2(20) := 'COMPLETED';
    c_status_failed    CONSTANT VARCHAR2(20) := 'FAILED';
    c_status_warning   CONSTANT VARCHAR2(20) := 'WARNING';
    
    c_severity_error   CONSTANT VARCHAR2(20) := 'ERROR';
    c_severity_warning CONSTANT VARCHAR2(20) := 'WARNING';
    c_severity_info    CONSTANT VARCHAR2(20) := 'INFO';
    
    -- =========================================================================
    -- Types
    -- =========================================================================
    TYPE t_string_array IS TABLE OF VARCHAR2(4000) INDEX BY PLS_INTEGER;
    TYPE t_number_array IS TABLE OF NUMBER INDEX BY PLS_INTEGER;
    
    TYPE t_job_stats IS RECORD (
        rows_processed NUMBER := 0,
        rows_inserted NUMBER := 0,
        rows_updated NUMBER := 0,
        rows_rejected NUMBER := 0,
        start_time TIMESTAMP,
        end_time TIMESTAMP
    );
    
    TYPE t_dq_result IS RECORD (
        rule_id NUMBER,
        rule_name VARCHAR2(100),
        passed BOOLEAN,
        failed_count NUMBER,
        message VARCHAR2(4000)
    );
    
    TYPE t_dq_results IS TABLE OF t_dq_result INDEX BY PLS_INTEGER;
    
    -- =========================================================================
    -- Job Management
    -- =========================================================================
    FUNCTION start_job(
        p_job_name VARCHAR2,
        p_parameters VARCHAR2 DEFAULT NULL
    ) RETURN NUMBER;
    
    PROCEDURE end_job(
        p_job_id NUMBER,
        p_status VARCHAR2 DEFAULT c_status_completed,
        p_error_message VARCHAR2 DEFAULT NULL
    );
    
    PROCEDURE update_job_stats(
        p_job_id NUMBER,
        p_rows_processed NUMBER DEFAULT NULL,
        p_rows_inserted NUMBER DEFAULT NULL,
        p_rows_updated NUMBER DEFAULT NULL,
        p_rows_rejected NUMBER DEFAULT NULL
    );
    
    -- =========================================================================
    -- Step Management
    -- =========================================================================
    FUNCTION start_step(
        p_job_id NUMBER,
        p_step_name VARCHAR2,
        p_step_order NUMBER DEFAULT NULL
    ) RETURN NUMBER;
    
    PROCEDURE end_step(
        p_step_id NUMBER,
        p_status VARCHAR2 DEFAULT c_status_completed,
        p_rows_affected NUMBER DEFAULT NULL,
        p_error_message VARCHAR2 DEFAULT NULL
    );
    
    -- =========================================================================
    -- Error Handling
    -- =========================================================================
    PROCEDURE log_error(
        p_job_id NUMBER,
        p_step_id NUMBER DEFAULT NULL,
        p_source_table VARCHAR2 DEFAULT NULL,
        p_source_key VARCHAR2 DEFAULT NULL,
        p_error_code NUMBER DEFAULT NULL,
        p_error_message VARCHAR2 DEFAULT NULL,
        p_source_data VARCHAR2 DEFAULT NULL
    );
    
    -- =========================================================================
    -- Data Quality
    -- =========================================================================
    FUNCTION run_dq_checks(
        p_job_id NUMBER,
        p_table_name VARCHAR2
    ) RETURN t_dq_results;
    
    -- =========================================================================
    -- ETL Operations
    -- =========================================================================
    PROCEDURE truncate_table(
        p_table_name VARCHAR2,
        p_preserve_stats BOOLEAN DEFAULT TRUE
    );
    
    PROCEDURE exchange_partition(
        p_target_table VARCHAR2,
        p_partition_name VARCHAR2,
        p_staging_table VARCHAR2
    );
    
    FUNCTION scd_type1_merge(
        p_target_table VARCHAR2,
        p_source_table VARCHAR2,
        p_key_columns VARCHAR2,
        p_update_columns VARCHAR2
    ) RETURN t_job_stats;
    
    FUNCTION scd_type2_merge(
        p_target_table VARCHAR2,
        p_source_table VARCHAR2,
        p_key_columns VARCHAR2,
        p_track_columns VARCHAR2
    ) RETURN t_job_stats;
    
    -- =========================================================================
    -- Utilities
    -- =========================================================================
    FUNCTION get_config(p_key VARCHAR2) RETURN VARCHAR2;
    PROCEDURE set_config(p_key VARCHAR2, p_value VARCHAR2);
    FUNCTION table_exists(p_table_name VARCHAR2) RETURN BOOLEAN;
    FUNCTION get_row_count(p_table_name VARCHAR2) RETURN NUMBER;
    
END etl_framework;
/

-- =============================================================================
-- ETL PACKAGE BODY
-- =============================================================================

CREATE OR REPLACE PACKAGE BODY etl_framework AS
    
    -- Private variables
    g_debug_mode BOOLEAN := FALSE;
    g_current_job_id NUMBER;
    
    -- =========================================================================
    -- Private Procedures
    -- =========================================================================
    
    PROCEDURE debug_log(p_message VARCHAR2) IS
    BEGIN
        IF g_debug_mode THEN
            DBMS_OUTPUT.PUT_LINE(TO_CHAR(SYSTIMESTAMP, 'HH24:MI:SS.FF3') || ' DEBUG: ' || p_message);
        END IF;
    END debug_log;
    
    -- =========================================================================
    -- Job Management Implementation
    -- =========================================================================
    
    FUNCTION start_job(
        p_job_name VARCHAR2,
        p_parameters VARCHAR2 DEFAULT NULL
    ) RETURN NUMBER
    IS
        PRAGMA AUTONOMOUS_TRANSACTION;
        v_job_id NUMBER;
    BEGIN
        INSERT INTO etl_job_log (job_name, parameters)
        VALUES (p_job_name, p_parameters)
        RETURNING job_id INTO v_job_id;
        
        COMMIT;
        
        g_current_job_id := v_job_id;
        debug_log('Started job ' || v_job_id || ': ' || p_job_name);
        
        RETURN v_job_id;
    END start_job;
    
    PROCEDURE end_job(
        p_job_id NUMBER,
        p_status VARCHAR2 DEFAULT c_status_completed,
        p_error_message VARCHAR2 DEFAULT NULL
    )
    IS
        PRAGMA AUTONOMOUS_TRANSACTION;
    BEGIN
        UPDATE etl_job_log
        SET end_time = SYSTIMESTAMP,
            status = p_status,
            error_message = SUBSTR(p_error_message, 1, 4000)
        WHERE job_id = p_job_id;
        
        COMMIT;
        
        debug_log('Ended job ' || p_job_id || ' with status: ' || p_status);
    END end_job;
    
    PROCEDURE update_job_stats(
        p_job_id NUMBER,
        p_rows_processed NUMBER DEFAULT NULL,
        p_rows_inserted NUMBER DEFAULT NULL,
        p_rows_updated NUMBER DEFAULT NULL,
        p_rows_rejected NUMBER DEFAULT NULL
    )
    IS
        PRAGMA AUTONOMOUS_TRANSACTION;
    BEGIN
        UPDATE etl_job_log
        SET rows_processed = NVL(p_rows_processed, rows_processed),
            rows_inserted = NVL(p_rows_inserted, rows_inserted),
            rows_updated = NVL(p_rows_updated, rows_updated),
            rows_rejected = NVL(p_rows_rejected, rows_rejected)
        WHERE job_id = p_job_id;
        
        COMMIT;
    END update_job_stats;
    
    -- =========================================================================
    -- Step Management Implementation
    -- =========================================================================
    
    FUNCTION start_step(
        p_job_id NUMBER,
        p_step_name VARCHAR2,
        p_step_order NUMBER DEFAULT NULL
    ) RETURN NUMBER
    IS
        PRAGMA AUTONOMOUS_TRANSACTION;
        v_step_id NUMBER;
        v_order NUMBER;
    BEGIN
        IF p_step_order IS NULL THEN
            SELECT NVL(MAX(step_order), 0) + 1 INTO v_order
            FROM etl_step_log
            WHERE job_id = p_job_id;
        ELSE
            v_order := p_step_order;
        END IF;
        
        INSERT INTO etl_step_log (job_id, step_name, step_order, start_time, status)
        VALUES (p_job_id, p_step_name, v_order, SYSTIMESTAMP, c_status_running)
        RETURNING step_id INTO v_step_id;
        
        COMMIT;
        
        debug_log('Started step ' || v_step_id || ': ' || p_step_name);
        
        RETURN v_step_id;
    END start_step;
    
    PROCEDURE end_step(
        p_step_id NUMBER,
        p_status VARCHAR2 DEFAULT c_status_completed,
        p_rows_affected NUMBER DEFAULT NULL,
        p_error_message VARCHAR2 DEFAULT NULL
    )
    IS
        PRAGMA AUTONOMOUS_TRANSACTION;
    BEGIN
        UPDATE etl_step_log
        SET end_time = SYSTIMESTAMP,
            status = p_status,
            rows_affected = p_rows_affected,
            error_message = SUBSTR(p_error_message, 1, 4000)
        WHERE step_id = p_step_id;
        
        COMMIT;
        
        debug_log('Ended step ' || p_step_id || ' with status: ' || p_status);
    END end_step;
    
    -- =========================================================================
    -- Error Handling Implementation
    -- =========================================================================
    
    PROCEDURE log_error(
        p_job_id NUMBER,
        p_step_id NUMBER DEFAULT NULL,
        p_source_table VARCHAR2 DEFAULT NULL,
        p_source_key VARCHAR2 DEFAULT NULL,
        p_error_code NUMBER DEFAULT NULL,
        p_error_message VARCHAR2 DEFAULT NULL,
        p_source_data VARCHAR2 DEFAULT NULL
    )
    IS
        PRAGMA AUTONOMOUS_TRANSACTION;
    BEGIN
        INSERT INTO etl_errors (
            job_id, step_id, source_table, source_key,
            error_code, error_message, source_data
        ) VALUES (
            p_job_id, p_step_id, p_source_table, p_source_key,
            NVL(p_error_code, SQLCODE),
            NVL(p_error_message, SQLERRM),
            p_source_data
        );
        
        COMMIT;
    END log_error;
    
    -- =========================================================================
    -- Data Quality Implementation
    -- =========================================================================
    
    FUNCTION run_dq_checks(
        p_job_id NUMBER,
        p_table_name VARCHAR2
    ) RETURN t_dq_results
    IS
        v_results t_dq_results;
        v_result t_dq_result;
        v_count NUMBER;
        v_step_id NUMBER;
        v_idx NUMBER := 0;
    BEGIN
        v_step_id := start_step(p_job_id, 'DATA_QUALITY_CHECKS');
        
        FOR rule IN (
            SELECT rule_id, rule_name, rule_type, rule_sql, severity
            FROM etl_dq_rules
            WHERE table_name = p_table_name
            AND is_active = 'Y'
            ORDER BY rule_id
        ) LOOP
            BEGIN
                -- Execute the rule SQL (should return count of violations)
                EXECUTE IMMEDIATE rule.rule_sql INTO v_count;
                
                v_idx := v_idx + 1;
                v_result.rule_id := rule.rule_id;
                v_result.rule_name := rule.rule_name;
                v_result.failed_count := v_count;
                v_result.passed := (v_count = 0);
                v_result.message := CASE 
                    WHEN v_count = 0 THEN 'Passed'
                    ELSE 'Failed: ' || v_count || ' violations'
                END;
                
                v_results(v_idx) := v_result;
                
                -- Log if failed
                IF v_count > 0 AND rule.severity = c_severity_error THEN
                    log_error(
                        p_job_id => p_job_id,
                        p_step_id => v_step_id,
                        p_source_table => p_table_name,
                        p_error_message => 'DQ Rule ' || rule.rule_name || ' failed with ' || v_count || ' violations'
                    );
                END IF;
                
            EXCEPTION
                WHEN OTHERS THEN
                    v_idx := v_idx + 1;
                    v_result.rule_id := rule.rule_id;
                    v_result.rule_name := rule.rule_name;
                    v_result.passed := FALSE;
                    v_result.failed_count := -1;
                    v_result.message := 'Error executing rule: ' || SQLERRM;
                    v_results(v_idx) := v_result;
            END;
        END LOOP;
        
        end_step(v_step_id, c_status_completed, v_idx);
        
        RETURN v_results;
    END run_dq_checks;
    
    -- =========================================================================
    -- ETL Operations Implementation
    -- =========================================================================
    
    PROCEDURE truncate_table(
        p_table_name VARCHAR2,
        p_preserve_stats BOOLEAN DEFAULT TRUE
    )
    IS
        v_sql VARCHAR2(500);
    BEGIN
        v_sql := 'TRUNCATE TABLE ' || DBMS_ASSERT.SQL_OBJECT_NAME(p_table_name);
        IF NOT p_preserve_stats THEN
            v_sql := v_sql || ' DROP STORAGE';
        ELSE
            v_sql := v_sql || ' REUSE STORAGE';
        END IF;
        
        EXECUTE IMMEDIATE v_sql;
        
        debug_log('Truncated table: ' || p_table_name);
    END truncate_table;
    
    PROCEDURE exchange_partition(
        p_target_table VARCHAR2,
        p_partition_name VARCHAR2,
        p_staging_table VARCHAR2
    )
    IS
        v_sql VARCHAR2(1000);
    BEGIN
        v_sql := 'ALTER TABLE ' || DBMS_ASSERT.SQL_OBJECT_NAME(p_target_table) ||
                 ' EXCHANGE PARTITION ' || DBMS_ASSERT.SIMPLE_SQL_NAME(p_partition_name) ||
                 ' WITH TABLE ' || DBMS_ASSERT.SQL_OBJECT_NAME(p_staging_table) ||
                 ' WITHOUT VALIDATION';
        
        EXECUTE IMMEDIATE v_sql;
        
        debug_log('Exchanged partition ' || p_partition_name);
    END exchange_partition;
    
    FUNCTION scd_type1_merge(
        p_target_table VARCHAR2,
        p_source_table VARCHAR2,
        p_key_columns VARCHAR2,
        p_update_columns VARCHAR2
    ) RETURN t_job_stats
    IS
        v_stats t_job_stats;
        v_sql CLOB;
        v_keys t_string_array;
        v_updates t_string_array;
        v_on_clause VARCHAR2(4000);
        v_update_clause VARCHAR2(4000);
        v_insert_cols VARCHAR2(4000);
        v_insert_vals VARCHAR2(4000);
    BEGIN
        v_stats.start_time := SYSTIMESTAMP;
        
        -- Parse key columns
        SELECT TRIM(REGEXP_SUBSTR(p_key_columns, '[^,]+', 1, LEVEL))
        BULK COLLECT INTO v_keys
        FROM DUAL
        CONNECT BY LEVEL <= REGEXP_COUNT(p_key_columns, ',') + 1;
        
        -- Build ON clause
        FOR i IN 1..v_keys.COUNT LOOP
            v_on_clause := v_on_clause || 
                CASE WHEN i > 1 THEN ' AND ' END ||
                't.' || v_keys(i) || ' = s.' || v_keys(i);
        END LOOP;
        
        -- Parse update columns
        SELECT TRIM(REGEXP_SUBSTR(p_update_columns, '[^,]+', 1, LEVEL))
        BULK COLLECT INTO v_updates
        FROM DUAL
        CONNECT BY LEVEL <= REGEXP_COUNT(p_update_columns, ',') + 1;
        
        -- Build UPDATE clause
        FOR i IN 1..v_updates.COUNT LOOP
            v_update_clause := v_update_clause ||
                CASE WHEN i > 1 THEN ', ' END ||
                't.' || v_updates(i) || ' = s.' || v_updates(i);
        END LOOP;
        
        -- Build INSERT columns and values
        v_insert_cols := p_key_columns || ',' || p_update_columns;
        v_insert_vals := REGEXP_REPLACE(v_insert_cols, '([^,]+)', 's.\1');
        
        -- Build and execute MERGE
        v_sql := 'MERGE INTO ' || p_target_table || ' t ' ||
                 'USING ' || p_source_table || ' s ' ||
                 'ON (' || v_on_clause || ') ' ||
                 'WHEN MATCHED THEN UPDATE SET ' || v_update_clause || ', t.last_updated = SYSDATE ' ||
                 'WHEN NOT MATCHED THEN INSERT (' || v_insert_cols || ', created_date, last_updated) ' ||
                 'VALUES (' || v_insert_vals || ', SYSDATE, SYSDATE)';
        
        debug_log('Executing MERGE: ' || SUBSTR(v_sql, 1, 200) || '...');
        
        EXECUTE IMMEDIATE v_sql;
        
        v_stats.rows_processed := SQL%ROWCOUNT;
        v_stats.end_time := SYSTIMESTAMP;
        
        RETURN v_stats;
    END scd_type1_merge;
    
    FUNCTION scd_type2_merge(
        p_target_table VARCHAR2,
        p_source_table VARCHAR2,
        p_key_columns VARCHAR2,
        p_track_columns VARCHAR2
    ) RETURN t_job_stats
    IS
        v_stats t_job_stats;
        v_step_id NUMBER;
    BEGIN
        v_stats.start_time := SYSTIMESTAMP;
        
        -- Step 1: Close changed records
        EXECUTE IMMEDIATE 
            'UPDATE ' || p_target_table || ' t ' ||
            'SET end_date = SYSDATE - 1, is_current = ''N'', last_updated = SYSDATE ' ||
            'WHERE is_current = ''Y'' ' ||
            'AND EXISTS ( ' ||
            '  SELECT 1 FROM ' || p_source_table || ' s ' ||
            '  WHERE s.' || REPLACE(p_key_columns, ',', ' = t.' || ' AND s.') || ' = t.' || 
                   SUBSTR(p_key_columns, INSTR(p_key_columns, ',', -1) + 1) ||
            '  AND (' || REPLACE(p_track_columns, ',', ' <> t.' || ' OR s.') || ' <> t.' ||
                   SUBSTR(p_track_columns, INSTR(p_track_columns, ',', -1) + 1) || '))';
        
        v_stats.rows_updated := SQL%ROWCOUNT;
        
        -- Step 2: Insert new records and changed records
        EXECUTE IMMEDIATE
            'INSERT INTO ' || p_target_table || ' (' ||
            p_key_columns || ',' || p_track_columns || 
            ',start_date, end_date, is_current, version, created_date, last_updated) ' ||
            'SELECT s.' || REPLACE(p_key_columns || ',' || p_track_columns, ',', ', s.') ||
            ', SYSDATE, DATE ''9999-12-31'', ''Y'' ' ||
            ', NVL((SELECT MAX(version) + 1 FROM ' || p_target_table || ' t2 WHERE t2.' ||
                   REPLACE(p_key_columns, ',', ' = s.' || ' AND t2.') || ' = s.' ||
                   SUBSTR(p_key_columns, INSTR(p_key_columns, ',', -1) + 1) || '), 1) ' ||
            ', SYSDATE, SYSDATE ' ||
            'FROM ' || p_source_table || ' s ' ||
            'WHERE NOT EXISTS ( ' ||
            '  SELECT 1 FROM ' || p_target_table || ' t ' ||
            '  WHERE t.' || REPLACE(p_key_columns, ',', ' = s.' || ' AND t.') || ' = s.' ||
                   SUBSTR(p_key_columns, INSTR(p_key_columns, ',', -1) + 1) ||
            '  AND t.is_current = ''Y'')';
        
        v_stats.rows_inserted := SQL%ROWCOUNT;
        v_stats.rows_processed := v_stats.rows_inserted + v_stats.rows_updated;
        v_stats.end_time := SYSTIMESTAMP;
        
        RETURN v_stats;
    END scd_type2_merge;
    
    -- =========================================================================
    -- Utilities Implementation
    -- =========================================================================
    
    FUNCTION get_config(p_key VARCHAR2) RETURN VARCHAR2
    IS
        v_value VARCHAR2(4000);
    BEGIN
        SELECT config_value INTO v_value
        FROM etl_config
        WHERE config_key = p_key;
        
        RETURN v_value;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            RETURN NULL;
    END get_config;
    
    PROCEDURE set_config(p_key VARCHAR2, p_value VARCHAR2)
    IS
    BEGIN
        MERGE INTO etl_config t
        USING (SELECT p_key AS config_key, p_value AS config_value FROM DUAL) s
        ON (t.config_key = s.config_key)
        WHEN MATCHED THEN
            UPDATE SET config_value = s.config_value, modified_date = SYSDATE
        WHEN NOT MATCHED THEN
            INSERT (config_key, config_value) VALUES (s.config_key, s.config_value);
        
        COMMIT;
    END set_config;
    
    FUNCTION table_exists(p_table_name VARCHAR2) RETURN BOOLEAN
    IS
        v_count NUMBER;
    BEGIN
        SELECT COUNT(*) INTO v_count
        FROM user_tables
        WHERE table_name = UPPER(p_table_name);
        
        RETURN v_count > 0;
    END table_exists;
    
    FUNCTION get_row_count(p_table_name VARCHAR2) RETURN NUMBER
    IS
        v_count NUMBER;
    BEGIN
        EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM ' || DBMS_ASSERT.SQL_OBJECT_NAME(p_table_name)
        INTO v_count;
        
        RETURN v_count;
    END get_row_count;
    
END etl_framework;
/

-- =============================================================================
-- SAMPLE ETL JOB USING THE FRAMEWORK
-- =============================================================================

CREATE OR REPLACE PROCEDURE etl_daily_sales_load
IS
    v_job_id NUMBER;
    v_step_id NUMBER;
    v_stats etl_framework.t_job_stats;
    v_dq_results etl_framework.t_dq_results;
    v_load_date DATE := TRUNC(SYSDATE) - 1;
    v_error_count NUMBER := 0;
BEGIN
    -- Start job
    v_job_id := etl_framework.start_job(
        p_job_name => 'DAILY_SALES_LOAD',
        p_parameters => 'load_date=' || TO_CHAR(v_load_date, 'YYYY-MM-DD')
    );
    
    BEGIN
        -- =====================================================================
        -- Step 1: Extract - Load staging table
        -- =====================================================================
        v_step_id := etl_framework.start_step(v_job_id, 'EXTRACT_TO_STAGING');
        
        -- Truncate staging
        etl_framework.truncate_table('STG_SALES');
        
        -- Load from source
        INSERT /*+ APPEND */ INTO stg_sales (
            sale_id, sale_date, customer_id, product_id,
            quantity, unit_price, discount_pct, load_date
        )
        SELECT 
            sale_id,
            sale_date,
            customer_id,
            product_id,
            quantity,
            unit_price,
            discount_pct,
            SYSDATE
        FROM source_sales@source_db
        WHERE sale_date = v_load_date;
        
        v_stats.rows_inserted := SQL%ROWCOUNT;
        COMMIT;
        
        etl_framework.end_step(v_step_id, etl_framework.c_status_completed, v_stats.rows_inserted);
        
        -- =====================================================================
        -- Step 2: Data Quality Checks
        -- =====================================================================
        v_dq_results := etl_framework.run_dq_checks(v_job_id, 'STG_SALES');
        
        -- Check for critical failures
        FOR i IN 1..v_dq_results.COUNT LOOP
            IF NOT v_dq_results(i).passed THEN
                v_error_count := v_error_count + 1;
                DBMS_OUTPUT.PUT_LINE('DQ Failed: ' || v_dq_results(i).rule_name || 
                                    ' - ' || v_dq_results(i).message);
            END IF;
        END LOOP;
        
        IF v_error_count > 0 THEN
            RAISE_APPLICATION_ERROR(-20001, 'Data quality checks failed: ' || v_error_count || ' rules failed');
        END IF;
        
        -- =====================================================================
        -- Step 3: Transform - Clean and enrich data
        -- =====================================================================
        v_step_id := etl_framework.start_step(v_job_id, 'TRANSFORM');
        
        -- Calculate derived columns
        UPDATE stg_sales
        SET gross_amount = quantity * unit_price,
            discount_amount = quantity * unit_price * NVL(discount_pct, 0),
            net_amount = quantity * unit_price * (1 - NVL(discount_pct, 0));
        
        v_stats.rows_updated := SQL%ROWCOUNT;
        COMMIT;
        
        etl_framework.end_step(v_step_id, etl_framework.c_status_completed, v_stats.rows_updated);
        
        -- =====================================================================
        -- Step 4: Load - Merge into fact table
        -- =====================================================================
        v_step_id := etl_framework.start_step(v_job_id, 'LOAD_FACT_TABLE');
        
        MERGE INTO fact_sales t
        USING (
            SELECT 
                s.sale_id,
                d.date_key,
                c.customer_key,
                p.product_key,
                s.quantity,
                s.unit_price,
                s.discount_amount,
                s.net_amount
            FROM stg_sales s
            JOIN dim_date d ON s.sale_date = d.calendar_date
            JOIN dim_customer c ON s.customer_id = c.customer_id AND c.is_current = 'Y'
            JOIN dim_product p ON s.product_id = p.product_id AND p.is_current = 'Y'
        ) s
        ON (t.sale_id = s.sale_id)
        WHEN MATCHED THEN
            UPDATE SET 
                t.quantity = s.quantity,
                t.unit_price = s.unit_price,
                t.discount_amount = s.discount_amount,
                t.net_amount = s.net_amount,
                t.last_updated = SYSDATE
        WHEN NOT MATCHED THEN
            INSERT (sale_key, sale_id, date_key, customer_key, product_key,
                    quantity, unit_price, discount_amount, net_amount,
                    created_date, last_updated)
            VALUES (fact_sales_seq.NEXTVAL, s.sale_id, s.date_key, s.customer_key, s.product_key,
                    s.quantity, s.unit_price, s.discount_amount, s.net_amount,
                    SYSDATE, SYSDATE);
        
        v_stats.rows_processed := SQL%ROWCOUNT;
        COMMIT;
        
        etl_framework.end_step(v_step_id, etl_framework.c_status_completed, v_stats.rows_processed);
        
        -- =====================================================================
        -- Step 5: Post-load aggregation
        -- =====================================================================
        v_step_id := etl_framework.start_step(v_job_id, 'AGGREGATE_DAILY_SUMMARY');
        
        MERGE INTO sales_daily_summary t
        USING (
            SELECT 
                date_key,
                product_key,
                SUM(quantity) AS total_quantity,
                SUM(net_amount) AS total_amount,
                COUNT(*) AS transaction_count
            FROM fact_sales
            WHERE date_key = (SELECT date_key FROM dim_date WHERE calendar_date = v_load_date)
            GROUP BY date_key, product_key
        ) s
        ON (t.date_key = s.date_key AND t.product_key = s.product_key)
        WHEN MATCHED THEN
            UPDATE SET 
                t.total_quantity = s.total_quantity,
                t.total_amount = s.total_amount,
                t.transaction_count = s.transaction_count,
                t.last_updated = SYSDATE
        WHEN NOT MATCHED THEN
            INSERT (date_key, product_key, total_quantity, total_amount, 
                    transaction_count, created_date, last_updated)
            VALUES (s.date_key, s.product_key, s.total_quantity, s.total_amount,
                    s.transaction_count, SYSDATE, SYSDATE);
        
        v_stats.rows_updated := SQL%ROWCOUNT;
        COMMIT;
        
        etl_framework.end_step(v_step_id, etl_framework.c_status_completed, v_stats.rows_updated);
        
        -- =====================================================================
        -- Update job statistics and complete
        -- =====================================================================
        etl_framework.update_job_stats(
            p_job_id => v_job_id,
            p_rows_processed => etl_framework.get_row_count('STG_SALES'),
            p_rows_inserted => v_stats.rows_inserted,
            p_rows_updated => v_stats.rows_updated
        );
        
        etl_framework.end_job(v_job_id, etl_framework.c_status_completed);
        
        DBMS_OUTPUT.PUT_LINE('ETL Job completed successfully');
        
    EXCEPTION
        WHEN OTHERS THEN
            -- Log the error
            etl_framework.log_error(
                p_job_id => v_job_id,
                p_error_code => SQLCODE,
                p_error_message => SQLERRM || CHR(10) || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE
            );
            
            -- End the job with failure status
            etl_framework.end_job(v_job_id, etl_framework.c_status_failed, SQLERRM);
            
            -- Re-raise the exception
            RAISE;
    END;
    
END etl_daily_sales_load;
/

-- =============================================================================
-- SAMPLE DATA QUALITY RULES
-- =============================================================================

INSERT INTO etl_dq_rules (rule_id, rule_name, rule_type, table_name, column_name, rule_sql, severity)
VALUES (1, 'NULL_SALE_ID', 'NULL_CHECK', 'STG_SALES', 'SALE_ID',
        'SELECT COUNT(*) FROM stg_sales WHERE sale_id IS NULL', 'ERROR');

INSERT INTO etl_dq_rules (rule_id, rule_name, rule_type, table_name, column_name, rule_sql, severity)
VALUES (2, 'NEGATIVE_QUANTITY', 'RANGE_CHECK', 'STG_SALES', 'QUANTITY',
        'SELECT COUNT(*) FROM stg_sales WHERE quantity < 0', 'ERROR');

INSERT INTO etl_dq_rules (rule_id, rule_name, rule_type, table_name, column_name, rule_sql, severity)
VALUES (3, 'INVALID_CUSTOMER', 'LOOKUP_CHECK', 'STG_SALES', 'CUSTOMER_ID',
        'SELECT COUNT(*) FROM stg_sales s WHERE NOT EXISTS (SELECT 1 FROM dim_customer c WHERE c.customer_id = s.customer_id)', 'ERROR');

INSERT INTO etl_dq_rules (rule_id, rule_name, rule_type, table_name, column_name, rule_sql, severity)
VALUES (4, 'FUTURE_SALE_DATE', 'RANGE_CHECK', 'STG_SALES', 'SALE_DATE',
        'SELECT COUNT(*) FROM stg_sales WHERE sale_date > SYSDATE', 'WARNING');

COMMIT;

