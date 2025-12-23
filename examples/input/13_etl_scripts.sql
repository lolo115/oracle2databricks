-- ============================================================================
-- File: 13_etl_scripts.sql
-- Description: ETL patterns, data transformation, and data warehouse operations
-- ============================================================================

-- -----------------------------------------------------------------------------
-- 1. Staging Table Load
-- -----------------------------------------------------------------------------

-- Truncate and load staging
TRUNCATE TABLE stg_employees;

INSERT INTO stg_employees (
    employee_id, first_name, last_name, email, hire_date, job_id, salary, department_id, load_date
)
SELECT 
    employee_id,
    TRIM(UPPER(first_name)),
    TRIM(UPPER(last_name)),
    LOWER(email),
    hire_date,
    job_id,
    salary,
    department_id,
    SYSDATE
FROM source_employees@source_db;

COMMIT;

-- Load with data cleansing
INSERT INTO stg_customers (
    customer_id, customer_name, email, phone, address, load_date, source_system
)
SELECT 
    customer_id,
    -- Clean name
    INITCAP(TRIM(REGEXP_REPLACE(customer_name, '\s+', ' '))),
    -- Validate and clean email
    CASE 
        WHEN REGEXP_LIKE(email, '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$')
        THEN LOWER(TRIM(email))
        ELSE NULL
    END,
    -- Format phone
    REGEXP_REPLACE(phone, '[^0-9]', ''),
    -- Clean address
    TRIM(address),
    SYSDATE,
    'CRM'
FROM source_customers@crm_db
WHERE customer_id IS NOT NULL;

COMMIT;

-- -----------------------------------------------------------------------------
-- 2. Slowly Changing Dimension Type 1 (Overwrite)
-- -----------------------------------------------------------------------------

-- SCD Type 1 - Update in place
MERGE INTO dim_customer t
USING stg_customers s
ON (t.customer_id = s.customer_id)
WHEN MATCHED THEN
    UPDATE SET
        t.customer_name = s.customer_name,
        t.email = s.email,
        t.phone = s.phone,
        t.address = s.address,
        t.last_updated = SYSDATE
WHEN NOT MATCHED THEN
    INSERT (customer_key, customer_id, customer_name, email, phone, address, 
            created_date, last_updated)
    VALUES (dim_customer_seq.NEXTVAL, s.customer_id, s.customer_name, s.email, 
            s.phone, s.address, SYSDATE, SYSDATE);

COMMIT;

-- -----------------------------------------------------------------------------
-- 3. Slowly Changing Dimension Type 2 (Historical)
-- -----------------------------------------------------------------------------

-- SCD Type 2 - Close existing records for changed rows
UPDATE dim_product t
SET 
    t.end_date = SYSDATE - 1,
    t.is_current = 'N',
    t.last_updated = SYSDATE
WHERE t.is_current = 'Y'
AND EXISTS (
    SELECT 1 FROM stg_products s
    WHERE s.product_id = t.product_id
    AND (
        NVL(s.product_name, '~') <> NVL(t.product_name, '~') OR
        NVL(s.category, '~') <> NVL(t.category, '~') OR
        NVL(s.price, -1) <> NVL(t.price, -1)
    )
);

-- SCD Type 2 - Insert new versions for changed rows
INSERT INTO dim_product (
    product_key, product_id, product_name, category, price,
    start_date, end_date, is_current, version, created_date
)
SELECT 
    dim_product_seq.NEXTVAL,
    s.product_id,
    s.product_name,
    s.category,
    s.price,
    SYSDATE,
    DATE '9999-12-31',
    'Y',
    NVL((SELECT MAX(version) + 1 FROM dim_product WHERE product_id = s.product_id), 1),
    SYSDATE
FROM stg_products s
WHERE EXISTS (
    SELECT 1 FROM dim_product t
    WHERE t.product_id = s.product_id
    AND t.is_current = 'N'
    AND t.end_date = SYSDATE - 1
);

-- SCD Type 2 - Insert completely new products
INSERT INTO dim_product (
    product_key, product_id, product_name, category, price,
    start_date, end_date, is_current, version, created_date
)
SELECT 
    dim_product_seq.NEXTVAL,
    s.product_id,
    s.product_name,
    s.category,
    s.price,
    SYSDATE,
    DATE '9999-12-31',
    'Y',
    1,
    SYSDATE
FROM stg_products s
WHERE NOT EXISTS (
    SELECT 1 FROM dim_product t WHERE t.product_id = s.product_id
);

COMMIT;

-- -----------------------------------------------------------------------------
-- 4. Slowly Changing Dimension Type 3 (Previous Value)
-- -----------------------------------------------------------------------------

-- SCD Type 3 - Keep current and previous values
MERGE INTO dim_employee t
USING stg_employees s
ON (t.employee_id = s.employee_id)
WHEN MATCHED THEN
    UPDATE SET
        t.previous_department = CASE 
            WHEN t.current_department <> s.department_id 
            THEN t.current_department 
            ELSE t.previous_department 
        END,
        t.department_change_date = CASE 
            WHEN t.current_department <> s.department_id 
            THEN SYSDATE 
            ELSE t.department_change_date 
        END,
        t.current_department = s.department_id,
        t.last_updated = SYSDATE
WHEN NOT MATCHED THEN
    INSERT (employee_key, employee_id, employee_name, current_department,
            previous_department, department_change_date, created_date, last_updated)
    VALUES (dim_employee_seq.NEXTVAL, s.employee_id, 
            s.first_name || ' ' || s.last_name, s.department_id,
            NULL, NULL, SYSDATE, SYSDATE);

COMMIT;

-- -----------------------------------------------------------------------------
-- 5. Fact Table Load
-- -----------------------------------------------------------------------------

-- Simple fact table load
INSERT INTO fact_sales (
    sale_key,
    date_key,
    customer_key,
    product_key,
    store_key,
    quantity,
    unit_price,
    discount_amount,
    total_amount,
    load_date
)
SELECT 
    fact_sales_seq.NEXTVAL,
    d.date_key,
    c.customer_key,
    p.product_key,
    st.store_key,
    s.quantity,
    s.unit_price,
    s.discount_amount,
    s.quantity * s.unit_price - s.discount_amount,
    SYSDATE
FROM stg_sales s
JOIN dim_date d ON TRUNC(s.sale_date) = d.calendar_date
JOIN dim_customer c ON s.customer_id = c.customer_id AND c.is_current = 'Y'
JOIN dim_product p ON s.product_id = p.product_id AND p.is_current = 'Y'
JOIN dim_store st ON s.store_id = st.store_id;

COMMIT;

-- Incremental fact load with deduplication
INSERT INTO fact_orders (
    order_key, date_key, customer_key, product_key, 
    quantity, amount, load_date
)
SELECT 
    fact_orders_seq.NEXTVAL,
    d.date_key,
    c.customer_key,
    p.product_key,
    s.quantity,
    s.amount,
    SYSDATE
FROM stg_orders s
JOIN dim_date d ON TRUNC(s.order_date) = d.calendar_date
JOIN dim_customer c ON s.customer_id = c.customer_id AND c.is_current = 'Y'
JOIN dim_product p ON s.product_id = p.product_id AND p.is_current = 'Y'
WHERE NOT EXISTS (
    -- Prevent duplicates
    SELECT 1 FROM fact_orders f
    WHERE f.source_order_id = s.order_id
);

COMMIT;

-- -----------------------------------------------------------------------------
-- 6. Data Aggregation for Summary Tables
-- -----------------------------------------------------------------------------

-- Daily sales summary
MERGE INTO sales_daily_summary t
USING (
    SELECT 
        TRUNC(sale_date) AS sale_date,
        store_id,
        product_category,
        COUNT(*) AS transaction_count,
        SUM(quantity) AS total_quantity,
        SUM(amount) AS total_amount,
        AVG(amount) AS avg_transaction
    FROM sales
    WHERE TRUNC(sale_date) = TRUNC(SYSDATE) - 1
    GROUP BY TRUNC(sale_date), store_id, product_category
) s
ON (t.sale_date = s.sale_date AND t.store_id = s.store_id AND t.product_category = s.product_category)
WHEN MATCHED THEN
    UPDATE SET
        t.transaction_count = s.transaction_count,
        t.total_quantity = s.total_quantity,
        t.total_amount = s.total_amount,
        t.avg_transaction = s.avg_transaction,
        t.last_updated = SYSDATE
WHEN NOT MATCHED THEN
    INSERT (sale_date, store_id, product_category, transaction_count,
            total_quantity, total_amount, avg_transaction, created_date)
    VALUES (s.sale_date, s.store_id, s.product_category, s.transaction_count,
            s.total_quantity, s.total_amount, s.avg_transaction, SYSDATE);

COMMIT;

-- Monthly rollup
INSERT INTO sales_monthly_summary (
    year_month, region, category,
    total_sales, total_quantity, customer_count,
    avg_order_value, created_date
)
SELECT 
    TO_CHAR(sale_date, 'YYYY-MM') AS year_month,
    region,
    category,
    SUM(amount) AS total_sales,
    SUM(quantity) AS total_quantity,
    COUNT(DISTINCT customer_id) AS customer_count,
    AVG(amount) AS avg_order_value,
    SYSDATE
FROM fact_sales f
JOIN dim_store s ON f.store_key = s.store_key
JOIN dim_product p ON f.product_key = p.product_key
WHERE f.sale_date >= TRUNC(ADD_MONTHS(SYSDATE, -1), 'MM')
AND f.sale_date < TRUNC(SYSDATE, 'MM')
GROUP BY TO_CHAR(sale_date, 'YYYY-MM'), region, category;

COMMIT;

-- -----------------------------------------------------------------------------
-- 7. Data Quality Checks
-- -----------------------------------------------------------------------------

-- Check for nulls in required fields
INSERT INTO etl_data_quality_log (
    check_date, table_name, check_type, check_description, 
    failed_count, check_query
)
SELECT 
    SYSDATE,
    'STG_CUSTOMERS',
    'NULL_CHECK',
    'Null customer_name',
    COUNT(*),
    'SELECT * FROM stg_customers WHERE customer_name IS NULL'
FROM stg_customers
WHERE customer_name IS NULL
HAVING COUNT(*) > 0;

-- Check for duplicates
INSERT INTO etl_data_quality_log (
    check_date, table_name, check_type, check_description,
    failed_count, check_query
)
SELECT 
    SYSDATE,
    'STG_ORDERS',
    'DUPLICATE_CHECK',
    'Duplicate order_id',
    COUNT(*) - COUNT(DISTINCT order_id),
    'SELECT order_id, COUNT(*) FROM stg_orders GROUP BY order_id HAVING COUNT(*) > 1'
FROM stg_orders
HAVING COUNT(*) > COUNT(DISTINCT order_id);

-- Check referential integrity
INSERT INTO etl_data_quality_log (
    check_date, table_name, check_type, check_description,
    failed_count, check_query
)
SELECT 
    SYSDATE,
    'STG_ORDERS',
    'REFERENTIAL_CHECK',
    'Invalid customer_id',
    COUNT(*),
    'SELECT * FROM stg_orders o WHERE NOT EXISTS (SELECT 1 FROM dim_customer c WHERE c.customer_id = o.customer_id)'
FROM stg_orders o
WHERE NOT EXISTS (
    SELECT 1 FROM dim_customer c WHERE c.customer_id = o.customer_id
)
HAVING COUNT(*) > 0;

-- Threshold check
INSERT INTO etl_data_quality_log (
    check_date, table_name, check_type, check_description,
    failed_count, check_query
)
SELECT 
    SYSDATE,
    'STG_ORDERS',
    'THRESHOLD_CHECK',
    'Order amount > $100,000',
    COUNT(*),
    'SELECT * FROM stg_orders WHERE amount > 100000'
FROM stg_orders
WHERE amount > 100000
HAVING COUNT(*) > 0;

COMMIT;

-- -----------------------------------------------------------------------------
-- 8. Change Data Capture (CDC)
-- -----------------------------------------------------------------------------

-- Process CDC records from change table
INSERT INTO target_customers (
    customer_id, customer_name, email, status,
    valid_from, valid_to, operation, load_date
)
SELECT 
    customer_id,
    customer_name,
    email,
    CASE operation 
        WHEN 'D' THEN 'DELETED'
        ELSE status
    END,
    change_timestamp,
    LEAD(change_timestamp, 1, DATE '9999-12-31') 
        OVER (PARTITION BY customer_id ORDER BY change_timestamp),
    operation,
    SYSDATE
FROM cdc_customers
WHERE change_timestamp > (
    SELECT NVL(MAX(valid_from), DATE '1900-01-01') 
    FROM target_customers
)
ORDER BY change_timestamp;

COMMIT;

-- -----------------------------------------------------------------------------
-- 9. Data Transformation Patterns
-- -----------------------------------------------------------------------------

-- Denormalization
INSERT INTO denorm_order_details (
    order_id, order_date, customer_id, customer_name, customer_email,
    product_id, product_name, category, quantity, unit_price, line_total,
    store_id, store_name, region, load_date
)
SELECT 
    o.order_id,
    o.order_date,
    c.customer_id,
    c.customer_name,
    c.email,
    p.product_id,
    p.product_name,
    p.category,
    od.quantity,
    od.unit_price,
    od.quantity * od.unit_price,
    s.store_id,
    s.store_name,
    s.region,
    SYSDATE
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
JOIN order_details od ON o.order_id = od.order_id
JOIN products p ON od.product_id = p.product_id
JOIN stores s ON o.store_id = s.store_id
WHERE o.order_date >= TRUNC(SYSDATE) - 1
AND o.order_date < TRUNC(SYSDATE);

-- Pivoting data for reporting
INSERT INTO monthly_sales_pivot (
    product_id, product_name, year,
    jan_sales, feb_sales, mar_sales, apr_sales, may_sales, jun_sales,
    jul_sales, aug_sales, sep_sales, oct_sales, nov_sales, dec_sales
)
SELECT * FROM (
    SELECT 
        p.product_id,
        p.product_name,
        EXTRACT(YEAR FROM s.sale_date) AS year,
        EXTRACT(MONTH FROM s.sale_date) AS month,
        SUM(s.amount) AS total_sales
    FROM sales s
    JOIN products p ON s.product_id = p.product_id
    GROUP BY p.product_id, p.product_name, 
             EXTRACT(YEAR FROM s.sale_date),
             EXTRACT(MONTH FROM s.sale_date)
)
PIVOT (
    SUM(total_sales)
    FOR month IN (
        1 AS jan_sales, 2 AS feb_sales, 3 AS mar_sales,
        4 AS apr_sales, 5 AS may_sales, 6 AS jun_sales,
        7 AS jul_sales, 8 AS aug_sales, 9 AS sep_sales,
        10 AS oct_sales, 11 AS nov_sales, 12 AS dec_sales
    )
);

-- Unpivoting data
INSERT INTO sales_by_channel (
    product_id, year, channel, sales_amount
)
SELECT product_id, year, channel, sales_amount
FROM yearly_channel_sales
UNPIVOT (
    sales_amount FOR channel IN (
        online_sales AS 'ONLINE',
        store_sales AS 'STORE',
        wholesale_sales AS 'WHOLESALE'
    )
);

-- -----------------------------------------------------------------------------
-- 10. Partition Management for ETL
-- -----------------------------------------------------------------------------

-- Add partition for new month
ALTER TABLE fact_sales ADD PARTITION p_202407 
VALUES LESS THAN (DATE '2024-08-01');

-- Exchange partition for fast loading
-- Create staging table with same structure
CREATE TABLE stg_sales_exchange AS
SELECT * FROM fact_sales WHERE 1 = 0;

-- Load data into staging
INSERT /*+ APPEND */ INTO stg_sales_exchange
SELECT * FROM external_sales_data;

-- Exchange partition
ALTER TABLE fact_sales 
EXCHANGE PARTITION p_202406 WITH TABLE stg_sales_exchange;

-- Drop old partition (archiving)
ALTER TABLE fact_sales DROP PARTITION p_202301;

-- Truncate partition for reload
ALTER TABLE fact_sales TRUNCATE PARTITION p_202406;

-- -----------------------------------------------------------------------------
-- 11. Error Handling in ETL
-- -----------------------------------------------------------------------------

-- Load with error logging
INSERT INTO target_orders (
    order_id, customer_id, order_date, amount
)
SELECT 
    order_id,
    customer_id,
    TO_DATE(order_date_str, 'YYYY-MM-DD'),
    TO_NUMBER(amount_str)
FROM stg_orders_text
LOG ERRORS INTO err$_target_orders ('ETL_' || TO_CHAR(SYSDATE, 'YYYYMMDD'))
REJECT LIMIT UNLIMITED;

COMMIT;

-- Move rejected records to error table
INSERT INTO etl_rejected_records (
    source_table, source_data, error_message, reject_date
)
SELECT 
    'STG_ORDERS_TEXT',
    order_id || '|' || customer_id || '|' || order_date_str || '|' || amount_str,
    ora_err_mesg$,
    SYSDATE
FROM err$_target_orders
WHERE ora_err_tag$ = 'ETL_' || TO_CHAR(SYSDATE, 'YYYYMMDD');

-- Clean up error log
DELETE FROM err$_target_orders 
WHERE ora_err_tag$ = 'ETL_' || TO_CHAR(SYSDATE, 'YYYYMMDD');

COMMIT;

-- -----------------------------------------------------------------------------
-- 12. ETL Audit and Logging
-- -----------------------------------------------------------------------------

-- Log ETL start
INSERT INTO etl_job_log (
    job_id, job_name, start_time, status, parameters
)
VALUES (
    etl_job_seq.NEXTVAL,
    'DAILY_SALES_LOAD',
    SYSDATE,
    'RUNNING',
    'LOAD_DATE=' || TO_CHAR(SYSDATE, 'YYYY-MM-DD')
);

COMMIT;

-- Update ETL completion
UPDATE etl_job_log
SET 
    end_time = SYSDATE,
    status = 'COMPLETED',
    rows_processed = (SELECT COUNT(*) FROM stg_sales WHERE load_date = TRUNC(SYSDATE)),
    rows_inserted = 1000,
    rows_updated = 50,
    rows_rejected = 5
WHERE job_name = 'DAILY_SALES_LOAD'
AND start_time = (SELECT MAX(start_time) FROM etl_job_log WHERE job_name = 'DAILY_SALES_LOAD');

COMMIT;

-- Record row counts
INSERT INTO etl_row_counts (
    load_date, table_name, row_count, captured_at
)
SELECT 
    TRUNC(SYSDATE),
    table_name,
    num_rows,
    SYSDATE
FROM user_tables
WHERE table_name IN ('FACT_SALES', 'DIM_CUSTOMER', 'DIM_PRODUCT', 'DIM_DATE');

COMMIT;

