-- ============================================================================
-- File: 06_date_functions.sql
-- Description: Date and timestamp functions and operations
-- ============================================================================

-- -----------------------------------------------------------------------------
-- 1. Current Date/Time Functions
-- -----------------------------------------------------------------------------

-- SYSDATE - current date and time (database server)
SELECT SYSDATE FROM DUAL;

-- SYSTIMESTAMP - current timestamp with timezone (database server)
SELECT SYSTIMESTAMP FROM DUAL;

-- CURRENT_DATE - current date (session timezone)
SELECT CURRENT_DATE FROM DUAL;

-- CURRENT_TIMESTAMP - current timestamp (session timezone)
SELECT CURRENT_TIMESTAMP FROM DUAL;

-- LOCALTIMESTAMP - local timestamp without timezone
SELECT LOCALTIMESTAMP FROM DUAL;

-- DBTIMEZONE and SESSIONTIMEZONE
SELECT DBTIMEZONE FROM DUAL;
SELECT SESSIONTIMEZONE FROM DUAL;

-- Comparing server vs session time
SELECT SYSDATE AS server_time,
       CURRENT_DATE AS session_time,
       SYSTIMESTAMP AS server_timestamp,
       CURRENT_TIMESTAMP AS session_timestamp
FROM DUAL;

-- -----------------------------------------------------------------------------
-- 2. Date Arithmetic
-- -----------------------------------------------------------------------------

-- Add/subtract days
SELECT SYSDATE + 7 AS next_week FROM DUAL;
SELECT SYSDATE - 30 AS thirty_days_ago FROM DUAL;
SELECT hire_date, hire_date + 90 AS probation_end FROM employees;

-- Add/subtract hours, minutes, seconds
SELECT SYSDATE + 1/24 AS plus_one_hour FROM DUAL;
SELECT SYSDATE + 1/24/60 AS plus_one_minute FROM DUAL;
SELECT SYSDATE + 1/24/60/60 AS plus_one_second FROM DUAL;
SELECT SYSDATE + 2.5/24 AS plus_2_5_hours FROM DUAL;

-- Difference between dates (returns days)
SELECT SYSDATE - hire_date AS days_employed FROM employees;
SELECT TRUNC(SYSDATE - hire_date) AS whole_days_employed FROM employees;

-- Convert days difference to other units
SELECT employee_id,
       hire_date,
       TRUNC(SYSDATE - hire_date) AS days,
       TRUNC((SYSDATE - hire_date) / 7) AS weeks,
       TRUNC((SYSDATE - hire_date) / 30) AS approx_months,
       TRUNC((SYSDATE - hire_date) / 365) AS approx_years
FROM employees;

-- -----------------------------------------------------------------------------
-- 3. ADD_MONTHS Function
-- -----------------------------------------------------------------------------

-- Add months
SELECT SYSDATE, ADD_MONTHS(SYSDATE, 1) AS next_month FROM DUAL;
SELECT SYSDATE, ADD_MONTHS(SYSDATE, 6) AS six_months_later FROM DUAL;
SELECT SYSDATE, ADD_MONTHS(SYSDATE, 12) AS next_year FROM DUAL;

-- Subtract months
SELECT SYSDATE, ADD_MONTHS(SYSDATE, -3) AS three_months_ago FROM DUAL;

-- Handle end-of-month dates
SELECT ADD_MONTHS(DATE '2024-01-31', 1) AS end_of_feb FROM DUAL;  -- Returns last day of Feb
SELECT ADD_MONTHS(DATE '2024-03-31', -1) AS end_of_feb FROM DUAL;

-- Calculate contract end dates
SELECT employee_id, hire_date,
       ADD_MONTHS(hire_date, 12) AS first_anniversary,
       ADD_MONTHS(hire_date, 60) AS five_year_anniversary
FROM employees;

-- -----------------------------------------------------------------------------
-- 4. MONTHS_BETWEEN Function
-- -----------------------------------------------------------------------------

-- Calculate months between dates
SELECT MONTHS_BETWEEN(SYSDATE, DATE '2020-01-01') AS months_since_2020 FROM DUAL;
SELECT MONTHS_BETWEEN(DATE '2024-06-15', DATE '2024-01-15') AS months_diff FROM DUAL;

-- Applied to employee data
SELECT employee_id, hire_date,
       MONTHS_BETWEEN(SYSDATE, hire_date) AS months_employed,
       TRUNC(MONTHS_BETWEEN(SYSDATE, hire_date)) AS whole_months,
       TRUNC(MONTHS_BETWEEN(SYSDATE, hire_date) / 12) AS years_employed
FROM employees;

-- -----------------------------------------------------------------------------
-- 5. NEXT_DAY and LAST_DAY Functions
-- -----------------------------------------------------------------------------

-- NEXT_DAY - find next occurrence of a weekday
SELECT SYSDATE, NEXT_DAY(SYSDATE, 'MONDAY') AS next_monday FROM DUAL;
SELECT SYSDATE, NEXT_DAY(SYSDATE, 'FRIDAY') AS next_friday FROM DUAL;
SELECT SYSDATE, NEXT_DAY(SYSDATE, 1) AS next_sunday FROM DUAL;  -- 1=Sunday in US

-- LAST_DAY - last day of the month
SELECT SYSDATE, LAST_DAY(SYSDATE) AS month_end FROM DUAL;
SELECT DATE '2024-02-15', LAST_DAY(DATE '2024-02-15') AS feb_end FROM DUAL;

-- First day of month (using LAST_DAY)
SELECT SYSDATE, LAST_DAY(ADD_MONTHS(SYSDATE, -1)) + 1 AS month_start FROM DUAL;

-- First day of next month
SELECT SYSDATE, LAST_DAY(SYSDATE) + 1 AS next_month_start FROM DUAL;

-- Days remaining in month
SELECT SYSDATE, LAST_DAY(SYSDATE) - SYSDATE AS days_remaining FROM DUAL;

-- -----------------------------------------------------------------------------
-- 6. ROUND and TRUNC for Dates
-- -----------------------------------------------------------------------------

-- TRUNC to various precision
SELECT SYSDATE AS original,
       TRUNC(SYSDATE) AS trunc_day,
       TRUNC(SYSDATE, 'MM') AS trunc_month,
       TRUNC(SYSDATE, 'Q') AS trunc_quarter,
       TRUNC(SYSDATE, 'YEAR') AS trunc_year,
       TRUNC(SYSDATE, 'WW') AS trunc_week,
       TRUNC(SYSDATE, 'IW') AS trunc_iso_week,
       TRUNC(SYSDATE, 'HH') AS trunc_hour,
       TRUNC(SYSDATE, 'MI') AS trunc_minute
FROM DUAL;

-- ROUND to various precision
SELECT SYSDATE AS original,
       ROUND(SYSDATE) AS round_day,
       ROUND(SYSDATE, 'MM') AS round_month,
       ROUND(SYSDATE, 'YEAR') AS round_year,
       ROUND(SYSDATE, 'HH') AS round_hour
FROM DUAL;

-- Use TRUNC for date comparisons (ignore time)
SELECT * FROM employees
WHERE TRUNC(hire_date) = TRUNC(SYSDATE);

-- Group by month using TRUNC
SELECT TRUNC(hire_date, 'MM') AS hire_month,
       COUNT(*) AS hire_count
FROM employees
GROUP BY TRUNC(hire_date, 'MM')
ORDER BY hire_month;

-- -----------------------------------------------------------------------------
-- 7. EXTRACT Function
-- -----------------------------------------------------------------------------

-- Extract date components
SELECT SYSDATE,
       EXTRACT(YEAR FROM SYSDATE) AS year,
       EXTRACT(MONTH FROM SYSDATE) AS month,
       EXTRACT(DAY FROM SYSDATE) AS day
FROM DUAL;

-- Extract from timestamp
SELECT SYSTIMESTAMP,
       EXTRACT(YEAR FROM SYSTIMESTAMP) AS year,
       EXTRACT(MONTH FROM SYSTIMESTAMP) AS month,
       EXTRACT(DAY FROM SYSTIMESTAMP) AS day,
       EXTRACT(HOUR FROM SYSTIMESTAMP) AS hour,
       EXTRACT(MINUTE FROM SYSTIMESTAMP) AS minute,
       EXTRACT(SECOND FROM SYSTIMESTAMP) AS second
FROM DUAL;

-- Extract timezone info
SELECT SYSTIMESTAMP,
       EXTRACT(TIMEZONE_HOUR FROM SYSTIMESTAMP) AS tz_hour,
       EXTRACT(TIMEZONE_MINUTE FROM SYSTIMESTAMP) AS tz_minute,
       EXTRACT(TIMEZONE_REGION FROM SYSTIMESTAMP) AS tz_region,
       EXTRACT(TIMEZONE_ABBR FROM SYSTIMESTAMP) AS tz_abbr
FROM DUAL;

-- Use in WHERE clause
SELECT * FROM employees
WHERE EXTRACT(YEAR FROM hire_date) = 2005;

SELECT * FROM employees
WHERE EXTRACT(MONTH FROM hire_date) = 6;

-- Group by extracted component
SELECT EXTRACT(YEAR FROM hire_date) AS hire_year,
       COUNT(*) AS emp_count
FROM employees
GROUP BY EXTRACT(YEAR FROM hire_date)
ORDER BY hire_year;

-- -----------------------------------------------------------------------------
-- 8. TO_CHAR for Date Formatting
-- -----------------------------------------------------------------------------

-- Basic date formats
SELECT SYSDATE,
       TO_CHAR(SYSDATE, 'YYYY-MM-DD') AS iso_date,
       TO_CHAR(SYSDATE, 'DD/MM/YYYY') AS eu_date,
       TO_CHAR(SYSDATE, 'MM/DD/YYYY') AS us_date,
       TO_CHAR(SYSDATE, 'DD-MON-YYYY') AS oracle_date
FROM DUAL;

-- Date with time
SELECT SYSDATE,
       TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS') AS datetime,
       TO_CHAR(SYSDATE, 'DD-MON-YYYY HH:MI:SS AM') AS datetime_12h
FROM DUAL;

-- Day and month names
SELECT SYSDATE,
       TO_CHAR(SYSDATE, 'DAY') AS day_name,
       TO_CHAR(SYSDATE, 'Day') AS day_name_init,
       TO_CHAR(SYSDATE, 'DY') AS day_abbrev,
       TO_CHAR(SYSDATE, 'MONTH') AS month_name,
       TO_CHAR(SYSDATE, 'Month') AS month_name_init,
       TO_CHAR(SYSDATE, 'MON') AS month_abbrev
FROM DUAL;

-- Special format elements
SELECT SYSDATE,
       TO_CHAR(SYSDATE, 'D') AS day_of_week,        -- 1-7
       TO_CHAR(SYSDATE, 'DD') AS day_of_month,      -- 01-31
       TO_CHAR(SYSDATE, 'DDD') AS day_of_year,      -- 001-366
       TO_CHAR(SYSDATE, 'W') AS week_of_month,      -- 1-5
       TO_CHAR(SYSDATE, 'WW') AS week_of_year,      -- 01-53
       TO_CHAR(SYSDATE, 'IW') AS iso_week,          -- 01-53
       TO_CHAR(SYSDATE, 'Q') AS quarter             -- 1-4
FROM DUAL;

-- Year formats
SELECT SYSDATE,
       TO_CHAR(SYSDATE, 'YYYY') AS year_4digit,
       TO_CHAR(SYSDATE, 'YY') AS year_2digit,
       TO_CHAR(SYSDATE, 'YEAR') AS year_spelled,
       TO_CHAR(SYSDATE, 'IYYY') AS iso_year
FROM DUAL;

-- Ordinal indicators
SELECT SYSDATE,
       TO_CHAR(SYSDATE, 'DDth') AS day_ordinal,
       TO_CHAR(SYSDATE, 'DDspth') AS day_spelled_ordinal,
       TO_CHAR(SYSDATE, 'Ddspth') AS day_spelled_ordinal_init
FROM DUAL;

-- Julian date
SELECT SYSDATE,
       TO_CHAR(SYSDATE, 'J') AS julian_day
FROM DUAL;

-- Format with fill mode (FM) to remove padding
SELECT SYSDATE,
       TO_CHAR(SYSDATE, 'Month DD, YYYY') AS padded,
       TO_CHAR(SYSDATE, 'FMMonth DD, YYYY') AS no_padding
FROM DUAL;

-- Custom format with literals
SELECT TO_CHAR(SYSDATE, '"Today is" Day, Month DD, YYYY') AS formatted FROM DUAL;
SELECT TO_CHAR(SYSDATE, 'YYYY"/"MM"/"DD') AS formatted FROM DUAL;

-- -----------------------------------------------------------------------------
-- 9. TO_DATE Function
-- -----------------------------------------------------------------------------

-- Parse date strings
SELECT TO_DATE('2024-06-15', 'YYYY-MM-DD') AS parsed_date FROM DUAL;
SELECT TO_DATE('15/06/2024', 'DD/MM/YYYY') AS parsed_date FROM DUAL;
SELECT TO_DATE('June 15, 2024', 'Month DD, YYYY') AS parsed_date FROM DUAL;
SELECT TO_DATE('15-JUN-24', 'DD-MON-YY') AS parsed_date FROM DUAL;

-- Parse with time
SELECT TO_DATE('2024-06-15 14:30:00', 'YYYY-MM-DD HH24:MI:SS') AS parsed_datetime FROM DUAL;
SELECT TO_DATE('06/15/2024 2:30 PM', 'MM/DD/YYYY HH:MI AM') AS parsed_datetime FROM DUAL;

-- Handle different century with RR vs YY
SELECT TO_DATE('15-JUN-95', 'DD-MON-RR') AS rr_date FROM DUAL;  -- 1995
SELECT TO_DATE('15-JUN-25', 'DD-MON-RR') AS rr_date FROM DUAL;  -- 2025

-- With FX (exact matching)
SELECT TO_DATE('2024-06-15', 'FXYYYY-MM-DD') AS exact_date FROM DUAL;

-- With NLS parameters
SELECT TO_DATE('15 Juin 2024', 'DD Month YYYY', 'NLS_DATE_LANGUAGE=FRENCH') AS french_date FROM DUAL;

-- -----------------------------------------------------------------------------
-- 10. Timestamp Functions
-- -----------------------------------------------------------------------------

-- TO_TIMESTAMP
SELECT TO_TIMESTAMP('2024-06-15 14:30:45.123456', 'YYYY-MM-DD HH24:MI:SS.FF') AS ts FROM DUAL;

-- TO_TIMESTAMP_TZ
SELECT TO_TIMESTAMP_TZ('2024-06-15 14:30:45 US/Eastern', 'YYYY-MM-DD HH24:MI:SS TZR') AS ts_tz FROM DUAL;
SELECT TO_TIMESTAMP_TZ('2024-06-15 14:30:45 -05:00', 'YYYY-MM-DD HH24:MI:SS TZH:TZM') AS ts_tz FROM DUAL;

-- CAST between date/timestamp
SELECT CAST(SYSDATE AS TIMESTAMP) AS date_to_timestamp FROM DUAL;
SELECT CAST(SYSTIMESTAMP AS DATE) AS timestamp_to_date FROM DUAL;

-- FROM_TZ - add timezone to timestamp
SELECT FROM_TZ(TIMESTAMP '2024-06-15 14:30:00', 'US/Eastern') AS ts_with_tz FROM DUAL;
SELECT FROM_TZ(TIMESTAMP '2024-06-15 14:30:00', '-05:00') AS ts_with_tz FROM DUAL;

-- AT TIME ZONE - convert between timezones
SELECT SYSTIMESTAMP AT TIME ZONE 'UTC' AS utc_time FROM DUAL;
SELECT SYSTIMESTAMP AT TIME ZONE 'US/Pacific' AS pacific_time FROM DUAL;

-- SYS_EXTRACT_UTC
SELECT SYS_EXTRACT_UTC(SYSTIMESTAMP) AS utc_timestamp FROM DUAL;

-- -----------------------------------------------------------------------------
-- 11. Interval Data Types and Functions
-- -----------------------------------------------------------------------------

-- INTERVAL YEAR TO MONTH
SELECT SYSDATE + INTERVAL '1' YEAR AS plus_1_year FROM DUAL;
SELECT SYSDATE + INTERVAL '6' MONTH AS plus_6_months FROM DUAL;
SELECT SYSDATE + INTERVAL '1-6' YEAR TO MONTH AS plus_1_year_6_months FROM DUAL;

-- INTERVAL DAY TO SECOND
SELECT SYSDATE + INTERVAL '7' DAY AS plus_7_days FROM DUAL;
SELECT SYSDATE + INTERVAL '2' HOUR AS plus_2_hours FROM DUAL;
SELECT SYSDATE + INTERVAL '30' MINUTE AS plus_30_minutes FROM DUAL;
SELECT SYSDATE + INTERVAL '5 4:30:00' DAY TO SECOND AS plus_interval FROM DUAL;

-- NUMTOYMINTERVAL
SELECT SYSDATE + NUMTOYMINTERVAL(18, 'MONTH') AS plus_18_months FROM DUAL;
SELECT SYSDATE + NUMTOYMINTERVAL(2.5, 'YEAR') AS plus_2_5_years FROM DUAL;

-- NUMTODSINTERVAL
SELECT SYSDATE + NUMTODSINTERVAL(2.5, 'DAY') AS plus_2_5_days FROM DUAL;
SELECT SYSDATE + NUMTODSINTERVAL(36, 'HOUR') AS plus_36_hours FROM DUAL;

-- TO_YMINTERVAL
SELECT SYSDATE + TO_YMINTERVAL('01-06') AS plus_1_year_6_months FROM DUAL;

-- TO_DSINTERVAL
SELECT SYSDATE + TO_DSINTERVAL('5 04:30:00') AS plus_interval FROM DUAL;

-- Extract from interval
SELECT EXTRACT(DAY FROM TO_DSINTERVAL('5 04:30:00')) AS days FROM DUAL;
SELECT EXTRACT(HOUR FROM TO_DSINTERVAL('5 04:30:00')) AS hours FROM DUAL;

-- -----------------------------------------------------------------------------
-- 12. Date Calculations and Business Logic
-- -----------------------------------------------------------------------------

-- Calculate age
SELECT employee_id, hire_date,
       TRUNC(MONTHS_BETWEEN(SYSDATE, hire_date) / 12) AS years_worked,
       MOD(TRUNC(MONTHS_BETWEEN(SYSDATE, hire_date)), 12) AS months_worked
FROM employees;

-- Business days calculation (simplified - excludes weekends)
SELECT SYSDATE AS start_date,
       SYSDATE + 10 AS plus_10_calendar_days,
       SYSDATE + 10 + 
           TRUNC((SYSDATE + 10 - TRUNC(SYSDATE, 'IW')) / 7) * 2 AS plus_10_business_days_approx
FROM DUAL;

-- Find fiscal year (July start)
SELECT hire_date,
       CASE 
           WHEN EXTRACT(MONTH FROM hire_date) >= 7 
           THEN EXTRACT(YEAR FROM hire_date)
           ELSE EXTRACT(YEAR FROM hire_date) - 1
       END AS fiscal_year
FROM employees;

-- Calculate quarter
SELECT hire_date,
       'Q' || TO_CHAR(hire_date, 'Q') || ' ' || TO_CHAR(hire_date, 'YYYY') AS quarter
FROM employees;

-- Date range overlaps
WITH date_ranges AS (
    SELECT DATE '2024-01-01' AS start1, DATE '2024-06-30' AS end1,
           DATE '2024-04-01' AS start2, DATE '2024-12-31' AS end2
    FROM DUAL
)
SELECT 
    CASE 
        WHEN start1 <= end2 AND end1 >= start2 THEN 'Overlaps'
        ELSE 'No Overlap'
    END AS overlap_status,
    GREATEST(start1, start2) AS overlap_start,
    LEAST(end1, end2) AS overlap_end
FROM date_ranges;

-- Generate date series using hierarchical query
SELECT TRUNC(SYSDATE) - LEVEL + 1 AS date_value
FROM DUAL
CONNECT BY LEVEL <= 30
ORDER BY date_value;

-- Find employees with anniversary this month
SELECT employee_id, first_name, hire_date
FROM employees
WHERE EXTRACT(MONTH FROM hire_date) = EXTRACT(MONTH FROM SYSDATE)
AND EXTRACT(DAY FROM hire_date) = EXTRACT(DAY FROM SYSDATE);

-- Calculate time until next birthday/anniversary
SELECT employee_id, hire_date,
       ADD_MONTHS(hire_date, 
           (EXTRACT(YEAR FROM SYSDATE) - EXTRACT(YEAR FROM hire_date) + 1) * 12
       ) AS next_anniversary,
       ADD_MONTHS(hire_date, 
           (EXTRACT(YEAR FROM SYSDATE) - EXTRACT(YEAR FROM hire_date) + 1) * 12
       ) - SYSDATE AS days_until
FROM employees;

