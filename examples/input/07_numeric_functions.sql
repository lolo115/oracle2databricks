-- ============================================================================
-- File: 07_numeric_functions.sql
-- Description: Numeric, mathematical, and trigonometric functions
-- ============================================================================

-- -----------------------------------------------------------------------------
-- 1. Rounding Functions
-- -----------------------------------------------------------------------------

-- ROUND
SELECT ROUND(123.456) AS round_default FROM DUAL;           -- 123
SELECT ROUND(123.456, 0) AS round_0 FROM DUAL;              -- 123
SELECT ROUND(123.456, 1) AS round_1 FROM DUAL;              -- 123.5
SELECT ROUND(123.456, 2) AS round_2 FROM DUAL;              -- 123.46
SELECT ROUND(123.456, -1) AS round_neg1 FROM DUAL;          -- 120
SELECT ROUND(125.456, -1) AS round_neg1_mid FROM DUAL;      -- 130
SELECT ROUND(123.456, -2) AS round_neg2 FROM DUAL;          -- 100

-- Applied to salary
SELECT employee_id, salary,
       ROUND(salary, -3) AS salary_rounded_1000,
       ROUND(salary / 12, 2) AS monthly_salary
FROM employees;

-- TRUNC (truncate towards zero)
SELECT TRUNC(123.456) AS trunc_default FROM DUAL;           -- 123
SELECT TRUNC(123.456, 0) AS trunc_0 FROM DUAL;              -- 123
SELECT TRUNC(123.456, 1) AS trunc_1 FROM DUAL;              -- 123.4
SELECT TRUNC(123.456, 2) AS trunc_2 FROM DUAL;              -- 123.45
SELECT TRUNC(123.456, -1) AS trunc_neg1 FROM DUAL;          -- 120
SELECT TRUNC(128.456, -1) AS trunc_neg1_mid FROM DUAL;      -- 120
SELECT TRUNC(-123.456, 1) AS trunc_negative FROM DUAL;      -- -123.4

-- CEIL (round up to nearest integer)
SELECT CEIL(123.001) AS ceil FROM DUAL;                     -- 124
SELECT CEIL(123.999) AS ceil FROM DUAL;                     -- 124
SELECT CEIL(-123.001) AS ceil_negative FROM DUAL;           -- -123
SELECT CEIL(-123.999) AS ceil_negative FROM DUAL;           -- -123

-- FLOOR (round down to nearest integer)
SELECT FLOOR(123.001) AS floor FROM DUAL;                   -- 123
SELECT FLOOR(123.999) AS floor FROM DUAL;                   -- 123
SELECT FLOOR(-123.001) AS floor_negative FROM DUAL;         -- -124
SELECT FLOOR(-123.999) AS floor_negative FROM DUAL;         -- -124

-- Comparing rounding functions
SELECT 
    n,
    ROUND(n) AS rounded,
    TRUNC(n) AS truncated,
    CEIL(n) AS ceiling,
    FLOOR(n) AS floored
FROM (
    SELECT 3.7 AS n FROM DUAL UNION ALL
    SELECT 3.3 FROM DUAL UNION ALL
    SELECT -3.7 FROM DUAL UNION ALL
    SELECT -3.3 FROM DUAL
);

-- -----------------------------------------------------------------------------
-- 2. Absolute Value and Sign
-- -----------------------------------------------------------------------------

-- ABS
SELECT ABS(100) AS abs_positive FROM DUAL;                  -- 100
SELECT ABS(-100) AS abs_negative FROM DUAL;                 -- 100
SELECT ABS(0) AS abs_zero FROM DUAL;                        -- 0
SELECT ABS(-3.14159) AS abs_decimal FROM DUAL;              -- 3.14159

-- SIGN
SELECT SIGN(100) AS sign_positive FROM DUAL;                -- 1
SELECT SIGN(-100) AS sign_negative FROM DUAL;               -- -1
SELECT SIGN(0) AS sign_zero FROM DUAL;                      -- 0

-- Practical use of SIGN
SELECT salary,
       SIGN(salary - 5000) AS above_5000,
       CASE SIGN(salary - 5000)
           WHEN 1 THEN 'Above'
           WHEN 0 THEN 'Equal'
           WHEN -1 THEN 'Below'
       END AS salary_status
FROM employees;

-- -----------------------------------------------------------------------------
-- 3. Modulo and Remainder
-- -----------------------------------------------------------------------------

-- MOD
SELECT MOD(10, 3) AS mod_result FROM DUAL;                  -- 1
SELECT MOD(10, -3) AS mod_neg_divisor FROM DUAL;            -- 1
SELECT MOD(-10, 3) AS mod_neg_dividend FROM DUAL;           -- -1
SELECT MOD(10, 10) AS mod_equal FROM DUAL;                  -- 0

-- REMAINDER (different from MOD for negative numbers)
SELECT REMAINDER(10, 3) AS remainder FROM DUAL;             -- 1
SELECT REMAINDER(10, -3) AS remainder_neg FROM DUAL;        -- 1

-- Check for even/odd numbers
SELECT employee_id,
       MOD(employee_id, 2) AS mod_result,
       CASE MOD(employee_id, 2)
           WHEN 0 THEN 'Even'
           ELSE 'Odd'
       END AS even_odd
FROM employees;

-- -----------------------------------------------------------------------------
-- 4. Power and Root Functions
-- -----------------------------------------------------------------------------

-- POWER
SELECT POWER(2, 10) AS power_result FROM DUAL;              -- 1024
SELECT POWER(10, 3) AS power_result FROM DUAL;              -- 1000
SELECT POWER(2, -1) AS power_negative FROM DUAL;            -- 0.5
SELECT POWER(4, 0.5) AS square_root FROM DUAL;              -- 2

-- SQRT (square root)
SELECT SQRT(16) AS sqrt FROM DUAL;                          -- 4
SELECT SQRT(2) AS sqrt_2 FROM DUAL;                         -- 1.41421356...
SELECT SQRT(0) AS sqrt_zero FROM DUAL;                      -- 0

-- EXP (e raised to power)
SELECT EXP(1) AS e FROM DUAL;                               -- 2.71828...
SELECT EXP(2) AS e_squared FROM DUAL;                       -- 7.38905...
SELECT EXP(0) AS e_zero FROM DUAL;                          -- 1

-- -----------------------------------------------------------------------------
-- 5. Logarithmic Functions
-- -----------------------------------------------------------------------------

-- LN (natural logarithm)
SELECT LN(2.71828) AS ln_e FROM DUAL;                       -- ~1
SELECT LN(10) AS ln_10 FROM DUAL;                           -- 2.302585...

-- LOG (logarithm with specified base)
SELECT LOG(10, 100) AS log10_100 FROM DUAL;                 -- 2
SELECT LOG(2, 8) AS log2_8 FROM DUAL;                       -- 3
SELECT LOG(10, 1000) AS log10_1000 FROM DUAL;               -- 3

-- LOG10 (Oracle 21c+) / common logarithm workaround
SELECT LOG(10, 100) AS log10 FROM DUAL;
SELECT LN(100) / LN(10) AS log10_calc FROM DUAL;            -- 2

-- Relationship between EXP and LN
SELECT EXP(LN(100)) AS exp_ln FROM DUAL;                    -- 100

-- -----------------------------------------------------------------------------
-- 6. Trigonometric Functions
-- -----------------------------------------------------------------------------

-- Constants
SELECT 
    ACOS(-1) AS pi,                                         -- π
    ACOS(-1) / 180 AS rad_per_degree                        -- Conversion factor
FROM DUAL;

-- SIN, COS, TAN (input in radians)
SELECT SIN(0) AS sin_0 FROM DUAL;                           -- 0
SELECT COS(0) AS cos_0 FROM DUAL;                           -- 1
SELECT TAN(0) AS tan_0 FROM DUAL;                           -- 0

-- Convert degrees to radians for trig functions
SELECT SIN(90 * ACOS(-1) / 180) AS sin_90_degrees FROM DUAL;    -- 1
SELECT COS(180 * ACOS(-1) / 180) AS cos_180_degrees FROM DUAL;  -- -1

-- Inverse trig functions (return radians)
SELECT ASIN(0) AS asin_0 FROM DUAL;                         -- 0
SELECT ACOS(1) AS acos_1 FROM DUAL;                         -- 0
SELECT ATAN(0) AS atan_0 FROM DUAL;                         -- 0
SELECT ATAN2(1, 1) AS atan2 FROM DUAL;                      -- π/4

-- Hyperbolic functions
SELECT SINH(0) AS sinh FROM DUAL;
SELECT COSH(0) AS cosh FROM DUAL;
SELECT TANH(0) AS tanh FROM DUAL;

-- Convert radians to degrees
SELECT 
    ACOS(-1) AS pi_radians,
    ACOS(-1) * 180 / ACOS(-1) AS pi_degrees                 -- 180
FROM DUAL;

-- -----------------------------------------------------------------------------
-- 7. Width_Bucket
-- -----------------------------------------------------------------------------

-- Distribute values into buckets
SELECT salary,
       WIDTH_BUCKET(salary, 0, 30000, 6) AS salary_bucket
FROM employees;

-- Create histogram buckets
SELECT WIDTH_BUCKET(salary, 2000, 25000, 5) AS bucket,
       MIN(salary) AS min_sal,
       MAX(salary) AS max_sal,
       COUNT(*) AS emp_count
FROM employees
GROUP BY WIDTH_BUCKET(salary, 2000, 25000, 5)
ORDER BY bucket;

-- Bucket with bounds
SELECT salary,
       WIDTH_BUCKET(salary, 5000, 20000, 3) AS bucket,
       CASE WIDTH_BUCKET(salary, 5000, 20000, 3)
           WHEN 0 THEN 'Below Range'
           WHEN 1 THEN 'Low'
           WHEN 2 THEN 'Medium'
           WHEN 3 THEN 'High'
           WHEN 4 THEN 'Above Range'
       END AS salary_range
FROM employees;

-- -----------------------------------------------------------------------------
-- 8. Greatest and Least
-- -----------------------------------------------------------------------------

-- GREATEST - returns largest value
SELECT GREATEST(10, 20, 30) AS greatest FROM DUAL;          -- 30
SELECT GREATEST('A', 'B', 'C') AS greatest FROM DUAL;       -- C
SELECT GREATEST(DATE '2024-01-01', DATE '2024-06-15', DATE '2024-03-20') AS greatest FROM DUAL;

-- LEAST - returns smallest value
SELECT LEAST(10, 20, 30) AS least FROM DUAL;                -- 10
SELECT LEAST('A', 'B', 'C') AS least FROM DUAL;             -- A

-- NULL handling
SELECT GREATEST(10, NULL, 30) AS greatest FROM DUAL;        -- NULL
SELECT LEAST(10, NULL, 30) AS least FROM DUAL;              -- NULL

-- Practical uses
SELECT employee_id, salary, commission_pct,
       GREATEST(salary, salary * NVL(commission_pct, 0)) AS effective_pay,
       LEAST(salary, 10000) AS capped_salary
FROM employees;

-- -----------------------------------------------------------------------------
-- 9. Conversion and Formatting
-- -----------------------------------------------------------------------------

-- TO_NUMBER
SELECT TO_NUMBER('123.45') AS num FROM DUAL;
SELECT TO_NUMBER('$1,234.56', '$9,999.99') AS num FROM DUAL;
SELECT TO_NUMBER('1234.56', '9999.99') AS num FROM DUAL;
SELECT TO_NUMBER('-1234', 'S9999') AS num FROM DUAL;
SELECT TO_NUMBER('12.34%', '99.99%') * 100 AS percentage FROM DUAL;

-- TO_CHAR for numbers
SELECT TO_CHAR(1234.56, '9999.99') AS formatted FROM DUAL;
SELECT TO_CHAR(1234.56, '09999.99') AS zero_padded FROM DUAL;
SELECT TO_CHAR(1234.56, '$9,999.99') AS currency FROM DUAL;
SELECT TO_CHAR(1234.56, 'L9,999.99') AS local_currency FROM DUAL;
SELECT TO_CHAR(-1234.56, '9999.99MI') AS negative_suffix FROM DUAL;
SELECT TO_CHAR(1234.56, '9.99EEEE') AS scientific FROM DUAL;
SELECT TO_CHAR(0.25, '90.99') AS with_leading_zero FROM DUAL;

-- Number format elements
SELECT 
    salary,
    TO_CHAR(salary, '99,999') AS with_comma,
    TO_CHAR(salary, '00000') AS zero_padded,
    TO_CHAR(salary, 'FM99999') AS no_spaces,
    TO_CHAR(salary, '$99,999.00') AS currency,
    TO_CHAR(salary / 1000, '99.9K') AS in_thousands
FROM employees;

-- Hexadecimal conversion
SELECT TO_CHAR(255, 'XXXX') AS hex FROM DUAL;               -- FF
SELECT TO_NUMBER('FF', 'XXXX') AS decimal FROM DUAL;        -- 255

-- Roman numerals
SELECT TO_CHAR(2024, 'RN') AS roman FROM DUAL;              -- MMXXIV
SELECT TO_CHAR(LEVEL, 'FMRN') AS roman
FROM DUAL
CONNECT BY LEVEL <= 10;

-- -----------------------------------------------------------------------------
-- 10. Miscellaneous Numeric Functions
-- -----------------------------------------------------------------------------

-- NANVL (Not A Number handling)
SELECT NANVL(SQRT(-1), 0) AS nanvl_result FROM DUAL;

-- BITAND (bitwise AND)
SELECT BITAND(6, 3) AS bitand FROM DUAL;                    -- 2 (110 AND 011 = 010)
SELECT BITAND(15, 9) AS bitand FROM DUAL;                   -- 9 (1111 AND 1001 = 1001)

-- Bitwise operations (using BITAND)
-- OR: a + b - BITAND(a, b)
-- XOR: a + b - 2 * BITAND(a, b)
-- NOT: -1 - a (for two's complement)
SELECT 
    6 AS a, 3 AS b,
    BITAND(6, 3) AS bit_and,
    6 + 3 - BITAND(6, 3) AS bit_or,
    6 + 3 - 2 * BITAND(6, 3) AS bit_xor
FROM DUAL;

-- BIN_TO_NUM (binary to number)
SELECT BIN_TO_NUM(1, 1, 0, 1) AS decimal FROM DUAL;         -- 13

-- Random numbers (DBMS_RANDOM package)
-- SELECT DBMS_RANDOM.VALUE AS random_0_to_1 FROM DUAL;
-- SELECT DBMS_RANDOM.VALUE(1, 100) AS random_1_to_100 FROM DUAL;
-- SELECT TRUNC(DBMS_RANDOM.VALUE(1, 101)) AS random_integer FROM DUAL;

-- ORA_HASH (hash function)
SELECT ORA_HASH('Hello World') AS hash_value FROM DUAL;
SELECT ORA_HASH('Hello World', 99) AS hash_0_to_99 FROM DUAL;
SELECT ORA_HASH('Hello World', 99, 123) AS hash_with_seed FROM DUAL;

-- Standard hash
SELECT STANDARD_HASH('Hello World', 'MD5') AS md5_hash FROM DUAL;
SELECT STANDARD_HASH('Hello World', 'SHA256') AS sha256_hash FROM DUAL;
SELECT STANDARD_HASH('Hello World', 'SHA512') AS sha512_hash FROM DUAL;

-- -----------------------------------------------------------------------------
-- 11. Complex Calculations
-- -----------------------------------------------------------------------------

-- Compound interest calculation
SELECT 
    10000 AS principal,
    5 AS rate_percent,
    10 AS years,
    ROUND(10000 * POWER(1 + 5/100, 10), 2) AS future_value
FROM DUAL;

-- Distance calculation (Euclidean)
SELECT 
    SQRT(POWER(x2 - x1, 2) + POWER(y2 - y1, 2)) AS distance
FROM (SELECT 0 AS x1, 0 AS y1, 3 AS x2, 4 AS y2 FROM DUAL);

-- Haversine formula (distance between coordinates)
WITH coords AS (
    SELECT 
        37.7749 AS lat1, -122.4194 AS lon1,  -- San Francisco
        34.0522 AS lat2, -118.2437 AS lon2   -- Los Angeles
    FROM DUAL
)
SELECT ROUND(
    6371 * 2 * ASIN(SQRT(
        POWER(SIN((lat2 - lat1) * ACOS(-1) / 180 / 2), 2) +
        COS(lat1 * ACOS(-1) / 180) * COS(lat2 * ACOS(-1) / 180) *
        POWER(SIN((lon2 - lon1) * ACOS(-1) / 180 / 2), 2)
    )), 2) AS distance_km
FROM coords;

-- Percentage calculations
SELECT employee_id, salary,
       ROUND(salary / (SELECT SUM(salary) FROM employees) * 100, 4) AS pct_of_total,
       ROUND(salary / (SELECT AVG(salary) FROM employees) * 100, 2) AS pct_of_avg
FROM employees;

-- Moving average calculation
SELECT employee_id, hire_date, salary,
       ROUND(AVG(salary) OVER (ORDER BY hire_date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS moving_avg_3
FROM employees;

-- Z-score calculation
SELECT employee_id, salary,
       ROUND((salary - (SELECT AVG(salary) FROM employees)) / 
             (SELECT STDDEV(salary) FROM employees), 2) AS z_score
FROM employees;

