-- ============================================================================
-- File: 22_regex_functions.sql
-- Description: Regular expression functions in Oracle
-- ============================================================================

-- -----------------------------------------------------------------------------
-- 1. REGEXP_LIKE - Pattern Matching in Conditions
-- -----------------------------------------------------------------------------

-- Basic pattern matching
SELECT first_name, last_name
FROM employees
WHERE REGEXP_LIKE(first_name, '^A');  -- Starts with A

SELECT first_name, last_name
FROM employees
WHERE REGEXP_LIKE(last_name, 'son$');  -- Ends with 'son'

SELECT first_name
FROM employees
WHERE REGEXP_LIKE(first_name, '^[AEIOU]');  -- Starts with vowel

-- Case-insensitive matching
SELECT first_name
FROM employees
WHERE REGEXP_LIKE(first_name, 'john', 'i');  -- 'i' = case insensitive

-- Match parameters
-- 'i' - case insensitive
-- 'c' - case sensitive (default)
-- 'n' - allows . to match newline
-- 'm' - multiline mode (^ and $ match line boundaries)
-- 'x' - ignore whitespace in pattern

-- Email validation
SELECT email
FROM employees
WHERE REGEXP_LIKE(email, '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$');

-- Phone number validation (US format)
SELECT phone_number
FROM employees
WHERE REGEXP_LIKE(phone_number, '^\d{3}[.-]?\d{3}[.-]?\d{4}$');

-- Contains only letters
SELECT first_name
FROM employees
WHERE REGEXP_LIKE(first_name, '^[A-Za-z]+$');

-- Contains at least one digit
SELECT email
FROM employees
WHERE REGEXP_LIKE(email, '[0-9]');

-- Multiple patterns with alternation
SELECT first_name
FROM employees
WHERE REGEXP_LIKE(first_name, '^(John|Jane|James)$');

-- Word boundary matching
SELECT first_name
FROM employees
WHERE REGEXP_LIKE(first_name, '\bAnn\b');  -- Match 'Ann' not 'Anna'

-- -----------------------------------------------------------------------------
-- 2. REGEXP_SUBSTR - Extract Substrings
-- -----------------------------------------------------------------------------

-- Basic extraction
SELECT REGEXP_SUBSTR('Hello World', '\w+') AS first_word FROM DUAL;
SELECT REGEXP_SUBSTR('Hello World', '\w+', 1, 2) AS second_word FROM DUAL;

-- Syntax: REGEXP_SUBSTR(source, pattern, position, occurrence, match_param, subexpr)

-- Extract numbers
SELECT REGEXP_SUBSTR('Price: $123.45', '\d+\.?\d*') AS number FROM DUAL;  -- 123.45
SELECT REGEXP_SUBSTR('Order 12345 confirmed', '\d+') AS order_num FROM DUAL;

-- Extract email parts
SELECT 
    'john.doe@example.com' AS email,
    REGEXP_SUBSTR('john.doe@example.com', '[^@]+', 1, 1) AS username,
    REGEXP_SUBSTR('john.doe@example.com', '[^@]+', 1, 2) AS domain
FROM DUAL;

-- Extract with subexpression (capture groups)
SELECT REGEXP_SUBSTR('john.doe@example.com', '([^@]+)@(.+)', 1, 1, NULL, 1) AS username FROM DUAL;
SELECT REGEXP_SUBSTR('john.doe@example.com', '([^@]+)@(.+)', 1, 1, NULL, 2) AS domain FROM DUAL;

-- Extract all occurrences (using CONNECT BY)
SELECT LEVEL AS position,
       REGEXP_SUBSTR('apple,banana,cherry,date', '[^,]+', 1, LEVEL) AS item
FROM DUAL
CONNECT BY LEVEL <= REGEXP_COUNT('apple,banana,cherry,date', ',') + 1;

-- Extract phone number parts
SELECT 
    REGEXP_SUBSTR('(555) 123-4567', '\d+', 1, 1) AS area_code,
    REGEXP_SUBSTR('(555) 123-4567', '\d+', 1, 2) AS exchange,
    REGEXP_SUBSTR('(555) 123-4567', '\d+', 1, 3) AS subscriber
FROM DUAL;

-- Extract URL components
WITH urls AS (
    SELECT 'https://www.example.com:8080/path/page.html?query=1' AS url FROM DUAL
)
SELECT 
    REGEXP_SUBSTR(url, '^(\w+)://', 1, 1, NULL, 1) AS protocol,
    REGEXP_SUBSTR(url, '://([^:/]+)', 1, 1, NULL, 1) AS host,
    REGEXP_SUBSTR(url, ':(\d+)', 1, 1, NULL, 1) AS port,
    REGEXP_SUBSTR(url, '://[^/]+(/[^?]*)', 1, 1, NULL, 1) AS path,
    REGEXP_SUBSTR(url, '\?(.+)$', 1, 1, NULL, 1) AS query
FROM urls;

-- -----------------------------------------------------------------------------
-- 3. REGEXP_REPLACE - Pattern-Based Replacement
-- -----------------------------------------------------------------------------

-- Basic replacement
SELECT REGEXP_REPLACE('Hello World', 'World', 'Oracle') AS result FROM DUAL;

-- Remove all digits
SELECT REGEXP_REPLACE('abc123def456', '\d', '') AS result FROM DUAL;  -- abcdef

-- Remove extra spaces
SELECT REGEXP_REPLACE('Hello    World   !', ' +', ' ') AS result FROM DUAL;  -- Hello World !

-- Format phone number
SELECT REGEXP_REPLACE('5551234567', '(\d{3})(\d{3})(\d{4})', '(\1) \2-\3') AS formatted FROM DUAL;

-- Mask credit card (keep last 4 digits)
SELECT REGEXP_REPLACE('1234567890123456', '^\d{12}', '************') AS masked FROM DUAL;

-- Mask email
SELECT REGEXP_REPLACE('john.doe@example.com', '(.)([^@]+)(@.+)', '\1***\3') AS masked FROM DUAL;

-- Remove HTML tags
SELECT REGEXP_REPLACE('<p>Hello <b>World</b></p>', '<[^>]*>', '') AS plain_text FROM DUAL;

-- Convert camelCase to snake_case
SELECT REGEXP_REPLACE('firstName', '([a-z])([A-Z])', '\1_\2') AS snake_case FROM DUAL;
SELECT LOWER(REGEXP_REPLACE('firstName', '([a-z])([A-Z])', '\1_\2')) AS lower_snake FROM DUAL;

-- Normalize whitespace (tabs, newlines to single space)
SELECT REGEXP_REPLACE('Hello' || CHR(9) || CHR(10) || 'World', '\s+', ' ') AS normalized FROM DUAL;

-- Add thousand separators
SELECT REGEXP_REPLACE(TO_CHAR(1234567890), '(\d)(?=(\d{3})+$)', '\1,') AS formatted FROM DUAL;

-- Replace repeated characters
SELECT REGEXP_REPLACE('Hellooooo', '(.)\1+', '\1') AS result FROM DUAL;  -- Helo

-- Swap first and last name
SELECT REGEXP_REPLACE('John Doe', '(\w+)\s+(\w+)', '\2, \1') AS swapped FROM DUAL;

-- -----------------------------------------------------------------------------
-- 4. REGEXP_INSTR - Find Position of Pattern
-- -----------------------------------------------------------------------------

-- Basic position finding
SELECT REGEXP_INSTR('Hello World', 'World') AS position FROM DUAL;  -- 7

-- Find first digit
SELECT REGEXP_INSTR('abc123def', '\d') AS first_digit_pos FROM DUAL;  -- 4

-- Find Nth occurrence
SELECT REGEXP_INSTR('a1b2c3d4', '\d', 1, 1) AS first_digit FROM DUAL;   -- 2
SELECT REGEXP_INSTR('a1b2c3d4', '\d', 1, 2) AS second_digit FROM DUAL;  -- 4
SELECT REGEXP_INSTR('a1b2c3d4', '\d', 1, 3) AS third_digit FROM DUAL;   -- 6

-- Return end position (return_opt = 1)
SELECT REGEXP_INSTR('abc123def', '\d+', 1, 1, 0) AS start_pos FROM DUAL;  -- 4
SELECT REGEXP_INSTR('abc123def', '\d+', 1, 1, 1) AS end_pos FROM DUAL;    -- 7

-- Find subexpression position
SELECT REGEXP_INSTR('john.doe@example.com', '([^@]+)@(.+)', 1, 1, 0, 'i', 2) AS domain_pos FROM DUAL;

-- Find word boundary
SELECT REGEXP_INSTR('The quick brown fox', '\bquick\b') AS word_pos FROM DUAL;

-- -----------------------------------------------------------------------------
-- 5. REGEXP_COUNT - Count Occurrences
-- -----------------------------------------------------------------------------

-- Count digits
SELECT REGEXP_COUNT('abc123def456ghi789', '\d') AS digit_count FROM DUAL;  -- 9
SELECT REGEXP_COUNT('abc123def456ghi789', '\d+') AS number_count FROM DUAL;  -- 3

-- Count words
SELECT REGEXP_COUNT('The quick brown fox', '\w+') AS word_count FROM DUAL;  -- 4

-- Count specific character
SELECT REGEXP_COUNT('banana', 'a') AS a_count FROM DUAL;  -- 3

-- Count vowels
SELECT REGEXP_COUNT('Hello World', '[aeiouAEIOU]') AS vowel_count FROM DUAL;

-- Count sentences (approximately)
SELECT REGEXP_COUNT('Hello. World! How are you?', '[.!?]') AS sentence_count FROM DUAL;

-- Count occurrences in column
SELECT first_name, 
       REGEXP_COUNT(first_name, '[aeiouAEIOU]') AS vowel_count
FROM employees
ORDER BY vowel_count DESC;

-- Count email addresses in text
SELECT REGEXP_COUNT('Contact us at info@example.com or support@example.com', 
                    '[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}') AS email_count FROM DUAL;

-- -----------------------------------------------------------------------------
-- 6. Complex Pattern Examples
-- -----------------------------------------------------------------------------

-- Validate IP address
SELECT 
    ip_address,
    CASE WHEN REGEXP_LIKE(ip_address, 
        '^((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$')
    THEN 'Valid' ELSE 'Invalid' END AS is_valid
FROM (
    SELECT '192.168.1.1' AS ip_address FROM DUAL UNION ALL
    SELECT '256.1.1.1' FROM DUAL UNION ALL
    SELECT '192.168.1' FROM DUAL
);

-- Validate date format (YYYY-MM-DD)
SELECT 
    date_str,
    CASE WHEN REGEXP_LIKE(date_str, '^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01])$')
    THEN 'Valid' ELSE 'Invalid' END AS is_valid
FROM (
    SELECT '2024-06-15' AS date_str FROM DUAL UNION ALL
    SELECT '2024-13-15' FROM DUAL UNION ALL
    SELECT '24-06-15' FROM DUAL
);

-- Parse log entries
WITH log_entries AS (
    SELECT '2024-06-15 10:30:45 ERROR User authentication failed for user: john_doe' AS log_line FROM DUAL
)
SELECT 
    REGEXP_SUBSTR(log_line, '^\d{4}-\d{2}-\d{2}') AS log_date,
    REGEXP_SUBSTR(log_line, '\d{2}:\d{2}:\d{2}') AS log_time,
    REGEXP_SUBSTR(log_line, '\s(DEBUG|INFO|WARNING|ERROR|FATAL)\s', 1, 1, NULL, 1) AS log_level,
    REGEXP_SUBSTR(log_line, '(ERROR|INFO|WARNING|DEBUG|FATAL)\s(.+)$', 1, 1, NULL, 2) AS message
FROM log_entries;

-- Extract key-value pairs
WITH data AS (
    SELECT 'name=John;age=30;city=New York' AS kv_string FROM DUAL
)
SELECT 
    REGEXP_SUBSTR(kv_string, 'name=([^;]+)', 1, 1, NULL, 1) AS name,
    REGEXP_SUBSTR(kv_string, 'age=([^;]+)', 1, 1, NULL, 1) AS age,
    REGEXP_SUBSTR(kv_string, 'city=([^;]+)', 1, 1, NULL, 1) AS city
FROM data;

-- Clean and standardize data
SELECT 
    input_string,
    TRIM(REGEXP_REPLACE(
        REGEXP_REPLACE(input_string, '[^A-Za-z0-9\s]', ''),  -- Remove special chars
        '\s+', ' '  -- Normalize spaces
    )) AS cleaned
FROM (
    SELECT '  Hello!!!  World  @#$  ' AS input_string FROM DUAL
);

-- Extract hashtags from text
SELECT LEVEL AS position,
       REGEXP_SUBSTR('Check out #Oracle #SQL #Database tips!', '#\w+', 1, LEVEL) AS hashtag
FROM DUAL
CONNECT BY LEVEL <= REGEXP_COUNT('Check out #Oracle #SQL #Database tips!', '#\w+');

-- -----------------------------------------------------------------------------
-- 7. REGEXP in PL/SQL
-- -----------------------------------------------------------------------------

DECLARE
    v_email VARCHAR2(100) := 'john.doe@example.com';
    v_phone VARCHAR2(20) := '(555) 123-4567';
    v_text VARCHAR2(1000) := 'Order #12345 shipped to John Doe on 2024-06-15';
    
    -- Pattern constants
    c_email_pattern CONSTANT VARCHAR2(200) := '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$';
    c_phone_pattern CONSTANT VARCHAR2(100) := '^\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}$';
BEGIN
    -- Validate email
    IF REGEXP_LIKE(v_email, c_email_pattern) THEN
        DBMS_OUTPUT.PUT_LINE('Valid email: ' || v_email);
    END IF;
    
    -- Validate phone
    IF REGEXP_LIKE(v_phone, c_phone_pattern) THEN
        DBMS_OUTPUT.PUT_LINE('Valid phone: ' || v_phone);
    END IF;
    
    -- Extract order number
    DBMS_OUTPUT.PUT_LINE('Order: ' || REGEXP_SUBSTR(v_text, '#(\d+)', 1, 1, NULL, 1));
    
    -- Extract name
    DBMS_OUTPUT.PUT_LINE('Name: ' || REGEXP_SUBSTR(v_text, 'to ([A-Za-z]+ [A-Za-z]+)', 1, 1, NULL, 1));
    
    -- Extract date
    DBMS_OUTPUT.PUT_LINE('Date: ' || REGEXP_SUBSTR(v_text, '\d{4}-\d{2}-\d{2}'));
    
    -- Format phone
    DBMS_OUTPUT.PUT_LINE('Formatted: ' || 
        REGEXP_REPLACE(REGEXP_REPLACE(v_phone, '[^\d]', ''), '(\d{3})(\d{3})(\d{4})', '(\1) \2-\3'));
END;
/

-- Function to validate and format data
CREATE OR REPLACE FUNCTION clean_phone_number(p_phone VARCHAR2)
RETURN VARCHAR2
IS
    v_digits VARCHAR2(20);
BEGIN
    -- Extract only digits
    v_digits := REGEXP_REPLACE(p_phone, '[^\d]', '');
    
    -- Validate length
    IF LENGTH(v_digits) <> 10 THEN
        RETURN NULL;
    END IF;
    
    -- Format
    RETURN REGEXP_REPLACE(v_digits, '(\d{3})(\d{3})(\d{4})', '(\1) \2-\3');
END clean_phone_number;
/

-- Function to extract all matches
CREATE OR REPLACE FUNCTION extract_all_matches(
    p_text VARCHAR2,
    p_pattern VARCHAR2
)
RETURN VARCHAR2
IS
    v_result VARCHAR2(4000);
    v_match VARCHAR2(1000);
    v_count NUMBER;
BEGIN
    v_count := REGEXP_COUNT(p_text, p_pattern);
    
    FOR i IN 1..v_count LOOP
        v_match := REGEXP_SUBSTR(p_text, p_pattern, 1, i);
        v_result := v_result || CASE WHEN i > 1 THEN ',' END || v_match;
    END LOOP;
    
    RETURN v_result;
END extract_all_matches;
/

SELECT extract_all_matches('Email: a@b.com and c@d.com', '[A-Za-z]+@[A-Za-z]+\.[A-Za-z]+') FROM DUAL;

