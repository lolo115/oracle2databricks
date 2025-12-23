-- ============================================================================
-- File: 05_string_functions.sql
-- Description: String manipulation functions and operations
-- ============================================================================

-- -----------------------------------------------------------------------------
-- 1. Basic String Functions
-- -----------------------------------------------------------------------------

-- UPPER, LOWER, INITCAP
SELECT UPPER('hello world') AS upper_text FROM DUAL;
SELECT LOWER('HELLO WORLD') AS lower_text FROM DUAL;
SELECT INITCAP('hello world') AS initcap_text FROM DUAL;
SELECT INITCAP('JOHN DOE') AS initcap_name FROM DUAL;

-- Applied to columns
SELECT first_name, 
       UPPER(first_name) AS upper_name,
       LOWER(first_name) AS lower_name,
       INITCAP(LOWER(first_name)) AS proper_name
FROM employees;

-- LENGTH and LENGTHB
SELECT LENGTH('Hello World') AS char_length FROM DUAL;
SELECT LENGTHB('Hello World') AS byte_length FROM DUAL;
SELECT first_name, LENGTH(first_name) AS name_length FROM employees;

-- LENGTHC, LENGTH2, LENGTH4 (Unicode)
SELECT LENGTHC('Hello') AS lengthc FROM DUAL;
SELECT LENGTH2('Hello') AS length2 FROM DUAL;
SELECT LENGTH4('Hello') AS length4 FROM DUAL;

-- -----------------------------------------------------------------------------
-- 2. Substring Functions
-- -----------------------------------------------------------------------------

-- SUBSTR
SELECT SUBSTR('Hello World', 1, 5) AS substring FROM DUAL;  -- 'Hello'
SELECT SUBSTR('Hello World', 7) AS substring FROM DUAL;      -- 'World'
SELECT SUBSTR('Hello World', -5) AS substring FROM DUAL;     -- 'World' (from end)
SELECT SUBSTR('Hello World', -5, 3) AS substring FROM DUAL;  -- 'Wor'

-- SUBSTRB (byte-based)
SELECT SUBSTRB('Hello World', 1, 5) AS substring FROM DUAL;

-- SUBSTRC, SUBSTR2, SUBSTR4 (Unicode)
SELECT SUBSTRC('Hello', 1, 3) AS substrc FROM DUAL;

-- Applied to columns
SELECT first_name,
       SUBSTR(first_name, 1, 3) AS first_3_chars,
       SUBSTR(first_name, -2) AS last_2_chars
FROM employees;

-- -----------------------------------------------------------------------------
-- 3. String Search Functions
-- -----------------------------------------------------------------------------

-- INSTR - find position of substring
SELECT INSTR('Hello World', 'o') AS position FROM DUAL;           -- 5
SELECT INSTR('Hello World', 'o', 6) AS position FROM DUAL;        -- 8 (start from position 6)
SELECT INSTR('Hello World', 'o', 1, 2) AS position FROM DUAL;     -- 8 (2nd occurrence)
SELECT INSTR('Hello World', 'x') AS position FROM DUAL;           -- 0 (not found)

-- INSTRB, INSTRC, INSTR2, INSTR4
SELECT INSTRB('Hello World', 'o') AS position FROM DUAL;

-- Find multiple occurrences
SELECT first_name,
       INSTR(first_name, 'a') AS first_a,
       INSTR(first_name, 'a', 1, 2) AS second_a
FROM employees
WHERE INSTR(first_name, 'a') > 0;

-- -----------------------------------------------------------------------------
-- 4. String Concatenation
-- -----------------------------------------------------------------------------

-- CONCAT function (only 2 arguments)
SELECT CONCAT('Hello', ' World') AS concatenated FROM DUAL;
SELECT CONCAT(CONCAT('A', 'B'), 'C') AS concatenated FROM DUAL;

-- Concatenation operator ||
SELECT 'Hello' || ' ' || 'World' AS concatenated FROM DUAL;
SELECT first_name || ' ' || last_name AS full_name FROM employees;

-- CONCAT_WS (Oracle 21c+)
SELECT CONCAT_WS(', ', 'One', 'Two', 'Three') AS result FROM DUAL;

-- Concatenation with NULLs
SELECT 'Hello' || NULL || 'World' AS with_null FROM DUAL;  -- 'HelloWorld'
SELECT CONCAT('Hello', NULL) AS with_null FROM DUAL;       -- 'Hello'

-- -----------------------------------------------------------------------------
-- 5. Trimming Functions
-- -----------------------------------------------------------------------------

-- TRIM
SELECT TRIM('  Hello World  ') AS trimmed FROM DUAL;
SELECT TRIM(LEADING ' ' FROM '  Hello  ') AS trimmed FROM DUAL;
SELECT TRIM(TRAILING ' ' FROM '  Hello  ') AS trimmed FROM DUAL;
SELECT TRIM(BOTH ' ' FROM '  Hello  ') AS trimmed FROM DUAL;

-- TRIM with specific characters
SELECT TRIM('x' FROM 'xxxHelloxxx') AS trimmed FROM DUAL;
SELECT TRIM(LEADING '0' FROM '000123') AS trimmed FROM DUAL;

-- LTRIM and RTRIM
SELECT LTRIM('  Hello  ') AS ltrimmed FROM DUAL;
SELECT RTRIM('  Hello  ') AS rtrimmed FROM DUAL;
SELECT LTRIM('xxxHello', 'x') AS ltrimmed FROM DUAL;
SELECT RTRIM('Helloxxx', 'x') AS rtrimmed FROM DUAL;

-- LTRIM/RTRIM with character set
SELECT LTRIM('xyxHelloxyxy', 'xy') AS trimmed FROM DUAL;  -- 'Helloxyxy'

-- Applied to columns
SELECT '|' || TRIM(first_name) || '|' AS trimmed_name FROM employees;

-- -----------------------------------------------------------------------------
-- 6. Padding Functions
-- -----------------------------------------------------------------------------

-- LPAD
SELECT LPAD('123', 10, '0') AS padded FROM DUAL;      -- '0000000123'
SELECT LPAD('Hello', 10) AS padded FROM DUAL;         -- '     Hello'
SELECT LPAD('Hello', 10, '*') AS padded FROM DUAL;    -- '*****Hello'

-- RPAD
SELECT RPAD('123', 10, '0') AS padded FROM DUAL;      -- '1230000000'
SELECT RPAD('Hello', 10) AS padded FROM DUAL;         -- 'Hello     '
SELECT RPAD('Hello', 10, '-') AS padded FROM DUAL;    -- 'Hello-----'

-- Formatting with LPAD/RPAD
SELECT LPAD(employee_id, 6, '0') AS emp_code,
       RPAD(first_name, 15) || RPAD(last_name, 20) AS formatted_name
FROM employees;

-- -----------------------------------------------------------------------------
-- 7. Replace and Translate Functions
-- -----------------------------------------------------------------------------

-- REPLACE
SELECT REPLACE('Hello World', 'World', 'Oracle') AS replaced FROM DUAL;
SELECT REPLACE('Hello World', 'o', '0') AS replaced FROM DUAL;
SELECT REPLACE('Hello World', ' ', '') AS replaced FROM DUAL;  -- Remove spaces

-- REPLACE with columns
SELECT email, REPLACE(email, '@', ' [at] ') AS safe_email FROM employees;

-- TRANSLATE (character-by-character replacement)
SELECT TRANSLATE('Hello', 'el', '31') AS translated FROM DUAL;  -- 'H311o'
SELECT TRANSLATE('12345', '12345', 'abcde') AS translated FROM DUAL;  -- 'abcde'

-- Remove characters with TRANSLATE
SELECT TRANSLATE('Hello123World', '0123456789', ' ') AS letters_only FROM DUAL;

-- ROT13 encoding with TRANSLATE
SELECT TRANSLATE('Hello', 
       'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz',
       'NOPQRSTUVWXYZABCDEFGHIJKLMnopqrstuvwxyzabcdefghijklm') AS rot13 
FROM DUAL;

-- Keep only digits
SELECT TRANSLATE('Phone: (123) 456-7890', 
       TRANSLATE('Phone: (123) 456-7890', '0123456789', ''),
       RPAD(' ', LENGTH(TRANSLATE('Phone: (123) 456-7890', '0123456789', '')), ' ')
) AS digits_only FROM DUAL;

-- -----------------------------------------------------------------------------
-- 8. ASCII and Character Functions
-- -----------------------------------------------------------------------------

-- ASCII - get ASCII value
SELECT ASCII('A') AS ascii_val FROM DUAL;  -- 65
SELECT ASCII('a') AS ascii_val FROM DUAL;  -- 97
SELECT ASCII('1') AS ascii_val FROM DUAL;  -- 49

-- CHR - get character from ASCII value
SELECT CHR(65) AS character FROM DUAL;     -- 'A'
SELECT CHR(97) AS character FROM DUAL;     -- 'a'
SELECT CHR(10) AS newline FROM DUAL;       -- newline character

-- NCHR - Unicode character
SELECT NCHR(65) AS character FROM DUAL;

-- Building strings with CHR
SELECT 'Line1' || CHR(13) || CHR(10) || 'Line2' AS multi_line FROM DUAL;

-- ASCIISTR - convert to ASCII representation
SELECT ASCIISTR('ËÑÏÜÈ') AS ascii_str FROM DUAL;

-- UNISTR - create Unicode string
SELECT UNISTR('\0041\0042\0043') AS unicode_str FROM DUAL;  -- 'ABC'

-- -----------------------------------------------------------------------------
-- 9. Soundex and Phonetic Functions
-- -----------------------------------------------------------------------------

-- SOUNDEX
SELECT SOUNDEX('Smith') AS soundex FROM DUAL;
SELECT SOUNDEX('Smyth') AS soundex FROM DUAL;

-- Find similar sounding names
SELECT first_name FROM employees
WHERE SOUNDEX(first_name) = SOUNDEX('Steven');

-- Compare soundex
SELECT 'Smith', 'Smyth', 
       CASE WHEN SOUNDEX('Smith') = SOUNDEX('Smyth') 
            THEN 'Sound alike' ELSE 'Different' END AS comparison
FROM DUAL;

-- -----------------------------------------------------------------------------
-- 10. String Comparison and Matching
-- -----------------------------------------------------------------------------

-- LIKE patterns
SELECT * FROM employees WHERE first_name LIKE 'A%';
SELECT * FROM employees WHERE first_name LIKE '%a%';
SELECT * FROM employees WHERE first_name LIKE '____';  -- 4 characters
SELECT * FROM employees WHERE email LIKE '%\_%' ESCAPE '\';  -- contains underscore

-- Case-insensitive LIKE
SELECT * FROM employees WHERE UPPER(first_name) LIKE 'JOHN%';

-- NLS_LOWER/NLS_UPPER with locale
SELECT NLS_LOWER('HELLO', 'NLS_SORT=GERMAN') AS lower_german FROM DUAL;
SELECT NLS_UPPER('hello', 'NLS_SORT=GERMAN') AS upper_german FROM DUAL;

-- NLSSORT for locale-specific sorting
SELECT first_name FROM employees
ORDER BY NLSSORT(first_name, 'NLS_SORT=GERMAN');

-- -----------------------------------------------------------------------------
-- 11. Reverse and Repeat
-- -----------------------------------------------------------------------------

-- REVERSE
SELECT REVERSE('Hello') AS reversed FROM DUAL;  -- 'olleH'

-- Check palindrome
SELECT 
    'radar' AS word,
    CASE WHEN 'radar' = REVERSE('radar') THEN 'Palindrome' ELSE 'Not' END AS is_palindrome
FROM DUAL;

-- Repeat (using RPAD trick)
SELECT RPAD('*', 10, '*') AS stars FROM DUAL;  -- '**********'
SELECT LPAD(' ', 10, 'Ab') AS repeated FROM DUAL;

-- -----------------------------------------------------------------------------
-- 12. Quote Functions
-- -----------------------------------------------------------------------------

-- Quoting strings
SELECT q'[It's a nice day]' AS quoted FROM DUAL;
SELECT q'{It's a "nice" day}' AS quoted FROM DUAL;
SELECT q'<It's a 'nice' day>' AS quoted FROM DUAL;

-- DUMP - internal representation
SELECT DUMP('ABC') AS dump_result FROM DUAL;
SELECT DUMP('ABC', 16) AS dump_hex FROM DUAL;
SELECT DUMP('ABC', 10) AS dump_decimal FROM DUAL;

-- VSIZE - storage size
SELECT VSIZE('Hello') AS vsize FROM DUAL;
SELECT first_name, VSIZE(first_name) AS storage_size FROM employees;

-- -----------------------------------------------------------------------------
-- 13. String Splitting and Parsing
-- -----------------------------------------------------------------------------

-- Split string using SUBSTR and INSTR
WITH test_data AS (
    SELECT 'apple,banana,cherry' AS csv_string FROM DUAL
)
SELECT 
    SUBSTR(csv_string, 1, INSTR(csv_string, ',') - 1) AS first_item,
    SUBSTR(csv_string, INSTR(csv_string, ',') + 1, 
           INSTR(csv_string, ',', 1, 2) - INSTR(csv_string, ',') - 1) AS second_item,
    SUBSTR(csv_string, INSTR(csv_string, ',', 1, 2) + 1) AS third_item
FROM test_data;

-- Using REGEXP_SUBSTR for splitting (covered in regex file)
SELECT REGEXP_SUBSTR('apple,banana,cherry', '[^,]+', 1, 1) AS item1,
       REGEXP_SUBSTR('apple,banana,cherry', '[^,]+', 1, 2) AS item2,
       REGEXP_SUBSTR('apple,banana,cherry', '[^,]+', 1, 3) AS item3
FROM DUAL;

-- -----------------------------------------------------------------------------
-- 14. Complex String Operations
-- -----------------------------------------------------------------------------

-- Format phone number
SELECT phone_number,
       '(' || SUBSTR(phone_number, 1, 3) || ') ' ||
       SUBSTR(phone_number, 5, 3) || '-' ||
       SUBSTR(phone_number, 9) AS formatted_phone
FROM employees
WHERE phone_number IS NOT NULL;

-- Extract domain from email
SELECT email,
       SUBSTR(email, INSTR(email, '@') + 1) AS domain
FROM employees;

-- Mask sensitive data
SELECT first_name,
       SUBSTR(first_name, 1, 1) || RPAD('*', LENGTH(first_name) - 1, '*') AS masked_name
FROM employees;

-- Generate initials
SELECT first_name, last_name,
       SUBSTR(first_name, 1, 1) || SUBSTR(last_name, 1, 1) AS initials
FROM employees;

-- Word count
SELECT 'This is a test string' AS text,
       LENGTH('This is a test string') - LENGTH(REPLACE('This is a test string', ' ', '')) + 1 AS word_count
FROM DUAL;

-- Capitalize first letter of each word (custom implementation)
SELECT INITCAP('hello world, this is a TEST') AS proper_case FROM DUAL;

-- Remove multiple spaces
SELECT REGEXP_REPLACE('Hello    World    Test', ' +', ' ') AS single_spaces FROM DUAL;

-- Extract Nth word
WITH test_data AS (
    SELECT 'one two three four five' AS sentence FROM DUAL
)
SELECT 
    REGEXP_SUBSTR(sentence, '\S+', 1, 3) AS third_word
FROM test_data;

