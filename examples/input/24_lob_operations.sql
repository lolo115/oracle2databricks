-- ============================================================================
-- File: 24_lob_operations.sql
-- Description: LOB (Large Object) operations - CLOB, BLOB, BFILE
-- ============================================================================

-- -----------------------------------------------------------------------------
-- 1. LOB Table Creation
-- -----------------------------------------------------------------------------

-- Create table with LOB columns
CREATE TABLE documents (
    doc_id NUMBER PRIMARY KEY,
    doc_name VARCHAR2(100),
    doc_content CLOB,
    doc_binary BLOB,
    external_file BFILE,
    created_date DATE DEFAULT SYSDATE
);

-- Table with LOB storage options
CREATE TABLE large_documents (
    doc_id NUMBER PRIMARY KEY,
    content CLOB,
    binary_data BLOB
)
LOB (content) STORE AS SECUREFILE (
    TABLESPACE users
    ENABLE STORAGE IN ROW
    CHUNK 8192
    COMPRESS MEDIUM
    DEDUPLICATE
    CACHE READS
)
LOB (binary_data) STORE AS SECUREFILE (
    TABLESPACE users
    DISABLE STORAGE IN ROW
    COMPRESS HIGH
    NOCACHE
);

-- -----------------------------------------------------------------------------
-- 2. Basic LOB Operations in SQL
-- -----------------------------------------------------------------------------

-- Insert with LOB
INSERT INTO documents (doc_id, doc_name, doc_content)
VALUES (1, 'Sample Document', 'This is a sample document content.');

-- Insert EMPTY_CLOB() / EMPTY_BLOB()
INSERT INTO documents (doc_id, doc_name, doc_content, doc_binary)
VALUES (2, 'Empty LOBs', EMPTY_CLOB(), EMPTY_BLOB());

-- Update LOB
UPDATE documents
SET doc_content = 'Updated content for the document.'
WHERE doc_id = 1;

-- Select LOB
SELECT doc_id, doc_name, doc_content FROM documents;

-- LOB length
SELECT doc_id, doc_name, 
       DBMS_LOB.GETLENGTH(doc_content) AS content_length
FROM documents;

-- LOB substring
SELECT doc_id, 
       DBMS_LOB.SUBSTR(doc_content, 100, 1) AS first_100_chars
FROM documents;

-- LOB comparison (using DBMS_LOB.COMPARE)
SELECT doc_id, doc_name
FROM documents
WHERE DBMS_LOB.COMPARE(doc_content, 'Updated content for the document.') = 0;

-- Convert CLOB to VARCHAR2 (if small enough)
SELECT doc_id, TO_CHAR(doc_content) AS content_text
FROM documents
WHERE DBMS_LOB.GETLENGTH(doc_content) < 4000;

-- -----------------------------------------------------------------------------
-- 3. DBMS_LOB Package - CLOB Operations
-- -----------------------------------------------------------------------------

DECLARE
    v_clob CLOB;
    v_temp_clob CLOB;
    v_buffer VARCHAR2(32767);
    v_amount NUMBER := 32767;
    v_offset NUMBER := 1;
    v_length NUMBER;
    v_position NUMBER;
BEGIN
    -- Create temporary CLOB
    DBMS_LOB.CREATETEMPORARY(v_clob, TRUE);
    
    -- Write to CLOB
    v_buffer := 'Hello World! This is a test CLOB content.';
    DBMS_LOB.WRITEAPPEND(v_clob, LENGTH(v_buffer), v_buffer);
    
    -- Get length
    v_length := DBMS_LOB.GETLENGTH(v_clob);
    DBMS_OUTPUT.PUT_LINE('CLOB length: ' || v_length);
    
    -- Read from CLOB
    v_amount := v_length;
    DBMS_LOB.READ(v_clob, v_amount, 1, v_buffer);
    DBMS_OUTPUT.PUT_LINE('Content: ' || v_buffer);
    
    -- Search in CLOB
    v_position := DBMS_LOB.INSTR(v_clob, 'test', 1, 1);
    DBMS_OUTPUT.PUT_LINE('Position of "test": ' || v_position);
    
    -- Substring
    v_buffer := DBMS_LOB.SUBSTR(v_clob, 5, 7);  -- 5 chars starting at position 7
    DBMS_OUTPUT.PUT_LINE('Substring: ' || v_buffer);
    
    -- Append more content
    DBMS_LOB.WRITEAPPEND(v_clob, 11, ' More text.');
    
    -- Copy CLOB
    DBMS_LOB.CREATETEMPORARY(v_temp_clob, TRUE);
    DBMS_LOB.COPY(v_temp_clob, v_clob, DBMS_LOB.GETLENGTH(v_clob), 1, 1);
    
    -- Trim CLOB
    DBMS_LOB.TRIM(v_clob, 20);  -- Keep only first 20 characters
    
    -- Erase portion
    v_amount := 5;
    DBMS_LOB.ERASE(v_clob, v_amount, 1);  -- Erase 5 chars from position 1
    
    -- Compare CLOBs
    IF DBMS_LOB.COMPARE(v_clob, v_temp_clob) != 0 THEN
        DBMS_OUTPUT.PUT_LINE('CLOBs are different');
    END IF;
    
    -- Free temporary CLOBs
    DBMS_LOB.FREETEMPORARY(v_clob);
    DBMS_LOB.FREETEMPORARY(v_temp_clob);
END;
/

-- -----------------------------------------------------------------------------
-- 4. DBMS_LOB Package - BLOB Operations
-- -----------------------------------------------------------------------------

DECLARE
    v_blob BLOB;
    v_raw RAW(32767);
    v_amount NUMBER;
    v_offset NUMBER := 1;
BEGIN
    -- Create temporary BLOB
    DBMS_LOB.CREATETEMPORARY(v_blob, TRUE);
    
    -- Write RAW data to BLOB
    v_raw := UTL_RAW.CAST_TO_RAW('Binary content');
    DBMS_LOB.WRITEAPPEND(v_blob, UTL_RAW.LENGTH(v_raw), v_raw);
    
    -- Get length
    DBMS_OUTPUT.PUT_LINE('BLOB length: ' || DBMS_LOB.GETLENGTH(v_blob));
    
    -- Read from BLOB
    v_amount := DBMS_LOB.GETLENGTH(v_blob);
    DBMS_LOB.READ(v_blob, v_amount, 1, v_raw);
    
    -- Convert back to string
    DBMS_OUTPUT.PUT_LINE('Content: ' || UTL_RAW.CAST_TO_VARCHAR2(v_raw));
    
    -- Append more data
    v_raw := UTL_RAW.CAST_TO_RAW(' More binary data');
    DBMS_LOB.WRITEAPPEND(v_blob, UTL_RAW.LENGTH(v_raw), v_raw);
    
    -- Free temporary BLOB
    DBMS_LOB.FREETEMPORARY(v_blob);
END;
/

-- -----------------------------------------------------------------------------
-- 5. Converting Between LOB Types
-- -----------------------------------------------------------------------------

DECLARE
    v_clob CLOB;
    v_blob BLOB;
    v_amount NUMBER;
    v_dest_offset NUMBER := 1;
    v_src_offset NUMBER := 1;
    v_lang_context NUMBER := DBMS_LOB.DEFAULT_LANG_CTX;
    v_warning NUMBER;
BEGIN
    -- Create temporary LOBs
    DBMS_LOB.CREATETEMPORARY(v_clob, TRUE);
    DBMS_LOB.CREATETEMPORARY(v_blob, TRUE);
    
    -- Write to CLOB
    DBMS_LOB.WRITEAPPEND(v_clob, 20, 'Convert me to BLOB!');
    
    -- CLOB to BLOB conversion
    v_amount := DBMS_LOB.GETLENGTH(v_clob);
    DBMS_LOB.CONVERTTOBLOB(
        dest_lob     => v_blob,
        src_clob     => v_clob,
        amount       => v_amount,
        dest_offset  => v_dest_offset,
        src_offset   => v_src_offset,
        blob_csid    => DBMS_LOB.DEFAULT_CSID,
        lang_context => v_lang_context,
        warning      => v_warning
    );
    
    DBMS_OUTPUT.PUT_LINE('BLOB length after conversion: ' || DBMS_LOB.GETLENGTH(v_blob));
    
    -- Reset for reverse conversion
    DBMS_LOB.TRIM(v_clob, 0);
    v_dest_offset := 1;
    v_src_offset := 1;
    v_amount := DBMS_LOB.GETLENGTH(v_blob);
    
    -- BLOB to CLOB conversion
    DBMS_LOB.CONVERTTOCLOB(
        dest_lob     => v_clob,
        src_blob     => v_blob,
        amount       => v_amount,
        dest_offset  => v_dest_offset,
        src_offset   => v_src_offset,
        blob_csid    => DBMS_LOB.DEFAULT_CSID,
        lang_context => v_lang_context,
        warning      => v_warning
    );
    
    DBMS_OUTPUT.PUT_LINE('CLOB content: ' || DBMS_LOB.SUBSTR(v_clob, 100, 1));
    
    -- Cleanup
    DBMS_LOB.FREETEMPORARY(v_clob);
    DBMS_LOB.FREETEMPORARY(v_blob);
END;
/

-- -----------------------------------------------------------------------------
-- 6. Working with BFILE (External Files)
-- -----------------------------------------------------------------------------

-- Create directory object (DBA privilege required)
-- CREATE DIRECTORY doc_dir AS '/path/to/documents';

DECLARE
    v_bfile BFILE;
    v_blob BLOB;
    v_dest_offset NUMBER := 1;
    v_src_offset NUMBER := 1;
    v_exists NUMBER;
    v_length NUMBER;
BEGIN
    -- Initialize BFILE locator
    v_bfile := BFILENAME('DOC_DIR', 'sample.txt');
    
    -- Check if file exists
    IF DBMS_LOB.FILEEXISTS(v_bfile) = 1 THEN
        -- Open BFILE for reading
        DBMS_LOB.FILEOPEN(v_bfile, DBMS_LOB.FILE_READONLY);
        
        -- Get file length
        v_length := DBMS_LOB.GETLENGTH(v_bfile);
        DBMS_OUTPUT.PUT_LINE('File length: ' || v_length);
        
        -- Load BFILE into BLOB
        DBMS_LOB.CREATETEMPORARY(v_blob, TRUE);
        DBMS_LOB.LOADFROMFILE(v_blob, v_bfile, v_length);
        
        -- Close BFILE
        DBMS_LOB.FILECLOSE(v_bfile);
        
        DBMS_OUTPUT.PUT_LINE('Loaded ' || DBMS_LOB.GETLENGTH(v_blob) || ' bytes');
        
        DBMS_LOB.FREETEMPORARY(v_blob);
    ELSE
        DBMS_OUTPUT.PUT_LINE('File does not exist');
    END IF;
EXCEPTION
    WHEN OTHERS THEN
        IF DBMS_LOB.FILEISOPEN(v_bfile) = 1 THEN
            DBMS_LOB.FILECLOSE(v_bfile);
        END IF;
        RAISE;
END;
/

-- Insert BFILE reference
INSERT INTO documents (doc_id, doc_name, external_file)
VALUES (3, 'External File', BFILENAME('DOC_DIR', 'external.txt'));

-- -----------------------------------------------------------------------------
-- 7. LOB in Procedures and Functions
-- -----------------------------------------------------------------------------

-- Procedure to append text to CLOB
CREATE OR REPLACE PROCEDURE append_to_clob(
    p_doc_id NUMBER,
    p_text VARCHAR2
)
IS
    v_clob CLOB;
BEGIN
    -- Select FOR UPDATE to lock the row
    SELECT doc_content INTO v_clob
    FROM documents
    WHERE doc_id = p_doc_id
    FOR UPDATE;
    
    -- Append text
    DBMS_LOB.WRITEAPPEND(v_clob, LENGTH(p_text), p_text);
    
    COMMIT;
END append_to_clob;
/

-- Function to search in CLOB
CREATE OR REPLACE FUNCTION find_in_clob(
    p_doc_id NUMBER,
    p_search VARCHAR2,
    p_occurrence NUMBER DEFAULT 1
)
RETURN NUMBER
IS
    v_clob CLOB;
    v_position NUMBER;
BEGIN
    SELECT doc_content INTO v_clob
    FROM documents
    WHERE doc_id = p_doc_id;
    
    v_position := DBMS_LOB.INSTR(v_clob, p_search, 1, p_occurrence);
    
    RETURN v_position;
END find_in_clob;
/

-- Function to get CLOB chunk
CREATE OR REPLACE FUNCTION get_clob_chunk(
    p_doc_id NUMBER,
    p_start NUMBER,
    p_length NUMBER
)
RETURN VARCHAR2
IS
    v_clob CLOB;
BEGIN
    SELECT doc_content INTO v_clob
    FROM documents
    WHERE doc_id = p_doc_id;
    
    RETURN DBMS_LOB.SUBSTR(v_clob, p_length, p_start);
END get_clob_chunk;
/

-- Procedure to copy LOB between rows
CREATE OR REPLACE PROCEDURE copy_document_content(
    p_source_id NUMBER,
    p_target_id NUMBER
)
IS
    v_source_clob CLOB;
    v_target_clob CLOB;
    v_length NUMBER;
BEGIN
    -- Get source CLOB
    SELECT doc_content INTO v_source_clob
    FROM documents
    WHERE doc_id = p_source_id;
    
    -- Get target CLOB for update
    SELECT doc_content INTO v_target_clob
    FROM documents
    WHERE doc_id = p_target_id
    FOR UPDATE;
    
    -- Clear target and copy
    DBMS_LOB.TRIM(v_target_clob, 0);
    v_length := DBMS_LOB.GETLENGTH(v_source_clob);
    DBMS_LOB.COPY(v_target_clob, v_source_clob, v_length, 1, 1);
    
    COMMIT;
END copy_document_content;
/

-- -----------------------------------------------------------------------------
-- 8. Streaming LOB Data
-- -----------------------------------------------------------------------------

DECLARE
    v_clob CLOB;
    v_buffer VARCHAR2(32767);
    v_chunk_size CONSTANT NUMBER := 32767;
    v_offset NUMBER := 1;
    v_amount NUMBER;
    v_total_length NUMBER;
BEGIN
    -- Get the LOB
    SELECT doc_content INTO v_clob
    FROM documents
    WHERE doc_id = 1;
    
    v_total_length := DBMS_LOB.GETLENGTH(v_clob);
    
    -- Process in chunks
    WHILE v_offset <= v_total_length LOOP
        v_amount := LEAST(v_chunk_size, v_total_length - v_offset + 1);
        
        DBMS_LOB.READ(v_clob, v_amount, v_offset, v_buffer);
        
        -- Process chunk (e.g., write to file, send over network, etc.)
        DBMS_OUTPUT.PUT_LINE('Processing chunk at offset ' || v_offset || ', size ' || v_amount);
        
        v_offset := v_offset + v_amount;
    END LOOP;
END;
/

-- Writing large content in chunks
DECLARE
    v_clob CLOB;
    v_large_text VARCHAR2(32767);
    v_chunks NUMBER := 10;
BEGIN
    -- Initialize CLOB
    SELECT doc_content INTO v_clob
    FROM documents
    WHERE doc_id = 1
    FOR UPDATE;
    
    -- Clear existing content
    DBMS_LOB.TRIM(v_clob, 0);
    
    -- Build and write content in chunks
    FOR i IN 1..v_chunks LOOP
        v_large_text := 'Chunk ' || i || ': ' || RPAD('X', 1000, 'X') || CHR(10);
        DBMS_LOB.WRITEAPPEND(v_clob, LENGTH(v_large_text), v_large_text);
    END LOOP;
    
    COMMIT;
    
    DBMS_OUTPUT.PUT_LINE('Final CLOB size: ' || DBMS_LOB.GETLENGTH(v_clob));
END;
/

-- -----------------------------------------------------------------------------
-- 9. LOB Indexing and Search (Oracle Text)
-- -----------------------------------------------------------------------------

-- Create Oracle Text index on CLOB column
/*
CREATE INDEX doc_content_idx ON documents(doc_content) 
INDEXTYPE IS CTXSYS.CONTEXT;

-- Search using CONTAINS
SELECT doc_id, doc_name
FROM documents
WHERE CONTAINS(doc_content, 'search term') > 0;

-- Search with scoring
SELECT doc_id, doc_name, SCORE(1) AS relevance
FROM documents
WHERE CONTAINS(doc_content, 'search term', 1) > 0
ORDER BY SCORE(1) DESC;
*/

-- -----------------------------------------------------------------------------
-- 10. SecureFile LOB Features
-- -----------------------------------------------------------------------------

-- Compression
/*
ALTER TABLE documents MODIFY LOB (doc_content) (COMPRESS HIGH);
ALTER TABLE documents MODIFY LOB (doc_content) (COMPRESS MEDIUM);
ALTER TABLE documents MODIFY LOB (doc_content) (NOCOMPRESS);

-- Deduplication
ALTER TABLE documents MODIFY LOB (doc_content) (DEDUPLICATE);
ALTER TABLE documents MODIFY LOB (doc_content) (KEEP_DUPLICATES);

-- Encryption
ALTER TABLE documents MODIFY LOB (doc_content) (ENCRYPT);
ALTER TABLE documents MODIFY LOB (doc_content) (DECRYPT);
*/

-- Query LOB storage information
SELECT table_name, column_name, segment_name, securefile, compression, deduplication
FROM user_lobs
WHERE table_name = 'DOCUMENTS';

-- -----------------------------------------------------------------------------
-- 11. Practical Examples
-- -----------------------------------------------------------------------------

-- Load file content into CLOB
CREATE OR REPLACE PROCEDURE load_file_to_clob(
    p_doc_id NUMBER,
    p_directory VARCHAR2,
    p_filename VARCHAR2
)
IS
    v_bfile BFILE;
    v_clob CLOB;
    v_dest_offset NUMBER := 1;
    v_src_offset NUMBER := 1;
    v_lang_context NUMBER := DBMS_LOB.DEFAULT_LANG_CTX;
    v_warning NUMBER;
BEGIN
    v_bfile := BFILENAME(p_directory, p_filename);
    
    IF DBMS_LOB.FILEEXISTS(v_bfile) = 1 THEN
        SELECT doc_content INTO v_clob
        FROM documents
        WHERE doc_id = p_doc_id
        FOR UPDATE;
        
        DBMS_LOB.TRIM(v_clob, 0);
        DBMS_LOB.FILEOPEN(v_bfile, DBMS_LOB.FILE_READONLY);
        
        DBMS_LOB.LOADCLOBFROMFILE(
            dest_lob     => v_clob,
            src_bfile    => v_bfile,
            amount       => DBMS_LOB.GETLENGTH(v_bfile),
            dest_offset  => v_dest_offset,
            src_offset   => v_src_offset,
            bfile_csid   => DBMS_LOB.DEFAULT_CSID,
            lang_context => v_lang_context,
            warning      => v_warning
        );
        
        DBMS_LOB.FILECLOSE(v_bfile);
        COMMIT;
    ELSE
        RAISE_APPLICATION_ERROR(-20001, 'File not found');
    END IF;
END load_file_to_clob;
/

-- Export CLOB to file using UTL_FILE
CREATE OR REPLACE PROCEDURE export_clob_to_file(
    p_doc_id NUMBER,
    p_directory VARCHAR2,
    p_filename VARCHAR2
)
IS
    v_clob CLOB;
    v_file UTL_FILE.FILE_TYPE;
    v_buffer VARCHAR2(32767);
    v_chunk_size CONSTANT NUMBER := 32767;
    v_offset NUMBER := 1;
    v_amount NUMBER;
    v_length NUMBER;
BEGIN
    SELECT doc_content INTO v_clob
    FROM documents
    WHERE doc_id = p_doc_id;
    
    v_length := DBMS_LOB.GETLENGTH(v_clob);
    
    v_file := UTL_FILE.FOPEN(p_directory, p_filename, 'W', v_chunk_size);
    
    WHILE v_offset <= v_length LOOP
        v_amount := LEAST(v_chunk_size, v_length - v_offset + 1);
        DBMS_LOB.READ(v_clob, v_amount, v_offset, v_buffer);
        UTL_FILE.PUT(v_file, v_buffer);
        v_offset := v_offset + v_amount;
    END LOOP;
    
    UTL_FILE.FCLOSE(v_file);
EXCEPTION
    WHEN OTHERS THEN
        IF UTL_FILE.IS_OPEN(v_file) THEN
            UTL_FILE.FCLOSE(v_file);
        END IF;
        RAISE;
END export_clob_to_file;
/

-- Compare two CLOBs
CREATE OR REPLACE FUNCTION compare_clobs(
    p_clob1 CLOB,
    p_clob2 CLOB
)
RETURN NUMBER
IS
BEGIN
    IF p_clob1 IS NULL AND p_clob2 IS NULL THEN
        RETURN 0;
    ELSIF p_clob1 IS NULL OR p_clob2 IS NULL THEN
        RETURN -1;
    ELSE
        RETURN DBMS_LOB.COMPARE(p_clob1, p_clob2);
    END IF;
END compare_clobs;
/

