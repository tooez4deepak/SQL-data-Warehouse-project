-- Purpose: Inspect metadata about tables and columns in the current SQL Server database.
-- This includes listing all tables, checking the structure of a specific table ('dim_customers'),
-- and retrieving detailed schema information for both tables and columns.

-- Retrieve all tables in the database
SELECT 
    *
FROM INFORMATION_SCHEMA.TABLES;

-- Retrieve all columns for the 'dim_customers' table
SELECT 
    *
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'dim_customers';

-- Retrieve specific metadata fields about all tables
SELECT 
    TABLE_CATALOG,     -- Database name
    TABLE_SCHEMA,      -- Schema name (e.g., dbo)
    TABLE_NAME,        -- Table name
    TABLE_TYPE         -- 'BASE TABLE' or 'VIEW'
FROM INFORMATION_SCHEMA.TABLES;

-- Retrieve column details for 'dim_customers'
SELECT 
    COLUMN_NAME, 
    DATA_TYPE, 
    IS_NULLABLE, 
    CHARACTER_MAXIMUM_LENGTH  -- Only applies to character-based columns
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'dim_customers';
