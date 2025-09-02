/*
=============================================================
Create Database and Schemas (PostgreSQL Version)
=============================================================
Script Purpose:
    This script creates a new database named 'DataWarehouse'.
    If the database exists, it is dropped and recreated.
    Additionally, it creates schemas: bronze, silver, and gold.

WARNING:
    Running this script will drop the entire 'DataWarehouse'
    database if it exists. All data in the database will be permanently deleted.
    Proceed with caution and ensure you have proper backups.
=============================================================
*/

-- Step 1: Drop and recreate the database
DROP DATABASE IF EXISTS DataWarehouse;
CREATE DATABASE DataWarehouse;

-- ⚠️ IMPORTANT: 
-- After running this part, reconnect to "DataWarehouses" before continuing.
-- In pgAdmin: right-click on datawarehouseanalytics → Query Tool

-- Step 2: Create Schemas
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- Step 3: Create Tables in gold schema
CREATE TABLE gold.dim_customers (
    customer_key     INT,
    customer_id      INT,
    customer_number  VARCHAR(50),
    first_name       VARCHAR(50),
    last_name        VARCHAR(50),
    country          VARCHAR(50),
    marital_status   VARCHAR(50),
    gender           VARCHAR(50),
    birthdate        DATE,
    create_date      DATE
);

CREATE TABLE gold.dim_products (
    product_key     INT,
    product_id      INT,
    product_number  VARCHAR(50),
    product_name    VARCHAR(50),
    category_id     VARCHAR(50),
    category        VARCHAR(50),
    subcategory     VARCHAR(50),
    maintenance     VARCHAR(50),
    cost            INT,
    product_line    VARCHAR(50),
    start_date      DATE
);

CREATE TABLE gold.fact_sales (
    order_number    VARCHAR(50),
    product_key     INT,
    customer_key    INT,
    order_date      DATE,
    shipping_date   DATE,
    due_date        DATE,
    sales_amount    INT,
    quantity        SMALLINT,
    price           INT
);

-- Step 4: (Optional) Load CSV files
-- Adjust the file paths to where your CSVs are stored on your machine
-- Use pgAdmin "Import/Export" tool if COPY cannot access local paths

-- Example:
-- TRUNCATE TABLE gold.dim_customers;
-- COPY gold.dim_customers
-- FROM '/absolute/path/to/gold.dim_customers.csv'
-- DELIMITER ',' CSV HEADER;

-- TRUNCATE TABLE gold.dim_products;
-- COPY gold.dim_products
-- FROM '/absolute/path/to/gold.dim_products.csv'
-- DELIMITER ',' CSV HEADER;

-- TRUNCATE TABLE gold.fact_sales;
-- COPY gold.fact_sales
-- FROM '/absolute/path/to/gold.fact_sales.csv'
-- DELIMITER ',' CSV HEADER;
