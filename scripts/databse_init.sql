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
