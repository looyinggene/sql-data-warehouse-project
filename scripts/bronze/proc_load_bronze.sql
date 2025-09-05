-- ================================================
-- Stored Procedure: Load Bronze Layer (PostgreSQL)
-- ================================================
CREATE OR REPLACE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql
AS $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
BEGIN
    RAISE NOTICE '================================================';
    RAISE NOTICE 'Loading Bronze Layer';
    RAISE NOTICE '================================================';

    ------------------------------------------------
    -- CRM Tables
    ------------------------------------------------

    -- crm_cust_info
    start_time := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: bronze.crm_cust_info';
    TRUNCATE TABLE bronze.crm_cust_info;

    RAISE NOTICE '>> Loading Data Into: bronze.crm_cust_info';
    CREATE TEMP TABLE tmp_crm_cust_info (
        cst_id TEXT,
        cst_key TEXT,
        cst_firstname TEXT,
        cst_lastname TEXT,
        cst_marital_status TEXT,
        cst_gndr TEXT,
        cst_create_date TEXT
    );

    COPY tmp_crm_cust_info
    FROM '/datasets/source_crm/cust_info.csv'
    DELIMITER ','
    CSV HEADER
    QUOTE '"'
    NULL '';

    INSERT INTO bronze.crm_cust_info
    SELECT
        CASE WHEN TRIM(cst_id) = '' OR TRIM(cst_id) = '0' THEN NULL ELSE TRIM(cst_id)::INT END,
        TRIM(cst_key),
        TRIM(cst_firstname),
        TRIM(cst_lastname),
        TRIM(cst_marital_status),
        TRIM(cst_gndr),
        CASE WHEN TRIM(cst_create_date) = '' OR TRIM(cst_create_date) = '0' THEN NULL ELSE TO_DATE(TRIM(cst_create_date), 'YYYY-MM-DD') END
    FROM tmp_crm_cust_info;

    DROP TABLE tmp_crm_cust_info;
    end_time := clock_timestamp();
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(epoch FROM end_time - start_time);

    -- crm_prd_info
    start_time := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: bronze.crm_prd_info';
    TRUNCATE TABLE bronze.crm_prd_info;

    RAISE NOTICE '>> Loading Data Into: bronze.crm_prd_info';
    CREATE TEMP TABLE tmp_crm_prd_info (
        prd_id TEXT,
        prd_key TEXT,
        prd_nm TEXT,
        prd_cost TEXT,
        prd_line TEXT,
        prd_start_dt TEXT,
        prd_end_dt TEXT
    );

    COPY tmp_crm_prd_info
    FROM '/datasets/source_crm/prd_info.csv'
    DELIMITER ','
    CSV HEADER
    QUOTE '"'
    NULL '';

    INSERT INTO bronze.crm_prd_info
    SELECT
        CASE WHEN TRIM(prd_id) = '' OR TRIM(prd_id) = '0' THEN NULL ELSE TRIM(prd_id)::INT END,
        TRIM(prd_key),
        TRIM(prd_nm),
        CASE WHEN TRIM(prd_cost) = '' OR TRIM(prd_cost) = '0' THEN NULL ELSE TRIM(prd_cost)::INT END,
        TRIM(prd_line),
        CASE WHEN TRIM(prd_start_dt) = '' OR TRIM(prd_start_dt) = '0' THEN NULL ELSE TO_TIMESTAMP(TRIM(prd_start_dt),'YYYY-MM-DD') END,
        CASE WHEN TRIM(prd_end_dt) = '' OR TRIM(prd_end_dt) = '0' THEN NULL ELSE TO_TIMESTAMP(TRIM(prd_end_dt),'YYYY-MM-DD') END
    FROM tmp_crm_prd_info;

    DROP TABLE tmp_crm_prd_info;
    end_time := clock_timestamp();
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(epoch FROM end_time - start_time);

    -- crm_sales_details
    start_time := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: bronze.crm_sales_details';
    TRUNCATE TABLE bronze.crm_sales_details;

    RAISE NOTICE '>> Loading Data Into: bronze.crm_sales_details';
    CREATE TEMP TABLE tmp_crm_sales_details (
        sls_ord_num TEXT,
        sls_prd_key TEXT,
        sls_cust_id TEXT,
        sls_order_dt TEXT,
        sls_ship_dt TEXT,
        sls_due_dt TEXT,
        sls_sales TEXT,
        sls_quantity TEXT,
        sls_price TEXT
    );

    COPY tmp_crm_sales_details
    FROM '/datasets/source_crm/sales_details.csv'
    DELIMITER ','
    CSV HEADER
    QUOTE '"'
    NULL '';

    INSERT INTO bronze.crm_sales_details
SELECT
    TRIM(sls_ord_num),
    TRIM(sls_prd_key),
    CASE WHEN TRIM(sls_cust_id) = '' OR TRIM(sls_cust_id) = '0' THEN NULL ELSE TRIM(sls_cust_id)::INT END,
    CASE 
            WHEN TRIM(sls_order_dt) = '' OR TRIM(sls_order_dt) = '0' THEN NULL
            WHEN TRIM(sls_order_dt) ~ '^\d{8}$' THEN 
                 CASE 
                     WHEN TO_DATE(TRIM(sls_order_dt),'YYYYMMDD') BETWEEN '1900-01-01' AND '2100-12-31'
                     THEN TO_DATE(TRIM(sls_order_dt),'YYYYMMDD')
                     ELSE NULL
                 END
            WHEN TRIM(sls_order_dt) ~ '^\d{4}-\d{2}-\d{2}$' THEN 
                 CASE 
                     WHEN TO_DATE(TRIM(sls_order_dt),'YYYY-MM-DD') BETWEEN '1900-01-01' AND '2100-12-31'
                     THEN TO_DATE(TRIM(sls_order_dt),'YYYY-MM-DD')
                     ELSE NULL
                 END
            ELSE NULL
        END
        ,
    CASE 
        WHEN TRIM(sls_ship_dt) = '' OR TRIM(sls_ship_dt) = '0' THEN NULL
        WHEN TRIM(sls_ship_dt) ~ '^\d{8}$' THEN TO_DATE(TRIM(sls_ship_dt),'YYYYMMDD')
        ELSE TO_DATE(TRIM(sls_ship_dt),'YYYY-MM-DD')
    END,
    CASE 
        WHEN TRIM(sls_due_dt) = '' OR TRIM(sls_due_dt) = '0' THEN NULL
        WHEN TRIM(sls_due_dt) ~ '^\d{8}$' THEN TO_DATE(TRIM(sls_due_dt),'YYYYMMDD')
        ELSE TO_DATE(TRIM(sls_due_dt),'YYYY-MM-DD')
    END,
    CASE WHEN TRIM(sls_sales) = '' OR TRIM(sls_sales) = '0' THEN NULL ELSE TRIM(sls_sales)::INT END,
    CASE WHEN TRIM(sls_quantity) = '' OR TRIM(sls_quantity) = '0' THEN NULL ELSE TRIM(sls_quantity)::INT END,
    CASE WHEN TRIM(sls_price) = '' OR TRIM(sls_price) = '0' THEN NULL ELSE TRIM(sls_price)::INT END
FROM tmp_crm_sales_details;


    DROP TABLE tmp_crm_sales_details;
    end_time := clock_timestamp();
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(epoch FROM end_time - start_time);

    ------------------------------------------------
    -- ERP Tables
    ------------------------------------------------

    -- erp_loc_a101
    start_time := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: bronze.erp_loc_a101';
    TRUNCATE TABLE bronze.erp_loc_a101;

    RAISE NOTICE '>> Loading Data Into: bronze.erp_loc_a101';
    CREATE TEMP TABLE tmp_erp_loc_a101 (
        cid TEXT,
        cntry TEXT
    );

    COPY tmp_erp_loc_a101
    FROM '/datasets/source_erp/loc_a101.csv'
    DELIMITER ','
    CSV HEADER
    QUOTE '"'
    NULL '';

    INSERT INTO bronze.erp_loc_a101
    SELECT TRIM(cid), TRIM(cntry)
    FROM tmp_erp_loc_a101;

    DROP TABLE tmp_erp_loc_a101;
    end_time := clock_timestamp();
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(epoch FROM end_time - start_time);

    -- erp_cust_az12
    start_time := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: bronze.erp_cust_az12';
    TRUNCATE TABLE bronze.erp_cust_az12;

    RAISE NOTICE '>> Loading Data Into: bronze.erp_cust_az12';
    CREATE TEMP TABLE tmp_erp_cust_az12 (
        cid TEXT,
        bdate TEXT,
        gen TEXT
    );

    COPY tmp_erp_cust_az12
    FROM '/datasets/source_erp/cust_az12.csv'
    DELIMITER ','
    CSV HEADER
    QUOTE '"'
    NULL '';

    INSERT INTO bronze.erp_cust_az12
    SELECT
        TRIM(cid),
        CASE WHEN TRIM(bdate) = '' OR TRIM(bdate) = '0' THEN NULL ELSE TO_DATE(TRIM(bdate),'YYYY-MM-DD') END,
        TRIM(gen)
    FROM tmp_erp_cust_az12;

    DROP TABLE tmp_erp_cust_az12;
    end_time := clock_timestamp();
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(epoch FROM end_time - start_time);

    -- erp_px_cat_g1v2
    start_time := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: bronze.erp_px_cat_g1v2';
    TRUNCATE TABLE bronze.erp_px_cat_g1v2;

    RAISE NOTICE '>> Loading Data Into: bronze.erp_px_cat_g1v2';
    CREATE TEMP TABLE tmp_erp_px_cat_g1v2 (
        id TEXT,
        cat TEXT,
        subcat TEXT,
        maintenance TEXT
    );

    COPY tmp_erp_px_cat_g1v2
    FROM '/datasets/source_erp/px_cat_g1v2.csv'
    DELIMITER ','
    CSV HEADER
    QUOTE '"'
    NULL '';

    INSERT INTO bronze.erp_px_cat_g1v2
    SELECT TRIM(id), TRIM(cat), TRIM(subcat), TRIM(maintenance)
    FROM tmp_erp_px_cat_g1v2;

    DROP TABLE tmp_erp_px_cat_g1v2;
    end_time := clock_timestamp();
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(epoch FROM end_time - start_time);

    RAISE NOTICE '==========================================';
    RAISE NOTICE 'Loading Bronze Layer is Completed';
    RAISE NOTICE '==========================================';
END;
$$;

-- Call the procedure
CALL bronze.load_bronze();
