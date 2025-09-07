-- Only retain latest records for unique cst_id 
SELECT * FROM(
SELECT *,
ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC)as flag_last
FROM bronze.crm_cust_info WHERE cst_id IS NOT NULL)AS t WHERE flag_last = 1

--Data Standardisation and Consistency
SELECT DISTINCT cst_marital_status
FROM bronze.crm_cust_info;

--prd_info table
--Check for Nulls of duplicates in the primary key
--Expectation : No result
SELECT prd_id,
COUNT(*)
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) >1 OR prd_id IS NULL;

--Check if got unwanted spaces for product names
SELECT prd_nm
FROM bronze.crm_prd_info
WHERE prd_nm != TRIM(prd_nm)

--Check if got negative or null product cost
SELECT prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;

--Check for Invalid Date Orders
SELECT *
FROM bronze.crm_prd_info
WHERE prd_end_dt < prd_start_dt

--sales_details table
--Convert order, ship and due dates to actual dates
--Check for invalid dates first, but actually in ddl already converted the dates to yyyy-month-day already
SELECT sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt < 0 ;

--Check invalid order dates
SELECT *
FROM bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt

SELECT DISTINCT
sls_sales,
sls_quantity,
sls_price
FROM bronze.crm_sales_details;

--erp_cust_az12
SELECT DISTINCT gen
FROM bronze.erp_cust_az12;

--erp_loc_a101
SELECT DISTINCT cntry
FROM bronze.erp_loc_a101
ORDER BY cntry;

--erp_px_cat_g1v2
--Check for unwanted spaces 
SELECT *
FROM bronze.erp_px_cat_g1v2
WHERE TRIM(maintenance) != maintenance OR TRIM(cat) != cat OR TRIM(subcat) != subcat;

--Data Consistency and Standardisation
SELECT DISTINCT
cat FROM 
bronze.erp_px_cat_g1v2;

SELECT DISTINCT
subcat FROM 
bronze.erp_px_cat_g1v2;

SELECT DISTINCT
maintenance FROM 
bronze.erp_px_cat_g1v2;




