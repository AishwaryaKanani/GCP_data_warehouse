/*
===============================================================================
Quality Checks (BigQuery / GoogleSQL) - Silver Layer
===============================================================================
Script Purpose:
    This script performs various quality checks for data consistency, accuracy,
    and standardization across the 'silver' layer. It includes checks for:
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields.

Usage Notes:
    - Run these checks after loading the Silver layer.
    - Investigate and resolve any discrepancies found during the checks.

BigQuery Notes:
    - Use `project.dataset.table` with backticks.
    - SQL Server LEN() -> BigQuery LENGTH(); LENGTH works on STRING/BYTES, so cast INT64 to STRING. [web:74]
    - SQL Server GETDATE() -> BigQuery CURRENT_DATE() (or CURRENT_TIMESTAMP()). [web:62]
===============================================================================
*/

-- ====================================================================
-- Checking `silver.crm_cust_info`
-- ====================================================================

-- Check for NULLs or Duplicates in Primary Key
-- Expectation: No Results
SELECT
  cst_id,
  COUNT(*) AS row_cnt
FROM `baraawarehouseproject.silver.crm_cust_info`
GROUP BY cst_id
HAVING row_cnt > 1 OR cst_id IS NULL;

-- Check for Unwanted Spaces
-- Expectation: No Results
SELECT
  cst_key
FROM `baraawarehouseproject.silver.crm_cust_info`
WHERE cst_key != TRIM(cst_key);  -- TRIM removes leading/trailing spaces [web:74]

-- Data Standardization & Consistency
SELECT DISTINCT
  cst_marital_status
FROM `baraawarehouseproject.silver.crm_cust_info`;


-- ====================================================================
-- Checking `silver.crm_prd_info`
-- ====================================================================

-- Check for NULLs or Duplicates in Primary Key
-- Expectation: No Results
SELECT
  prd_id,
  COUNT(*) AS row_cnt
FROM `baraawarehouseproject.silver.crm_prd_info`
GROUP BY prd_id
HAVING row_cnt > 1 OR prd_id IS NULL;

-- Check for Unwanted Spaces
-- Expectation: No Results
SELECT
  prd_nm
FROM `baraawarehouseproject.silver.crm_prd_info`
WHERE prd_nm != TRIM(prd_nm);  -- TRIM removes leading/trailing spaces [web:74]

-- Check for NULLs or Negative Values in Cost
-- Expectation: No Results
SELECT
  prd_cost
FROM `baraawarehouseproject.silver.crm_prd_info`
WHERE prd_cost < 0 OR prd_cost IS NULL;

-- Data Standardization & Consistency
SELECT DISTINCT
  prd_line
FROM `baraawarehouseproject.silver.crm_prd_info`;

-- Check for Invalid Date Orders (Start Date > End Date)
-- Expectation: No Results
SELECT
  *
FROM `baraawarehouseproject.silver.crm_prd_info`
WHERE prd_end_dt < prd_start_dt;


-- ====================================================================
-- Checking `silver.crm_sales_details`
-- ====================================================================

-- Check for Invalid raw numeric dates in Bronze (YYYYMMDD stored as INT64)
-- Expectation: No Invalid Dates
SELECT
  NULLIF(sls_due_dt, 0) AS sls_due_dt
FROM `baraawarehouseproject.bronze.crm_sales_details`
WHERE sls_due_dt <= 0
   OR LENGTH(CAST(sls_due_dt AS STRING)) != 8  -- LEN() -> LENGTH(), cast INT64 -> STRING [web:74]
   OR sls_due_dt > 20500101
   OR sls_due_dt < 19000101;

-- Check for Invalid Date Orders (Order Date > Shipping/Due Dates)
-- Expectation: No Results
SELECT
  *
FROM `baraawarehouseproject.silver.crm_sales_details`
WHERE sls_order_dt > sls_ship_dt
   OR sls_order_dt > sls_due_dt;

-- Check Data Consistency: Sales = Quantity * Price
-- Expectation: No Results
SELECT DISTINCT
  sls_sales,
  sls_quantity,
  sls_price
FROM `baraawarehouseproject.silver.crm_sales_details`
WHERE sls_sales != sls_quantity * sls_price
   OR sls_sales IS NULL
   OR sls_quantity IS NULL
   OR sls_price IS NULL
   OR sls_sales <= 0
   OR sls_quantity <= 0
   OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price;


-- ====================================================================
-- Checking `silver.erp_cust_az12`
-- ====================================================================

-- Identify Out-of-Range Dates
-- Expectation: Birthdates between 1924-01-01 and Today
SELECT DISTINCT
  bdate
FROM `baraawarehouseproject.silver.erp_cust_az12`
WHERE bdate < DATE '1924-01-01'
   OR bdate > CURRENT_DATE();  -- GETDATE() -> CURRENT_DATE() [web:62]

-- Data Standardization & Consistency
SELECT DISTINCT
  gen
FROM `baraawarehouseproject.silver.erp_cust_az12`;


-- ====================================================================
-- Checking `silver.erp_loc_a101`
-- ====================================================================

-- Data Standardization & Consistency
SELECT DISTINCT
  cntry
FROM `baraawarehouseproject.silver.erp_loc_a101`
ORDER BY cntry;


-- ====================================================================
-- Checking `silver.erp_px_cat_g1v2`
-- ====================================================================

-- Check for Unwanted Spaces
-- Expectation: No Results
SELECT
  *
FROM `baraawarehouseproject.silver.erp_px_cat_g1v2`
WHERE cat != TRIM(cat)
   OR subcat != TRIM(subcat)
   OR maintenance != TRIM(maintenance);  -- TRIM removes leading/trailing spaces [web:74]

-- Data Standardization & Consistency
SELECT DISTINCT
  maintenance
FROM `baraawarehouseproject.silver.erp_px_cat_g1v2`;
