/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to
    populate the 'silver' dataset tables from the 'bronze' dataset.

Actions Performed:
    - Truncates Silver tables.
    - Inserts transformed and cleansed data from Bronze into Silver tables.

Parameters:
    None.
    This stored procedure does not accept any parameters or return any values.

Usage Example:
    CALL `baraawarehouseproject.silver.load_silver`();
===============================================================================
*/

CREATE OR REPLACE PROCEDURE `baraawarehouseproject.silver.load_silver`()
BEGIN
  -- Batch timing variables
  DECLARE batch_start_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP();
  DECLARE step_start_ts  TIMESTAMP;
  DECLARE step_end_ts    TIMESTAMP;

  -- Basic status text you can SELECT at the end (BigQuery doesn't support PRINT like SQL Server)
  DECLARE status STRING DEFAULT 'STARTED';

  BEGIN
    -- =========================================================================
    -- Load CRM Tables
    -- =========================================================================

    -- -------------------------------------------------------------------------
    -- Loading silver.crm_cust_info
    -- -------------------------------------------------------------------------
    SET step_start_ts = CURRENT_TIMESTAMP();

    TRUNCATE TABLE `baraawarehouseproject.silver.crm_cust_info`; -- Remove all existing rows

    INSERT INTO `baraawarehouseproject.silver.crm_cust_info` (
      cst_id,
      cst_key,
      cst_firstname,
      cst_lastname,
      cst_marital_status,
      cst_gndr,
      cst_create_date
    )
    SELECT
      cst_id,
      cst_key,
      TRIM(cst_firstname) AS cst_firstname,
      TRIM(cst_lastname)  AS cst_lastname,
      CASE
        WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
        WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
        ELSE 'n/a'
      END AS cst_marital_status, -- Normalize marital status values
      CASE
        WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
        WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
        ELSE 'n/a'
      END AS cst_gndr, -- Normalize gender values
      cst_create_date
    FROM (
      SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
      FROM `baraawarehouseproject.bronze.crm_cust_info`
      WHERE cst_id IS NOT NULL
    ) t
    WHERE flag_last = 1; -- Keep most recent record per customer

    SET step_end_ts = CURRENT_TIMESTAMP();

    -- -------------------------------------------------------------------------
    -- Loading silver.crm_prd_info
    -- -------------------------------------------------------------------------
    SET step_start_ts = CURRENT_TIMESTAMP();

    TRUNCATE TABLE `baraawarehouseproject.silver.crm_prd_info`;

    INSERT INTO `baraawarehouseproject.silver.crm_prd_info` (
      prd_id,
      cat_id,
      prd_key,
      prd_nm,
      prd_cost,
      prd_line,
      prd_start_dt,
      prd_end_dt
    )
    SELECT
      prd_id,
      REPLACE(SUBSTR(prd_key, 1, 5), '-', '_') AS cat_id,  -- Extract category ID
      SUBSTR(prd_key, 7) AS prd_key,                       -- Extract product key
      prd_nm,
      IFNULL(prd_cost, 0) AS prd_cost,
      CASE
        WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
        WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
        WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
        WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
        ELSE 'n/a'
      END AS prd_line, -- Map product line codes to descriptive values
      DATE(prd_start_dt) AS prd_start_dt,
      DATE_SUB(
        DATE(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)),
        INTERVAL 1 DAY
      ) AS prd_end_dt -- End date = 1 day before next start date
    FROM `baraawarehouseproject.bronze.crm_prd_info`;

    SET step_end_ts = CURRENT_TIMESTAMP();

    -- -------------------------------------------------------------------------
    -- Loading silver.crm_sales_details
    -- -------------------------------------------------------------------------
    SET step_start_ts = CURRENT_TIMESTAMP();

    TRUNCATE TABLE `baraawarehouseproject.silver.crm_sales_details`;

  INSERT INTO `baraawarehouseproject.silver.crm_sales_details` ( 
      sls_ord_num,
      sls_prd_key,
      sls_cust_id,
      sls_order_dt,
      sls_ship_dt,
      sls_due_dt,
      sls_sales,
      sls_quantity,
      sls_price
  )  
  SELECT 
      ------------------------------------------------------------------
      -- Order & Reference Columns
      ------------------------------------------------------------------
      sls_ord_num,
      sls_prd_key,
      sls_cust_id,
      ------------------------------------------------------------------
      -- Date Cleaning & Standardization
      -- Convert YYYYMMDD numeric format into DATE
      -- Invalid values (0 or not 8 digits) become NULL
      ------------------------------------------------------------------
      CASE 
          WHEN sls_order_dt = 0 
              OR LENGTH(CAST(sls_order_dt AS STRING)) != 8 
          THEN NULL
          ELSE SAFE.PARSE_DATE('%Y%m%d', CAST(sls_order_dt AS STRING))
      END AS sls_order_dt,


      CASE 
          WHEN sls_ship_dt = 0 
              OR LENGTH(CAST(sls_ship_dt AS STRING)) != 8 
          THEN NULL
          ELSE SAFE.PARSE_DATE('%Y%m%d', CAST(sls_ship_dt AS STRING))
      END AS sls_ship_dt,


      CASE 
          WHEN sls_due_dt = 0 
              OR LENGTH(CAST(sls_due_dt AS STRING)) != 8 
          THEN NULL
          ELSE SAFE.PARSE_DATE('%Y%m%d', CAST(sls_due_dt AS STRING))
      END AS sls_due_dt,
      ------------------------------------------------------------------
      -- Check data consistency: Between Sales, Quntity, and Price
      -- >> Sales = Quntity * Price
      -- >> Values must not be NULL, Zero, or negative
      -- Sales Validation Logic
      -- Recalculate sales if:
      --   - NULL
      --   - <= 0
      --   - Not matching Quantity * Price
      ------------------------------------------------------------------
      CASE 
          WHEN sls_sales IS NULL 
              OR sls_sales <= 0 
              OR sls_sales != sls_quantity * ABS(sls_price)
          THEN sls_quantity * ABS(sls_price)
          ELSE sls_sales
      END AS sls_sales,
      ------------------------------------------------------------------
      -- Quantity (No transformation required)
      ------------------------------------------------------------------
      sls_quantity,
      ------------------------------------------------------------------
      -- Price Validation Logic
      -- Derive price if missing or invalid
      -- Protect against division by zero using NULLIF
      ------------------------------------------------------------------
    CASE
    WHEN sls_price IS NULL OR sls_price <= 0 THEN
      CAST(ROUND(SAFE_DIVIDE(sls_sales, NULLIF(sls_quantity, 0))) AS INT64)
    ELSE
      CAST(sls_price AS INT64)
  END AS sls_price

  FROM `baraawarehouseproject.bronze.crm_sales_details`;

    SET step_end_ts = CURRENT_TIMESTAMP();

    -- =========================================================================
    -- Load ERP Tables
    -- =========================================================================

    -- -------------------------------------------------------------------------
    -- Loading silver.erp_cust_az12
    -- -------------------------------------------------------------------------
    SET step_start_ts = CURRENT_TIMESTAMP();

    TRUNCATE TABLE `baraawarehouseproject.silver.erp_cust_az12`;

    INSERT INTO `baraawarehouseproject.silver.erp_cust_az12` (
      cid,
      bdate,
      gen
    )
    SELECT
      -- Remove 'NAS' prefix if present
      CASE
        WHEN STARTS_WITH(cid, 'NAS') THEN SUBSTR(cid, 4)
        ELSE cid
      END AS cid,
      -- Set future birthdates to NULL
      CASE
        WHEN bdate > CURRENT_DATE() THEN NULL
        ELSE bdate
      END AS bdate,
      -- Normalize gender values
      CASE
        WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
        WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
        ELSE 'n/a'
      END AS gen
    FROM `baraawarehouseproject.bronze.erp_cust_az12`;

    SET step_end_ts = CURRENT_TIMESTAMP();

    -- -------------------------------------------------------------------------
    -- Loading silver.erp_loc_a101
    -- -------------------------------------------------------------------------
    SET step_start_ts = CURRENT_TIMESTAMP();

    TRUNCATE TABLE `baraawarehouseproject.silver.erp_loc_a101`;

    INSERT INTO `baraawarehouseproject.silver.erp_loc_a101` (
      cid,
      cntry
    )
    SELECT
      REPLACE(cid, '-', '') AS cid,
      CASE
        WHEN TRIM(cntry) = 'DE' THEN 'Germany'
        WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
        WHEN cntry IS NULL OR TRIM(cntry) = '' THEN 'n/a'
        ELSE TRIM(cntry)
      END AS cntry -- Normalize/handle missing country values
    FROM `baraawarehouseproject.bronze.erp_loc_a101`;

    SET step_end_ts = CURRENT_TIMESTAMP();

    -- -------------------------------------------------------------------------
    -- Loading silver.erp_px_cat_g1v2 (straight copy)
    -- -------------------------------------------------------------------------
    SET step_start_ts = CURRENT_TIMESTAMP();

    TRUNCATE TABLE `baraawarehouseproject.silver.erp_px_cat_g1v2`;

    INSERT INTO `baraawarehouseproject.silver.erp_px_cat_g1v2` (
      id,
      cat,
      subcat,
      maintenance
    )
    SELECT
      id,
      cat,
      subcat,
      maintenance
    FROM `baraawarehouseproject.bronze.erp_px_cat_g1v2`;

    SET step_end_ts = CURRENT_TIMESTAMP();

    -- Procedure completed successfully
    SET status = CONCAT(
      'SUCCESS. Total duration (seconds): ',
      CAST(TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), batch_start_ts, SECOND) AS STRING)
    );

  EXCEPTION WHEN ERROR THEN
    -- If any statement fails, set status with BigQuery error variables
    SET status = CONCAT(
      'FAILED: ',
      @@error.message,
      ' | statement: ',
      @@error.statement_text
    );
  END;

  -- Return a status row (useful in UI / orchestration logs)
  SELECT status AS load_status, batch_start_ts AS batch_start_ts, CURRENT_TIMESTAMP() AS batch_end_ts;
END;
