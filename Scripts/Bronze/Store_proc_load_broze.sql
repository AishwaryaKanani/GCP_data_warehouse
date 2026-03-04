-- ===============================================================
-- Stored Procedure: Load Bronze Layer (GCS → Bronze)
-- ===============================================================

CREATE OR REPLACE PROCEDURE `baraawarehouseproject.bronze.load_bronze`()
BEGIN

  -- ============================================================
  -- CRM TABLES
  -- ============================================================

  TRUNCATE TABLE `baraawarehouseproject.bronze.crm_cust_info`;

  LOAD DATA INTO `baraawarehouseproject.bronze.crm_cust_info`
  FROM FILES (
    format = 'CSV',
    uris = ['gs://aish-dwh-bronze-bucket/source_crm/cust_info.csv'],
    skip_leading_rows = 1
  );

  TRUNCATE TABLE `baraawarehouseproject.bronze.crm_prd_info`;

  LOAD DATA INTO `baraawarehouseproject.bronze.crm_prd_info`
  FROM FILES (
    format = 'CSV',
    uris = ['gs://aish-dwh-bronze-bucket/source_crm/prd_info.csv'],
    skip_leading_rows = 1
  );

  TRUNCATE TABLE `baraawarehouseproject.bronze.crm_sales_details`;

  LOAD DATA INTO `baraawarehouseproject.bronze.crm_sales_details`
  FROM FILES (
    format = 'CSV',
    uris = ['gs://aish-dwh-bronze-bucket/source_crm/sales_details.csv'],
    skip_leading_rows = 1
  );

  -- ============================================================
  -- ERP TABLES
  -- ============================================================

  TRUNCATE TABLE `baraawarehouseproject.bronze.erp_loc_a101`;

 LOAD DATA INTO `baraawarehouseproject.bronze.erp_loc_a101`
(
  cid STRING,
  cntry STRING
)
FROM FILES (
  format = 'CSV',
  uris = ['gs://aish-dwh-bronze-bucket/source_erp/LOC_A101.csv'],
  skip_leading_rows = 1,
  field_delimiter = ','
);

  TRUNCATE TABLE `baraawarehouseproject.bronze.erp_cust_az12`;

  LOAD DATA INTO `baraawarehouseproject.bronze.erp_cust_az12`
  FROM FILES (
    format = 'CSV',
    uris = ['gs://aish-dwh-bronze-bucket/source_erp/CUST_AZ12.csv'],
    skip_leading_rows = 1
  );

  TRUNCATE TABLE `baraawarehouseproject.bronze.erp_px_cat_g1v2`;

  LOAD DATA INTO `baraawarehouseproject.bronze.erp_px_cat_g1v2`
  FROM FILES (
    format = 'CSV',
    uris = ['gs://aish-dwh-bronze-bucket/source_erp/PX_CAT_G1V2.csv'],
    skip_leading_rows = 1
  );

END;