/*
===============================================================================
Quality Checks (BigQuery / GoogleSQL) - Gold Layer
===============================================================================
Script Purpose:
    This script performs quality checks to validate the integrity, consistency,
    and accuracy of the Gold Layer. These checks ensure:
    - Uniqueness of surrogate keys in dimension views.
    - Referential integrity between fact and dimension views.
    - Validation of relationships in the data model for analytical purposes.

Usage Notes:
    - Run these checks after creating/loading the Gold layer.
    - Most checks are written with the expectation: "No rows returned".
===============================================================================
*/

-- ====================================================================
-- Checking `gold.dim_customers`
-- ====================================================================
-- Check uniqueness of customer_key (surrogate key)
-- Expectation: No results (no duplicates)
SELECT
  customer_key,
  COUNT(*) AS duplicate_count
FROM `baraawarehouseproject.gold.dim_customers`
GROUP BY customer_key
HAVING COUNT(*) > 1;  -- HAVING filters groups based on aggregate results [web:201]


-- ====================================================================
-- Checking `gold.dim_products`
-- ====================================================================
-- Check uniqueness of product_key (surrogate key)
-- Expectation: No results (no duplicates)
SELECT
  product_key,
  COUNT(*) AS duplicate_count
FROM `baraawarehouseproject.gold.dim_products`
GROUP BY product_key
HAVING COUNT(*) > 1;  -- HAVING filters groups based on aggregate results [web:201]


-- ====================================================================
-- Checking `gold.fact_sales` connectivity
-- ====================================================================
-- Referential integrity check:
-- Finds fact rows whose foreign keys don't match any dimension row
-- Expectation: No results (every fact row matches a customer and a product)
SELECT
  f.*
FROM `baraawarehouseproject.gold.fact_sales` AS f
LEFT JOIN `baraawarehouseproject.gold.dim_customers` AS c
  ON c.customer_key = f.customer_key
LEFT JOIN `baraawarehouseproject.gold.dim_products`  AS p
  ON p.product_key = f.product_key
WHERE p.product_key IS NULL
   OR c.customer_key IS NULL;
