/*
===============================================================================
Dimensions Exploration
===============================================================================

Purpose: 
    - Retrieve distinct dimension values from the gold layer for use in filtering, reporting, or validation.
    - These queries support lookup needs such as country lists and product hierarchies.

SQL Functions Used:
    - DISTINCT
    - ORDER BY
================================================================================
*/


-- Get a distinct list of countries from the customer dimension table
SELECT DISTINCT 
    country 
FROM gold.dim_customers
ORDER BY country;

-- Get a distinct list of product hierarchy (category > subcategory > product)
SELECT DISTINCT 
    category, 
    subcategory, 
    product_name 
FROM gold.dim_products
ORDER BY category, subcategory, product_name;

-- Get distinct product hierarchy, excluding rows where category is NULL
SELECT DISTINCT 
    category, 
    subcategory, 
    product_name 
FROM gold.dim_products
WHERE category IS NOT NULL  -- Exclude incomplete or unclassified product entries
ORDER BY category, subcategory, product_name;
