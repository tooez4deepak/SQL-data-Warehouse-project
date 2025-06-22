/*
===========================================================================================
Product Report
===========================================================================================
Purpose:
	- This report consolidates key product metrics and behaviors.
    - Create or update a reporting view `gold.report_products` that summarizes product performance based on sales data.
Highlights:
	1. Gathers essential fields such as product name, category, subcategory, and cost.
	2. Segments products by revenue to identify High-Performers, Mid-Range, or Low-Performers.
	3. Aggregates product-level metrics:
		- total orders
		- total sales
		- total quantity sold
		- total customers (unique)
		- lifespan (in months)
	4. Calculates valuable KPIs:
		- recency (months since last sale)
		- average order revenue (AOR)
		- average monthly revenue
=============================================================================================
*/

IF OBJECT_ID('gold.report_customers', 'V') IS NOT NULL
    DROP VIEW gold.report_customers;
GO

CREATE OR ALTER VIEW gold.report_products AS
WITH base_query AS (
    -- Join fact sales with product details to prepare base dataset
    SELECT
        p.product_key,
        p.product_name,
        p.category,
        p.subcategory,
        p.cost,
        f.order_number,
        f.order_date,
        f.sales_amount,
        f.quantity,
        f.customer_key
    FROM gold.fact_sales f
    LEFT JOIN gold.dim_products p
        ON p.product_key = f.product_key
),

aggregate_product AS (
    -- Aggregate metrics per product
    SELECT 
        product_key,
        product_name,
        category,
        subcategory,
        cost,
        DATEDIFF(month, MIN(order_date), MAX(order_date)) AS lifespan, -- active sales period
        MAX(order_date) AS last_order,
        COUNT(DISTINCT order_number) AS total_orders,
        COUNT(DISTINCT customer_key) AS total_customer,
        SUM(sales_amount) AS total_sales,
        SUM(quantity) AS total_quantity,
        ROUND(AVG(CAST(sales_amount AS float) / NULLIF(quantity, 0)), 1) AS avg_saling_price -- avg price per unit
    FROM base_query
    GROUP BY
        product_key,
        product_name,
        category,
        subcategory,
        cost
)

-- Final select with enriched KPIs and segmentation
SELECT
    product_key,
    product_name,
    category,
    subcategory,
    cost,
    last_order,
    DATEDIFF(month, last_order, GETDATE()) AS recency, -- time since last order
    CASE 
        WHEN total_sales > 50000 THEN 'High_performance'
        WHEN total_sales >= 10000 THEN 'Mid_performance'
        ELSE 'low-performance'
    END AS product_segment,
    lifespan,
    total_orders,
    total_sales,
    total_quantity,
    total_customer,
    avg_saling_price,
    -- Revenue per order
    CASE
        WHEN total_orders = 0 THEN 0
        ELSE total_sales / total_orders
    END AS avg_order_revenue,
    -- Revenue per month
    CASE
        WHEN lifespan = 0 THEN total_sales
        ELSE total_sales / lifespan
    END AS avg_monthly_revenue
FROM aggregate_product;
