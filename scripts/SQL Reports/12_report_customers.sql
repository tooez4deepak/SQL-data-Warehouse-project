/*
==============================================================================
Customer Report
==============================================================================
Purpose:
- This report consolidates key customer metrics and behaviors
- Create or update a reporting view `gold.report_customers` to summarize customer-level metrics.
Highlights:
	1. Gathers essential fields such as names, ages, and transaction details.
	2. Segments customers into categories (VIP, Regular, New) and age groups.
	3. Aggregates customer-level metrics:
		- total orders
		- total sales
		- total quantity purchased
		- total products
		- lifespan (in months)
	4. Calculates valuable KPIs:
		- recency (months since last order)
		- average order value
		- average monthly spend
- =============================================================================
*/

IF OBJECT_ID('gold.report_customers', 'V') IS NOT NULL
    DROP VIEW gold.report_customers;
GO

CREATE OR ALTER VIEW gold.report_customers AS
WITH customer_detail AS (
    -- Join fact sales with customer dimension and prepare flattened detail-level records
    SELECT 
        f.order_number,
        f.product_key,
        f.order_date,
        f.sales_amount,
        f.quantity,
        c.customer_key,
        c.customer_number,
        CONCAT(c.first_name, ' ', c.last_name) AS customer_name,
        DATEDIFF(year, c.birthdate, GETDATE()) AS age
    FROM gold.fact_sales f
    LEFT JOIN gold.dim_customers c
        ON c.customer_key = f.customer_key
    WHERE f.order_date IS NOT NULL
),

customer_aggregtion AS (
    -- Aggregate sales and behavior per customer
    SELECT
        customer_key,
        customer_number,
        customer_name,
        age,
        COUNT(order_number) AS total_orders,
        SUM(sales_amount) AS total_sales,
        SUM(quantity) AS total_quantity,
        COUNT(DISTINCT product_key) AS total_products,
        MAX(order_date) AS last_order,
        DATEDIFF(month, MIN(order_date), MAX(order_date)) AS lifespan
    FROM customer_detail
    GROUP BY customer_key, customer_number, customer_name, age
)

-- Final selection with segmentation and customer KPIs
SELECT 
    customer_key,
    customer_number,
    customer_name,
    age,
    
    -- Group age into human-readable brackets
    CASE
        WHEN age < 20 THEN 'Under 20'
        WHEN age BETWEEN 20 AND 29 THEN '20-29'
        WHEN age BETWEEN 30 AND 39 THEN '30-39'
        WHEN age BETWEEN 40 AND 49 THEN '40-49'
        WHEN age BETWEEN 50 AND 59 THEN '50-59'
        ELSE '60 and Above'
    END AS age_group,

    -- Segment customers based on sales volume and activity duration
    CASE
        WHEN lifespan >= 12 AND total_sales >= 5000 THEN 'VIP'
        WHEN lifespan >= 12 AND total_sales <= 5000 THEN 'Regular'
        ELSE 'NEW'
    END AS customer_segement,

    last_order,
    DATEDIFF(month, last_order, GETDATE()) AS recency,  -- months since last order
    total_orders,
    total_sales,
    total_quantity,
    total_products,
    lifespan,

    -- Average value per order
    CASE
        WHEN total_orders = 0 THEN 0
        ELSE total_sales / total_orders 
    END AS avg_order_value,

    -- Average spend per month of activity
    CASE
        WHEN lifespan = 0 THEN total_sales
        ELSE total_sales / lifespan
    END AS avg_monthly_spend
FROM customer_aggregtion;

