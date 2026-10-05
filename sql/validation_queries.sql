-- Enterprise Retail Lakehouse
-- Azure SQL Serving Layer Validation

-- Fact Sales
SELECT COUNT(*) AS FactSalesRecords
FROM dbo.fact_sales;

-- Customer Dimension
SELECT COUNT(*) AS DimCustomerRecords
FROM dbo.dim_customer;

-- Product Dimension
SELECT COUNT(*) AS DimProductRecords
FROM dbo.dim_product;

-- Region Dimension
SELECT COUNT(*) AS DimRegionRecords
FROM dbo.dim_region;

-- Daily Sales
SELECT COUNT(*) AS SalesDailyRecords
FROM dbo.sales_daily;

-- Customer 360
SELECT COUNT(*) AS Customer360Records
FROM dbo.customer_360;


-- Final consolidated validation
SELECT 'fact_sales' AS TableName, COUNT(*) AS RecordCount
FROM dbo.fact_sales

UNION ALL

SELECT 'dim_customer', COUNT(*)
FROM dbo.dim_customer

UNION ALL

SELECT 'dim_product', COUNT(*)
FROM dbo.dim_product

UNION ALL

SELECT 'dim_region', COUNT(*)
FROM dbo.dim_region

UNION ALL

SELECT 'sales_daily', COUNT(*)
FROM dbo.sales_daily

UNION ALL

SELECT 'customer_360', COUNT(*)
FROM dbo.customer_360;
