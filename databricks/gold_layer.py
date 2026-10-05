# Enterprise Retail Lakehouse
# Gold Layer - Business Transformations
#
# Purpose:
# Read validated Silver Delta datasets and create
# business-ready Gold analytical datasets.
#
# Expected Output:
# fact_sales      = 249,744
# dim_customer    = 19,998
# dim_product     = 1,998
# dim_region      = 10
# sales_daily     = 630
# customer_360    = 19,998

from pyspark.sql import functions as F


# ------------------------------------------------------------
# 1. Silver Paths
# ------------------------------------------------------------

silver_base = "abfss://silver@contosoretaillake976757.dfs.core.windows.net/"

customers_path = silver_base + "customers/"
products_path = silver_base + "products/"
orders_path = silver_base + "orders/"
orderdetails_path = silver_base + "orderdetails/"
regions_path = silver_base + "regions/"


# ------------------------------------------------------------
# 2. Gold Paths
# ------------------------------------------------------------

gold_base = "abfss://gold@contosoretaillake976757.dfs.core.windows.net/"

fact_sales_path = gold_base + "fact_sales/"
dim_customer_path = gold_base + "dim_customer/"
dim_product_path = gold_base + "dim_product/"
dim_region_path = gold_base + "dim_region/"
sales_daily_path = gold_base + "sales_daily/"
customer_360_path = gold_base + "customer_360/"


# ------------------------------------------------------------
# 3. Read Silver Delta Tables
# ------------------------------------------------------------

customers_silver = (
    spark.read.format("delta").load(customers_path)
)

products_silver = (
    spark.read.format("delta").load(products_path)
)

orders_silver = (
    spark.read.format("delta").load(orders_path)
)

orderdetails_silver = (
    spark.read.format("delta").load(orderdetails_path)
)

regions_silver = (
    spark.read.format("delta").load(regions_path)
)


# ------------------------------------------------------------
# 4. Fact Sales
# ------------------------------------------------------------

df_fact_sales = (
    orderdetails_silver.alias("od")
    .join(
        orders_silver.alias("o"),
        F.col("od.OrderID") == F.col("o.OrderID"),
        "inner"
    )
    .join(
        products_silver.alias("p"),
        F.col("od.ProductID") == F.col("p.ProductID"),
        "inner"
    )
    .join(
        customers_silver.alias("c"),
        F.col("o.CustomerID") == F.col("c.CustomerID"),
        "inner"
    )
    .join(
        regions_silver.alias("r"),
        F.col("c.RegionID") == F.col("r.RegionID"),
        "inner"
    )
    .select(
        F.col("o.OrderID"),
        F.col("od.OrderDetailID"),
        F.col("o.CustomerID"),
        F.col("od.ProductID"),
        F.col("c.RegionID"),
        F.col("o.OrderDate"),
        F.col("o.Status"),
        F.col("od.Quantity"),
        F.col("od.UnitPrice"),
        (
            F.col("od.Quantity") * F.col("od.UnitPrice")
        ).alias("SalesAmount")
    )
)

print("Fact Sales:", df_fact_sales.count())


# ------------------------------------------------------------
# 5. Customer Dimension
# ------------------------------------------------------------

df_dim_customer = (
    customers_silver.alias("c")
    .join(
        regions_silver.alias("r"),
        F.col("c.RegionID") == F.col("r.RegionID"),
        "inner"
    )
    .select(
        F.col("c.CustomerID"),
        F.concat_ws(
            " ",
            F.col("c.FirstName"),
            F.col("c.LastName")
        ).alias("CustomerName"),
        F.col("c.Email"),
        F.col("c.RegionID"),
        F.col("r.RegionName"),
        F.col("r.Country"),
        F.col("c.SignupDate"),
        F.col("c.ModifiedDate")
    )
)

print("Dim Customer:", df_dim_customer.count())


# ------------------------------------------------------------
# 6. Product Dimension
# ------------------------------------------------------------

df_dim_product = (
    products_silver
    .select(
        "ProductID",
        "ProductName",
        "Category",
        "UnitPrice",
        "ModifiedDate"
    )
)

print("Dim Product:", df_dim_product.count())


# ------------------------------------------------------------
# 7. Region Dimension
# ------------------------------------------------------------

df_dim_region = (
    regions_silver
    .select(
        "RegionID",
        "RegionName",
        "Country"
    )
)

print("Dim Region:", df_dim_region.count())


# ------------------------------------------------------------
# 8. Daily Sales Summary
# ------------------------------------------------------------

df_sales_daily = (
    df_fact_sales
    .groupBy("OrderDate")
    .agg(
        F.sum("SalesAmount").alias("TotalSales"),
        F.countDistinct("OrderID").alias("TotalOrders"),
        F.sum("Quantity").alias("TotalQuantity")
    )
    .orderBy("OrderDate")
)

print("Sales Daily:", df_sales_daily.count())


# ------------------------------------------------------------
# 9. Customer 360
# ------------------------------------------------------------

customer_sales = (
    df_fact_sales
    .groupBy("CustomerID")
    .agg(
        F.countDistinct("OrderID").alias("TotalOrders"),
        F.sum("Quantity").alias("TotalQuantity"),
        F.sum("SalesAmount").alias("TotalSales")
    )
)

df_customer_360 = (
    df_dim_customer
    .join(
        customer_sales,
        on="CustomerID",
        how="left"
    )
    .select(
        "CustomerID",
        "CustomerName",
        "Email",
        "RegionID",
        "RegionName",
        "Country",
        "SignupDate",
        F.coalesce(
            F.col("TotalOrders"),
            F.lit(0)
        ).alias("TotalOrders"),
        F.coalesce(
            F.col("TotalQuantity"),
            F.lit(0)
        ).alias("TotalQuantity"),
        F.coalesce(
            F.col("TotalSales"),
            F.lit(0)
        ).alias("TotalSales")
    )
)

print("Customer 360:", df_customer_360.count())


# ------------------------------------------------------------
# 10. Write Gold Delta Tables
# ------------------------------------------------------------

(
    df_fact_sales.write
    .format("delta")
    .mode("overwrite")
    .save(fact_sales_path)
)

(
    df_dim_customer.write
    .format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true")
    .save(dim_customer_path)
)

(
    df_dim_product.write
    .format("delta")
    .mode("overwrite")
    .save(dim_product_path)
)

(
    df_dim_region.write
    .format("delta")
    .mode("overwrite")
    .save(dim_region_path)
)

(
    df_sales_daily.write
    .format("delta")
    .mode("overwrite")
    .save(sales_daily_path)
)

(
    df_customer_360.write
    .format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true")
    .save(customer_360_path)
)


# ------------------------------------------------------------
# 11. Final Gold Validation
# ------------------------------------------------------------

print("Fact Sales:", df_fact_sales.count())
print("Dim Customer:", df_dim_customer.count())
print("Dim Product:", df_dim_product.count())
print("Dim Region:", df_dim_region.count())
print("Sales Daily:", df_sales_daily.count())
print("Customer 360:", df_customer_360.count())
