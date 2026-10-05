# Enterprise Retail Lakehouse
# Orders - Bronze to Silver
#
# Purpose:
# Read raw Orders data from ADLS Gen2 Bronze,
# standardize data types, apply data quality rules,
# quarantine invalid records, and write valid records
# to the Silver Delta layer.
#
# Expected Result:
# Bronze records          = 100,000
# Invalid CustomerID     = 2
# Future OrderDate       = 1
# Quarantine records     = 3
#
# Silver Output:
# Valid Silver records   = 99,997
# Duplicate OrderID      = 0
# Invalid CustomerID     = 0
# Future OrderDate       = 0

from pyspark.sql import functions as F


# ------------------------------------------------------------
# 1. Paths
# ------------------------------------------------------------

bronze_orders_path = (
    "abfss://bronze@contosoretaillake976757.dfs.core.windows.net/"
    "orders/Orders.csv"
)

silver_orders_path = (
    "abfss://silver@contosoretaillake976757.dfs.core.windows.net/"
    "orders/"
)

quarantine_orders_path = (
    "abfss://quarantine@contosoretaillake976757.dfs.core.windows.net/"
    "orders/"
)


# ------------------------------------------------------------
# 2. Read Bronze
# ------------------------------------------------------------

df_orders = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(bronze_orders_path)
)


# ------------------------------------------------------------
# 3. Data Type Standardization
# ------------------------------------------------------------

df_orders_typed = (
    df_orders
    .withColumn("OrderID", F.col("OrderID").cast("long"))
    .withColumn("CustomerID", F.col("CustomerID").cast("int"))
    .withColumn("OrderDate", F.to_date(F.col("OrderDate")))
    .withColumn("ModifiedDate", F.to_timestamp(F.col("ModifiedDate")))
)


# ------------------------------------------------------------
# 4. Remove Duplicate OrderID
# ------------------------------------------------------------

df_orders_clean = (
    df_orders_typed
    .dropDuplicates(["OrderID"])
)

print(
    "Clean Orders records:",
    df_orders_clean.count()
)


# ------------------------------------------------------------
# 5. Invalid CustomerID
# ------------------------------------------------------------

invalid_order_customer_records = (
    df_orders_clean.alias("o")
    .join(
        df_customers_valid.select("CustomerID").alias("c"),
        F.col("o.CustomerID") == F.col("c.CustomerID"),
        "left"
    )
    .filter(F.col("c.CustomerID").isNull())
    .select("o.*")
    .withColumn(
        "RejectionReason",
        F.lit("Invalid CustomerID")
    )
)


# ------------------------------------------------------------
# 6. Future OrderDate
# ------------------------------------------------------------

future_order_records = (
    df_orders_clean
    .filter(F.col("OrderDate") > F.current_date())
    .withColumn(
        "RejectionReason",
        F.lit("Future OrderDate")
    )
)


# ------------------------------------------------------------
# 7. Create Quarantine Dataset
# ------------------------------------------------------------

df_orders_quarantine = (
    invalid_order_customer_records
    .unionByName(future_order_records)
    .dropDuplicates(["OrderID"])
)

print(
    "Orders quarantine records:",
    df_orders_quarantine.count()
)


# ------------------------------------------------------------
# 8. Create Valid Orders
# ------------------------------------------------------------

df_orders_valid = (
    df_orders_clean.alias("o")
    .join(
        df_orders_quarantine.select("OrderID").alias("q"),
        F.col("o.OrderID") == F.col("q.OrderID"),
        "left_anti"
    )
)

print(
    "Valid Orders records:",
    df_orders_valid.count()
)


# ------------------------------------------------------------
# 9. Write Quarantine as Delta
# ------------------------------------------------------------

(
    df_orders_quarantine
    .write
    .format("delta")
    .mode("overwrite")
    .save(quarantine_orders_path)
)


# ------------------------------------------------------------
# 10. Write Silver as Delta
# ------------------------------------------------------------

(
    df_orders_valid
    .write
    .format("delta")
    .mode("overwrite")
    .save(silver_orders_path)
)


# ------------------------------------------------------------
# 11. Read Silver for Validation
# ------------------------------------------------------------

df_orders_silver = (
    spark.read
    .format("delta")
    .load(silver_orders_path)
)


# ------------------------------------------------------------
# 12. Final Validation
# ------------------------------------------------------------

remaining_duplicates = (
    df_orders_silver
    .groupBy("OrderID")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

invalid_customer_remaining = (
    df_orders_silver.alias("o")
    .join(
        df_customers_valid.select("CustomerID").alias("c"),
        F.col("o.CustomerID") == F.col("c.CustomerID"),
        "left_anti"
    )
    .count()
)

future_order_remaining = (
    df_orders_silver
    .filter(F.col("OrderDate") > F.current_date())
    .count()
)

print(
    "Remaining duplicate OrderIDs:",
    remaining_duplicates
)

print(
    "Remaining invalid CustomerIDs:",
    invalid_customer_remaining
)

print(
    "Remaining future OrderDates:",
    future_order_remaining
)

print(
    "Final Silver Orders records:",
    df_orders_silver.count()
)
