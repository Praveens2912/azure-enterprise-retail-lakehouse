# Enterprise Retail Lakehouse
# Products - Bronze to Silver
#
# Purpose:
# Read raw Products data from ADLS Gen2 Bronze,
# standardize data types, apply data quality rules,
# quarantine invalid records, and write valid records
# to the Silver Delta layer.
#
# Expected Result:
# Bronze records          = 2,000
# Missing Category       = 1
# Invalid UnitPrice      = 1
# Duplicate ProductID    = 0
# Quarantine records     = 2
#
# Silver Output:
# Valid Silver records   = 1,998
# Duplicate ProductID    = 0
# Blank Category         = 0
# Invalid UnitPrice      = 0

from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType


# ------------------------------------------------------------
# 1. Paths
# ------------------------------------------------------------

bronze_products_path = (
    "abfss://bronze@contosoretaillake976757.dfs.core.windows.net/"
    "products/Products.csv"
)

silver_products_path = (
    "abfss://silver@contosoretaillake976757.dfs.core.windows.net/"
    "products/"
)

quarantine_products_path = (
    "abfss://quarantine@contosoretaillake976757.dfs.core.windows.net/"
    "products/"
)


# ------------------------------------------------------------
# 2. Read Bronze
# ------------------------------------------------------------

df_products = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(bronze_products_path)
)


# ------------------------------------------------------------
# 3. Data Type Standardization
# ------------------------------------------------------------

df_products_typed = (
    df_products
    .withColumn("ProductID", F.col("ProductID").cast("int"))
    .withColumn(
        "UnitPrice",
        F.col("UnitPrice").cast(DecimalType(10, 2))
    )
    .withColumn(
        "Category",
        F.when(
            F.trim(F.col("Category")) == "",
            None
        ).otherwise(F.trim(F.col("Category")))
    )
    .withColumn(
        "ModifiedDate",
        F.to_timestamp(F.col("ModifiedDate"))
    )
)


# ------------------------------------------------------------
# 4. Duplicate ProductID Check
# ------------------------------------------------------------

duplicate_products = (
    df_products_typed
    .groupBy("ProductID")
    .count()
    .filter(F.col("count") > 1)
)

print(
    "Duplicate ProductIDs:",
    duplicate_products.count()
)


# ------------------------------------------------------------
# 5. Missing Category
# ------------------------------------------------------------

invalid_category_records = (
    df_products_typed
    .filter(
        F.col("Category").isNull() |
        (F.trim(F.col("Category")) == "")
    )
    .withColumn(
        "RejectionReason",
        F.lit("Missing Category")
    )
)


# ------------------------------------------------------------
# 6. Invalid UnitPrice
# ------------------------------------------------------------

invalid_price_records = (
    df_products_typed
    .filter(F.col("UnitPrice") <= 0)
    .withColumn(
        "RejectionReason",
        F.lit("Invalid UnitPrice")
    )
)


# ------------------------------------------------------------
# 7. Create Quarantine Dataset
# ------------------------------------------------------------

df_products_quarantine = (
    invalid_category_records
    .unionByName(invalid_price_records)
)

print(
    "Products quarantine records:",
    df_products_quarantine.count()
)


# ------------------------------------------------------------
# 8. Create Valid Products
# ------------------------------------------------------------

df_products_clean = (
    df_products_typed
    .filter(
        F.col("Category").isNotNull() &
        (F.trim(F.col("Category")) != "") &
        (F.col("UnitPrice") > 0)
    )
)

print(
    "Valid Products records:",
    df_products_clean.count()
)


# ------------------------------------------------------------
# 9. Write Quarantine as Delta
# ------------------------------------------------------------

(
    df_products_quarantine
    .write
    .format("delta")
    .mode("overwrite")
    .save(quarantine_products_path)
)


# ------------------------------------------------------------
# 10. Write Silver as Delta
# ------------------------------------------------------------

(
    df_products_clean
    .write
    .format("delta")
    .mode("overwrite")
    .save(silver_products_path)
)


# ------------------------------------------------------------
# 11. Read Silver for Validation
# ------------------------------------------------------------

df_products_silver = (
    spark.read
    .format("delta")
    .load(silver_products_path)
)


# ------------------------------------------------------------
# 12. Final Validation
# ------------------------------------------------------------

remaining_duplicates = (
    df_products_silver
    .groupBy("ProductID")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

blank_category_remaining = (
    df_products_silver
    .filter(
        F.col("Category").isNull() |
        (F.trim(F.col("Category")) == "")
    )
    .count()
)

invalid_price_remaining = (
    df_products_silver
    .filter(F.col("UnitPrice") <= 0)
    .count()
)

print(
    "Remaining duplicate ProductIDs:",
    remaining_duplicates
)

print(
    "Remaining blank Category:",
    blank_category_remaining
)

print(
    "Remaining invalid UnitPrice:",
    invalid_price_remaining
)

print(
    "Final Silver Products records:",
    df_products_silver.count()
)
