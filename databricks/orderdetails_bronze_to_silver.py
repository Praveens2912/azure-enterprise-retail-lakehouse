# Enterprise Retail Lakehouse
# OrderDetails - Bronze to Silver

from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType
from pyspark.sql.window import Window


# ------------------------------------------------------------
# 1. Paths
# ------------------------------------------------------------

bronze_orderdetails_path = (
    "abfss://bronze@contosoretaillake976757.dfs.core.windows.net/"
    "orderdetails/OrderDetails.csv"
)

bronze_products_path = (
    "abfss://bronze@contosoretaillake976757.dfs.core.windows.net/"
    "products/Products.csv"
)

silver_orderdetails_path = (
    "abfss://silver@contosoretaillake976757.dfs.core.windows.net/"
    "orderdetails/"
)

quarantine_orderdetails_path = (
    "abfss://quarantine@contosoretaillake976757.dfs.core.windows.net/"
    "orderdetails/"
)


# ------------------------------------------------------------
# 2. Read Bronze
# ------------------------------------------------------------

df_orderdetails = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(bronze_orderdetails_path)
)

df_products = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(bronze_products_path)
)


# ------------------------------------------------------------
# 3. Data Type Standardization
# ------------------------------------------------------------

df_orderdetails_typed = (
    df_orderdetails
    .withColumn("OrderDetailID", F.col("OrderDetailID").cast("long"))
    .withColumn("OrderID", F.col("OrderID").cast("long"))
    .withColumn("ProductID", F.col("ProductID").cast("long"))
    .withColumn("Quantity", F.col("Quantity").cast("int"))
    .withColumn(
        "UnitPrice",
        F.col("UnitPrice").cast(DecimalType(10, 2))
    )
    .withColumn(
        "ModifiedDate",
        F.to_timestamp(F.col("ModifiedDate"))
    )
)


# ------------------------------------------------------------
# 4. Duplicate Detection
# ------------------------------------------------------------

duplicate_window = (
    Window
    .partitionBy("OrderDetailID")
    .orderBy(F.col("ModifiedDate").asc())
)

duplicate_extra_records = (
    df_orderdetails_typed
    .withColumn("row_num", F.row_number().over(duplicate_window))
    .filter(F.col("row_num") > 1)
    .drop("row_num")
    .withColumn(
        "RejectionReason",
        F.lit("Duplicate OrderDetailID")
    )
)


# ------------------------------------------------------------
# 5. Remove Duplicate OrderDetailID
# ------------------------------------------------------------

df_orderdetails_clean = (
    df_orderdetails_typed
    .withColumn("row_num", F.row_number().over(duplicate_window))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)


# ------------------------------------------------------------
# 6. Invalid Quantity
# ------------------------------------------------------------

invalid_quantity_records = (
    df_orderdetails_clean
    .filter(F.col("Quantity") <= 0)
    .withColumn(
        "RejectionReason",
        F.lit("Invalid Quantity")
    )
)


# ------------------------------------------------------------
# 7. Invalid UnitPrice
# ------------------------------------------------------------

invalid_unitprice_records = (
    df_orderdetails_clean
    .filter(F.col("UnitPrice") <= 0)
    .withColumn(
        "RejectionReason",
        F.lit("Invalid UnitPrice")
    )
)


# ------------------------------------------------------------
# 8. Invalid ProductID
# ------------------------------------------------------------

valid_products = (
    df_products
    .select(F.col("ProductID").cast("long").alias("ProductID"))
    .dropDuplicates()
)

invalid_product_records = (
    df_orderdetails_clean
    .join(
        valid_products,
        on="ProductID",
        how="left_anti"
    )
    .withColumn(
        "RejectionReason",
        F.lit("Invalid ProductID")
    )
)


# ------------------------------------------------------------
# 9. Quarantine
# ------------------------------------------------------------

df_orderdetails_quarantine = (
    duplicate_extra_records
    .unionByName(invalid_quantity_records)
    .unionByName(invalid_unitprice_records)
    .unionByName(invalid_product_records)
)

print(
    "OrderDetails quarantine records:",
    df_orderdetails_quarantine.count()
)


# ------------------------------------------------------------
# 10. Valid Records
# ------------------------------------------------------------

df_orderdetails_valid = (
    df_orderdetails_clean
    .filter(
        (F.col("Quantity") > 0) &
        (F.col("UnitPrice") > 0)
    )
    .join(
        valid_products,
        on="ProductID",
        how="left_semi"
    )
)

print(
    "Valid OrderDetails records:",
    df_orderdetails_valid.count()
)


# ------------------------------------------------------------
# 11. Write Quarantine as Delta
# ------------------------------------------------------------

(
    df_orderdetails_quarantine
    .write
    .format("delta")
    .mode("overwrite")
    .save(quarantine_orderdetails_path)
)


# ------------------------------------------------------------
# 12. Write Silver as Delta
# ------------------------------------------------------------

(
    df_orderdetails_valid
    .write
    .format("delta")
    .mode("overwrite")
    .save(silver_orderdetails_path)
)


# ------------------------------------------------------------
# 13. Validate Silver
# ------------------------------------------------------------

df_orderdetails_silver = (
    spark.read
    .format("delta")
    .load(silver_orderdetails_path)
)

remaining_duplicates = (
    df_orderdetails_silver
    .groupBy("OrderDetailID")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

invalid_quantity_remaining = (
    df_orderdetails_silver
    .filter(F.col("Quantity") <= 0)
    .count()
)

invalid_unitprice_remaining = (
    df_orderdetails_silver
    .filter(F.col("UnitPrice") <= 0)
    .count()
)

invalid_product_remaining = (
    df_orderdetails_silver
    .join(
        valid_products,
        on="ProductID",
        how="left_anti"
    )
    .count()
)

print("Remaining duplicate OrderDetailIDs:", remaining_duplicates)
print("Remaining invalid Quantity:", invalid_quantity_remaining)
print("Remaining invalid UnitPrice:", invalid_unitprice_remaining)
print("Remaining invalid ProductID:", invalid_product_remaining)
print("Final Silver count:", df_orderdetails_silver.count())
