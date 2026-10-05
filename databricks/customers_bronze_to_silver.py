# Enterprise Retail Lakehouse
# Customers - Bronze to Silver
#
# Purpose:
# Read raw customer data from ADLS Gen2 Bronze,
# standardize data types, apply data quality rules,
# quarantine invalid records, and write valid records
# to the Silver Delta layer.

from pyspark.sql import functions as F
from pyspark.sql.window import Window


# ------------------------------------------------------------
# 1. Paths
# ------------------------------------------------------------

bronze_customers_path = (
    "abfss://bronze@contosoretaillake976757.dfs.core.windows.net/"
    "customers/Customers.csv"
)

bronze_regions_path = (
    "abfss://bronze@contosoretaillake976757.dfs.core.windows.net/"
    "regions/Regions.csv"
)

silver_customers_path = (
    "abfss://silver@contosoretaillake976757.dfs.core.windows.net/"
    "customers/"
)

quarantine_customers_path = (
    "abfss://quarantine@contosoretaillake976757.dfs.core.windows.net/"
    "customers/"
)


# ------------------------------------------------------------
# 2. Read Bronze
# ------------------------------------------------------------

df_customers = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(bronze_customers_path)
)

df_regions = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(bronze_regions_path)
)


# ------------------------------------------------------------
# 3. Data Type Standardization
# ------------------------------------------------------------

df_customers_typed = (
    df_customers
    .withColumn("CustomerID", F.col("CustomerID").cast("int"))
    .withColumn("RegionID", F.col("RegionID").cast("int"))
    .withColumn("SignupDate", F.to_date(F.col("SignupDate")))
    .withColumn("ModifiedDate", F.to_timestamp(F.col("ModifiedDate")))
    .withColumn(
        "Email",
        F.when(F.trim(F.col("Email")) == "", None)
         .otherwise(F.trim(F.col("Email")))
    )
)


# ------------------------------------------------------------
# 4. Data Quality Check - Duplicate CustomerID
# ------------------------------------------------------------

duplicate_customers = (
    df_customers_typed
    .groupBy("CustomerID")
    .count()
    .filter(F.col("count") > 1)
)

print(
    "Duplicate CustomerIDs:",
    duplicate_customers.count()
)


# ------------------------------------------------------------
# 5. Identify Duplicate Records for Quarantine
# ------------------------------------------------------------

duplicate_window = (
    Window
    .partitionBy("CustomerID")
    .orderBy(F.col("ModifiedDate").desc_nulls_last())
)

duplicate_extra_records = (
    df_customers_typed
    .withColumn("row_num", F.row_number().over(duplicate_window))
    .filter(F.col("row_num") > 1)
    .drop("row_num")
    .withColumn(
        "RejectionReason",
        F.lit("Duplicate CustomerID")
    )
)


# ------------------------------------------------------------
# 6. Remove Duplicate CustomerID
# ------------------------------------------------------------

df_customers_clean = (
    df_customers_typed
    .withColumn("row_num", F.row_number().over(duplicate_window))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)


# ------------------------------------------------------------
# 7. Validate RegionID
# ------------------------------------------------------------

df_regions_reference = (
    df_regions
    .select("RegionID")
    .dropDuplicates()
)

invalid_region_records = (
    df_customers_clean
    .join(
        df_regions_reference,
        on="RegionID",
        how="left_anti"
    )
    .withColumn(
        "RejectionReason",
        F.lit("Invalid RegionID")
    )
)


# ------------------------------------------------------------
# 8. Create Quarantine Dataset
# ------------------------------------------------------------

df_customers_quarantine = (
    duplicate_extra_records
    .unionByName(invalid_region_records)
)

print(
    "Customer quarantine records:",
    df_customers_quarantine.count()
)


# ------------------------------------------------------------
# 9. Create Valid Customer Dataset
# ------------------------------------------------------------

df_customers_valid = (
    df_customers_clean
    .join(
        df_regions_reference,
        on="RegionID",
        how="left_semi"
    )
)

print(
    "Valid customer records:",
    df_customers_valid.count()
)


# ------------------------------------------------------------
# 10. Write Quarantine as Delta
# ------------------------------------------------------------

(
    df_customers_quarantine
    .write
    .format("delta")
    .mode("overwrite")
    .save(quarantine_customers_path)
)


# ------------------------------------------------------------
# 11. Write Silver as Delta
# ------------------------------------------------------------

(
    df_customers_valid
    .write
    .format("delta")
    .mode("overwrite")
    .save(silver_customers_path)
)


# ------------------------------------------------------------
# 12. Validate Silver
# ------------------------------------------------------------

df_customers_silver = (
    spark.read
    .format("delta")
    .load(silver_customers_path)
)

print(
    "Silver customer records:",
    df_customers_silver.count()
)


# ------------------------------------------------------------
# 13. Final Validation
# ------------------------------------------------------------

remaining_duplicates = (
    df_customers_silver
    .groupBy("CustomerID")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

invalid_regions_remaining = (
    df_customers_silver
    .join(
        df_regions_reference,
        on="RegionID",
        how="left_anti"
    )
    .count()
)

print("Remaining duplicate CustomerIDs:", remaining_duplicates)
print("Remaining invalid RegionIDs:", invalid_regions_remaining)
print("Final Silver count:", df_customers_silver.count())
