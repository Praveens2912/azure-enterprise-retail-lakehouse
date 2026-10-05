# Enterprise Retail Lakehouse
# Regions - Bronze to Silver
#
# Purpose:
# Read raw Regions data from ADLS Gen2 Bronze,
# standardize and validate the reference data,
# and write valid records to the Silver Delta layer.
#
# Expected Result:
# Bronze records       = 10
# Duplicate RegionID  = 0
# Blank RegionName    = 0
# Blank Country       = 0
# Quarantine records  = 0
#
# Silver Output:
# Valid Silver records = 10
# Duplicate RegionID   = 0
# Blank RegionName     = 0
# Blank Country        = 0

from pyspark.sql import functions as F


# ------------------------------------------------------------
# 1. Paths
# ------------------------------------------------------------

bronze_regions_path = (
    "abfss://bronze@contosoretaillake976757.dfs.core.windows.net/"
    "regions/Regions.csv"
)

silver_regions_path = (
    "abfss://silver@contosoretaillake976757.dfs.core.windows.net/"
    "regions/"
)


# ------------------------------------------------------------
# 2. Read Bronze
# ------------------------------------------------------------

df_regions = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(bronze_regions_path)
)


# ------------------------------------------------------------
# 3. Data Type Standardization
# ------------------------------------------------------------

df_regions_typed = (
    df_regions
    .withColumn("RegionID", F.col("RegionID").cast("int"))
    .withColumn("RegionName", F.trim(F.col("RegionName")))
    .withColumn("Country", F.trim(F.col("Country")))
)


# ------------------------------------------------------------
# 4. Duplicate RegionID Check
# ------------------------------------------------------------

duplicate_regions = (
    df_regions_typed
    .groupBy("RegionID")
    .count()
    .filter(F.col("count") > 1)
)

print(
    "Duplicate RegionIDs:",
    duplicate_regions.count()
)


# ------------------------------------------------------------
# 5. Create Valid Regions
# ------------------------------------------------------------

df_regions_clean = (
    df_regions_typed
    .filter(
        F.col("RegionID").isNotNull() &
        F.col("RegionName").isNotNull() &
        (F.trim(F.col("RegionName")) != "") &
        F.col("Country").isNotNull() &
        (F.trim(F.col("Country")) != "")
    )
)

print(
    "Valid Regions records:",
    df_regions_clean.count()
)


# ------------------------------------------------------------
# 6. Write Silver as Delta
# ------------------------------------------------------------

(
    df_regions_clean
    .write
    .format("delta")
    .mode("overwrite")
    .save(silver_regions_path)
)


# ------------------------------------------------------------
# 7. Read Silver for Validation
# ------------------------------------------------------------

df_regions_silver = (
    spark.read
    .format("delta")
    .load(silver_regions_path)
)


# ------------------------------------------------------------
# 8. Final Validation
# ------------------------------------------------------------

remaining_duplicates = (
    df_regions_silver
    .groupBy("RegionID")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

blank_regionname_remaining = (
    df_regions_silver
    .filter(
        F.col("RegionName").isNull() |
        (F.trim(F.col("RegionName")) == "")
    )
    .count()
)

blank_country_remaining = (
    df_regions_silver
    .filter(
        F.col("Country").isNull() |
        (F.trim(F.col("Country")) == "")
    )
    .count()
)

print(
    "Remaining duplicate RegionIDs:",
    remaining_duplicates
)

print(
    "Remaining blank RegionName:",
    blank_regionname_remaining
)

print(
    "Remaining blank Country:",
    blank_country_remaining
)

print(
    "Final Silver Regions records:",
    df_regions_silver.count()
)
