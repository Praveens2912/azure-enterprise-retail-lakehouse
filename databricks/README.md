# Azure Databricks & PySpark

## Purpose

Azure Databricks was used for Bronze-to-Silver data transformation, data quality validation, quarantine handling, and Delta Lake processing.

## Processing Flow

```text
ADLS Gen2 Bronze
       ↓
Read source data with PySpark
       ↓
Data type standardization
       ↓
Data quality profiling
       ↓
Clean valid records
       ↓
Quarantine invalid records
       ↓
Write Silver as Delta
