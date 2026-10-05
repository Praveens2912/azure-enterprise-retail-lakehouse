# Azure Data Factory

## Purpose

Azure Data Factory is used for data ingestion, orchestration, metadata-driven processing, and loading Gold data into Azure SQL.

## Metadata-Driven Ingestion

The ingestion pipeline reads table configuration from Azure SQL instead of hard-coding each source.

```text
PipelineMetadata
       ↓
Lookup_PipelineMetadata
       ↓
ForEach_Table
       ↓
Dynamic Dataset Parameters
       ↓
Bronze Data Processing
