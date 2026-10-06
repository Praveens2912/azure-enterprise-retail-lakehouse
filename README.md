# Enterprise Retail Data Lakehouse & Analytics Platform on Azure

<p align="center">
  <img src="./azure-retail-lakehouse-pipeline.png"
       alt="End-to-End Azure Retail Lakehouse Pipeline"
       width="100%">
</p>

## Overview

An end-to-end enterprise-style retail data engineering platform built on Microsoft Azure.

The project demonstrates how raw retail data can be ingested, validated, transformed, quality-checked, stored using a Medallion Architecture, and served through Azure SQL for analytics consumption.

## Architecture

```mermaid
flowchart LR
    A[Source CSV Data] --> B[Azure Data Factory]
    B --> C[ADLS Gen2 - Bronze]
    C --> D[Azure Databricks]
    D --> E[PySpark Data Quality & Transformation]
    E --> F[ADLS Gen2 - Silver Delta]
    F --> G[Gold Delta Layer]
    G --> H[Azure Data Factory]
    H --> I[Azure SQL Database]
    I --> J[Analytics / BI]
