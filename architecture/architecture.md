# Enterprise Retail Lakehouse Architecture

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
