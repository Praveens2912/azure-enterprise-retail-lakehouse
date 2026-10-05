-- Enterprise Retail Lakehouse
-- ADF Metadata Configuration

CREATE TABLE dbo.PipelineMetadata
(
    TableName        VARCHAR(100) NOT NULL,
    SourceType       VARCHAR(50)  NOT NULL,
    SourceObject     VARCHAR(200) NOT NULL,
    WatermarkColumn  VARCHAR(100) NULL,
    TargetPath       VARCHAR(300) NOT NULL,
    LoadType         VARCHAR(50)  NOT NULL,
    IsActive         BIT          NOT NULL
);

INSERT INTO dbo.PipelineMetadata
(
    TableName,
    SourceType,
    SourceObject,
    WatermarkColumn,
    TargetPath,
    LoadType,
    IsActive
)
VALUES
(
    'Customers',
    'CSV',
    'Customers.csv',
    'ModifiedDate',
    'bronze/customers',
    'Full',
    1
),
(
    'Products',
    'CSV',
    'Products.csv',
    'ModifiedDate',
    'bronze/products',
    'Full',
    1
),
(
    'Orders',
    'CSV',
    'Orders.csv',
    'ModifiedDate',
    'bronze/orders',
    'Full',
    1
),
(
    'OrderDetails',
    'CSV',
    'OrderDetails.csv',
    'ModifiedDate',
    'bronze/orderdetails',
    'Full',
    1
),
(
    'Regions',
    'CSV',
    'Regions.csv',
    NULL,
    'bronze/regions',
    'Full',
    1
);

-- Validation
SELECT *
FROM dbo.PipelineMetadata;
