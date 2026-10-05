-- Enterprise Retail Lakehouse
-- Azure SQL Serving Layer

CREATE TABLE dbo.fact_sales
(
    OrderID        BIGINT NOT NULL,
    OrderDetailID  BIGINT NOT NULL,
    CustomerID     INT NOT NULL,
    ProductID      INT NOT NULL,
    RegionID       INT NOT NULL,
    OrderDate      DATE NOT NULL,
    Status         VARCHAR(50) NULL,
    Quantity       INT NOT NULL,
    UnitPrice      DECIMAL(10,2) NOT NULL,
    SalesAmount    DECIMAL(18,2) NOT NULL,
    CONSTRAINT PK_fact_sales PRIMARY KEY (OrderDetailID)
);

CREATE TABLE dbo.dim_customer
(
    CustomerID    INT NOT NULL,
    CustomerName  VARCHAR(200) NULL,
    Email         VARCHAR(255) NULL,
    RegionID      INT NOT NULL,
    RegionName    VARCHAR(100) NOT NULL,
    Country       VARCHAR(100) NOT NULL,
    SignupDate    DATE NULL,
    ModifiedDate  DATETIME2 NULL,
    CONSTRAINT PK_dim_customer PRIMARY KEY (CustomerID)
);

CREATE TABLE dbo.dim_product
(
    ProductID    INT NOT NULL,
    ProductName  VARCHAR(200) NOT NULL,
    Category     VARCHAR(100) NOT NULL,
    UnitPrice    DECIMAL(10,2) NOT NULL,
    ModifiedDate DATETIME2 NULL,
    CONSTRAINT PK_dim_product PRIMARY KEY (ProductID)
);

CREATE TABLE dbo.dim_region
(
    RegionID    INT NOT NULL,
    RegionName  VARCHAR(100) NOT NULL,
    Country     VARCHAR(100) NOT NULL,
    CONSTRAINT PK_dim_region PRIMARY KEY (RegionID)
);

CREATE TABLE dbo.sales_daily
(
    OrderDate      DATE NOT NULL,
    TotalSales     DECIMAL(18,2) NOT NULL,
    TotalOrders    INT NOT NULL,
    TotalQuantity  BIGINT NOT NULL,
    CONSTRAINT PK_sales_daily PRIMARY KEY (OrderDate)
);

CREATE TABLE dbo.customer_360
(
    CustomerID    INT NOT NULL,
    CustomerName  VARCHAR(200) NULL,
    Email         VARCHAR(255) NULL,
    RegionID      INT NOT NULL,
    RegionName    VARCHAR(100) NULL,
    Country       VARCHAR(100) NOT NULL,
    SignupDate    DATE NULL,
    TotalOrders   INT NOT NULL,
    TotalQuantity BIGINT NOT NULL,
    TotalSales    DECIMAL(18,2) NOT NULL,
    CONSTRAINT PK_customer_360 PRIMARY KEY (CustomerID)
);
