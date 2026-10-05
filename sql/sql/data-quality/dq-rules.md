# Data Quality Rules

The project uses data quality validation during Bronze-to-Silver processing.

## Customers

| Rule | Expected Result |
|---|---|
| CustomerID must be unique | No duplicates |
| Email can be missing | NULL allowed |
| RegionID must exist in Regions | Invalid records quarantined |

### Observed Issues

- 1 duplicate CustomerID
- 1 invalid RegionID
- 1 missing Email

## Products

| Rule | Expected Result |
|---|---|
| ProductID must be unique | No duplicates |
| UnitPrice must be greater than 0 | Invalid records quarantined |
| Category should be populated | Invalid records quarantined |

### Observed Issues

- 1 negative UnitPrice
- 1 missing Category

## Orders

| Rule | Expected Result |
|---|---|
| OrderID must be unique | No duplicates |
| CustomerID must exist in valid Customers | Invalid records quarantined |
| OrderDate cannot be in the future | Invalid records quarantined |

### Observed Issues

- 2 invalid CustomerID relationships
- 1 future OrderDate

## OrderDetails

| Rule | Expected Result |
|---|---|
| OrderDetailID must be unique | Duplicate records handled |
| Quantity must be greater than 0 | Invalid records quarantined |
| UnitPrice must be greater than 0 | Invalid records quarantined |
| ProductID must exist in Products | Invalid records quarantined |

### Observed Issues

- 1 duplicate OrderDetailID
- 1 negative Quantity
- 1 negative UnitPrice
- 1 invalid ProductID

## Regions

The Regions reference data was validated for basic integrity and no data quality issues were identified.

## Quarantine Strategy

Records that failed critical validation rules were separated from valid records and written to the Quarantine layer.

This prevents bad source records from contaminating the Silver layer while preserving the rejected records for investigation.

## Validation Result

| Table | Bronze | Quarantined | Valid Silver |
|---|---:|---:|---:|
| Customers | 20,000 | 2 | 19,998 |
| Products | 2,000 | 2 | 1,998 |
| Orders | 100,000 | 3 | 99,997 |
| OrderDetails | 250,000 | 4 | 249,996 |
| Regions | 10 | 0 | 10 |
