# Security

## Azure Authentication

The project uses Azure-native identity and access controls where applicable.

## Managed Identity

Azure Data Factory uses a System-Assigned Managed Identity to access ADLS Gen2.

Azure Databricks uses an Access Connector with Managed Identity for Unity Catalog access to ADLS Gen2.

## RBAC

Storage access is controlled using Azure RBAC.

The following role was used where required:

`Storage Blob Data Contributor`

## Unity Catalog

Unity Catalog was used to configure:

- Storage Credential
- External Locations
- ADLS Gen2 access

## SQL Security

Azure SQL Database uses SQL authentication for the development environment.

Credentials are not stored in the GitHub repository.

## Security Best Practices

- Never commit passwords or access keys.
- Never commit connection strings containing credentials.
- Use Managed Identity where supported.
- Apply least-privilege RBAC.
- Store secrets securely rather than in source code.
