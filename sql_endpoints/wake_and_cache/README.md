## Purpose

This notebook is unique in a way since it is a python notebook which runs on a Data Science and Engineering type workspace, but is a utility for SQL Endpoints.

This notebook is a simple pattern to "wake up" a Databricks SQL endpoint and prepare it for a large load.
The common usecase we find with many clients is that they have a large set of reporting to power through on say Monday mornings.

This script will simply start up the endpoint (before your reports) and cache some key tables to enable better performance when the reports do start.

##### NOTE: 

This caching process may not **necessarily** scale the endpoint up, which would also greatly increase reporting performance if done beforehand.

##### NOTE2a: 

A [Databricks Personal Access Token](https://docs.databricks.com/dev-tools/api/latest/authentication.html#:~:text=Generate%20a%20personal%20access%20token,-This%20section%20describes&text=Settings%20in%20the%20lower%20left,the%20Generate%20New%20Token%20button.) is required to connect to the sql endpoint from this notebook.

##### NOTE2b: 

We strongly suggest keeping your access token in an [Azure Keyvault backed secret scope](https://docs.microsoft.com/en-us/azure/databricks/security/secrets/secret-scopes).
