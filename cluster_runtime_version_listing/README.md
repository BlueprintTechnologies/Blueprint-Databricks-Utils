## Purpose
This notebook is attended to help find what versions of Databricks Runtime is available.  
The output of the API call returns the value for Databricks Runtime to be used to create a cluster.
A good use case is with Azure Data Factory to start a Job Cluster with a particular Runtime value.

##### NOTE: 

A [Databricks Personal Access Token](https://docs.databricks.com/dev-tools/api/latest/authentication.html#:~:text=Generate%20a%20personal%20access%20token,-This%20section%20describes&text=Settings%20in%20the%20lower%20left,the%20Generate%20New%20Token%20button.) is required to connect to the sql endpoint from this notebook.


##### NOTE2a: 

Knowing the URL of your databricks instance is needed as well.

##### NOTE2b: 

We strongly suggest keeping your access token and databricks instance URL in an [Azure Keyvault backed secret scope](https://docs.microsoft.com/en-us/azure/databricks/security/secrets/secret-scopes).
