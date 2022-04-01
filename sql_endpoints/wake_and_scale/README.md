## Purpose
This notebook is a simple pattern to "wake up" a Databricks SQL endpoint and prepare it for a large load.
The common usecase we find with many clients is that they have a large set of reporting to power through on say Monday mornings.

This script will simply start up the endpoint (before your reports) and force it to scale up to quickly to enable better performance when the reports do start.

##### NOTE: 

With many simultaneous connections and queries to one endpoint, spark executors can run out of memory, causing some threads to throw exceptions waiting on `fetchone()`. This is okay for our purposes. We want to force the cluster to scale up and this is one way to create pressure.

##### NOTE2a: 

A [Databricks Personal Access Token](https://docs.databricks.com/dev-tools/api/latest/authentication.html#:~:text=Generate%20a%20personal%20access%20token,-This%20section%20describes&text=Settings%20in%20the%20lower%20left,the%20Generate%20New%20Token%20button.) is required to connect to the sql endpoint from this notebook.

##### NOTE2b: 

We strongly suggest keeping your access token in an [Azure Keyvault backed secret scope](https://docs.microsoft.com/en-us/azure/databricks/security/secrets/secret-scopes).
