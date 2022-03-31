# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC #Purpose
# MAGIC This notebook is a simple pattern to "wake up" a Databricks SQL endpoint and prepare it for a large load.
# MAGIC The common usecase we find with many clients is that they have a large set of reporting to power through on say Monday mornings.
# MAGIC 
# MAGIC This script will simply start up the endpoint (before your reports) and cache some key tables to enable better performance when the reports do start.
# MAGIC 
# MAGIC NOTE: This caching process may not **necessarily** scale the endpoint up, which would also greatly increase reporting performance if done beforehand.
# MAGIC 
# MAGIC NOTE2a: A [Databricks Personal Access Token](https://docs.databricks.com/dev-tools/api/latest/authentication.html#:~:text=Generate%20a%20personal%20access%20token,-This%20section%20describes&text=Settings%20in%20the%20lower%20left,the%20Generate%20New%20Token%20button.) is required to connect to the sql endpoint from this notebook.
# MAGIC 
# MAGIC NOTE2b: We strongly suggest keeping your access token in an [Azure Keyvault backed secret scope](https://docs.microsoft.com/en-us/azure/databricks/security/secrets/secret-scopes).

# COMMAND ----------

# DBTITLE 1,Notebook Parameters
# dbutils.widgets.text("server_host", defaultValue="adb-############.azuredatabricks.net", label="server_host")
# dbutils.widgets.text("endpoint_http_path", defaultValue="/sql/1.0/endpoints/##########", label="endpoint_http_path")
# dbutils.widgets.dropdown("custom_cache", "False", ["True", "False"])
# dbutils.widgets.text("max_cache_tables", defaultValue="10", label="max_cache_tables")

# COMMAND ----------

# DBTITLE 1,Imports
from databricks import sql

# COMMAND ----------

# DBTITLE 1,Gather SQL Endpoint Requirements
# we get our databricks api token from secrets backed by an azure keyvault
api_token = dbutils.secrets.get("db_api_scope","db_api_token")

# grab all of our widget values
server_host = dbutils.widgets.get("server_host")
endpoint = dbutils.widgets.get("endpoint_http_path")
custom_cache = dbutils.widgets.get("custom_cache")
max_cache_tables = int(dbutils.widgets.get("max_cache_tables"))

# COMMAND ----------

# DBTITLE 1,Start the SQL Endpoint
connection = sql.connect(
  server_hostname=server_host,
  http_path=endpoint,
  access_token=api_token)

# instantiate cursor
cursor = connection.cursor()

# COMMAND ----------

# DBTITLE 1,Collect a List of Tables to cache
if custom_cache == "False":
  # there are likely many catalogs/schemas/databases
  # we will just take one catalog
  cursor.catalogs()
  catalog = cursor.fetchone()[0]

  # get some tables in the selected catalog
  cursor.tables(catalog_name=catalog)
  catalog_tables = cursor.fetchall()
  cache_tables = [
    f'{x[1]}.{x[2]}'
    for i,x in enumerate(catalog_tables)
    if i < max_cache_tables
  ]
  
if custom_cache == "True":
  # we will only cache the defined tables in this list
  cache_tables = [
    'example.table1',
    'example.table2'
  ]

# COMMAND ----------

# DBTITLE 1,Cache the selected tables
for table in cache_tables:
  cursor.execute(f'CACHE SELECT * FROM {table}')
  result = cursor.fetchall()

# COMMAND ----------

# DBTITLE 1,Close Connection to DB SQL
cursor.close()
connection.close()
