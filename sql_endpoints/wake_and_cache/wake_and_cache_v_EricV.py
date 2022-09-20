# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ##Purpose
# MAGIC This notebook is a simple pattern to "wake up" a Databricks SQL endpoint and prepare it for a large load.
# MAGIC The common usecase we find with many clients is that they have a large set of reporting to power through on say Monday mornings.
# MAGIC 
# MAGIC This script will simply start up the endpoint (before your reports) and cache some key tables to enable better performance when the reports do start.
# MAGIC 
# MAGIC #####NOTE: 
# MAGIC 
# MAGIC This caching process may not **necessarily** scale the endpoint up, which would also greatly increase reporting performance if done beforehand.
# MAGIC 
# MAGIC #####NOTE2a: 
# MAGIC 
# MAGIC A [Databricks Personal Access Token](https://docs.databricks.com/dev-tools/api/latest/authentication.html#:~:text=Generate%20a%20personal%20access%20token,-This%20section%20describes&text=Settings%20in%20the%20lower%20left,the%20Generate%20New%20Token%20button.) is required to connect to the sql endpoint from this notebook.
# MAGIC 
# MAGIC #####NOTE2b: 
# MAGIC 
# MAGIC We strongly suggest keeping your access token in an [Azure Keyvault backed secret scope](https://docs.microsoft.com/en-us/azure/databricks/security/secrets/secret-scopes).

# COMMAND ----------

# DBTITLE 1,PIP Install the Databricks SQL Connector
#%pip install databricks-sql-connector

# COMMAND ----------

# DBTITLE 1,Notebook Parameters
##dbutils.widgets.text("server_host", defaultValue="adb-############.azuredatabricks.net", label="server_host")
##dbutils.widgets.text("endpoint_http_path", defaultValue="/sql/1.0/endpoints/##########", label="endpoint_http_path")
##dbutils.widgets.text("max_cache_tables", defaultValue="10", label="max_cache_tables")
#dbutils.widgets.text("database_to_cache", defaultValue="default", label="database_to_cache")
#dbutils.widgets.remove("catalog_dot_database_to_cache")

# COMMAND ----------

# DBTITLE 1,Imports
from databricks import sql

# COMMAND ----------

# DBTITLE 1,Gather SQL Endpoint Requirements
# we get our databricks api token from secrets backed by an azure keyvault
api_token = dbutils.secrets.get("ericv-adls-secrets","overwatch-pat-token")

# grab all of our widget values
server_host = dbutils.widgets.get("server_host")
endpoint = dbutils.widgets.get("endpoint_http_path")
max_cache_tables = int(dbutils.widgets.get("max_cache_tables"))
target_database = dbutils.widgets.get("database_to_cache")

# COMMAND ----------

# DBTITLE 1,Start the SQL Endpoint
connection = sql.connect(
  server_hostname=server_host,
  http_path=endpoint,
  access_token=api_token)

# instantiate cursor
cursor = connection.cursor()

# COMMAND ----------

tables = list(spark.catalog.listTables(target_database))
tables_to_cache = []
for x in tables:
  tables_to_cache.append(f'{x[1]}.{x[0]}')
  
tables_to_cache

# COMMAND ----------

# DBTITLE 1,Cache the selected tables
for table in tables_to_cache:
  sql_command_1 = f'select * from {table} limit 2'
  #sql_command_2 = f'select * from {table} where rand() <= .3'
  print(sql_command_1)
  try:
    cursor.execute(sql_command_1)
    result = cursor.fetchall()
    for row in result:
      print(row)
  except:
    pass

# COMMAND ----------

# DBTITLE 1,Close Connection to DB SQL
cursor.close()
connection.close()

# COMMAND ----------


