# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ##Purpose
# MAGIC This notebook is a simple pattern to "wake up" a Databricks SQL endpoint and prepare it for a large load.
# MAGIC The common usecase we find with many clients is that they have a large set of reporting to power through on say Monday mornings.
# MAGIC 
# MAGIC This script will simply start up the endpoint (before your reports) and force it to scale up to quickly to enable better performance when the reports do start.
# MAGIC 
# MAGIC #####NOTE: 
# MAGIC 
# MAGIC With many simultaneous connections and queries to one endpoint, spark executors can run out of memory, causing some threads to throw exceptions waiting on `fetchone()`. This is okay for our purposes. We want to force the cluster to scale up and this is one way to create pressure.
# MAGIC 
# MAGIC #####NOTE2a: 
# MAGIC 
# MAGIC A [Databricks Personal Access Token](https://docs.databricks.com/dev-tools/api/latest/authentication.html#:~:text=Generate%20a%20personal%20access%20token,-This%20section%20describes&text=Settings%20in%20the%20lower%20left,the%20Generate%20New%20Token%20button.) is required to connect to the sql endpoint from this notebook.
# MAGIC 
# MAGIC #####NOTE2b: 
# MAGIC 
# MAGIC We strongly suggest keeping your access token in an [Azure Keyvault backed secret scope](https://docs.microsoft.com/en-us/azure/databricks/security/secrets/secret-scopes).

# COMMAND ----------

# DBTITLE 1,Notebook Parameters
# dbutils.widgets.text("server_host", defaultValue="adb-############.azuredatabricks.net", label="server_host")
# dbutils.widgets.text("endpoint_http_path", defaultValue="/sql/1.0/endpoints/##########", label="endpoint_http_path")
# dbutils.widgets.dropdown("custom_cache", "False", ["True", "False"])
# dbutils.widgets.text("max_threads", defaultValue="10", label="max_threads")
# dbutils.widgets.dropdown("verbose", "True", ["True", "False"])

# COMMAND ----------

# DBTITLE 1,Imports
from databricks import sql
import threading

# COMMAND ----------

# DBTITLE 1,Gather SQL Endpoint Requirements
# we get our databricks api token from secrets backed by an azure keyvault
api_token = dbutils.secrets.get("db_api_scope","db_api_token")

# grab all of our widget values
server_host = dbutils.widgets.get("server_host")
endpoint = dbutils.widgets.get("endpoint_http_path")
custom_cache = dbutils.widgets.get("custom_cache")
max_threads = int(dbutils.widgets.get("max_threads"))

# get verbosity and cast to boolean
verbose = dbutils.widgets.get("verbose")
if verbose == "True":
  verbose = True
else:
  verbose = False

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
# Here the user can provide a list of tables they would like to
# cache on cluster nodes to improve downstream job performance
if custom_cache == "True":
  # we will only cache the defined tables in this list
  cache_tables = [
    'example.table1',
    'example.table2'
  ]
  
# if the user does not provide a custom list of table to cache
# we will simply pick some from their catalog
else:
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
    if i < max_threads
  ]
  
  
  
for table in cache_tables:
  print(table)

# COMMAND ----------

# DBTITLE 1,Close main thread connection to sql endpoint
# close connection
cursor.close()
connection.close()

# COMMAND ----------

# DBTITLE 1,Define worker function for each thread
def thread_worker(table, verbose):
  '''
    PURPOSE:
      this worker function for threads will create a unique sql endpoint
      connection and use it to perform a single SELECT * query on a table.
      
    PARAMS:
      table: (str) full name of table to query e.g. marketing.click_through
      verbose: (bool) determines if threads will report status updates via
                      print statements
  '''
  # get thread id
  tid = threading.get_ident()
  
  # create unique connection
  thread_connection = sql.connect(
    server_hostname=server_host,
    http_path=endpoint,
    access_token=api_token
  )

  # instantiate cursor
  thread_cursor = thread_connection.cursor()
  
  # run a sql command and get result
  thread_cursor.execute(f'SELECT * FROM {table}')
  if verbose:
    print(f"TID: {tid}: waiting on results from table: {table}.")
  result = thread_cursor.fetchone()
  if verbose:
    print(f"TID: {tid}: Done!")
  
  # close connection
  thread_cursor.close()
  thread_connection.close()

# COMMAND ----------

# DBTITLE 1,Submit many sql commands with multiple threads concurrently and wait for all to complete
threads = []
verbose = True
# submit many simultaneous requests to sql endpoint
for tid,table in enumerate(cache_tables):
  print(f"launching thread {tid}...")
  threads.append(
    threading.Thread(
      target=thread_worker,
      args=(table, verbose)
    )
  )
  threads[-1].start()
  
# join all threads back
for thread in threads:
  thread.join()

# COMMAND ----------


