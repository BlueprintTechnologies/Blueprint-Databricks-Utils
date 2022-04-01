# Databricks notebook source
# MAGIC %md
# MAGIC ##### URL to manually create runtime version string
# MAGIC https://docs.microsoft.com/en-us/azure/databricks/dev-tools/api/#--runtime-version-strings

# COMMAND ----------

import requests
import json
from pyspark.sql.functions import udf, col, explode
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ArrayType
from pyspark.sql import Row

def executeRestApi(verb, url, headers, body):
  res = None
  # Make API request, get response object back
  try:
    if verb == "get":
      res = requests.get(url, data=body, headers=headers)
    else:
      res = requests.post(url, data=body, headers=headers)
  except Exception as e:
    return e
  if res != None and res.status_code == 200:
    return res.text
  return None



# COMMAND ----------

# Utilized Key Vault as a Secret Scope to save off access token and urls
databricksURL = dbutils.secrets.get('key-vault-secret','secret-bricks-url')
AccessToken = dbutils.secrets.get('key-vault-secret','secret-bricks-accesstoken')
headers = {
      'content-type': "application/json",
      'Authorization': f'Bearer {AccessToken}'
  }

x = executeRestApi ('get',f'https://{databricksURL}/api/2.0/clusters/spark-versions',headers,None)
df = spark.read.json(sc.parallelize([x]))
display(df.select(explode("versions").alias("versions")).orderBy("versions.key"))

