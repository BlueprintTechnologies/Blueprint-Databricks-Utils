# Databricks notebook source
# DBTITLE 1,Import Libraries
# MAGIC %run ./class_SparkDataSummary

# COMMAND ----------

# DBTITLE 1,Retrieve Data via Notebook Widgets
# dbutils.widgets.text("csvFilePath", "/data/source", 'CSV File Path:')

csvFilePath = dbutils.widgets.get("csvFilePath")
csvs = [f.path for f in dbutils.fs.ls(csvFilePath)]

readOptions = {"header": True, "sep": "|"}
#df = spark.read.format("csv").options(**readOptions).load(files)

# COMMAND ----------

# DBTITLE 1,Retrieve Table Data
#dbutils.widgets.text('database','dbo','Database:')
#dbutils.widgets.text('table','myTable','Table:')

database = dbutils.widgets.get('database')
table = dbutils.widgets.get('table')

df = spark.sql(f"SELECT * FROM {database}.{table}")

# COMMAND ----------

# DBTITLE 1,Instantiate Summarizer
SDS = SparkDataSummary(df)

# COMMAND ----------

# DBTITLE 1,Data Profile (Boolean Columns)
SDS.summary('bool')

# COMMAND ----------

# DBTITLE 1,Data Profile (Numeric Columns)
SDS.summary('numeric')

# COMMAND ----------

# DBTITLE 1,Data Profile (String Columns)
SDS.summary("string")

# COMMAND ----------

# DBTITLE 1,Distinct Values Summary (Numeric & String Columns)
SDS.distinct_value_summary()
