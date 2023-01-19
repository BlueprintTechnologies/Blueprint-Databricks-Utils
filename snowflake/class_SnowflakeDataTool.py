# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

"""
Created on Monday, October 31, 2022 at 09:24 by Adrian Tullock <atullock@bpcs.com>
Copyright (C) 2022, by Blueprint Technologies. All Rights Reserved.
"""
class SnowflakeDataTool:
    def __init__(self):
        # Snowflake connection object
        self._sfConnection = self.connect()
        
        self._sfTables = []
    
    #----- PUBLIC METHODS-----#
    def connect(self, schema:str=None):
        """Connect to the Snowflake database
        :param schema(str): Optional parameter. Set the schema to use in queries.
        """
        options = self._connection_options()
        connObject = spark.read.format("snowflake").options(**options)
        self._sfConnection = connObject
        
        if not self.is_connected():
            print("Failed to establish a connection. Check your connection options.")
        else:
            self._query_table_list()
        
        return
    
    def is_connected(self):
        """Verify if connected to Snowflake database"""
        try:
            query = "SELECT * from current_version()"
            _ = self._sfConnection.option("query",query).load()
            status = True
        except Exception:
            status = False
            
        return status
    
    def list_tables(self):
        """View the list of tables in the database"""
        return self._sfTables
    
    def query_column_metadata(self, tableName):
        """Retrieve a table's (tableName) column metadata"""
        OP = 'ORDINAL_POSITION'
        columns = ['COLUMN_NAME','DATA_TYPE',OP,'IS_NULLABLE']
        table = tableName.upper()
        sqlQuery = f"SELECT {','.join(columns)} FROM information_schema.columns WHERE table_name = '{table}' ORDER BY {OP}"
        column_data_df = self.sql(sqlQuery)
        
        return column_data_df
    
    def sql(self, query:str):
        """Execute a SQL query against the database"""
        try:
            results = self._sfConnection.option("query", query).load()
        except Exception as e:
            raise
                
        return results
    
    #----- PRIVATE METHODS -----#
    def _connection_options(self):
        """Internal function used to retrieve Snowflake connection options"""
        connOptions = {
            "sfUrl": "https://<address>.snowflakecomputing.com/",
            "sfUser": "User",
            "sfPassword": "Password"
            "sfDatabase": "Database",
            "sfSchema": self.snowflake_schema,
            "sfWarehouse": "Warehouse",
            "sfRole": "Role"
        }
        return connOptions
    
    def _query_table_list(self):
        query = "SELECT concat(t.TABLE_SCHEMA,'.',t.TABLE_NAME) AS SFTABLE FROM\
        information_schema.tables as t WHERE t.table_schema != 'INFORMATION_SCHEMA'"
        tableList = self.sql(query).collect()
        
        _ = [self._sfTables.append(t.asDict().get('SF_TABLE')) for t in tableList]
        reutrn None
