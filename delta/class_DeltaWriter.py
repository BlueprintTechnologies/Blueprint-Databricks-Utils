# Databricks notebook source
# MAGIC %md
# MAGIC ##Purpose
# MAGIC This notebook accesses a class for writing, modifying, merging into delta tables. Notable features include simplified steps for appending metadata and inferred detection of merge columns for merge operations
# MAGIC ####Use Cases: 
# MAGIC * A: Databricks jobs that routinely update delta tables (merge and overwrite scenarios)
# MAGIC * B: Databricks notebooks that utilize reusable "dynamic SQL" for DDL and DML
# MAGIC ####NOTE:
# MAGIC It is recommended to define **table schemas** and supply them to functions when possible to maximize utility
# MAGIC 
# MAGIC Example:  
# MAGIC layer = ['bronze','silver','gold'][0]
# MAGIC bronze_dw = DeltaWriter(layer)
# MAGIC 
# MAGIC df = my_data_retrieval_function()
# MAGIC 
# MAGIC if write_mode == "create":
# MAGIC     bronze_dw.write_delta_table(df, "new_table", metadata_date="2022-09-21")
# MAGIC elif write_mode == "merge":
# MAGIC     bronze_dw.merge_delta_table(df, "existing_table", merge_col="id", inferUpdateCols=True, metadata_date="2022-09-21")

# COMMAND ----------
from pyspark.sql.functions import to_date, lit
from pyspark.sql.types import FloatType, LongType, StringType

from datetime import datetime
import pytz
import pandas as pd

# COMMAND ----------

class CommonDateTime:
    
    @staticmethod
    def get_CurrentUSCentralDateTime(dateFormat="%Y%m%d") -> str:
        timeUTC = pytz.utc
        timezoneCentral = pytz.timezone('US/Central')
        return dt.datetime.now(timezoneCentral).strftime(dateFormat)

# COMMAND ----------

class DeltaWriter:
    def __init__(self, table_layer):
        '''
        This class manages write, merge, and overwrite operations on delta tables
        :param table_layer (str): Domain in which delta tables reside (e.g.: bronze, silver, gold)
        '''
        self.table_layer = table_layer.lower()
        self._known_tables = [f"{self.table_layer}.{x.name}" for x in spark.catalog.listTables(self.table_layer)]
        
    @staticmethod
    def _append_meta_data(df, metadata_dict):
        """Append metadata to dataframe"""
        for m in metadata_dict:
            df = df.withColumn(f'__{m}', lit(f'{metadata_dict[m]}'))
        
        date = CommonDateTime.get_CurrentUSCentralDateTime(dateFormat="%Y%m%d")
        md_df = df.withColumn("__Date", to_date(lit(date), "yyyyMMdd"))
        return md_df
    
    
    def _get_full_table_name(self, table_name:str, suffix=None):
        full_name = f"{self.table_layer}.{table_name.lower()}"
        
        if suffix is not None:
            full_name = f"{full_name}_{suffix}"
            
        return full_name
    
    
    def write_delta_table(self, data_df, table_name:str, schema=None, **metadata):
        """
        Write data into database table
        :param data_df: Dataframe with data to write
        :param table_name(str): name of the database table to write
        :param schema: schema to be applied to the database table
        :param metadata (kwargs): Keyword arguments for appending table metadata
        """
        if data_df is None:
            print("No new data supplied! Write operation aborted")
        else:
            table = self._get_full_table_name(table_name)
            print(f"Attempting to write {table}...")
            
            if type(data_df) is pd.core.frame.DataFrame:
                data_df = spark.createDataFrame(data_df, schema)
                
            if len(metadata) > 0:
                new_data_df = self._append_meta_data(data_df, metadata)
            else:
                new_data_df = data_df
                
            new_data_df.write.saveAsTable(table, format='delta', mode='overwrite')
            print("Table write successful!")

        return None
    
    
    def overwrite_delta_table(self, data_df, table_name:str, schema=None, drop_former=False, **metadata):
        """
        Overwrite data into an existing database table
        :param data_df: Dataframe with data to write
        :param table_name(str): name of the database table to write
        :param schema: schema to be applied to the database table
        :param drop_former (bool): Set True to drop pre-existing table
        :param metadata (kwargs): Keyword arguments for appending table metadata
        """
        if data_df is None:
            print("No new data supplied! Write operation aborted")
        else:
            if type(data_df) is pd.core.frame.DataFrame:
                data_df = spark.createDataFrame(data_df, schema)
                
            if data_df.count() == 0:
                print("No new data supplied! Write operation aborted")
            else:
                table = self._get_full_table_name(table_name)
                print(f"Attempting to write {table}...")
                    
                if len(metadata) > 0:
                new_data_df = self._append_meta_data(data_df, metadata)
                else:
                    new_data_df = data_df
                
                if drop_former:
                    # Drop table
                    spark.sql(f"DROP TABLE {table}")
                else:
                    # Clear table
                    spark.sql(f"TRUNCATE TABLE {table}")
                
                new_data_df.write.saveAsTable(table, format='delta', mode='overwrite')
                print("Table overwrite successful!")
                
        return None
    
    
    def merge_delta_table(self, merge_data_df, table_name:str, merge_col="id", schema=None, inferMergeCols=False, update_col=None, inferUpdateCols=False, **metadata):
        """
        Merge data into a delta table.
        :param merge_data_df: Dataframe with data to merge into delta table
        :param table_name(str): name of the database table to write to
        :param merge_col (str, list): column(s) to use for performing the merge operation
        :param schema: schema to be applied to the database table
        :param inferMergeCols (bool): Assume non-nullable Long, Float, and String fields as the merge columns
        :param update_col (str, list): column(s) to update for matches identifying during the merge operation
        :param inferUpdateCols (bool): Assume all columns not identified as merge columns will require updating
        :param metadata (kwargs): Keyword arguments for appending table metadata
        """
        def form_merge_query(table, merge_columns, update_columns=None):
            merge_cols = lambda cols: " AND ".join([f"{table}.{x} = tempDF.{x}" for x in cols])
            update_cols = lambda cols: ", ".join([f"{x} = tempDF.{x}" for x in cols])
            
            # Build the merge clause
            if len(merge_columns) < 2:
                    merge_clause = f"{table}.{merge_columns[0]} = tempDF.{merge_columns[0]}"
            else:
                merge_clause = merge_cols(merge_columns)
            
            # Build the update clause
            if update_columns is None:
                update_clause = None   
            else:
                if len(update_columns) < 2:
                    update_clause = f"{update_col[0]} = tempDF.{update_col[0]}"
                else:
                    update_clause = update_cols(update_columns)
            
            query_base = f"MERGE INTO {table} USING tempDF ON {merge_clause}"
            query_suffix = "WHEN NOT MATCHED THEN INSERT *"
            
            if update_clause is None:
                query_final = f"{query_base} {query_suffix}"
            else:
                query_final = f"{query_base} WHEN MATCHED THEN UPDATE SET {update_clause} {query_suffix}"
            
            return query_final
        
        
        """Merge new data into existing database table"""
        if merge_data_df is None:
            print("No new data supplied! Merge operation aborted")
        else:
            def print_status_message(table:str, success:bool):
                """Print database status message"""
                if success:
                    print(f"Table {table} updated successfully.")
                else:
                    print(f"Table {table} not found. Creating new table...")
                    self.write_delta_table(merge_data_df, table_name, data_source, schema)
                return None
            
            table = self._get_full_table_name(table_name)
            
            if type(merge_data_df) is pd.core.frame.DataFrame:
                merge_data_df = spark.createDataFrame(merge_data_df, schema)
                
            if len(metadata) > 0:
                new_data_df = self._append_meta_data(merge_data_df, metadata)
            else:
                new_data_df = merge_data_df
            
            if inferMergeCols and schema is not None:
                merge_col = [c.name for c in schema.fields \
                             if not c.nullable and c.dataType in (LongType(),FloatType(),StringType())]
            
            if table in self._known_tables:
                new_data_df.createOrReplaceTempView("tempDF")
                
                # Columns to perform merge on
                if type(merge_col) is str:
                    merge_col = [merge_col]
                
                # Columns to update on matches
                if update_col is None:
                    if inferUpdateCols:
                        update_col = [c for c in new_data_df.columns if c not in merge_col]
                else:
                    if type(update_col) is str:
                        update_col = [update_col]
                
                query = form_merge_query(table, merge_col, update_col)
                results = spark.sql(query)
                spark.catalog.dropTempView("tempDF")
                
                print_status_message(table, True)
                display(results)
            else:
                print_status_message(table, False)

        return None
    
    
    def alter_table_drop_column(self, table_name:str, column_name:str, retain_previous=False):
        """
        Alter a delta table by dropping a column
        :param table_name(str): name of the delta table to alter
        :param column_name(str): name of the column to be dropped
        :param retain_previous(bool): retain the previous delta table as "{table_name}_previous"
        """
        table = self._get_full_table_name(table_name)
        previous_table = f"{table}_previous"
        new_table = f"{table}_new"
        
        # Retrieve all data, minus the column to be dropped
        data_df = spark.sql(f"SELECT * FROM {table}").drop(column_name)
        new_data_df = data_df.drop(column_name)
        
        # Create the new table
        print(f"Attempting to rewrite {table}...")
        new_data_df.write.saveAsTable(new_table, format='delta', mode='overwrite')
        
        # Rename the tables
        _ = spark.sql(f"ALTER TABLE {table} RENAME TO {previous_table}")
        _ = spark.sql(f"ALTER TABLE {new_table} RENAME TO {table}")
        print("Table write successful!")
        
        if retain_previous:
            print(f"{previous_table} retained in database")
        else:
            _ = spark.sql(f"DROP TABLE {previous_table}")
        
        return None
