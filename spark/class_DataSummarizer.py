# Databricks notebook source
# DBTITLE 1,Package Imports

import pyspark.sql.functions as f
from pyspark.sql.types import *

# COMMAND ----------

# DBTITLE 1,Class Definition
"""
Created on Monday, December 5, 2022 at 19:24 by Adrian Tullock <atullock@bpcs.com>
Copyright (C) 2022, by Blueprint Technologies. All Rights Reserved.
"""
class SparkDataSummary:
    def __init__(self, df, dbName:str, tableName:str, dtypeChanges=None):
        """Initialization
        :param df (dataframe): Dataframe to be profiled
        :param dbName (str): Database name of table being profiled
        :param tableName (str): Name of the table being profiled
        :param dtypeChanges (dict): Accepts dict of column-type key-value pairs
        e.g.: {"col1":"integer", "col2":"bool"} will attempt to cast col1 and col2 as integer and boolean types, respectively.
        """
        self.dbName = dbName.lower()
        self.tableName = tableName.lower()
        self.df = self._convert_ts_to_string(df)
        self._summaryTypes = ('bool','numeric','string')
        
        if dtypeChanges is not None:
            self.update_dtypes(dtypeChanges)
        
        # Get typed dataframes
        self._boolean_df = self._typed_df("bool")
        self._numeric_df = self._typed_df("numeric")
        self._string_df = self._typed_df("string")
        
        # Count column values
        self.row_count = df.count()
        self._null_counts = self._get_null_counts()
        self._bool_counts = self._get_bool_counts()
        
        # Summary data
        self._summary = {"bool":None,"numeric":None,"string":None}
        self._distinct_value_summary = None
        self._string_data_profile = None
    
    #----- PUBLIC METHODS -----#
    def distinct_value_summary(self, showSummary=True):
        """Create and display a distinct value counts summary for numeric and string columns
        """
        distinct = lambda x: df.select(x).distinct().count()
        
        # Summarize string and numeric columns
        df_columns = self._string_df.columns.copy()
        df_columns.extend(self._numeric_df.columns)
        df = self.df.select(df_columns)
        
        summary_df = df.summary('count')
        row_template = summary_df.collect()[0].asDict()
        
        new_rows = []
        new_row = row_template.copy()
        new_row.update({'summary':'distinct'})
        __ = [new_row.update({c:str(distinct(c))}) for c in df_columns]
        new_rows.append(new_row)
        
        # Join column summary metrics
        summary_df = summary_df.unionByName(spark.createDataFrame(new_rows))
        summary_pivot_df = summary_df.unpivot("summary", df_columns,"ColumnName","value")
        counts_df = summary_pivot_df.where(
            "summary=='count'").withColumnRenamed(
            "value","TotalValues").drop("summary")
        
        distincts_df = summary_pivot_df.where(
            "summary=='distinct'").withColumnRenamed(
            "value","DistinctCount").withColumnRenamed(
            "ColumnName","Match").drop("summary")
        summary_pivot_df = counts_df.join(distincts_df, [counts_df.ColumnName==distincts_df.Match]).drop("Match")
        
        final_summary_df = summary_pivot_df.withColumn(
            "SourceDatabase",f.lit(self.dbName)).withColumn(
            "SourceTable",f.lit(self.tableName)).withColumn(
            "TotalRecords",f.lit(self.row_count).astype(LongType()))
        
        final_summary_df = final_summary_df.select(
            "SourceDatabase","SourceTable","TotalRecords","ColumnName",f.col("TotalValues").astype(LongType()),
            f.col("DistinctCount").astype(LongType())).orderBy(
            f.col("DistinctCount").desc())
        
        self._distinct_value_summary = final_summary_df
        
        if showSummary:
            print("Distinct Values Summary:")
            display(self._distinct_value_summary)
        
        return None
        
    def full_summary(self, showSummary=True):
        """Create and display all available summary data"""
        self.summarize('numeric', showSummary)
        self.summarize('string', showSummary)
        self.summarize('bool', showSummary)
        self.distinct_value_summary(showSummary)
        
        return None
    
    def get_distinct_value_summary(self):
        """Retrieve distinct value data summary
        """
        summary = self._distinct_value_summary
        
        if summary is None:
            self.distinct_value_summary(showSummary=False)
            summary = self._distinct_value_summary
        
        return summary
    
    def get_summary(self, dtype):
        """Retrieve a typed data summary
        :param dtype (str): Accepts these values: "bool", "numeric", "string"
        """
        assert dtype in self._summaryTypes, f"Invalid dtype: accepts {self._summaryTypes}"
        summary = self._summary.get(dtype)
        
        if summary is None:
            self.summarize(dtype, showSummary=False)
            summary = self._summary.get(dtype)
        
        return summary
    
    def get_unique_string_values_summary(self):
        """Retrieve unique string values summary
        """
        summary = self._string_data_profile
        
        if summary is None:
            self.unique_string_values_count(showSummary=False)
            summary = self._string_data_profile
        
        return summary
    
    def summarize(self, dtype='numeric', showSummary=True):
        """Create and display a profile for all columns of the specified 'dtype'
        :param dtype (str): Accepts these values: "bool", "numeric", "string"
        """
        assert dtype in self._summaryTypes, f"Invalid dtype: accepts {self._summaryTypes}"
        
        if self._summary.get(dtype) is None:
            df = {
                "bool":self._boolean_df,
                "numeric":self._numeric_df,
                "string":self._string_df
            }[dtype.lower()]

            row_template = df.summary('count').collect()[0].asDict()

            if len(row_template.keys()) < 2:
                # Manually make a row template
                row_template = self._make_row_template(df.columns)

            # Get df columns
            columns = [x for x in row_template.keys()][1:]

            new_rows = []

            if dtype == 'bool':
                # Add rows for true/false counts
                new_rows.append(self._bool_counts.get("ALL"))
                new_rows.append(self._bool_counts.get("T"))
                new_rows.append(self._bool_counts.get("F"))

            if dtype == 'string':
                # Add string column metadata
                new_row = self._create_new_row('distinct', row_template, rowType='distinct')
                new_rows.append(new_row)

            # Add row for null counts
            new_row = self._create_new_row('nulls', row_template)
            new_rows.append(new_row)

            # Add row for null %
            new_row = self._create_new_row('null%', row_template, rowType='percent')
            new_rows.append(new_row)

            if dtype == 'bool':
                summary_df = spark.createDataFrame(new_rows).select(list(row_template.keys()))
            elif dtype in ('numeric'):
                summary_df = df.summary()
                summary_df = summary_df.unionByName(spark.createDataFrame(new_rows))
            elif dtype == 'string':
                summary_df = df.summary('count')
                summary_df = summary_df.unionByName(spark.createDataFrame(new_rows))
                
            self._summary.update({dtype:summary_df})
            
        if showSummary:
            print(f"{dtype.title()} Data Summary:")
            display(self._summary.get(dtype))
        
        return None
    
    def unique_string_values_count(self, showSummary=False, returnProfile=False):
        """Identify and count unique values for each string column"""
        retValue = None
        if self._string_data_profile is None:
            strSummary = self.get_summary('string')
            str_columns = strSummary.columns[1:]
            
            # Create dataframe
            schema = self._get_summary_schema('unique_string_values')
            dtls_df = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema)
            
            # Extract string column values data
            for column in str_columns:
                col_df = self.df.select(column).withColumn('CountCol', f.lit(1))
                valueCounts = col_df.groupby(column).sum().collect()
                
                new_rows = []
                for vc in valueCounts:
                    v0 = vc[0]
                    v1 = vc[1]
                    new_rows.append((self.dbName, self.tableName, column, v0, v1))
                    
                dtls_df = dtls_df.union(spark.createDataFrame(new_rows, schema))
                
            self._string_data_profile = dtls_df
            
        if returnProfile:
            retValue = self._string_data_profile
            
        if showSummary:
            print("Unique String Values Summary:")
            display(self._string_data_profile)
        
        return retValue
    
    def update_dtypes(self, colTypes:str, returnSchema=True):
        """Cast columns as different types
        :param colTypes (dict): Accepts dict of column-type key-value pairs
        e.g.: "col1 int,col2 bool" will attempt to cast col1 and col2 as integer and boolean types, respectively.
        
        :param returnSchema (boolean): Returns new dataframe schema, if True
        """
        for column,dtype in zip(colTypes.keys(),colTypes.values()):
            sparkType = self._get_spark_data_type(dtype)
            try:
                self.df = self.df.withColumn(column, f.col(column).cast(sparkType))
            except Exception as e:
                print(f"Unable to cast column {column} of type {dtype} to {sparkType} \n{e}")
        
        # Reassemble the typed dataframes
        self._boolean_df = self._typed_df("bool")
        self._numeric_df = self._typed_df("numeric")
        self._string_df = self._typed_df("string")
        self._bool_counts = self._get_bool_counts()
        
        # Reset data summary
        self._summary = {"bool":None,"numeric":None,"string":None}
        
        if returnSchema:
            retValue = self.df.schema
        else:
            retValue = None
        
        return retValue
    
    def write_all_summary_data(self, profileDB='data_profile'):
        """Write all summary data to the database
        :param profileDB (str): Database to write profile data to
        """
        # Write typed data summaries
        for dtype in self._summaryTypes:
            self.write_type_summary(dtype, profileDB)
        
        # Write distinct value summary
        self.write_distinct_value_summary(profileDB)
        
        return None
    
    def write_distinct_value_summary(self, profileDB='data_profile'):
        """Write distinct value summary to the database
        :param profileDB (str): Database to write profile data to
        """
        summary_data = self._distinct_value_summary
        summary_type = "distinct_values"
        
        if self.row_count < 1:
            print(f"No data found in {self.tableName}. Skipping distinct value summary.")
        else:
            print(f"Attempting to write distinct value summary for {self.tableName} . . .")
            
            # Collect metadata rows
            summary_df = summary_data.withColumn("__date", f.current_date())
            summary_df = self._transform_summary(summary_df, summary_type)
            
            # Write summary to database
            self._summary_writer(summary_df, summary_type, profileDB)
        return None
    
    def write_type_summary(self, dtype:str, profileDB='data_profile'):
        """Write typed summary data to the database
        :param dtype (str): Data type summary to save
        :param profileDB (str): Database to write profile data to
        """
        assert dtype.lower() in self._summaryTypes, f"Invalid dtype: accepts {self._summaryTypes}"
        summary_data = self._summary.get(dtype)
        
        if len(summary_data.columns) < 2:
            print(f"No {dtype} columns found in {self.tableName}. Skipping . . .")
        else:
            print(f"Attempting to write {dtype} summary for {self.tableName} . . .")
            
            # Collect metadata rows
            new_rows_list = []
            for c in summary_data.columns[1:]:
                row_values = [self.dbName,self.tableName,c]
                _ = [row_values.append(x.asDict().get(c)) for x in summary_data.select(c).collect()]
                new_rows_list.append(row_values)
                
            columns = ['SourceDatabase','SourceTable','ColumnName']
            _ = [columns.append(x.asDict().get('summary').title()) for x in summary_data.select("summary").collect()]
            
            summary_df = spark.createDataFrame(new_rows_list, columns).withColumn("__date", f.current_date())
            summary_df = self._transform_summary(summary_df,dtype)
            
            # Write summary to database
            self._summary_writer(summary_df, dtype, profileDB)
        return None
    
    def write_unique_string_values_summary(self, profileDB='data_profile'):
        """Write unique string value summary to the database
        :param profileDB (str): Database to write profile data to
        """
        if self._string_data_profile is None:
            self.unique_string_values_count()
        
        summary_data = self._string_data_profile
        summary_type = "unique_string_values"
        
        if summary_data is None:
            print(f"No string data found in {self.tableName}. Skipping unique string values summary.")
        else:
            print(f"Attempting to write unique string values summary for {self.tableName} . . .")
            
            # Collect metadata rows
            summary_df = summary_data.withColumn("__date", f.current_date().astype(DateType()))
            
            # Write summary to database
            self._summary_writer(summary_df, summary_type, profileDB)
        return None
    
    #----- PRIVATE METHODS -----#
    @classmethod
    def _convert_ts_to_string(cls, df):
        """Helper function to convert timestamp columns into string columns"""
        # Cast timestamp columns as string values
        for column in [c[0] for c in df.dtypes if c[1] == 'timestamp']:
            df = df.withColumn(column, f.col(column).cast(StringType()))
            
        return df
    
    def _create_new_row(self, title:str, rowTemplate:dict, rowType:str='count'):
        """Helper function to create a new row entry
        :param title: Title for the metadata row
        :param rowTemplate: Template containing the columns to compute calculations for
        :param rowType: Accepts values in (count, percent, distinct)
        """
        columns = [x for x in rowTemplate.keys()][1:]
        new_row_dict = rowTemplate.copy()
        new_row_dict.update({'summary':title})
        if rowType == 'count':
            __ = [new_row_dict.update({c:str(self._null_counts.get(c))}) for c in columns]
        elif rowType == 'percent':
                __ = [new_row_dict.update({c:str(100*(self._null_counts.get(c)/self.row_count))}) for c in columns]
        elif rowType == 'distinct':
            distinct = lambda x: self._string_df.select(x).distinct().count()
            __ = [new_row_dict.update({c:str(distinct(c))}) for c in columns]
        
        return new_row_dict
    
    def _get_bool_counts(self):
        """Get the columnwise count for boolean values
        """
        true_counter = lambda c: self._boolean_df.where(f"{c} is true").count()
        false_counter = lambda c: self._boolean_df.where(f"{c} is false").count()
        true_counts = {'summary':"true"}
        false_counts = {'summary':"false"}
        value_counts = {'summary':"count"}
        
        for name in [x.name for x in self._boolean_df.schema]:
            t_count = true_counter(name)
            f_count = false_counter(name)
            value_count = t_count + f_count
            true_counts.update({name:str(t_count)})
            false_counts.update({name:str(f_count)})
            value_counts.update({name:str(value_count)})
            
        bool_counts = {'T':true_counts, 'F':false_counts, 'ALL':value_counts}
        return bool_counts
    
    def _get_null_counts(self):
        """Get the columnwise null count
        """
        null_counter = lambda c: self.df.where(f"{c} is null").count()
        null_counts = {}
        for name in [x.name for x in self.df.schema]:
            null_counts.update({name:null_counter(name)})
            
        return null_counts
    
    @classmethod
    def _get_spark_data_type(cls, typeName:str):
        """Identify a column's equivalent Spark data type"""
        binaryType = lambda x: any([t in x.lower() for t in ['bit']])
        boolType = lambda x: any([t in x.lower() for t in ['bool']])
        decimalType = lambda x: any([t in x.lower() for t in ['decimal','numeric']])
        doubleType = lambda x: any([t in x.lower() for t in ['double','money']])
        floatType = lambda x: any([t in x.lower() for t in ['float']])
        intType = lambda x: any([t in x.lower() for t in ['int','long']])
        strType = lambda x: any([t in x.lower() for t in ['char','date','string','time']])

        if binaryType(typeName):
            dataType = BinaryType()
        elif boolType(typeName):
            dataType = BooleanType()
        elif decimalType(typeName):
            dataType = DecimalType()
        elif doubleType(typeName):
            dataType = DoubleType()
        elif floatType(typeName):
            dataType = FloatType()
        elif intType(typeName):
            dataType = LongType()
        elif strType(typeName):
            dataType = StringType()
        else:
            dataType = StringType()

        return dataType
    
    @classmethod
    def _get_summary_schema(cls, schemaType):
        """Retrieve schema for summary data
        NOTE: timestamp type not yet implemented
        """
        schema = {'bool':
                  StructType([
                      StructField('SourceDatabase', StringType()),
                      StructField('SourceTable', StringType()),
                      StructField('ColumnName', StringType()),
                      StructField('Count', LongType()),
                      StructField('True', LongType()),
                      StructField('False', LongType()),
                      StructField('Nulls', LongType()),
                      StructField('Null%', DecimalType(5,2)),
                      StructField('__date', DateType())]),
                  'distinct_values':
                  StructType([
                      StructField('SourceDatabase', StringType()),
                      StructField('SourceTable', StringType()),
                      StructField('TotalRecords', LongType()),
                      StructField('ColumnName', StringType()),
                      StructField('TotalValues', LongType()),
                      StructField('DistinctCount', LongType()),
                      StructField('__date', DateType())]),
                  'numeric':
                  StructType([
                      StructField('SourceDatabase', StringType()),
                      StructField('SourceTable', StringType()),
                      StructField('ColumnName', StringType()),
                      StructField('Nulls', LongType()),
                      StructField('Null%', DecimalType(5,2)),
                      StructField('Count', LongType()),
                      StructField('Mean', FloatType()),
                      StructField('Stddev', FloatType()),
                      StructField('Min', FloatType()),
                      StructField('25%', FloatType()),
                      StructField('50%', FloatType()),
                      StructField('75%', FloatType()),
                      StructField('Max', FloatType()),
                      StructField('__date', DateType())]),
                  'string':
                  StructType([
                      StructField('SourceDatabase', StringType()),
                      StructField('SourceTable', StringType()),
                      StructField('ColumnName', StringType()),
                      StructField('Distinct', LongType()),
                      StructField('Nulls', LongType()),
                      StructField('Null%', DecimalType(5,2)),
                      StructField('Count', LongType()),
                      StructField('__date', DateType())]),
                  'timestamp':
                  StructType([
                      StructField('SourceDatabase', StringType()),
                      StructField('SourceTable', StringType()),
                      StructField('ColumnName', StringType()),
                      StructField('Distinct', LongType()),
                      StructField('Nulls', LongType()),
                      StructField('Null%', DecimalType(5,2)),
                      StructField('Count', LongType()),
                      StructField('__date', DateType())]),
                  'unique_string_values':
                  StructType([
                      StructField('SourceDatabase', StringType(), False),
                      StructField('SourceTable', StringType(), False),
                      StructField('ColumnName', StringType(), False),
                      StructField("ColumnValue", StringType(), True),
                      StructField("DistinctCount", IntegerType(), True)])
                 }[schemaType]
        
        return schema
    
    @classmethod
    def _make_row_template(cls, columns):
        """Helper function to manually create a row template
        :param columns (list): Column names to include in the summary"""
        template = {'summary':""}
        __ = [template.update({c:""}) for c in columns]
        
        return template
    
    @classmethod
    def _summary_writer(cls, df, dtype, profileDB):
        """Helper function to handle writing summaries to the database
        :param df (str): Dataframe carrying the profile data
        :param dtype (str): Data type summary to save
        :param profileDB (str): Database to write profile data to
        """
        if not spark.catalog.databaseExists(profileDB):
            print(f"Creating database: {profileDB}")
            spark.sql(f"CREATE DATABASE {profileDB}")
            
        spark.catalog.setCurrentDatabase(profileDB)
        table = dtype.lower() + "_summary"
        
        df.write.saveAsTable(table, mode='append', partitionBy=['__date','SourceDatabase','SourceTable'])
        _ = spark.sql(f"OPTIMIZE {profileDB}.{table}")
        print("Write successful!")
        return None
    
    @classmethod
    def _transform_summary(cls, df, schemaType):
        """Function for conforming summary data to their proper schemas"""
        schema = cls._get_summary_schema(schemaType)
        
        for field in schema:
            name = field.name
            dtype = field.dataType
            df = df.withColumn(name, f.col(name).cast(dtype))
            
        return df
    
    def _typed_df(self, dtype):
        """Subset a dataframe by column data types
        """
        assert dtype.lower() in self._summaryTypes, f"Invalid dtype: accepts {self._summaryTypes}"
        getType = lambda t: str(t).split("Type")[0].lower()
        
        dtypes = {
            'bool':('boolean'),
            'numeric':('binary', 'decimal', 'double', 'float', 'integer', 'long'),
            'string':('string', 'timestamp')
        }[dtype.lower()]
            
        # Dataframe filtered to typed columns
        fields = [f.name for f in self.df.schema if getType(f.dataType) in dtypes]
        
        return self.df.select(fields)