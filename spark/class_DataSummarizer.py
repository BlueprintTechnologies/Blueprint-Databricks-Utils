# Databricks notebook source
# DBTITLE 1,Package Imports
from pyspark.sql.types import BooleanType, FloatType, LongType, StringType

# COMMAND ----------

# DBTITLE 1,Class Definition
class SparkDataSummary:
    def __init__(self, df):
        self.df = df
        
        # Get typed dataframes
        self._boolean_df = self._typed_df("bool")
        self._numeric_df = self._typed_df("numeric")
        self._string_df = self._typed_df("string")
        
        # Count column values
        self.row_count = df.count()
        self._null_counts = self._get_null_counts()
        self._bool_counts = self._get_bool_counts()
        
    def _typed_df(self, dtype):
        """Subset a dataframe by column data types
        """
        if dtype == "bool":
            fields = [f.name for f in self.df.schema if f.dataType == BooleanType()]
        elif dtype == "numeric":
            fields = [f.name for f in self.df.schema if f.dataType in (FloatType(), LongType())]
        elif dtype == "string":
            fields = [f.name for f in self.df.schema if f.dataType == StringType()]
            
        return self.df.select(fields)
        
    def _get_null_counts(self):
        """Get the columnwise null count
        """
        null_counter = lambda c: self.df.where(f"{c} is null").count()
        null_counts = {}
        for name in [x.name for x in self.df.schema]:
            null_counts.update({name:null_counter(name)})
            
        return null_counts
    
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
        
    @classmethod
    def _make_row_template(cls, columns):
        """Helper function to manually create a row template
        :param columns (list): Column names to include in the summary"""
        template = {'summary':""}
        __ = [template.update({c:""}) for c in columns]
        
        return template
    
    def _create_new_row(self, title:str, rowTemplate:dict, rowType='count':str):
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
    
    def summary(self, dtype='numeric'):
        """Display a profile for all columns of the specified 'dtype'
        :param dtype (str): Accepts these values: "bool", "numeric", "string"
        """
        df = {
            "bool":self._boolean_df,
            "numeric":self._numeric_df,
            "string":self._string_df
        }[dtype]
        
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
        
        display(summary_df)
        
        return None
    
    
    def distinct_value_summary(self):
        """Display a distinct value counts summary for numeric and string columns
        """
        distinct = lambda x: df.select(x).distinct().count()
        
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
        
        summary_df = summary_df.unionByName(spark.createDataFrame(new_rows))
        
        display(summary_df)
        
        return None
