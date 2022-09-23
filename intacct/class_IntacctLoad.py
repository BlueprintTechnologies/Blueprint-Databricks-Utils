# Databricks notebook source
# MAGIC %md
# MAGIC ##Purpose
# MAGIC This notebook's code retrieves INTACCT files from Azure storage and loads them into a Spark dataframe
# MAGIC ####Use Cases: 
# MAGIC * A: Intacct data files that are stored as Azure blobs need to be combined and loaded into Databricks
# MAGIC * B: Transforming stored blobs into dataframes for conversion into delta tables
# MAGIC 
# MAGIC Example:  
# MAGIC source_dir = /mnt/storage/source=intacct/
# MAGIC layer = ['bronze','silver','gold'][0]
# MAGIC bronze_dw = DeltaWriter(layer)
# MAGIC 
# MAGIC IL = IntacctLoad(source_dir, 'GLENTRY')
# MAGIC if fl_df is not None:
# MAGIC     if write_mode == "create":
# MAGIC         bronze_dw.write_delta_table(fl_df, "IntacctTable")

# COMMAND ----------

# DBTITLE 1,Import library functions
import pandas as pd
import datetime as dt
import pytz

# COMMAND ----------

class CommonDateTime:
    
    @staticmethod
    def get_CurrentUSCentralDateTime(dateFormat="%Y%m%d") -> str:
        timeUTC = pytz.utc
        timezoneCentral = pytz.timezone('US/Central')
        return dt.datetime.now(timezoneCentral).strftime(dateFormat)

# COMMAND ----------

class IntacctLoad:
    def __init__(self, source_dir, load_table):
        """
        :param source_dir (str): The mounted storage directory where Intacct files are located
        :param load_table (str): Intended to be name of the intended delta table. This is used to identify a group of files by a common name attribute.
        e.g.: GLBUDGET, GLENTRY, INTACCTLOCATION
        """
        self.save_date = CommonDateTime.get_CurrentUSCentralDateTime(dateFormat="%Y%m%d")
        self.dir_folders = dbutils.fs.ls(source_dir)
        self.load_table = load_table
        self._load_table_name = self.load_table.lstrip("INTACCT")
        self.files = {"fl":[],
                       "cr":[],
                       "upd":[],
                       "del":[],
                       "all":[]
                     }
        self._get_all_files_by_type()
        
        
    @staticmethod
    def _retrieve_csv(csv_path:str):
        """A special case for reading csv files. This ensures that columns ending with escape characters do not break schema integrity
        """
        rdd = spark.sparkContext.textFile(csv_path).map(lambda x: x.replace('\\','\\\\'))
        df = spark.read.csv(rdd, header=True, inferSchema=True, sep="|")
        return df
    
    def _save_table(self):
        return f"bronze.{self.load_table.lower()}"
        
    def _get_all_files_by_type(self):
        get_fl_csv_list = lambda csv_dir: [csv.path for csv in dbutils.fs.ls(f"{csv_dir}table={self.load_table}") if "all." in csv.path]
        get_diff_csv_list = lambda csv_dir,diffType: [csv.path for csv in dbutils.fs.ls(f"{csv_dir}table={self.load_table}") if "change." in csv.path and f"_{diffType}_" in csv.path]
        get_csv_list = lambda csv_dir: [csv.path for csv in dbutils.fs.ls(f"{csv_dir}table={self.load_table}")]
        
        for folder in self.dir_folders:
            sub_dir_folders = dbutils.fs.ls(folder.path)
            
            for sub_folder in sub_dir_folders:
                if self.load_table in sub_folder.name:
                    fl_files = get_fl_csv_list(folder.path)
                    if len(fl_files) > 0:
                        _ = [self.files["fl"].append(file) for file in fl_files]
                    
                    cr_files = get_diff_csv_list(folder.path, "cr")
                    if len(cr_files) > 0:
                        _ = [self.files["cr"].append(file) for file in cr_files]
                    
                    upd_files = get_diff_csv_list(folder.path, "upd")
                    if len(upd_files) > 0:
                        _ = [self.files["upd"].append(file) for file in upd_files]
                    
                    del_files = get_diff_csv_list(folder.path, "del")
                    if len(del_files) > 0:
                        _ = [self.files["del"].append(file) for file in del_files]
                    
                    all_files = get_csv_list(folder.path)
                    if len(all_files) > 0:
                        _ = [self.files["all"].append(file) for file in all_files]
        return None
    
        
    def build_df(self, fileType, version=2, drop_cols=None):
        """
        Version 1 is much slower and more resource-intensive than version 2, but it will account for structure-breaking escape characters
        Version 3 is intended to handle batches of comma-delimited files
        :param drop_cols (list): Columns to drop from individual files prior to union
        """
        def drop_columns(in_df, cols):
            for c in cols:
                in_df = in_df.drop(c)
                
            return in_df
        
        df = None
        if version == 1:
            file_list = self.files[fileType].copy()
            file_list.reverse()

            # Create initial Dataframe
            if len(file_list) > 0:
                df = self._retrieve_csv(file_list.pop())
                if drop_cols is not None:
                    df = drop_columns(df, drop_cols)
            
            # Collect remaining files
            while len(file_list) > 0:
                load_df = self._retrieve_csv(file_list.pop())
                if drop_cols is not None:
                    load_df = drop_columns(load_df, drop_cols)
                    
                df = df.union(load_df)
        
        elif version == 2:
            df = self._build_df_v2(fileType)
            
        elif version == 3:
            df = self._build_df_v3(fileType)
        
        # Append metatdata
        df = df.withColumn("__Date", to_date(col("ddsReadTime").astype("timestamp"), "yyyyMMdd"))
        return df
    
    def _build_df_v2(self, fileType):
        df = None
        file_list = self.files[fileType]
        
        # Create Dataframe
        if len(file_list) > 0:
            df = spark.read.format("csv").option("header", True).option("inferSchema", True).option("delimiter", "|").load(file_list)
        
        return df
    
    def _build_df_v3(self, fileType):
        df = None
        file_list = self.files[fileType]
        
        # Create Dataframe
        if len(file_list) > 0:
            df = spark.read.format("csv").option("header", True).option("inferSchema", True).load(file_list)
        
        return df
