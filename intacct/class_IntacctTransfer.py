# Databricks notebook source
# MAGIC %md
# MAGIC ##Purpose
# MAGIC This notebook contains functionality for managing files drops from Intacct DDS service. THe primary use is for unpacking and uploading files to an alternative Azure storage account. Code also exists for archiving files, restoring archives, and workarounds for Intacct DDS specific issues.
# MAGIC ####Use Cases: 
# MAGIC * A: Intacct DDS files need transferring to another Azure storage account
# MAGIC ####Dependencies:
# MAGIC * class_StorageAccount
# MAGIC * class_DeltaWriter
# MAGIC ####NOTE:
# MAGIC It is recommended to follow the implemented coding pattern and store credentials in an [Azure Keyvault backed secret scope](https://learn.microsoft.com/en-us/azure/databricks/security/secrets/secret-scopes#azure-key-vault-backed-scopes)  
# MAGIC [Budgets API documentation](https://developer.intacct.com/api/general-ledger/budgets/)
# MAGIC 
# MAGIC Example:  
# MAGIC IT = IntacctTransfer()
# MAGIC IT.transfer_zip_files()
# MAGIC IT.archive_old_blobs(30)

# COMMAND ----------
# DBTITLE 1,Import Libraries
from io import BytesIO
from zipfile import ZipFile
from pyspark.sql.types import *

# COMMAND ----------

# DBTITLE 1,Import StorageAccount Class
# MAGIC %run ../class_StorageAccount

# COMMAND ----------

# DBTITLE 1,Import DeltaWriter Class
# MAGIC %run ../class_DeltaWriter

# COMMAND ----------

# DBTITLE 1,Problem File Table Schema
def intacctdds_problem_schema():
    return StructType([
        StructField("file_name", StringType(), False),
        StructField("file_location", StringType(), True),
        StructField("table_name", StringType(), True),
        StructField("date", StringType(), True)
    ])

# COMMAND ----------

# DBTITLE 1,IntacctTransfer Definition
class IntacctTransfer:
    def __init__(self):
        # Source storage account setup
        self._source = StorageAccount('intacct_source')
        
        # Archive storage account
        self._archives = StorageAccount('intacct_archive')
        
        # Destination storage account setup
        self._destination = StorageAccount('myAcct')
        
        # Retrieve list of files in source account
        self.source_files = [x.name for x in self._source.container_client.list_blobs()]
        
        # Retrieve list of recent files in source account
        self.recent_files = [x.name for x in self._source.container_client.list_blobs() 
                                if x.last_modified.astimezone() >= self._source.yesterday()]
        
    
    def _archive_dest(self, file_name):
        save_date = file_name.split("_",maxsplit=1)[0].split(".")[-1].replace("-","")[0:-2]
        return f"/date={save_date}/{file_name}"
    
    def _validate_file(self, file_name, file_location, table_name, save_date):
        """ Validate incoming DDS files by name and log potentially defective files to control table
        """
        problem_file = lambda file: 'all.' not in file and 'cr' not in file and 'upd' not in file and 'del' not in file
        
        if problem_file(file_name):
            file = {'file_name':file_name,'file_location':file_location,'table_name':table_name,'date':save_date}
            problems_df = spark.createDataFrame(file)
            table_name = 'intacctdds_problem_files'
            dwc = DeltaWriter('control')
            dwc.merge_delta_table(problems_df, table_name, schema=intacctdds_problem_schema(),inferMergeCols=True, inferUpdateCols=True)
            
            print(f"New file flagged during validation: {file_name}")
        
        return None

    def transfer_data_files(self):
        files = [x for x in self.recent_files if not x.endswith(".zip")]
        
        if len(files) < 1:
            print(f"No files identified for transfer")
        else:
            print(f"Starting file transfer for {len(files)} file(s)")
            
        for file in files:
            blob = self.retrieve_source_blob(file)
            self._source.retrieve_blob(file)
            
            blob_content = blob.download_blob().readall()
            table_name, file_name = file.split(".", maxsplit=1)
            save_date = file_name.split("_",maxsplit=1)[0].split(".")[-1].replace("-","")
            
            file_location = self._destination.file_dest(save_date, file_name, table_name, raw=True)
            dest_blob = self._destination.retrieve_blob(file_location)
            
            print(f"checking blob location: {file_location}...")
            if not dest_blob.exists():
                dest_blob.upload_blob(blob_content)
                print(f"Transfer successful for {file}")
            else:
                print(f"{file} already exists")
                
        print("Transfer operation complete")
        return None
    
    def transfer_zip_files(self):
        zip_files = [x for x in self.recent_files if x.endswith(".zip")]
        
        if len(zip_files) < 1:
            print(f"No zip files identified for transfer")
        else:
            print(f"Starting file transfer for {len(zip_files)} zip file(s)")
            
        for z_file in zip_files:
            blobs = ZipFile(
                BytesIO(
                    self._source.container_client.download_blob(z_file).content_as_bytes()
                )
            )
            
            print(f"Starting file transfer for {len(blobs.filelist)} file(s) in zip file {z_file}")
            for file in blobs.filelist:
                blob = blobs.open(file.filename)
                blob_content = blob.read()
                table_name, file_name = file.filename.split(".", maxsplit=1)
                save_date = file_name.split("_",maxsplit=1)[0].split(".")[-1].replace("-","")
                
                file_location = self._destination.file_dest(save_date, file_name, table_name, raw=True)
                dest_blob = self._destination.retrieve_blob(file_location)
                
                print(f"checking blob location: {file_location}...")
                if not dest_blob.exists():
                    dest_blob.upload_blob(blob_content)
                    print(f"Transfer successful for {file.filename}")
                else:
                    print(f"{file.filename} already exists")
                
                self._validate_file(file.filename, file_location, table_name, save_date)
                    
        print("Transfer operation complete")
        return None
    
    def archive_old_blobs(self, days_old=30, dateByLastModified=False):
        '''
        Archive blobs from storage account older than "days_old" days (default = 30 days).
        param days_old (int): Archive files that were created/modified more than this number of days ago
        param dateByLastModified (bool): Determine file age by Last Modified date if True, by file title if False
        '''
        if dateByLastModified:
            x_days_old = lambda mod_date: (self._source.utc_ts.astimezone() - mod_date).days > days_old
            files = [x.name for x in self._source.container_client.list_blobs() if x_days_old(x.last_modified)]
        else:
            def x_days_old(label):
                year, month, day = [int(x) for x in label.split(".")[3].split("_",1)[0].split("-")]
                file_age = (self._source.utc_ts.astimezone() - datetime(year, month, day).astimezone()).days
                return file_age > days_old
            files = [x.name for x in self._source.container_client.list_blobs() if x_days_old(x.name)]
        
        for file in files:
            # Identify the archive location
            archive_location = self._archive_dest(file)
            archive_blob = self._archives.retrieve_blob(archive_location)
            
            # Grab blob from live storage location
            blob = self._source.retrieve_blob(file)
                
            if not archive_blob.exists():
                blob_content = blob.download_blob().readall()
                
                print(f"Archiving blob: {file}...")
                archive_blob.upload_blob(blob_content)
                print(f"{file} successfully archived.")
            else:
                print(f"Skipping {file} since it is already archived")
            
            blob.delete_blob()
            
        print("Archive operation complete")
        return None
    
    def _restore_all_old_blobs(self):
        '''
        NOT CURRENTLY IN USE
        Restore all archived blobs to the original storage account. 
        The restored files will not be deleted from archive.
        '''
        container_client = self._archives.container_client
        archived_files = [x.name for x in container_client.list_blobs() if x.name.endswith((".zip",".csv"))]
        
        if len(archived_files) < 1:
            print("No files found in this archive folder")
        else:
            print(f"Starting restoration for {len(archived_files)} archived file(s)")
        
        for file in archived_files:
            blob = self._archives.retrieve_blob(file)
            blob_content = blob.download_blob().readall()
            
            original_file = file.split("/")[1]
            restored_blob = self._source.retrieve_blob(original_file)
            
            print(f"restoring blob: {original_file}...")
            restored_blob.upload_blob(blob_content)
            
            print(f"{original_file} successfully restored.")
        
        print("Restoration operation complete")
        return None
      
    def comma_sep_to_pipe_sep_overwrite(self, blob_location, csv_name, table_name=None, org_name=None, save_date=None, raw=False):
        """This function replaces a comma-delmited csv file with a '|' delimited csv file
        Function needs some work regarding file location
        """
        
        blob = self._destination.retrieve_blob(blob_location)
        
        if blob.exists():
            blob_data = blob.download_blob().readall()
            reformatted_data = pd.read_csv(BytesIO(blob_data)).to_csv(index=False, sep="|")
            
            self._destination.upload_blob(reformatted_data, csv_name, table_name, org_name, save_date, 
                                         raw, overwrite=True)
        else:
            print("No blob identified for replacement")
            
        return None
    
    def add_column_overwrite(self, columns, blob_location, csv_name, table_name=None, org_name=None, save_date=None, raw=False):
        """This function adds columns to a '|' delimited csv file. Intended to address a flaw in Intacct data pull
        Function needs some work regarding file location
        """
        new_columns = False
        if type(columns) is str:
            cols = [columns]
            new_columns = True
        elif type(columns) is list:
            cols = columns
            new_columns = True
        else:
            print("Invalid column type passed")
        
        if new_columns:
            blob = self._destination.retrieve_blob(blob_location)
            blob_data = None
            if blob.exists():
                blob_data = blob.download_blob().readall()
                df = pd.read_csv(BytesIO(blob_data), sep="|")
                
                for c in cols:
                    df = df.assign(new_col=None)
                    df = df.rename({"new_col":c},axis=1)
                    
                modified_data = df.to_csv(index=False, sep="|")
                
                self._destination.upload_blob(modified_data, csv_name, table_name, org_name, save_date, 
                                              raw, overwrite=True)
        else:
            print("No blob identified for replacement")
            
        return None

# COMMAND ----------
