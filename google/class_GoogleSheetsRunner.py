# Databricks notebook source
# MAGIC %md
# MAGIC ##Purpose
# MAGIC This notebook contains a class for retrieving Google Sheets data and uploading the file into Azure storage.
# MAGIC ####Use Cases: 
# MAGIC * A: Google Sheets data needs migrating to an Azure storage account
# MAGIC ####NOTE:
# MAGIC This class assumes the Sheets API credentials are stored in JSON format as a Key vault secret
# MAGIC 
# MAGIC Example:  
# MAGIC Instantiate class  
# MAGIC gsr = GoogleSheetsRunner()  
# MAGIC gsr.run_clone_job("caseA", False)

# COMMAND ----------

# DBTITLE 1,Import library functions
import gspread
import pandas as pd
import time
from pyspark.sql.functions import col, monotonically_increasing_id

from datetime import date, datetime
from oauth2client.service_account import ServiceAccountCredentials
from json import loads

# COMMAND ----------

# DBTITLE1,Import StorageAccount class
# MAGIC %run ../class_StorageAccount

# COMMAND ----------
# DBTITLE 1,GoogleSheetsRunner class definition
class GoogleSheetsRunner:
    def __init__(self, storage_name, storage_key, storage_container):
        '''
        All files desired for archiving must be manually shared with the service account email
        (user@domain.iam.gserviceaccount.com)
        '''
        self._today = dt.date.today().strftime("%Y%m%d")
        
        self.sheets_client = self._activate_sheets_client()
        self._files = self.sheets_client.list_spreadsheet_files()
        
        # Storage account setup
        self._storage_account = StorageAccount('myAcct')
    
    def _activate_sheets_client(self):
        """Authenticate for access to the Google Sheets account
        """
        scopes = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/spreadsheets",\
                       "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]
        
        # Build API credentials
        credentials = ServiceAccountCredentials.from_json_keyfile_dict(
            loads(dbutils.secrets.getBytes(scope="key_vault",key="user-credentials")),
            scopes)
        return gspread.authorize(credentials)
    
    
    @staticmethod
    def _str_to_bool(value):
        if type(value) == str:
            value = value.lower()
            ret_value = False
            
            if value in ["true", "yes", "1"]:
                ret_value = True
        else:
            ret_value = value
            
        return ret_value
    
    def clone_blob_raw(self, file_key, file_name, header_column=None, storage_layers_dict={}):
        """
        Clone the contents of a Google Sheets document and store them in an Azure storage account
        :param file_key (str): Sheets file key
        :param file_name (str): Sheets file name
        :param header_column (list): Optional. Expected header column to aid sheets data retrieval
        :param storage_layers_dict (dict): Dict of ordered key-value pairs to determine the storage address
        """
        # Retrieve and encode the data
        sheet = self.sheets_client.open_by_key(file_key).sheet1
        sheet_data = sheet.get_all_records(expected_headers=header_column)
        df = pd.DataFrame(sheet_data)
        
        self._storage_account.upload_blob(df, file_name, table_name)
        
        return None
    
    
    def run_clone_job(self, job, second_run=False, buffer_window_seconds:int=10, storage_layers_dict={}):
        """Function to dynamically run clone jobs
        :param job (str): Takes following values: caseA, caseB
        :param second_run (bool/str): Process the second half of relevant sheets. Used to avoid hitting API request limits
        :param buffer_window_seconds (int): The number of seconds to wait between API calls to avoid hitting API request limits
        :param storage_layers_dict (dict): Dict of ordered key-value pairs to determine the storage address
        """
        clone_job = {"caseA":self._clone_case_A,
                     "caseB":self._clone_case_B}[job]
        
        if job in ("caseA"):
            clone_job(second_run, buffer_window_seconds, storage_layers_dict)
        elif job in ("caseB"):
            clone_job(storage_layers_dict)
            
        return None
    
    
    def _clone_case_A(self, second_run=False, buffer_window_seconds:int=10, storage_layers_dict={}):
        table_name = "sheetsdata1"
        my_data = lambda f: "my" in f['name']
        files = [(file["id"], file["name"]) for file in self._files if my_data(file)]
        
        bool_second_run = self._str_to_bool(second_run)
        files.sort(reverse=bool_second_run)
        num_files = len(files)
        num_to_process = round(0.5+num_files/2)
        
        # Account for even/odd scenarios
        if num_files % 2 > 0 and bool_second_run:
            num_to_process-=1
        
        files = files[0:num_to_process]
        for file in files:
            file_key = file[0]
            file_name = f"{file[1].replace(' ','').lower()}.csv"
            self.clone_blob_raw(file_key, file_name, ['Cost'], table_name=table_name)
            time.sleep(buffer_window_seconds)
        
        print("Clone operation complete")
        return None
    
    
    def _clone_case_B(self, storage_layers_dict={}):
        table_name = "sheetsdata2"
        other_data = lambda f: "my" not in f['name']
        files = [(file["id"], file["name"]) for file in self._files if other_data(file)]
        
        for file in files:
            file_key = file[0]
            file_name = f"{file[1].replace(' ','').lower()}.csv"
            self.clone_blob_raw(file_key, file_name, ['id'], table_name=table_name)
        
        print("Clone operation complete")
        return None
