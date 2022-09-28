# Databricks notebook source
# MAGIC %md
# MAGIC ##Purpose
# MAGIC This code manages data transactions for the ServiceTitan v2 API. There is functionality for handling client account credentials, table schemas, field parsing and session maintenance. The primary goal of the class is to retrieve data from the API and store it in an Azure storage account.  
# MAGIC ####Use Cases: 
# MAGIC * A: Data retrieval from the ServiceTitan v2 API is required
# MAGIC ####NOTE:  
# MAGIC [Servicetitan API documentation](https://developer.servicetitan.io/apis/)  
# MAGIC The following APIs are accounted for: appointments, calls, customers, invoices, jobs, memberships, tag types  
# MAGIC API credentials need to be configured within the class for use. The code expects them to be stored as Azure Key Vault secrets.  
# MAGIC ####Example:  
# MAGIC STC = ServiceTitanConnect('myOrganization')
# MAGIC stc.retrieve_data('Calls')
# MAGIC stc.upload_data_to_raw_storage('Calls)
# MAGIC df = stc.get_data('Calls')

# COMMAND ----------

# DBTITLE 1,Import library functions
from datetime import date, datetime, timedelta
from json import loads
from pandas import DataFrame as pd_DataFrame
from pyspark.sql.functions import col, monotonically_increasing_id
from pyspark.sql.types import *
from requests import get, post
from requests.structures import CaseInsensitiveDict

# COMMAND ----------

# MAGIC %run ../class_StorageAccount

# COMMAND ----------

# DBTITLE 1,ServiceTitanOrgKey class definition
class ServiceTitanOrgKey:        
    @classmethod
    def client_credentials(cls, org, version=2):
        if version == 1:
            credentials = {"client1":cls._client1(),
                           "client2":cls._client2()}[org]
        elif version == 2:
            credentials = cls._client_credentials_v2(org)
        
        return credentials
    
    @classmethod
    def _client_credentials_v2(cls,org):
        credentials = {"tenant_id":dbutils.secrets.get(scope="key_vault",key=f"{org}-tenant-id"),
                       "application_key":dbutils.secrets.get(scope="key_vault",key="servicetitan-applicationid"),
                       "client_id":dbutils.secrets.get(scope="key_vault",key=f"{org}-client-id"),
                       "client_secret":dbutils.secrets.get(scope="key_vault",key=f"{org}-client-secret")
                      }
        return credentials
    
    @staticmethod
    def _client1():
        return {"tenant_id":dbutils.secrets.get(scope="key_vault",key="client1-tenant-id"),
                "application_key":dbutils.secrets.get(scope="key_vault",key="servicetitan-applicationid"),
                "client_id":dbutils.secrets.get(scope="key_vault",key="client1-client-id"),
                "client_secret":dbutils.secrets.get(scope="key_vault",key="client1-client-secret")
               }
    
    @staticmethod
    def _client2():
        return {"tenant_id":dbutils.secrets.get(scope="key_vault",key="client2-tenant-id"),
                "application_key":dbutils.secrets.get(scope="key_vault",key="servicetitan-applicationid"),
                "client_id":dbutils.secrets.get(scope="key_vault",key="client2-client-id"),
                "client_secret":dbutils.secrets.get(scope="key_vault",key="client2-client-secret")
               }

# COMMAND ----------

# DBTITLE 1,ServiceTitanSchema class definition
class ServiceTitanSchema:
    @classmethod
    def get_api_type(cls, api_type_ind):
        """Retrieve API type from prescribed list"""
        api_type_l = api_type_ind.lower()
        api_types = {
            'a':'appointments',
            'c':'calls',
            'cr':'customers',
            'i':'invoices',
            'ii':'invoice-items',
            'invoice items':'invoice-items',
            'j':'jobs',
            'jtt':'job-tag-types',
            'tt':'tag-types',
            'tag types':'tag-types'}

        if api_type_l in api_types.keys():
            ret_type = api_types[api_type_l]
        elif api_type_l in api_types.values():
            ret_type = api_type_l
        else:
            print(f"Invalid api_type: {api_type_ind}")
            ret_type = None
            
        return ret_type
    @staticmethod
    def find_schema(api_type_ind):
        api_type = ServiceTitanSchema.get_api_type(api_type_ind)
        
        api_schema = {
            'appointments':ServiceTitanSchema.appointment_schema,
            'calls':ServiceTitanSchema.call_schema,
            'customers':ServiceTitanSchema.customer_schema,
            'invoices':ServiceTitanSchema.invoice_schema,
            'invoice-items':ServiceTitanSchema.invoice_item_schema,
            'jobs':ServiceTitanSchema.job_schema,
            'job-tag-types':ServiceTitanSchema.job_tagtype_schema,
            'tag-types':ServiceTitanSchema.tag_type_schema}[api_type]
        
        return api_schema()
        
    @staticmethod
    def find_table_name(api_type_ind):
        api_type = ServiceTitanSchema.get_api_type(api_type_ind)
        
        table_name = {
            'appointments':"servicetitanv2_appointment",
            'calls':"servicetitanv2_call",
            'customers':"servicetitanv2_customer",
            'invoices':"servicetitanv2_invoice",
            'invoice-items':"servicetitanv2_invoice_item",
            'jobs':"servicetitanv2_job",
            'job-tag-types':"servicetitanv2_job_tagtype",
            'tag-types':"servicetitanv2_tagtype"}[api_type]
        
        return table_name
    
    @staticmethod
    def appointment_schema():
        return StructType([
            StructField("id", LongType(), False),
            StructField("jobId", LongType(), True),
            StructField("appointmentNumber", StringType(), True),
            StructField("start", StringType(), True),
            StructField("end", StringType(), True),
            StructField("arrivalWindowStart", StringType(), True),
            StructField("arrivalWindowEnd", StringType(), True),
            StructField("status", StringType(), True),
            StructField("specialInstructions", StringType(), True),
            StructField("createdOn", StringType(), True),
            StructField("modifiedOn", StringType(), True),
            StructField("tenantId", StringType(), False)])
    
    @staticmethod
    def call_schema():
        return StructType([
            StructField("leadCall", MapType(StringType(), StringType()), False),
            StructField('businessUnit', MapType(StringType(), StringType(), True), True),
            StructField('id', LongType(), False),
            StructField('jobNumber', StringType(), True),
            StructField('projectId', LongType(), False), 
            StructField('tenantId', StringType(), False),
            StructField('type', MapType(StringType(), StringType(), True), True),
            StructField('leadCall_duration', StringType(), True),
            StructField('leadCall_createdOn', StringType(), True),
            StructField('leadCall_receivedOn', StringType(), True),
            StructField('leadCall_direction', StringType(), True),
            StructField('leadCall_to', StringType(), True),
            StructField('leadCall_id', StringType(), True),
            StructField('leadCall_callType', StringType(), True),
            StructField('leadCall_recordingUrl', StringType(), True),
            StructField('leadCall_from', StringType(), True),
            StructField('leadCall_createdBy', StringType(), True),
            StructField('leadCall_voiceMailUrl', StringType(), True),
            StructField('leadCall_modifiedOn', StringType(), True),
            StructField('leadCall_active', StringType(), True),
            StructField('leadCall_customer_phoneSettings', StringType(), True), 
            StructField('leadCall_customer_address', StringType(), True),
            StructField('leadCall_customer_customFields', StringType(), True),
            StructField('leadCall_customer_active', StringType(), True), 
            StructField('leadCall_customer_doNotMail', StringType(), True),
            StructField('leadCall_customer_type', StringType(), True),
            StructField('leadCall_customer_createdOn', StringType(), True), 
            StructField('leadCall_customer_memberships', StringType(), True),
            StructField('leadCall_customer_hasActiveMembership', StringType(), True),
            StructField('leadCall_customer_modifiedOn', StringType(), True), 
            StructField('leadCall_customer_doNotService', StringType(), True),
            StructField('leadCall_customer_importId', StringType(), True),
            StructField('leadCall_customer_balance', StringType(), True), 
            StructField('leadCall_customer_createdBy', StringType(), True),
            StructField('leadCall_customer_name', StringType(), True), 
            StructField('leadCall_customer_id', StringType(), True),
            StructField('leadCall_customer_contacts', StringType(), True),
            StructField('leadCall_customer_email', StringType(), True), 
            StructField('leadCall_campaign_name', StringType(), True),
            StructField('leadCall_campaign_active', StringType(), True),
            StructField('leadCall_campaign_modifiedOn', StringType(), True),
            StructField('leadCall_campaign_id', StringType(), True),
            StructField('leadCall_campaign_category', StringType(), True),
            StructField('leadCall_reason_name', StringType(), True),
            StructField('leadCall_reason_active', StringType(), True),
            StructField('leadCall_reason_id', StringType(), True), 
            StructField('leadCall_reason_lead', StringType(), True),
            StructField('leadCall_agent_name', StringType(), True),
            StructField('leadCall_agent_externalId', StringType(), True),
            StructField('leadCall_agent_id', StringType(), True)])
    
    @staticmethod
    def customer_schema():
        return StructType([
            StructField("id", LongType(), False),
            StructField("active", BooleanType(), True),
            StructField("name", StringType(), True),
            StructField("type", StringType(), True),
            StructField("address", MapType(StringType(), StringType()), True),
            StructField("customFields", ArrayType(MapType(StringType(), StringType())), True),
            StructField("balance", StringType(), True),
            StructField("doNotMail", BooleanType(), True),
            StructField("doNotService", BooleanType(), True),
            StructField("createdOn", StringType(), True),
            StructField("createdById", LongType(), True),
            StructField("modifiedOn", StringType(), True),
            StructField("mergedToId", LongType(), True),
            StructField("tenantId", StringType(), True)])
    
    @staticmethod
    def invoice_schema():
        return StructType([
            StructField('id', LongType(), False),
            StructField('syncStatus', StringType(), True),
            StructField('summary', StringType(), True),
            StructField('referenceNumber', StringType(), True),
            StructField('invoiceDate', StringType(), True),
            StructField('dueDate', StringType(), True),
            StructField('subTotal', StringType(), True),
            StructField('salesTax', StringType(), True),
            StructField('salesTaxCode', MapType(StringType(), StringType(), True), True),
            StructField('total', StringType(), True),
            StructField('balance', StringType(), True),
            StructField('customerAddress', MapType(StringType(), StringType(), True), True),
            StructField('locationAddress', MapType(StringType(), StringType(), True), True),
            StructField('termName', StringType(), True),
            StructField('createdBy', StringType(), True),
            StructField('modifiedOn', StringType(), True),
            StructField('adjustmentToId', StringType(), True),
            StructField('projectId', StringType(), True),
            StructField('royalty', MapType(StringType(), StringType(), True), True),
            StructField('commissionEligibilityDate', StringType(), True),
            StructField('items', StringType(), True),
            StructField('customFields', ArrayType(MapType(StringType(), StringType(), True), True), True),
            StructField('tenantId', StringType(), False),
            StructField('customer_name', StringType(), True),
            StructField('customer_id', LongType(), True),
            StructField('businessUnit_name', StringType(), True),
            StructField('businessUnit_id', IntegerType(), True),
            StructField('batch_name', StringType(), True),
            StructField('batch_number', IntegerType(), True),
            StructField('batch_id', LongType(), True),
            StructField('job_id', LongType(), True),
            StructField('job_number', IntegerType(), True),
            StructField('job_type', StringType(), True),
            StructField('employeeInfo_id', LongType(), True),
            StructField('employeeInfo_name', StringType(), True),
            StructField('employeeInfo_modifiedOn', StringType(), True)])
    
    @staticmethod
    def invoice_item_schema():
        return StructType([
            StructField("cost", StringType(), True),
            StructField("quantity", StringType(), True),
            StructField("serviceDate", StringType(), True),
            StructField("taxable", StringType(), True),
            StructField("displayName", StringType(), True),
            StructField("description", StringType(), True),
            StructField("soldHours", StringType(), True),
            StructField("inventory", StringType(), True),
            StructField("type", StringType(), True),
            StructField("membershipTypeId", StringType(), True),
            StructField("skuName", StringType(), True),
            StructField("modifiedOn", StringType(), True),
            StructField("total", StringType(), True),
            StructField("price", StringType(), True),
            StructField("inventoryLocation", StringType(), True),
            StructField("id", StringType(), False),
            StructField("skuId", StringType(), True),
            StructField("totalCost", StringType(), True),
            StructField("order", StringType(), True),
            StructField("itemGroup_name", StringType(), True),
            StructField("itemGroup_rootId", StringType(), True),
            StructField("assetAccount_detailType", StringType(), True),
            StructField("assetAccount_name", StringType(), True),
            StructField("assetAccount_number", StringType(), True),
            StructField("assetAccount_type", StringType(), True),
            StructField("costOfSaleAccount_detailType", StringType(), True),
            StructField("costOfSaleAccount_name", StringType(), True),
            StructField("costOfSaleAccount_number", StringType(), True),
            StructField("costOfSaleAccount_type", StringType(), True),
            StructField("generalLedgerAccount_detailType", StringType(), True),
            StructField("generalLedgerAccount_name", StringType(), True),
            StructField("generalLedgerAccount_number", StringType(), True),
            StructField("generalLedgerAccount_type", StringType(), True),
            StructField("invoiceId", LongType(), False),
            StructField("tenantId", StringType(), False)])
    
    @staticmethod
    def job_schema():
        return StructType([
            StructField("id", LongType(), False),
            StructField("jobNumber", StringType(), False),
            StructField("customerId", LongType(), False),
            StructField("locationId", LongType(), False),
            StructField("jobStatus", StringType(), False),
            StructField("completedOn", StringType(), True),
            StructField("businessUnitId", LongType(), False),
            StructField("jobTypeId", LongType(), False),
            StructField("priority", StringType(), False),
            StructField("campaignId", LongType(), False),
            StructField("summary", StringType(), True),
            StructField("customFields", ArrayType(MapType(StringType(), StringType())), False),
            StructField("appointmentCount", ShortType(), False),
            StructField("firstAppointmentId", LongType(), False),
            StructField("lastAppointmentId", LongType(), False),
            StructField("recallForId", LongType(), True),
            StructField("warrantyId", LongType(), True),
            StructField("jobGeneratedLeadSource", MapType(StringType(), StringType()), True),
            StructField("noCharge", BooleanType(), False),
            StructField("notificationsEnabled", BooleanType(), False),
            StructField("createdOn", StringType(), False),
            StructField("createdById", LongType(), False),
            StructField("modifiedOn", StringType(), False),
            StructField("tagTypeIds", ArrayType(LongType()), False),
            StructField("leadCallId", LongType(), True),
            StructField("bookingId", LongType(), True),
            StructField("soldById", LongType(), True),
            StructField("externalData", ArrayType(MapType(StringType(), StringType())), True),
            StructField("tenantId", StringType(), False),
            StructField("generatedFromId", StringType(), True),
            StructField("generatedById", StringType(), True)])
        
    @staticmethod
    def job_tagtype_schema():
        return StructType([
            StructField("jobId", LongType(), False),
            StructField("tenantId", StringType(), False),
            StructField("tagTypeId", LongType(), False)])
        
    @staticmethod
    def tag_type_schema():
        return StructType([
            StructField("id", LongType(), False),
            StructField("name", StringType(), True),
            StructField("active", BooleanType(), True),
            StructField("tenantId", StringType(), False)])
    
    
    @staticmethod
    def find_internal_schema(schema_ind:str):
        internal_schema = {
            'invoice':ServiceTitanSchema._invoice_interim_schema,
            'job':ServiceTitanSchema._job_interim_schema,
            'leadCall':ServiceTitanSchema._leadCall_interim_schema
            }[schema_ind]
        
        return internal_schema()
    
    @staticmethod
    def _invoice_interim_schema():
        return StructType([
            StructField("id", LongType(), False),
            StructField("syncStatus", StringType(), True),
            StructField("summary", StringType(), True),
            StructField("referenceNumber", StringType(), True),
            StructField("invoiceDate", StringType(), True),
            StructField("dueDate", StringType(), True),
            StructField("subTotal", StringType(), True),
            StructField("salesTax", StringType(), True),
            StructField("salesTaxCode", MapType(StringType(), StringType()), True),
            StructField("total", StringType(), True),
            StructField("balance", StringType(), True),
            StructField("customer", MapType(StringType(), StringType()), True),
            StructField("customerAddress", MapType(StringType(), StringType()), True),
            StructField("locationAddress", MapType(StringType(), StringType()), True),
            StructField("businessUnit", MapType(StringType(), StringType()), True),
            StructField("termName", StringType(), True),
            StructField("createdBy", StringType(), True),
            StructField("batch", MapType(StringType(), StringType()), True),
            StructField("modifiedOn", StringType(), True),
            StructField("adjustmentToId", StringType(), True),
            StructField("job", MapType(StringType(), StringType()), True),
            StructField("projectId", StringType(), True),
            StructField("royalty", MapType(StringType(), StringType()), True),
            StructField("employeeInfo", MapType(StringType(), StringType()), True),
            StructField("commissionEligibilityDate", StringType(), True),
            StructField("items", StringType(), True),
            StructField("customFields", ArrayType(MapType(StringType(), StringType())), True),
            StructField("tenantId", StringType(), False)])
    
    @staticmethod
    def _job_interim_schema():
        return StructType([
            StructField("id", LongType(), False),
            StructField("jobNumber", StringType(), False),
            StructField("customerId", LongType(), False),
            StructField("locationId", LongType(), False),
            StructField("jobStatus", StringType(), False),
            StructField("completedOn", StringType(), True),
            StructField("businessUnitId", LongType(), False),
            StructField("jobTypeId", LongType(), False),
            StructField("priority", StringType(), False),
            StructField("campaignId", LongType(), False),
            StructField("summary", StringType(), True),
            StructField("customFields", ArrayType(MapType(StringType(), StringType())), False),
            StructField("appointmentCount", ShortType(), False),
            StructField("firstAppointmentId", LongType(), False),
            StructField("lastAppointmentId", LongType(), False),
            StructField("recallForId", LongType(), True),
            StructField("warrantyId", LongType(), True),
            StructField("jobGeneratedLeadSource", MapType(StringType(), StringType()), True),
            StructField("noCharge", BooleanType(), False),
            StructField("notificationsEnabled", BooleanType(), False),
            StructField("createdOn", StringType(), False),
            StructField("createdById", LongType(), False),
            StructField("modifiedOn", StringType(), False),
            StructField("tagTypeIds", ArrayType(LongType()), False),
            StructField("leadCallId", LongType(), True),
            StructField("bookingId", LongType(), True),
            StructField("soldById", LongType(), True),
            StructField("externalData", ArrayType(MapType(StringType(), StringType())), True),
            StructField("tenantId", StringType(), False)])
    
    @staticmethod
    def _leadCall_interim_schema():
        return StructType([
            StructField("leadCall", MapType(StringType(), StringType()), False),
            StructField('link_id', LongType(), False),
            StructField('leadCall_agent', MapType(StringType(), StringType(), True), True),
            StructField('leadCall_campaign', MapType(StringType(), StringType(), True), True),
            StructField('leadCall_customer', MapType(StringType(), StringType(), True), True),
            StructField('leadCall_reason', MapType(StringType(), StringType(), True), True)])

# COMMAND ----------

# DBTITLE 1,InvoiceItemHandler class definition
class InvoiceItemHandler:
    def __init__(self, invoiceItems_df=None):
        # List of collected invoice items
        self.items_list = []
        
        self._subColumns = {
            "itemGroup":("name","rootId"),
            "assetAccount":("detailType","name","number","type"),
            "costOfSaleAccount":("detailType","name","number","type"),
            "generalLedgerAccount":("detailType","name","number","type")
        }
        
        self._itemKeys = ('itemGroup','assetAccount','cost','quantity','serviceDate','taxable','displayName','description'\
                         ,'soldHours','inventory','type','membershipTypeId','costOfSaleAccount','skuName','modifiedOn',\
                         'total','price','inventoryLocation','id','skuId','totalCost','order','generalLedgerAccount')
        
        # Get invoice item list from dataframe
        self.build_invoice_items(invoiceItems_df)
    
    
    def build_invoice_items(self, item_data):
        """Get invoice items from dataframe. Results are stored in the 'items_list' variable of the instance"""
        
        def build_item_entries(table_row):
            entries_list = []
            
            item_dict = table_row.asDict()
            invoiceId = item_dict.get("invoiceId")
            tenantId = item_dict.get("tenantId")
            items_str = item_dict.get("items")
            items_list = self._items_str_to_list(items_str)
            for item in items_list:
                item["invoiceId"] = invoiceId
                item["tenantId"] = tenantId
                entries_list.append(item)

            return entries_list
        
        if item_data is not None:
            __ = [[self.items_list.append(r) for r in build_item_entries(row)] for row in item_data]
        
        return None
    
    
    def _items_str_to_list(self,item_str:str):
        """Transforms invoice items string into a list of dict objects"""
        def dict_update(data_dict):
            """Update dict with sub-column values"""
            for sourceCol in self._subColumns.keys():
                if data_dict[sourceCol] is None:
                    for subCol in self._subColumns[sourceCol]:
                        colName = f"{sourceCol}_{subCol}"
                        data_dict[colName] = None
                else:
                    for subCol in self._subColumns[sourceCol]:
                        value = data_dict[sourceCol].lstrip("{").rstrip("}").split(f"{subCol}=",1)[1].split(",",1)[0]
                        if value == "null":
                            value = None
                        colName = f"{sourceCol}_{subCol}"
                        data_dict[colName] = value
                    
                _ = data_dict.pop(sourceCol)
            return data_dict
                
        
        # Gather list of items for the entry
        itemStr_list = item_str.lstrip("[{").rstrip("}]").split("}, {")
        
        itemList = []
        for item in itemStr_list:
            item_dict = {}
            for keyIdx1 in range(len(self._itemKeys)):
                keyIdx2 = keyIdx1
                if keyIdx1 < len(self._itemKeys)-1:
                    keyIdx2+=1
                
                key1 = self._itemKeys[keyIdx1]
                key2 = self._itemKeys[keyIdx2]
                    
                value = item.split(f"{key1}=")[-1].split(f", {key2}")[0]
                if value == "null":
                    value = None
                    
                item_dict[key1] = value
                
            newItem_dict = dict_update(item_dict)
            itemList.append(newItem_dict)

        return itemList

# COMMAND ----------

# DBTITLE 1,CallsLeadCallParser class definition
class CallsLeadCallParser:
    @staticmethod
    def parse_leadCall(df):
        """
        Parse the leadCall column and expand the row entries into separate columns
        """
        link_id = 'link_id'
        leadCall = 'leadCall'
        leadCall_subcol = lambda subcol: f'leadCall_{subcol}'
        leadCall_deep_subcolumns = ('customer','campaign','reason','agent')
        leadCall_shallow_subcolumns = ['duration','createdOn','receivedOn','direction','to','id',
                                       'callType','recordingUrl','from','createdBy','voiceMailUrl',
                                       'modifiedOn','active']
        
        if type(df) == pd.DataFrame:
            id_df = spark.createDataFrame(df).\
            withColumn('businessUnit', col('businessUnit').astype(MapType(StringType(), StringType(), True))).\
            withColumn('type', col('type').astype(MapType(StringType(), StringType(), True))).\
            withColumn(link_id, monotonically_increasing_id())
        else:
            id_df = df.withColumn(link_id, monotonically_increasing_id())
        
        # Extract shallow subcolumns
        shallow_sc_df = id_df.select(leadCall,link_id)
        
        for subcolumn in leadCall_shallow_subcolumns:
            shallow_sc_df = shallow_sc_df.withColumn(f'{leadCall_subcol(subcolumn)}', col(leadCall).getItem(subcolumn))
        
        shallow_sc_df = shallow_sc_df.drop(leadCall)
        
        # Extract deep subcolumns from leadCall
        deep_sc_df = id_df.select(leadCall,link_id)
        
        for subcolumn in leadCall_deep_subcolumns:
            deep_sc_df = deep_sc_df.withColumn(f'{leadCall_subcol(subcolumn)}', col(leadCall).getItem(subcolumn))
        
        data = deep_sc_df.collect()
        
        # list to hold processed data rows
        data_rows = []
        ###
        # Append subcolumns to dataframe
        for d in data:
            row_dict = d.asDict()
            for c in leadCall_deep_subcolumns:
                subcol = leadCall_subcol(c)
                #print(row_dict)
                if row_dict[subcol] is not None:
                    subcol_data = loads(CallsLeadCallParser._string_to_json(row_dict[subcol],subcol))
                    row_dict.update({subcol:subcol_data})
                    
            data_rows.append(row_dict)
            
        nested_sc_df = spark.createDataFrame(data_rows, schema=ServiceTitanSchema.find_internal_schema(leadCall))
            
        # Extract nested columns from leadCall subcolumns
        for subcolumn in leadCall_deep_subcolumns:
            column = leadCall_subcol(subcolumn)
            column_keys = CallsLeadCallParser._get_keys(column)
            
            for k in column_keys:
                nested_column = f'{column}_{k}'
                nested_sc_df = nested_sc_df.withColumn(nested_column, col(column).getItem(nested_column))
                
            nested_sc_df = nested_sc_df.drop(column)
            
        nested_sc_df = nested_sc_df.drop(leadCall)
        
        ret_df = id_df.join(shallow_sc_df,on=link_id,how='left').\
        join(nested_sc_df,on=link_id,how='left').\
        drop(link_id)
        
        return ret_df
        
        
    @staticmethod
    def _get_keys(parent_column):
        # Set of keys whose order reflects the format returned by the API. Critical for data integrity
        keys = {
            "leadCall_customer": ['phoneSettings','address','customFields','active','doNotMail','type','createdOn',
                                  'memberships','hasActiveMembership','modifiedOn','doNotService','importId','balance',
                                  'createdBy','name','id','contacts','email'],
            "leadCall_campaign": ['name','active','modifiedOn','id','category'],
            "leadCall_reason": ['name','active','id','lead'],
            "leadCall_agent": ['name','externalId','id']
        }[parent_column]
        
        return keys
    
    @staticmethod
    def _string_to_json(strVal:str, column):
        
        def split_join(inputStr:str, parentCol:str, key1:str, key2:str):
            frontEnd, value = inputStr.split(key1,1)
            value = value.split('=',1)[-1]
            
            if key2 == "END":
                frontEnd = f'{frontEnd}"{parentCol}_{key1}"="{value}"'
                frontEnd = frontEnd.replace('}"','"}')
                backEnd = ""
            else:
                value, backEnd = value.split(f', {key2}',1)
                frontEnd = f'{frontEnd}"{parentCol}_{key1}"="{value}"'
                backEnd = f', {key2}{backEnd}'
                
            return frontEnd, backEnd

        # Get subcolumn keys
        keys = CallsLeadCallParser._get_keys(column)
        keys.append("END")
        
        if column == 'leadCall_customer' and 'importId' not in strVal:
            keys.remove("importId")
            
        # Initialize process
        strVal = strVal.replace('=",','=,').replace('"',"''") # pre-cleaning step for anomalous cases
        retString = ""
        k1, k2 = keys[0:2]
        front, back = split_join(strVal, column, k1, k2)
        retString +=front
        
        # Run conversion process
        for k1, k2 in zip(keys[1:-1],keys[2:]):
            front, back = split_join(back, column, k1, k2)
            retString +=front
            
        # Vital cleaning/catch-case steps
        retString = retString.replace('"="','":"')
        retString = retString.replace('\t','').replace('\ ',' ')
        
        return retString

# COMMAND ----------

# DBTITLE 1,ServiceTitanConnect class definition
class ServiceTitanConnect:
    def __init__(self, organization):
        
        self.organization = organization.lower().replace(" ","")
        
        self._application_key = None
        self._tenant = None
        self._access_token = None
        self._expiry = None
        self._today = date.today().strftime("%Y%m%d")
        
        # Results retrieved from API request
        self._data = {"appointments": pd_DataFrame(),
                      "calls": pd_DataFrame(),
                      "customers": pd_DataFrame(),
                      "invoices": pd_DataFrame(),
                      "jobs": pd_DataFrame(),
                      "memberships": pd_DataFrame(),
                      "tag-types": pd_DataFrame()
                     }
        
        # Most recent page of data pulled
        self._recent_page = {"appointments": 0,
                             "calls": 0,
                             "customers": 0,
                             "invoices": 0,
                             "jobs": 0,
                             "memberships": 0,
                             "tag-types": 0
                            }
        
        # flag indicating whether more records exist to pull
        self._hasMore = {"appointments": True,
                         "calls": True,
                         "customers": True,
                         "invoices": True,
                         "jobs": True,
                         "memberships": True,
                         "tag-types": True
                        }
        
        self._PAGE_SIZE = 1000 # default to 1000 records per request
        
        # Initialize instance variables
        self._initialize()
        
        # Storage account setup
        self._storage_account = StorageAccount('azdlap')
        
    # Initialize instance variables
    def _initialize(self):
        keys = ServiceTitanOrgKey.client_credentials(self.organization)
        
        self._application_key = keys.pop("application_key")
        self._tenant = keys.pop("tenant_id")
        client_id = keys.pop("client_id")
        client_secret = keys.pop("client_secret")
        self._access_token = self._authenticate(client_id, client_secret)
        return None
    
    def _expiry_check(self):
        final_minute = lambda: (self._expiry - datetime.now()).seconds < 61
        status = False
        
        if final_minute():
            status = True
            print("Token is about to expire.")
            
        return status
    
    @staticmethod
    def get_job_tagtypes():
        '''
        Get table of jobs and exploded tag types from bronze database
        '''
        return spark.sql("SELECT id as jobId, tenantId, explode(tagTypeIds) as tagTypeId FROM bronze.servicetitanv2_job")
    
    @staticmethod
    def get_invoice_items():
        '''
        To Do: rename columns, add to scheduled process
        '''
        item_data = spark.sql("SELECT id as invoiceId, tenantId, items FROM bronze.servicetitanv2_invoice where items is not null").collect()
        
        ii_handler = InvoiceItemHandler(item_data)
        
        return spark.createDataFrame(ii_handler.items_list, schema=ServiceTitanSchema.find_schema("ii"))
    
    @staticmethod
    def _modified_after():
        today = datetime.today()
        hr = datetime.today().hour
        mnt = datetime.today().minute
        sec = datetime.today().second
        
        req_date = today-timedelta(days=1,hours=hr,minutes=mnt,seconds=sec)-timedelta(0,1)
        return req_date.strftime("%m-%d-%Y %H:%M:%S")
        
    @staticmethod
    def _modified_on_or_after():
        req_date = date.today()-timedelta(1)
        return req_date.strftime("%m-%d-%Y %H:%M:%S")
    
    @staticmethod
    def _api_type(api_type):
        """Retrieve API type from prescribed list"""
        success = None
        api_type_l = api_type.lower()
        api_types = {
            'a':'appointments',
            'c':'calls',
            'cm':'memberships',
            'cr':'customers',
            'i':'invoices',
            'j':'jobs',
            'm':'memberships',
            'tt':'tag-types',
            'tag types':'tag-types'}
        
        if api_type_l in api_types.keys():
            ret_type = api_types[api_type_l]
            success = True
        elif api_type_l in api_types.values():
            ret_type = api_type_l
            success = True
        else:
            print(f"Invalid api_type: {api_type}")
            success = False
            ret_type = None
            
        return {'success': success, 'api_type': ret_type}
    
    def _POST(self, url, data):
        headers = CaseInsensitiveDict()
        headers["Content-Type"] = "application/x-www-form-urlencoded"
        
        post_resp = post(url, headers=headers, data=data)
        
        return post_resp
    
    
    def _GET(self, url, params={}):
        headers = CaseInsensitiveDict()
        headers["Authorization"] = self._access_token
        headers["ST-App-Key"] = self._application_key
        
        get_resp = get(url, headers=headers)
        
        return get_resp
    
    
    def _authenticate(self, c_id, c_secret):
        token = ""
        get_token = lambda s: str(s).replace('"','').split(':')[1].split(',')[0]
        get_expiry = lambda s: datetime.now() + timedelta(seconds=-60+int(str(s).replace('"','').split(':')[2].split(',')[0]))
        
        
        auth_url = "https://auth.servicetitan.io/connect/token"
        data = {'grant_type':'client_credentials', 'client_id':c_id,'client_secret':c_secret}
        response = self._POST(auth_url, data)
        
        if response.status_code == 200:
            token = get_token(response.content)
            self._expiry = get_expiry(response.content)
            print("New authentication token acquired")
        else:
            print(f'status: {response.status_code} \n{response.content}')
        
        self.response = response
        return token
    
    
    def get_access_token(self):
        return self._access_token
    
    
    def _get_req_params(self, param_dict, api_type):
        '''
        COMMON PARAMETERS
        page (int32): The logical number of page to return, starting from 1

        pageSize (int32): How many records to return (50 by default)
        
        ADDITIONAL PARAMETERS
        activeOnly/active (boolean): Filter for non-deprecated record types.
        
        includeTotal (boolean): Whether total count should be returned
        
        modifiedAfter (string): String in date-time format (yyyy-mm-dd or yyyy-mm-dd hh:mm:ss).
        
        modifiedOnOrAfter (string): String in date-time format (yyyy-mm-dd or yyyy-mm-dd hh:mm:ss) 
                                    Return items modified on or after certain date/time (in UTC)
        
        '''
        param_str = ""
        request_params = lambda d: "?"+"&".join([f"{x[0]}={x[1]}" for x in zip(d.keys(),d.values())])
        
        self._recent_page[api_type]+=1
        param_dict["page"] = self._recent_page[api_type]
        param_dict["pageSize"] = self._PAGE_SIZE
        
        if api_type == "appointments":
            param_dict["modifiedOnOrAfter"] = self._modified_on_or_after()
            param_dict["includeTotal"] = True
        elif api_type == "calls":
            param_dict["modifiedAfter"] = self._modified_after()
            param_dict["activeOnly"] = True
        elif api_type in ("customers", "tag-types"):
            param_dict["includeTotal"] = True
            param_dict["active"] = True
        elif api_type in ("invoices", "jobs"):
            param_dict["modifiedOnOrAfter"] = self._modified_on_or_after()
            param_dict["includeTotal"] = True
        elif api_type == "memberships":
            param_dict["modifiedOnOrAfter"] = self._modified_on_or_after()
            param_dict["includeTotal"] = True
            param_dict["active"] = True
        
        param_str = request_params(param_dict)
        
        return param_str
    
    
    def _build_api_url(self, api_type, params):
        base_url = "https://api.servicetitan.io/"
        req_params = self._get_req_params(params, api_type)

        api_name = {"appointments":"jpm",
                    "calls":"telecom",
                    "customers":"crm",
                    "invoices":"accounting",
                    "jobs":"jpm",
                    "memberships":"memberships",
                    "tag-types":"settings"}[api_type]
        url = f"{base_url}{api_name}/v2/tenant/{self._tenant}/{api_type}/{req_params}"
        
        return url
    
    def _fetch_data(self, api_type, params):
        '''Support function for aggregating collected data'''
        
        def expiry_check():
            final_minute = lambda: (self._expiry - datetime.now()).seconds < 61
            status = False
            
            if final_minute():
                status = True
            return status
        
        def stall(sec):
            stall_time = datetime.now() + timedelta(seconds=sec)
            time_left = True
            
            while time_left:
                if stall_time < datetime.now():
                    time_left = False
                
            return None
        
        STATUS_OK = True
        while STATUS_OK & self._hasMore[api_type]:
            api_url = self._build_api_url(api_type, params)
            response = self._GET(api_url)
            self.response = response
            
            if response.status_code != 200:
                print(f'status: {response.status_code} \n{response.content}')
                STATUS_OK = False
                
            if STATUS_OK:
                data = loads(response.content)
                self._hasMore[api_type] = data["hasMore"]
                records_df = pd_DataFrame(data['data'])
                
                self._data[api_type] = self._data[api_type].append(records_df)
                
                if expiry_check():
                    # reset connection timer
                    checkpoint_count = data.get("page")*data.get("pageSize")
                    print(f'Processed {checkpoint_count} of {data.get("totalCount")} results')
                    stall(15)
                    self._initialize()
                
        
        self._data[api_type] = self._data[api_type].assign(tenantId=self._tenant)
        self._data[api_type] = self._data[api_type].reset_index(drop=True)
        print(f'Processed {data.get("totalCount")} results')
        return None
    
    
    def retrieve_data(self, api_type_ind, params={}):
        '''
        param api_type_ind (str): API type indicator. 
            Takes values "a" (appointments), "c" (calls), "cr" (customers), "i" (invoices), "j" (jobs), or "tt" (tag types)
        param params (dict): key-value pairs of api request parameters
        View relevant API at https://developer.servicetitan.io/apis for params documentation
        '''
        API = self._api_type(api_type_ind)
        
        # Perform pre-query validation
        precheck_ok = True
        if API["success"]:
            api_type = API["api_type"]
        else:
            precheck_ok = False
            
        if not self._hasMore[api_type]:
            print(f"Query complete!")
            precheck_ok = False
            
        if precheck_ok:
            self._fetch_data(api_type, params)
        
        return None
    
    
    def get_data(self, api_type_ind):
        """
        param api_type_ind (str): API type indicator. 
            Takes values "a" (appointments), "c" (calls), "cr" (customers), "i" (invoices), "j" (jobs), 
            "m" (memberships), or "tt" (tag types)
        """
        # Helper function for parsing out invoice columns
        def parse_invoice_columns(df):
            map_columns = ['customer','businessUnit','batch','job','employeeInfo']
            subcolumns = {
                'customer':('id','name'),
                'businessUnit':('id','name'),
                'batch':('id','name','number'),
                'job':('id','number','type'),
                'employeeInfo':('id','name','modifiedOn')
            }
            
            int_type_cols = ('businessUnit_id','batch_number','job_number')
            long_type_cols = ('customer_id','batch_id','job_id','employeeInfo_id')
            
            data_df = spark.createDataFrame(df, schema=ServiceTitanSchema.find_internal_schema('invoice'))
            
            for mc in map_columns:
                for sc in subcolumns[mc]:
                    new_column = f'{mc}_{sc}'
                    if new_column in int_type_cols:
                        data_df = data_df.withColumn(new_column, col(mc).getItem(sc).cast(IntegerType()))
                    elif new_column in long_type_cols:
                        data_df = data_df.withColumn(new_column, col(mc).getItem(sc).cast(LongType()))
                    else:
                        data_df = data_df.withColumn(new_column, col(mc).getItem(sc))
                data_df = data_df.drop(mc)
            
            return data_df
        
        # Helper function for extracting lead source data
        def extract_leadSource_columns(df):
            extractValues = lambda col, key: [str(val[key]) for val in col]
            leadSource_subcolumns = {'jobId':'generatedFromId','employeeId':'generatedById'}
            
            if 'generatedById' not in df.columns:
                data_df = spark.createDataFrame(df, schema=ServiceTitanSchema.find_internal_schema('job'))
                
                for key,value in zip(leadSource_subcolumns.keys(), leadSource_subcolumns.values()):
                    data_df = data_df.withColumn(value, col('jobGeneratedLeadSource').getItem(key))
                
                ret_df = data_df
            else:
                ret_df = spark.createDataFrame(df, schema=ServiceTitanSchema.job_schema())
            
            return ret_df
        
        api_type = self._api_type(api_type_ind)["api_type"]
        data = self._data[api_type]
        
        if api_type == 'jobs':
            data = data.fillna({'recallForId':0,'warrantyId':0,'leadCallId':0,'bookingId':0,'soldById':0})
            data = data.astype({'recallForId':int,'warrantyId':int,'leadCallId':int,'bookingId':int,'soldById':int})
            spark_df = extract_leadSource_columns(data)
        elif api_type == 'calls':
            spark_df = CallsLeadCallParser.parse_leadCall(data)
        elif api_type == 'invoices':
            spark_df = parse_invoice_columns(data)
        else:
            spark_df = spark.createDataFrame(data, schema=ServiceTitanSchema.find_schema(api_type))
        
        return spark_df
    
    
    def upload_to_raw_storage(self, api_type_ind):
        '''
        Upload data to storage account (raw layer)
        param api_type_ind (str): API type indicator. 
            Takes values "a" (appointments), "c" (calls), "cr" (customers), "i" (invoices), "j" (jobs), 
            "m" (memberships), or "tt" (tag types)
        '''
        api_type = self._api_type(api_type_ind)["api_type"]
        get_file_name = lambda x: f"{x}_{api_type}_{self._today}.csv"
        if self._data[api_type].empty:
            print("Data not found! Upload operation aborted")
        else:
            file_name = get_file_name(self.organization)
            # Retrieve and encode the data
            print(f"Attempting file: {file_name}")
            df = self._data[api_type].copy()

            self._storage_account.upload_blob(df, file_name, overwrite=True, organization=self.organization, table=api_type)
        
        return None
