# Databricks notebook source
# MAGIC %md
# MAGIC ##Purpose
# MAGIC This notebook contains functionality for extracting GLBUDGET data via the Intacct Budget API and uploading the data into an Azure storage account.
# MAGIC ####Use Cases: 
# MAGIC * A: Any scenario where Intacct GLBUDGET data is required from the API
# MAGIC * B: Template for accessing Google Analytics data and combining into a dataframe
# MAGIC ####NOTE:
# MAGIC It is recommended to follow the implemented coding pattern and store credentials in an [Azure Keyvault backed secret scope](https://learn.microsoft.com/en-us/azure/databricks/security/secrets/secret-scopes#azure-key-vault-backed-scopes)
# MAGIC 
# MAGIC Example:  
# MAGIC IB = IntacctBudget()
# MAGIC budgetid = IB.budgets_list_df.BUDGETID[0]
# MAGIC page_size = 2000
# MAGIC page = 0
# MAGIC 
# MAGIC budget_details_df = IB.get_budget_details(budgetid, pagesize=page_size)
# MAGIC records_in_response = len(budget_details_df)
# MAGIC 
# MAGIC while records_in_response > page_size:
# MAGIC     page+=1
# MAGIC     record_offset = page*page_size
# MAGIC     more_details_df = IB.get_budget_details(budgetid, offset=record_offset, pagesize=page_size)
# MAGIC     records_in_response = len(more_details_df)
# MAGIC     budget_details_df = budget_details_df.append(more_details_df)
# MAGIC 
# MAGIC budget_details_df = budget_details_df.reset_index(drop=True)
# MAGIC IB.upload_data_to_raw_storage(budget_details_df)

# COMMAND ----------
# DBTITLE 1,Import Namespaces
import pandas as pd
import requests
import xmltodict

from datetime import datetime
from io import BytesIO
from requests.structures import CaseInsensitiveDict
from uuid import uuid4

# COMMAND ----------

# DBTITLE 1,Import StorageAccount Class
%run ../../Common/class_StorageAccount

# COMMAND ----------

# DBTITLE 1,Class for Managing an Active Session
class IntacctSession:
    def __init__(self, session_id=None, session_time=None, session_timeout=None):
        # Session attributes
        self.session_id = session_id
        self.time_start = session_time
        self.time_out = session_timeout
        self._active = False
        
        if self.session_id is not None:
            self.time_start = self._convert_to_timestamp(session_time)
            self.time_out = self._convert_to_timestamp(session_timeout)
            self._active = True
        
    def _convert_to_timestamp(self, ts):
        """Transform ts (timestamp string) argument into UTC timestamp"""
        ts_dt = datetime.strptime(ts, "%Y-%m-%dT%H:%M:%S%z")
        return datetime.utcfromtimestamp(ts_dt.timestamp())
    
    def is_active(self):
        """Boolean indicator of whether a session is active"""
        THIRTY_SECOND_THRESHOLD = 30
        
        if self._active:
            time_remaining = self.time_out - datetime.utcnow()
            self._active = time_remaining.seconds > THIRTY_SECOND_THRESHOLD
            
        return self._active

# COMMAND ----------

# DBTITLE 1,Class for Utilizing Intacct Budget API
class IntacctBudget:
    def __init__(self):
        self.today = datetime.utcnow().strftime("%Y%m%d")
        
        # Web Services credentials
        self._senderid = dbutils.secrets.get(scope="key_vault",key="intacct-senderid")
        self._ws_password = dbutils.secrets.get(scope="key_vault",key="intacct-ws-password")
        self._controlid = str(uuid4())
        
        # login credentials
        self._userid = dbutils.secrets.get(scope="key_vault",key="intacct-user")
        self._password = dbutils.secrets.get(scope="key_vault",key="intacct-user-password")
        self._companyid = dbutils.secrets.get(scope="key_vault",key="intacct-senderid")
        
        # Variable to hold the most recent response. Useful in debugging API calls
        self._last_response = None
        
        # Create API session
        s_id, s_time, s_timeout = self._get_api_session()
        self._session = IntacctSession(s_id, s_time, s_timeout)
        
        self.budgets_list_df = self._get_budget_list()
        self.raw_data = None
        
        # Storage account setup
        self._storage_account = StorageAccount('azdlap', 'intacct')
    
        
    @staticmethod
    def bytes_to_df(bytes_data):
        return pd.read_csv(BytesIO(bytes_data))
    
    def _web_services_credentials(self):
        '''Retrieve Web Services credentials'''
        ws_credentials = {
            "senderid":self._senderid,
            "pass":self._ws_password,
            "controlid":self._controlid
        }
        return ws_credentials
    
    def _login_credentials(self):
        '''Retrieve login credentials'''
        credentials = {
            "userid":self._userid,
            "pass":self._password,
            "companyid":self._companyid
        }
        return credentials
    
    def _auth_mode(self, mode):
        '''Build xml for selected authentication mode'''
        auth_xml = ""
        if mode == "login":
            auth_xml = f"""<login>
              <userid>{self._userid}</userid>
              <companyid>{self._companyid}</companyid>
              <password>{self._password}</password>
            </login>"""
        elif mode == "session":
            auth_xml = f"""<sessionid>{self._session.session_id}</sessionid>"""
        return auth_xml
    
    def _build_xml(self, function_xml, auth_mode="login"):
        ws = self._web_services_credentials()
        login = self._login_credentials()
        function_id = datetime.now().strftime("%Y%m%d-%H%M%S")
        auth_xml = self._auth_mode(auth_mode)
        
        xml = f"""<?xml version="1.0" encoding="UTF-8"?>
        <request>
          <control>
            <senderid>{ws["senderid"]}</senderid>
            <password>{ws["pass"]}</password>
            <controlid>{ws["controlid"]}</controlid>
            <uniqueid>false</uniqueid>
            <dtdversion>3.0</dtdversion>
            <includewhitespace>false</includewhitespace>
          </control>
          <operation>
            <authentication>
              {auth_xml}
            </authentication>
            <content>
              <function controlid="{function_id}">
                {function_xml}
              </function>
            </content>
          </operation>
        </request>
        """
        
        return xml
    
    def get_recent_response(self):
        """Retrieve the most recent API response object"""
        return self._last_response
    
    def _post(self, xml):
        headers = CaseInsensitiveDict()
        headers["Content-Type"] = "application/xml"
        api_endpoint = "https://api.intacct.com/ia/xml/xmlgw.phtml"
        
        response = requests.post(api_endpoint, headers=headers, data=xml)
        return response
    
    
    def _get_api_session(self):
        '''
        Documentation: https://developer.intacct.com/api/company-console/api-sessions/
        '''
        api_session_xml = "<getAPISession/>"
        
        xml = self._build_xml(api_session_xml)
        
        response = self._post(xml)
        
        session_id = None
        session_ts = None
        session_timeout = None
        
        if response.ok:
            resp_xml = xmltodict.parse(response.content)
            if "<status>failure" in response.text:
                print(f"API Response: {resp_xml['response']['errormessage']['error']['description2']}")
            if resp_xml["response"]["operation"]["result"]["status"]  == "success":
                session_id = resp_xml["response"]["operation"]["result"]["data"]["api"]["sessionid"]
                session_ts = resp_xml["response"]["operation"]["authentication"]["sessiontimestamp"]
                session_timeout = resp_xml["response"]["operation"]["authentication"]["sessiontimeout"]
            else:
                print("Failed to establish a session")
        else:
            print("POST request failed")
        
        return session_id, session_ts, session_timeout
    
    def budget_lookup(self):
        '''Get Budget object definition'''
        
        budget_xml = f"""<lookup>
          <object>GLBUDGETHEADER</object>
        </lookup>
        """
        if self._session.is_active():
            xml = self._build_xml(budget_xml, auth_mode="session")
        else:
            xml = self._build_xml(budget_xml)
        
        response = self._post(xml)
        
        if response.ok:
            resp_xml = xmltodict.parse(response.content)
            if resp_xml["response"]["operation"]["result"]["status"]  == "success":
                resp_data = resp_xml['response']['operation']['result']['data']['Type']['Fields']['Field']
            else:
                resp_data = response.content
        
        return resp_data
        
    
    def budget_detail_lookup(self):
        '''Get Budget Detail object definition'''
        detail_xml = f"""<lookup>
          <object>GLBUDGETITEM</object>
        </lookup>
        """
        
        if self._session.is_active():
            xml = self._build_xml(detail_xml, auth_mode="session")
        else:
            xml = self._build_xml(detail_xml)
        
        response = self._post(xml)
        
        if response.ok:
            resp_xml = xmltodict.parse(response.content)
            if resp_xml["response"]["operation"]["result"]["status"]  == "success":
                resp_data = resp_xml['response']['operation']['result']['data']['Type']['Fields']['Field']
            else:
                resp_data = response.content
        
        return resp_data
    
    def _get_budget_list(self):
        '''Query and list Budgets'''
        
        list_budgets_xml = """<query>
          <object>GLBUDGETHEADER</object>
          <select>
            <field>RECORDNO</field> <field>BUDGETID</field> <field>DESCRIPTION</field> <field>SYSTEMGENERATED</field> <field>DEFAULT_BUDGET</field> <field>USER</field> <field>STATUS</field> <field>WHENCREATED</field> <field>WHENMODIFIED</field> <field>CREATEDBY</field> <field>MODIFIEDBY</field> <field>ISCONSOLIDATED</field> <field>CURRENCY</field> <field>ISPABUDGET</field> <field>MEGAENTITYKEY</field> <field>MEGAENTITYID</field> <field>MEGAENTITYNAME</field> <field>EXTERNALID</field> <field>RECORD_URL</field>
          </select>
          <orderby>
            <order>
              <field>RECORDNO</field>
              <ascending/>
            </order>
          </orderby>
          <options>
            <returnformat>csv</returnformat>
          </options>
          <pagesize>1000</pagesize>
        </query>
        """
        
        if self._session.is_active():
            xml = self._build_xml(list_budgets_xml, auth_mode="session")
        else:
            xml = self._build_xml(list_budgets_xml)
        
        response = self._post(xml)
        
        if response.ok:
            resp_df = self.bytes_to_df(response.content)
        else:
            resp_df = pd.DataFrame()
            
        return resp_df
    
    def get_budget_details(self, budgetid="WG Consol Budget", offset=0, pagesize=2000):
        """Query and list Budget Details
        param budgetid (str): BUDGETID from budget list
        param offset (int): number of records to skip in query
        parm pagesize (int): number of records to return. Default is 2000
        """
        
        list_details_xml = f"""<query>
          <object>GLBUDGETITEM</object>
          <select>
            <field>RECORDNO</field> <field>BUDGETKEY</field> <field>BUDGETID</field> <field>CURRENCY</field> <field>SYSTEMGENERATED</field> <field>PERIODKEY</field> <field>ACCOUNTKEY</field> <field>DEPTKEY</field> <field>LOCATIONKEY</field> <field>AMOUNT</field> <field>BASEDON</field> <field>GROWBY</field> <field>PERPERIOD</field> <field>NOTE</field> <field>ACCT_NO</field> <field>ACCOUNT_TYPE</field> <field>ACCTTITLE</field> <field>NORMALBALANCE</field> <field>STATISTICAL</field> <field>DEPT_NO</field> <field>DEPTITLE</field> <field>LOCATION_NO</field> <field>LOCATIONTITLE</field> <field>PERIODNAME</field> <field>REPORTINGPERIODNAME</field> <field>PSTARTDATE</field> <field>PENDDATE</field> <field>MEGAENTITYKEY</field> <field>MEGAENTITYID</field> <field>MEGAENTITYNAME</field> <field>PROJECTDIMKEY</field> <field>PROJECTID</field> <field>PROJECTNAME</field> <field>CUSTOMERDIMKEY</field> <field>CUSTOMERID</field> <field>CUSTOMERNAME</field> <field>VENDORDIMKEY</field> <field>VENDORID</field> <field>VENDORNAME</field> <field>ITEMDIMKEY</field> <field>ITEMID</field> <field>ITEMNAME</field> <field>WAREHOUSEDIMKEY</field> <field>WAREHOUSEID</field> <field>WAREHOUSENAME</field> <field>EMPLOYEEDIMKEY</field> <field>EMPLOYEEID</field> <field>EMPLOYEENAME</field> <field>CLASSDIMKEY</field> <field>CLASSID</field> <field>CLASSNAME</field> <field>EXTERNALID</field> <field>RECORD_URL</field>
          </select>
          <filter>
            <equalto>
              <field>BUDGETID</field>
              <value>{budgetid}</value>
            </equalto>
          </filter>
          <orderby>
            <order>
              <field>RECORDNO</field>
              <descending/>
            </order>
          </orderby>
          <options>
            <returnformat>csv</returnformat>
          </options>
          <pagesize>{pagesize}</pagesize>
          <offset>{offset}</offset>
        </query>
        """
        
        if self._session.is_active():
            xml = self._build_xml(list_details_xml, auth_mode="session")
        else:
            xml = self._build_xml(list_details_xml)
        
        response = self._post(xml)
        
        if response.ok:
            resp_df = self.bytes_to_df(response.content)
            self.raw_data = response
        else:
            resp_df = pd.DataFrame()
            
        return resp_df
    
    def get_budget_by_id(self, budgetid):
        get_budget_xml = f"""<readByName>
          <object>GLBUDGETHEADER</object>
          <keys>{budgetid}</keys>
          <returnFormat>csv</returnFormat>
        </readByName>
        """
        
        
        if self._session.is_active():
            xml = self._build_xml(get_budget_xml, auth_mode="session")
        else:
            xml = self._build_xml(get_budget_xml)
        
        response = self._post(xml)
        
        if response.ok:
            resp_df = self.bytes_to_df(response.content)
        else:
            resp_df = pd.DataFrame()
            
        return resp_df
    
    def upload_data_to_raw_storage(self, data_df):
        """Upload data to raw storage account"""
        csv_name = "budgetdetails.csv"
        table = "GLBUDGET"
        
        bytes_data = data_df.to_csv(index=False, encoding="utf8").encode()
        self._storage_account.upload_blob(bytes_data, csv_name, metadata_Date=self.today)
        
        return None
