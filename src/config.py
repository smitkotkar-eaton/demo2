# -*- coding: utf-8 -*-
"""
Created on Mon Sep 26 17:25:48 2022

@author: E9780837
"""

import os
import pandas as pd

# %% Identify environment
env = os.environ
if 'WEBSITE_SITE_NAME' in env.keys():
    websize_name = env['WEBSITE_SITE_NAME']
    ENV = 'cloud'

    del websize_name
else:
    ENV = 'local'

# %% ERP Constants

RECORD_ID = 11111
RGE_REQ_ID = [10000, 2147483647]

DICT_MAP = {
    'Model': ['', 'Model_Id', 'MissingData'],
    'CustomerName': ['', 'Owner_Party_Name', 'MissingData'],
    'InstalledDate': ["%Y-%m-%dT%H:%M:%S", 'InstallDate', 'MissingData'],
    'CtoNumber': ['', 'Item/CTO', 'MissingData'],
    'BatteryModel': ['', 'BatteryModel', 'MissingData'],
    'BatteryDateCode': ['%m/%Y', 'BatteryDateCode', 'DateCode'],
    'BatteryInstallDate': ['%d-%b-%y', 'BatteryDateCode', 'DateCode'],
    'BattLastExchgDate': ['%m/%Y', 'BatteryDateCode', 'DateCode'],
    'BattMfdDate': ['%m/%Y', 'BatteryDateCode', 'DateCode'],
    'BatteryMfr': ['', 'BatteryMfg', 'MissingData'],
    'BatteryType': ['', 'BatteryType', 'MissingData'],
    'BattQuantity': ['', 'Num_of_Jars', 'MissingData'],
    'AcCapServDate': ['%m/%Y', 'CapServDateCode', 'DateCode'],
    'DcCapServDate': ['%m/%Y', 'CapServDateCode', 'DateCode'],
    'FanServDate': ['%m/%Y', 'FanServDateCode', 'DateCode'],
    'FanServDateElec': ['%m/%Y', 'FanServDateCode', 'DateCode'],
    'Upm1FanServDate': ['%m/%Y', 'FanServDateCode', 'DateCode']
}


DICT_FORM_MANDATE = {
    'SerialNumber': 'str',
    'CreationDate': "%Y-%m-%dT%H:%M:%S",
    'CreatedBy': 'str',
    'LastUpdateDate': "%Y-%m-%dT%H:%M:%S",
    'LastUpdatedBy': 'str',
    'RequestId': 'str',
    'Status': 'str'
                    }
ADLS_CONF = {
    'container' : 'reports',
    'fileName' : 'output_iLead.csv'
            }

SQL_CONF = {
    'table_name' : 'ERPSummaryData'
           }

KEY_VAULT = "https://cip-ilead-key-vault-prod.vault.azure.net/" 

# %% Folder Structure
if ENV == 'local':
    from src.get_path import basepath

    DIR_PROJECT = str(basepath.parent)
    print(DIR_PROJECT)
    DIR_TEST = os.path.join(DIR_PROJECT, 'test')
    DIR_REF = os.path.join(DIR_PROJECT, 'reference_files')
    DIR_RESULT = os.path.join(DIR_PROJECT, 'result')
    DIR_INPUT = os.path.join(DIR_PROJECT, 'data')

    sql_dict_const = pd.read_csv(f'{DIR_REF}/set_config.csv')
    sql_dict_const = sql_dict_const.set_index(sql_dict_const.name)
    sql_dict_const = sql_dict_const.to_dict()['value']

    adls_dict_const = pd.read_csv(f'{DIR_REF}/set_config_adls.csv')
    adls_dict_const = adls_dict_const.set_index(adls_dict_const.name)
    adls_dict_const = adls_dict_const.to_dict()['value']

    # ADLS PROXY
    os.environ["HTTP_PROXY"] = "http://proxy.apac.etn.com:8080"
    os.environ["HTTPS_PROXY"] = "http://proxy.apac.etn.com:8080"
