
"""@file



@brief


@details


@copyright 2021 Eaton Corporation. All Rights Reserved.
@note Eaton Corporation claims proprietary rights to the material disclosed
here on. This technical information may not be reproduced or used without
direct written permission from Eaton Corporation.
"""

# %% *** Setup Environment ***

from utils.class_iLead_contact import ilead_contact
import logging
import utils.io_adopter.local as io_local
import pandas as pd
from datetime import datetime
from utils.io_adopter.class_adlsFunc import adlsFunc
#from azure.storage.filedatalake import DataLakeServiceClient
from utils import AppLogger
import re
logger = AppLogger(__name__)

# %% Define class
io_adls = adlsFunc()
class IO():

    @staticmethod
    def read_adls(config) -> pd.DataFrame:
        """
        :input: configuration of ADLS (json)
        Method to read files from adls container

            - Fetches connection string , storage account name from key vault
            - Fetch file name from config
            - Read file from adls with fetched configurations

        
        :return: data read from azure datalake Storage.
        :rtype: pandas dataframe.

        """
        connection_string_key = config['adls_config']['connection_string']
        storage_account_name_key = config['adls_config']['storage_account_name']
        try:
            logger.app_info('IO: read_csv_adls: Reading credentials')
            credentials=io_adls.read_credentials(ls_cred=[connection_string_key,storage_account_name_key])
            
            formatted_connection_string_key = connection_string_key.replace('-', '_')
            # Accessing values from the credentials dictionary
            connection_string = credentials.get(formatted_connection_string_key)
            container_name = config['adls_dir']['container_name']
            directory_name = config['adls_dir']['directory_name']
            if 'file_name' in config['adls_dir'] and config['adls_dir']['file_name'] != "":
                file_name = config['adls_dir']['file_name']
                logger.app_info(f"IO: read_csv_adls: File name In adls_dir: {file_name}")
            else:
                file_name = io_adls.list_ADLS_directory_contents(connection_string, container_name, directory_name)
                logger.app_info(f"IO: read_csv_adls: File name from list: {file_name}")
         
            if 'sep' in list(config.keys()):
                sep = config['sep']
            else:
                sep = ','

            result= io_adls.input_file_read(connection_string, container_name, file_name, directory_name=directory_name, sep=sep)
            logger.app_info(f"IO: read_csv_adls: Type: {type(result)}, Shape: {result.shape}")

            return result
        except Exception as e:
            logger.app_info(f'exception in read_csv : {e}')
            return e

    @staticmethod
    def write_adls(config,dataset):
        """
        :input: configuration of ADLS (json)
        Method to write files into adls container

            - Fetches connection string , storage account name from key vault
            - Fetch file name from config
            - Write file into adls with fetched configurations

        
        :return: filename written into azure datalake Storage.
        :rtype: pandas dataframe.

        """
        logger.app_info('IO: write_csv_adls: Starting write_csv_adls')
        connection_string_key = config['adls_config']['connection_string']
        storage_account_name_key = config['adls_config']['storage_account_name']
        try:
            escape_char_for_services = ""
            credentials=io_adls.read_credentials(ls_cred=[connection_string_key,storage_account_name_key])
            formatted_connection_string_key = connection_string_key.replace('-', '_')
            # Accessing values from the credentials dictionary
            connection_string = credentials.get(formatted_connection_string_key)
            output_container_name=config['adls_dir']['container_name']
            output_directory_name= config['adls_dir']['directory_name']
            if "services" in output_directory_name:
                escape_char_for_services = "services"

            if 'file_name' in config['adls_dir'] and config['adls_dir']['file_name'] != "":
                file_name= config['adls_dir']['file_name']
                if file_name.endswith(".csv"):
                    file_name = file_name[:-4]

            else:
                 file_name = io_adls.list_ADLS_directory_contents(connection_string, output_container_name,output_directory_name)
                 
            file_name = file_name.split('.')[0]
            logger.app_info(f'IO: write_csv_adls: Filename: {file_name}')
            output_file_name = f"{file_name}"

            result= io_adls.output_file_write(connection_string, dataset, output_container_name,output_file_name, output_directory_name, escape_char_for_services)
            return result
        except Exception as e:
            return e

    # *** CSV ***
    @staticmethod
    def read_csv(mode, config) -> pd.DataFrame:

        if mode == 'local':
            return io_local.read_csv_local(config)
        elif mode == 'azure-adls':
            logger.app_info(f'IO: read_csv: Mode {mode}')
            return IO.read_adls(config)
        else:
            logger.app_info(f'IO: read_csv: Mode {mode} is not implemented')
            raise ValueError ('Not implemented or unknow mode')

    @staticmethod
    def write_csv(mode, config, data):
        logger.app_info('IO: write_csv: Starting write_csv')

        if mode == 'local':
            return io_local.write_csv_local(config, data)
        elif mode == 'azure-adls':
            logger.app_info(f'IO: write_csv: Shape of data: {data.shape}')
            return IO.write_adls(config,data)
        else:
            logger.app_info(f'IO: write_csv: Mode {mode} is not implemented')
            raise ValueError ('Not implemented or unknow mode')

    # *** JSON ***
    # @staticmethod
    def read_json(mode, config):

        if mode == 'local':
            return io_local.read_json_local(config)
        else:
            logger.app_info(f'Mode {mode} is not implemented')
            raise ValueError ('Not implemented or unknow mode')



#%%
