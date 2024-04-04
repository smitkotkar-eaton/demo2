#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
| Â© 2022 Eaton Corporation. All rights reserved.
| iLead_ERP_sync
| Author: PriyeshAgrawal@eaton.com
| Created On: 10/03/2022

logger.debug('A debug message')
logger.info('An info message')
logger.warning('There is something wrong')
logger.error('An error has happened.')
logger.critical('Fatal error occured. Cannot continue')

"""
# pylint: disable=R0914, W1203
import logging
import azure.functions as func
from src import config
from utils.io_adopter.class_adlsFunc import adlsFunc
from src.class_ERP import ERP
from src.class_ProSQL import ProSQL
from src.class_identify_updates import IdentifyUpdates 

def main(mytimer: func.TimerRequest):
    '''
    Main function to call other functions.

    Parameters
    ----------
    None

    Returns
    -------
    Status: Success or Failure

    '''
    logging.info("Started Main file of ERP")
    # reading from config file
    logging.info("Started reading from config file")
    adls_config = config.ADLS_CONF
    sql_config = config.SQL_CONF
    dict_map = config.DICT_MAP

    # initializing class
    _adls = adlsFunc()

    # reading parameters form config file
    logging.info("Started reading parameters form config file")
    connection_string = _adls.read_credentials()["adlsconnectionstring"]
    dict_const = _adls.read_credentials(
        ["sql-pass", "sql-user", "sql-url", "sql-database1", "sql-db"])
    con_name = adls_config["container"]
    file_name = adls_config["fileName"]
    table_name = sql_config["table_name"]
    erp_url = _adls.read_credentials()["erp_url"]
    func_key = _adls.read_credentials()["func_key"]

    # Calling read function for ADLS and SQL table
    try: 
        logging.info("Initializing class: Pro SQL")
        _sql = ProSQL(dict_const)
        logging.info("Initializing class: ERP")
        _erp = ERP()
        logging.info("Initializing class: Identify Updates")
        _id_ups = IdentifyUpdates()


        logging.info("Reading Data: Output iLead")
        ilead_df = _adls.input_file_read(
            connection_string,
            container_name=con_name,
            file_name=file_name)
        logging.info(f"ilead df size: {ilead_df.shape[0]}")

        logging.info("Reading Data: ERP Table") 
        erp_df = _sql.read_sql_table(tbl_name=table_name)
        logging.info(f"erp df size: {erp_df.shape[0]}")

        # Calling Function to identify updated UPS
        df_updated = _id_ups.update_values(erp_df, ilead_df)
        logging.info(f"Size of data: {df_updated.shape[0]}")
        # df_updated = df_updated.loc[ :2, : ]
        # logging.info(f"{df_updated}")

    except Exception as exc:
        raise exc

    try:
        # Converting df to json data
        logging.info("Starting to convert dataframe to json")
        bol, json_data = _erp.create_json(df_updated, dict_map)
    except Exception as exc:
        raise Exception(
            f'{10*"*"} Failure: {exc} {10*"*"}')
    if bol:
        # API post calling
        logging.info("Starting API post call for erp sync")
        content, status = _erp.post_json(json_data, erp_url, func_key)
        logging.info(
            f'{10*"*"} Response: {content}, Status: {status} {10*"*"}')
    else:
        logging.info("No updates to post in ERP Summary table")
