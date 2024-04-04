#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
| Â© 2024 Eaton Corporation. All rights reserved.
| iLeads_BLSS_ERP
| Author: PriyeshAgrawal@eaton.com
| Created On: 31/01/2024

logger.debug('A debug message')
logger.info('An info message')
logger.warning('There is something wrong')
logger.error('An error has happened.')
logger.critical('Fatal error occured. Cannot continue')

"""
# pylint: disable=R0914, W1203
import logging
import os
import json
import azure.functions as func
from src.class_asset import Asset
from src.class_blssrm import blssrm
from utils.io import IO

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
    logging.info("Started Main file of BLSSRM")
    try:
        config_dir = os.path.join(os.path.dirname(__file__), "../src")
        config_file = os.path.join(config_dir, "config_blssrm.json")

        # Read the configuration file
        with open(config_file, 'r') as config_file:
            config = json.load(config_file)
        mode = config["mode"]

        # asset_obj = Asset()
        # df_asset_merged_with_ilead = asset_obj.main_assets()
        obj = blssrm()

        merged_df = IO.read_csv(
                mode,
                {
                    "file_dir": config["file"]["dir_results"],
                    "file_name": config["file"]["Processed"]["processed_assets"]["file_name"],
                    "adls_config": config['adls']['Processed']['adls_credentials'],
                    "adls_dir": config['adls']['Processed']["processed_assets"]
                }
            )

        # obj.main_blssrm_pipeline(df_asset_merged_with_ilead)
        obj.main_blssrm_pipeline(merged_df)

    except Exception as exc:
        raise exc