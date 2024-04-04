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
import azure.functions as func
from src.class_asset import Asset
from src.class_blssrm import blssrm


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

        asset_obj = Asset()
        df_asset_merged_with_ilead = asset_obj.main_assets()

        obj = blssrm()

        obj.main_blssrm_pipeline(df_asset_merged_with_ilead)

    except Exception as exc:
        raise exc
