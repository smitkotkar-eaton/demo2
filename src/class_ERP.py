# -*- coding: utf-8 -*-
"""
Created on Mon Sep 26 17:24:38 2022

@author: E9780837
"""
# %% Set Environment
import random
import json
from datetime import datetime
import requests
import pandas as pd
import numpy as np
import src.config as CONF


# %%
class ERP:
    '''
    class of ERP
    '''

    def create_json(self, df_input_org, dict_map):
        '''
        Generate Json input for ERP sync.

        Parameters
        ----------
        df_input_org : pandas DataFrame. Serial number with updates for specified
        ERP column.
        dict_map : dictionary.  Mapping of ERP columns to iLead columns.

        Raises
        ------
        TypeError: if input is not a pandas datafra,e.
        ValueError: if data does not have all required columns.

        Returns
        -------
        bool. indicates if data has any changes.
        json. Updates for ERP sync with mandatory columns and changes.

        '''
        # *** Input ***
        if not isinstance(df_input_org, pd.core.frame.DataFrame):
            raise TypeError('Input is not a dataframe')

        df_input = df_input_org.copy()
        df_input = df_input.fillna("").astype(str)
        del df_input_org

        if df_input.empty:
            return False, ""

        exp_cols = list(dict_map.keys())
        exp_cols.append('SerialNumber')
        if not set(exp_cols).issubset(set(df_input.columns)):
            ls_missing = set(exp_cols).difference(df_input.columns)
            ls_missing = ' and '.join(ls_missing)
            raise ValueError(
                f"{ls_missing} are not available in the dataframe")

        df_input['has_updates'] = df_input[list(dict_map.keys())].apply(
            lambda x: len(''.join(x)), axis=1)
        df_input = df_input.loc[df_input['has_updates'] > 0, exp_cols]

        if df_input.empty:
            return False, pd.DataFrame()

        # *** Process ***
        # Add mandatory columns
        cur_date = datetime.now()

        df_input['CreationDate'] = cur_date.strftime(
            CONF.DICT_FORM_MANDATE['CreationDate'])
        df_input['CreatedBy'] = CONF.RECORD_ID
        df_input['LastUpdateDate'] = cur_date.strftime(
            CONF.DICT_FORM_MANDATE['LastUpdateDate'])
        df_input['LastUpdatedBy'] = CONF.RECORD_ID

        ls_random = [
            (random.randrange(CONF.RGE_REQ_ID[0], CONF.RGE_REQ_ID[1]))
            for i in range(df_input.shape[0])]
        df_input['RequestId'] = ls_random
        df_input['Status'] = 'PENDING'

        df_input_org = df_input.copy()
        ls_cols = list(CONF.DICT_FORM_MANDATE.keys())
        ls_cols.extend(list(CONF.DICT_MAP.keys()))
        df_input = df_input_org[ls_cols]

        # Convert
        df_input = df_input.replace(r'^\s*$', np.nan, regex=True)
        json_str = json.dumps(
            [row.dropna().to_dict() for index, row in df_input.iterrows()])
        json_data = json.loads(json_str)

        json_4_api = {}
        json_4_api['ERPSummarys'] = json_data

        return True, json_4_api

    def post_json(self, json_data, erp_url, func_key):
        '''
        Post Json data to ERP.

        Parameters
        ----------
        json_data : pandas DataFrame. Serial number with updates for specified
        ERP column.
        dict_map : dictionary.  Mapping of ERP columns to iLead columns.

        Returns
        -------
        StatusCode: StatusCode of Success or Failure
        '''
        try:
            response = requests.post(
                f'{erp_url}',
                headers={'x-functions-key': func_key},
                json=json_data,
                verify=False
            )
            return response.content, response.status_code

        except Exception as exc:
            return exc, 0