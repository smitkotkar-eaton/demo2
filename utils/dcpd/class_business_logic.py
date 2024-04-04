"""@file
@brief
@details
@copyright 2023 Eaton Corporation. All Rights Reserved.
@note Eaton Corporation claims proprietary rights to the material disclosed
here on. This technical information may not be reproduced or used without
direct written permission from Eaton Corporation.
"""
# %% ***** Setup Environment *****
import json
import os
import re
import sys
import traceback
import pandas as pd
from utils import IO
from utils import AppLogger
import logging
#import utils.json_creator as js
logger = AppLogger(__name__)

class BusinessLogic:

    def __init__(self):
        config_dir = os.path.join(os.path.dirname(__file__), "../../config")
        config_file = os.path.join(config_dir, "config_dcpd.json")

        # Read the configuration file
        with open(config_file,'r') as config_file:
            config = json.load(config_file)
        #self.config=js.read_json(config_file)
        self.config = config
        self.mode = self.config.get("conf.env", "azure-adls")


        # Read Reference: Product from Serial Number
        try:
            ref_prod_fr_srnum = IO.read_csv(
                    self.mode,
                    {'file_dir': self.config['file']['dir_ref'],
                    'file_name': self.config['file']['Reference']['decode_sr_num'],
                    'adls_config': self.config['adls']['Reference']['adls_credentials'],
                    'adls_dir': self.config['adls']['Reference']['decode_sr_num']
                    })
            ref_prod_fr_srnum['SerialNumberPattern'] = ref_prod_fr_srnum['SerialNumberPattern'].str.lower()
            self.ref_prod_fr_srnum = ref_prod_fr_srnum
        except Exception as e:
            logger.app_info(f'Error in reading reference file: {e}')
            logger.app_info(traceback.format_exc())
            raise e

    def idetify_product_fr_serial(self, ar_serialnumber):

        df_data = pd.DataFrame(data={'SerialNumber': ar_serialnumber})
        df_data.SerialNumber = df_data.SerialNumber.fillna("").str.lower()

        # ref_prod_fr_srnum
        ref_prod = self.ref_prod_fr_srnum
        ref_prod = ref_prod[ref_prod.flag_keep]
        ref_prod['SerialNumberPattern'] = ref_prod['SerialNumberPattern'].str.lower()

        # Pre-Process Data
        ls_prod = ref_prod.Product.unique()
        df_data['Product'] = ''
        for prod in ls_prod:
            # prod = ls_prod [0]
            ls_pattern = ref_prod.loc[
                ref_prod.Product == prod, 'SerialNumberPattern']

            df_data['flag_prod'] = df_data.SerialNumber.str.startswith(
                tuple(ls_pattern))
            df_data['flag_prod'] = df_data['flag_prod'].fillna(False)
            df_data.loc[df_data['flag_prod'], 'Product'] = prod
            df_data = df_data.drop('flag_prod', axis=1)

        dict_pat = {'526?-3421': 'STS', '26??-0820': 'RPP'}
        for pat in dict_pat:
            prod = dict_pat[pat]
            df_data['flag_prod'] = df_data.SerialNumber.apply(
                lambda x: re.search(pat, x) != None)
            df_data['flag_prod'] = df_data['flag_prod'].fillna(False)
            df_data.loc[df_data['flag_prod'], 'Product'] = prod
            df_data = df_data.drop('flag_prod', axis=1)

        return df_data['Product']

# %%
