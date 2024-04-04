import json
import logging
import os
import pandas as pd
import datetime as dt

config_dir = os.path.join(os.path.dirname(__file__))
config_file = os.path.join(config_dir, "config_blssrm.json")
with open(config_file, 'r') as config_file:
    config = json.load(config_file)
mode = config.get("mode", "azure")
if mode == "local":
    path = os.getcwd()
    path = os.path.dirname(path)
    os.chdir(path)

from utils.io import IO
from utils.format_data import Format
from utils import AppLogger

logger = AppLogger(__name__)

class Asset:

    def __init__(self):
        config_dir = os.path.join(os.path.dirname(__file__))
        config_file = os.path.join(config_dir, "config_blssrm.json")
        with open(config_file, 'r') as conf_file:
            self.config = json.load(conf_file)
        self.mode = self.config.get("mode", "azure")
        self.format = Format()
        self.msg_done = ": DONE"
        self.msg_start = ": STARTED"

    def main_assets(self) -> pd.DataFrame:
        try:
            # read ilead_output data
            _step = "Read output_ilead data"
            logger.app_info(f"{_step}: {self.msg_start}")
            df_ilead = self.read_lead_output()
            logger.app_success(_step)

            # read assets data
            _step = "Read assets data"
            logger.app_info(f"{_step}: {self.msg_start}")
            df_asset = self.asset_data()
            df_asset_merged = self.asset_lead_merge(df_ilead,df_asset)
            logger.app_success(_step)

            return df_asset_merged

        except Exception as excp:
            raise excp

    def read_lead_output(self) -> pd.DataFrame:
        '''
        read ilead_output data.

        Returns
        -------
        df_ilead_output: Data frame
        '''


        df_ilead_output = IO.read_csv(
            self.mode,
            {
                "file_dir": self.config["file"]["dir_data"],
                "file_name": self.config["file"]["Raw"]["ilead_output"]["file_name"],
                "adls_config": self.config["adls"]["Raw"]["adls_credentials"],
                "adls_dir": self.config["adls"]["Raw"]["ilead_output"],
            }
        )
        if not isinstance(df_ilead_output,pd.DataFrame):
            raise TypeError("The file doesnot exist")

        if df_ilead_output.empty:
            raise ValueError("The file is empty.")
        if 'SerialNumber' not in df_ilead_output.columns:
            raise ValueError("SerialNumber column not found.")

        return df_ilead_output


    def asset_data(self) -> pd.DataFrame:
        '''
        read assets data.

        Returns
        -------
        merged_df: Data frame
        '''

        df_assets = IO.read_csv(
            self.mode,
            {
                "file_dir": self.config["file"]["dir_data"],
                "file_name": self.config["file"]["Raw"]["assets_data"]["file_name"],
                "adls_config": self.config["adls"]["Raw"]["adls_credentials"],
                "adls_dir": self.config["adls"]["Raw"]["assets_data"],
            },
        )
        if not isinstance(df_assets, pd.DataFrame):
            raise TypeError("The file doesnot exist")

        if df_assets.empty:
            raise ValueError("The file is empty.")

        return df_assets

    def asset_lead_merge(self,df_ilead,df_assets):
        '''
        input-
        df_ilead: Dataframe[ilead_output data]
        df_assets: Dataframe [ATD data]
        Filters assets data gets the records with common latest serial numbers in df_assets and df_ilead.
        'Current' == true
        'PredictPulseContractType' != blank

        Returns
        -------
        merged_asset_df: Merged Data frame
        '''
        try:
            # date format change after renaming
            _step = "date format change after renaming"
            logger.app_info(f"{_step}: {self.msg_start}")
            input_format = self.config["database"]["assets"]["Dictionary Format"]
            df_assets = self.format.format_data(df_assets, input_format)
            logger.app_success(_step)
            #df_assets['SerialNumber'] = df_assets['SerialNumber'].str.lower()
            df_ilead['SerialNumber'] = df_ilead['SerialNumber'].str.lower()

            filtered_df = df_assets[
                (df_assets['Current']) &
                (df_assets['PredictPulseContractType'] != '')
                ]
            merged_df = pd.merge(df_ilead, filtered_df, on='SerialNumber', how='inner')
            logger.app_info(f'Shape of merged_df : {merged_df.shape}')
            filtered_df = merged_df.sort_values(by=['SerialNumber', 'SystemModStamp'], ascending=False)
            filtered_df = filtered_df.drop_duplicates(subset='SerialNumber', keep='first')

            _step = "formatting output"
            logger.app_info(f"{_step}: {self.msg_start}")
            asset_output_format = self.config["output_format"]["assets_mapping"]
            merged_asset_df = self.format.format_output(filtered_df, asset_output_format)
            logger.app_success(_step)
            logger.app_info(f'Shape of ilead_output and assets merged data after formatting : {merged_asset_df.shape}')
            if merged_asset_df.empty:
                raise ValueError("The file is empty.")
            return merged_asset_df


        except Exception as exc:
            raise exc

if __name__ == "__main__": 
    obj = Asset()
    df_updated = obj.main_assets()
