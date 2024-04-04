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
from src.class_identify_updates import IdentifyUpdates

identify_updates = IdentifyUpdates()
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

    def main_assets(self) -> pd.DataFrame: # pragma: no cover
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


        df_ilead = IO.read_csv(
            self.mode,
            {
                "file_dir": self.config["file"]["dir_data"],
                "file_name": self.config["file"]["Raw"]["ilead_output"]["file_name"],
                "adls_config": self.config["adls"]["Raw"]["adls_credentials"],
                "adls_dir": self.config["adls"]["Raw"]["ilead_output"],
            }
        )
        if not isinstance(df_ilead,pd.DataFrame):
            raise TypeError("The file doesnot exist")

        if df_ilead.empty:
            raise ValueError("The file is empty.")
        if 'SerialNumber' not in df_ilead.columns:
            raise ValueError("SerialNumber column not found.")

        df_ilead_reshaped = identify_updates.reshaping_ileads(df_ilead)
        for i in range(1,4):
            df_ilead_reshaped[f'BatteryDateCode_{i}']= df_ilead_reshaped['BatteryDateCode']
            df_ilead_reshaped[f'FanServDateCode_{i}']= df_ilead_reshaped['FanServDateCode']
            df_ilead_reshaped[f'CapServDateCode_{i}']= df_ilead_reshaped['CapServDateCode']

        df_ilead_merged=pd.merge(df_ilead ,df_ilead_reshaped, on="SerialNumber", how='left', suffixes=('','_r') )
        input_format = self.config["database"]["output_ilead"]["Dictionary Format"]
        df_ilead_format = self.format.format_data(df_ilead_merged, input_format)
        return df_ilead_format


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
            }
        )
        if not isinstance(df_assets, pd.DataFrame):
            raise TypeError("The file doesnot exist")

        if df_assets.empty:
            raise ValueError("The file is empty.")

        input_format = self.config["database"]["assets"]["Dictionary Format"]
        df_assets = self.format.format_data(df_assets, input_format)

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
            #df_assets['SerialNumber'] = df_assets['SerialNumber'].str.lower()
            logger.app_info(f'Shape of df_assets : {df_assets.shape}')
            df_ilead['SerialNumber'] = df_ilead['SerialNumber'].str.lower()
            if not df_ilead['SerialNumber'].equals(df_ilead['SerialNumber'].str.lower()):
                raise ValueError

            logger.app_info(f'Shape of df_ilead : {df_ilead.shape}')

            df_assets = df_assets[(df_assets['SerialNumber'] != '')]
            logger.app_info(f'Shape of df_assets without blank srnum : {df_assets.shape}')

            df_ilead = df_ilead[(df_ilead['SerialNumber'] != '')]
            logger.app_info(f'Shape of df_ilead without blank srnum : {df_ilead.shape}')

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
            _step = "Write merged assets to result directory"
            IO.write_csv(
                self.mode,
                {
                    "file_dir": self.config["file"]["dir_results"],
                    "file_name": self.config["file"]["Processed"]["processed_assets"]["file_name"],
                    "adls_config": self.config["adls"]["Processed"]["adls_credentials"],
                    "adls_dir": self.config["adls"]["Processed"]["processed_assets"],
                },
                merged_asset_df,
            )
            logger.app_success(_step)
            return merged_asset_df


        except Exception as exc:
            raise exc

if __name__ == "__main__":
    obj = Asset()
    df_updated = obj.main_assets()
