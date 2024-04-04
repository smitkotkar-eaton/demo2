import pytest
import pandas as pd
import os
from pathlib import Path
import sys
sys.path.append(str(Path(__file__).parent.parent))
from src.class_asset import Asset

asset=Asset()
asset.config['file']['dir_data'] = r".\test\ip"
asset.config['file']['dir_results'] = r".\test\out\\"


class Test_Asset:

    def test_lead_not_existing(self):
        asset.config["file"]["Raw"]["ilead_output"]["file_name"] = "output_iLead_notexist.csv"
        with pytest.raises(Exception) as excinfo:
            flag = asset.read_lead_output()
        assert excinfo.type == Exception


    def test_lead_exists_but_is_empty(self):
        asset.config["file"]["Raw"]["ilead_output"]["file_name"] = "output_iLead_empty.csv"
        with pytest.raises(Exception) as excinfo:
            flag = asset.read_lead_output()
        assert excinfo.type == Exception


    def test_asset_not_existing(self):
        asset.config["file"]["Raw"]["assets_data"]["file_name"] = "asset_data_notexist.csv"
        with pytest.raises(Exception) as excinfo:
            flag = asset.asset_data()
        assert excinfo.type == Exception

    def test_asset_exists_but_is_empty(self):
        asset.config["file"]["Raw"]["assets_data"]["file_name"] = "asset_data_empty.csv"
        with pytest.raises(Exception) as excinfo:
            flag = asset.asset_data()
        assert excinfo.type == Exception

    def test_asset_lead_merge(self):
        '''
        Test Case to check asset_data() in class_assets.py
        Checks if Current value is TRUE
        PredictPulseContractType in result is not null
        Returns
        -------

        '''
        
       
        df_ilead = pd.read_csv("test/ip/output_iLead_test.csv")
        df_assets = pd.read_csv("test/ip/assets_data_test.csv")

        # Call the asset_data function
        merged_asset_df = asset.asset_lead_merge(df_ilead,df_assets)

        expected_columns = ['SerialNumber', 'ContractStartDate', 'ContractEndDate',
                            'PredictPulseContractType', 'Current', 'SystemModStamp',
                            'BatteryJarsPerTray+', 'BatteryTraysPerString+', 'BatteryMonitoring+',
                            'NumberOfCabinets+', 'ElectricRoomCleanliness+', 'Upm2FanServiceDate+',
                            'Upm3FanServiceDate+', 'Upm4FanServiceDate+', 'LastServiceDate',
                            'BatteryStrings+', 'BatteryCabinetType+',
                            'ElectricAirFilterServiceDate+']

        # Check if all expected columns are present in the actual DataFrame
        for col in expected_columns:
            assert col in merged_asset_df.columns


        # Assert that the 'Current' filter is applied correctly
        assert all(merged_asset_df['Current'])

        # Assert that the 'PredictPulseContractType' filter is applied correctly
        assert not any (merged_asset_df['PredictPulseContractType'] == '')

        df_assets.rename(columns={'XML_SerialNumber__c': 'AssetSerial'}, inplace=True)
        with pytest.raises(Exception) as excinfo:
            merged_df = asset.asset_lead_merge(df_ilead, df_assets)
        assert excinfo.type == KeyError




    def test_asset_lead_merge_empty_dataframes(self):
        # Test with empty input DataFrames
        with pytest.raises(Exception) as excinfo:
            merged_df = asset.asset_lead_merge(pd.DataFrame(), pd.DataFrame())
        assert excinfo.type == KeyError








