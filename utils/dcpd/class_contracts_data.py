"""@file class_contracts_data.py.

@brief : For DCPD business; analyze contracts data from SalesForce to be consumed
by lead generation


@details :
    DCPD has two tables for contracts data (contracts and renewal contracts).
    Code summaries both the datasets to understand if a unit is currently under
    active contract

    1. Contracts: has warranty and startup details
    2. Renewal contract: has contracts data (other than warranty)


@copyright 2023 Eaton Corporation. All Rights Reserved.
@note Eaton Corporation claims proprietary rights to the material disclosed
here on. This technical information may not be reproduced or used without
direct written permission from Eaton Corporation.
"""

# %% ***** Setup Environment *****
# import all required modules

import os
import json
import time
config_dir = os.path.join(os.path.dirname(__file__), "../../config")
config_file = os.path.join(config_dir, "config_dcpd.json") 
with open(config_file,'r') as config_file:
    config = json.load(config_file)
mode = config.get("conf.env", "azure-adls")
if mode == "local":
    path = os.getcwd()
    path = os.path.join(path.split('ileads_lead_generation')[0], 'ileads_lead_generation')
    os.chdir(path)

import re
import traceback
from string import punctuation
from typing import Tuple
from datetime import datetime
import pandas as pd
import numpy as np
from utils.dcpd.class_business_logic import BusinessLogic
from utils.dcpd.class_serial_number import SerialNumber
from utils.dcpd.class_common_srnum_ops import SearchSrnum
from utils import IO
from utils import Filter
from utils import AppLogger
from utils.format_data import Format


logger = AppLogger(__name__)

punctuation = punctuation + " "


# from utils import Filter


class Contract:
    """Class will extract and process contract data and renewal data."""

    def __init__(self):  # ='local'
        """Initialise environment variables, class instance and variables used
        throughout the modules."""

        # class instance
        self.srnum = SerialNumber()
        self.bus_logic = BusinessLogic()
        self.srnum_ops = SearchSrnum()
        self.format = Format()
        config_dir = os.path.join(os.path.dirname(__file__), "../../config")
        config_file = os.path.join(config_dir, "config_dcpd.json") 

        # Read the configuration file
        with open(config_file,'r') as config_file:
            config = json.load(config_file)
        #self.config=js.read_json(config_file)
        self.config = config
        self.mode = self.config.get("conf.env", "azure-adls")
        if self.mode == "local":
            path = os.getcwd()
            path = os.path.join(path.split('ileads_lead_generation')[0], 'ileads_lead_generation')
            os.chdir(path)
        self.valid_ext_srnum = self.config['valid_ext_srnum']

        self.ls_cols_startup = self.config["contracts"]["config_cols"][
            "ls_cols_startup"
        ]
        self.pat_single_srnum = self.config["contracts"]["srnum_pattern"][
            "pat_single_srnum"
        ]
        self.pat_mob = self.config["contracts"]["srnum_pattern"]["pat_mob"]

        self.dict_decode_contract = self.config["contracts"]["config_cols"][
            "dict_decode_contract"
        ]
        self.dict_contract = self.config["contracts"]["config_cols"]["dict_contract"]

        self.dict_char = self.config["contracts"]["srnum_pattern"]["dict_char"]
        # self.prep_contract_cols = self.config['contracts']['config_cols']['prep_contract_cols']

        # steps
        self.main_contract = "main contract"
        self.preprocess_contract = "PreProcess: Contracts Data"
        self.preprocess_renewal = "PreProcess renewal data"
        self.merge_data = "Merge Data: Contract and Renewal"
        self.export_contract = "Export contracts data"


    #  ***** Main *****
    def main_contracts(self) -> None:  # pragma: no cover
        """
        Contain pipeline for contracts and renewal data.

        :raises Exception: Collects any / all exception.

        """
        try:
            # PreProcess: Contracts Data
            df_contract = self.pipeline_contract

            # PreProcess : Renewal data
            df_renewal = self.pipeline_renewal()

            # Merge Contract and Renewal Data
            df_contract = self.merge_contract_and_renewal(
                df_contract, df_renewal
            )

            # Decode Contract Type
            df_contract = self.pipeline_decode_contract_type(df_contract)

            # Buiseness needs the data for latest expiration date hence sorting by Contract_Expiration_Date before dropping duplicates
            df_contract.sort_values(by=['Contract_Expiration_Date'], ascending=False, inplace=True)
            df_contract.drop_duplicates(["ContractNumber", "SerialNumber"], inplace=True, keep='first')

            # Write data for validation before validating contract with install data
            IO.write_csv(
                self.mode,
                {
                    "file_dir": self.config["file"]["dir_results"]
                    + self.config["file"]["dir_validation"],
                    "file_name": self.config["file"]["Processed"]["contracts"][
                        "validation"
                    ],
                    "adls_config": self.config["adls"]["Processed"]["adls_credentials"],
                    "adls_dir": self.config["adls"]["Processed"]["contracts"]["validation"],
                },
                df_contract,
            )

            # Merge Summarised contract and install base data.
            df_install_contract_merge = self.merge_contract_install(
                df_contract)
            
            df_install_contract_merge = df_install_contract_merge.sort_values(['Start_date'], ascending=False)
            df_install_contract_merge = df_install_contract_merge.drop_duplicates(subset=["ContractNumber", "SerialNumber"])

            # Export Data
            IO.write_csv(
                self.mode,
                {
                    "file_dir": self.config["file"]["dir_results"]
                    + self.config["file"]["dir_intermediate"],
                    "file_name": self.config["file"]["Processed"]["contracts"][
                        "file_name"
                    ],
                    "adls_config": self.config["adls"]["Processed"]["adls_credentials"],
                    "adls_dir": self.config["adls"]["Processed"]["contracts"],
                },
                df_install_contract_merge,
            )
            logger.app_info('Successfully executed')
        except Exception as excp:
            logger.app_info(f" main function failed, {excp} ")
            logger.app_info(self.main_contract, f"{traceback.print_exc()}")
            raise Exception('f"{self.main_contract}: Failed') from excp

    # ***** Pipelines *****
    @property
    def pipeline_contract(self) -> pd.DataFrame:  # pragma: no cover
        """
        Pipeline to pre-process contracts data to.

            - Identify startups
            - Extract serial number from text
            - Validate serial number (serial number should exist in installbase)

        :raises Exception: Raised if unknown data type provided.
        :return: processed contracts data with serial numbers.
        :rtype: pandas dataframe.

        """
        try:
            # Read raw contracts data
            _step = "Read raw contracts data"

            df_contract = IO.read_csv(
                self.mode,
                {
                    "file_dir": self.config["file"]["dir_data"],
                    "file_name": self.config["file"]["Raw"]["contracts"]["file_name"],
                    "adls_config": self.config["adls"]["Raw"]["adls_credentials"],
                    "adls_dir": self.config["adls"]["Raw"]["contracts"],
                },
            )

            df_contract['Original_Sales_Order__c'] = df_contract['Original_Sales_Order__c'].str.split(
                r'/|\s|&|,|-|\D+|;|([\(\)])')
            df_contract = df_contract.explode('Original_Sales_Order__c').reset_index(drop=True)
            df_contract["Original_Sales_Order__c"] = df_contract["Original_Sales_Order__c"].astype(str)

            df_contract["BillingAddress"] = df_contract["BillingStreet"]
            input_format = self.config["database"]["contracts"]["Dictionary Format"]
            df_contract = self.format.format_data(df_contract, input_format)
            df_contract.reset_index(drop=True, inplace=True)
            logger.app_success(_step)

            # Identify Startups
            _step = "Identify Startups"
            df_contract.loc[:, ["was_startedup", "startup_date"]] = self.id_startup(
                df_contract[self.ls_cols_startup]
            )

            # df_contract = df_contract[
            #     df_contract['startup_date'].dt.date <= datetime.now().date()
            # ]

            logger.app_success(_step)

            # Identify Serial Numbers
            _step = "Identify Serial Number"
            df_contract_srnum = self.pipeline_id_srnum(df_contract)

            IO.write_csv(
                self.mode,
                {
                    "file_dir": self.config["file"]["dir_results"]
                    + self.config["file"]["dir_validation"],
                    "file_name": "contract_sr_num_validation.csv",
                    "adls_config": self.config["adls"]["Processed"]["adls_credentials"],
                    "adls_dir": self.config["adls"]["Processed"]["contracts"]["contract_srnum_validation"],
                },
                df_contract_srnum,
            )

            logger.app_success(_step)
            # Validate Serial number from InstallBase
            _step = "Validate Serial number"


            df_contract_with_notna_for_sn = df_contract_srnum[pd.notna(df_contract_srnum["SerialNumber"])]
            del df_contract_srnum
            # df_contract_srnum = self.pipeline_validate_srnum(df_contract_srnum)
            df_contract_srnum_validated = self.validate_contract_install_sr_num(df_contract_with_notna_for_sn)
            del df_contract_with_notna_for_sn
            df_contract_srnum_validated.drop(["KeySerial"],axis=1,inplace=True)
            df_contract_srnum_validated["KeySerial"] = "contracts"
            logger.app_success(_step)

            ls_of_contracts_with_sn = df_contract_srnum_validated.loc[df_contract_srnum_validated["flag_validinstall"] == True, "ContractNumber"].unique()

            df_contract_with_na_for_sn = df_contract.loc[~df_contract.ContractNumber.isin(ls_of_contracts_with_sn), ["ContractNumber", "Original_Sales_Order__c"]]
            df_contract_with_na_for_sn.rename({"Original_Sales_Order__c":"SO"}, axis=1, inplace=True)
            df_contract_with_na_for_sn["flag_validinstall"] = True
            df_contract_with_na_for_sn["src"] = "SO"
            df_contract_with_na_for_sn.drop_duplicates(["ContractNumber"], inplace=True)

            # df_contract_with_na_for_sn = df_contract_with_na_for_sn.drop(["SerialNumber"], axis=1)
            df_install = self.read_processed_installbase()
            df_install["SO"] = df_install["SO"].astype(str)
            df_install_temp = df_install[["SO", "SerialNumber_M2M"]]
            df_install_temp.rename({"SerialNumber_M2M": "SerialNumber"}, axis=1, inplace=True)
            df_install_temp.drop_duplicates(["SO", "SerialNumber"], inplace=True)
            del df_install


            # df_contract_with_na_for_sn = df_contract_with_na_for_sn.merge(df_contract, on="ContractNumber", how="inner")

            df_contract_with_na_for_sn = df_contract_with_na_for_sn.merge(df_install_temp, on="SO", how="inner")
            df_contract_srnum_validated = pd.concat([df_contract_with_na_for_sn, df_contract_srnum_validated])
            del df_contract_with_na_for_sn,  df_install_temp

            # Merge data
            _step = "Merge Data: Contract and SerialNumber"
            df_contract = self.merge_contract_and_srnum(df_contract, df_contract_srnum_validated)
            logger.app_success(_step)

            logger.app_success(self.preprocess_contract)

        except Exception as excp:
            logger.app_info(" Pipeline cotract failed ")
            logger.app_info(self.preprocess_contract, f"{traceback.print_exc()}")
            raise Exception('f"{self.preprocess_contract}: Failed') from excp


        return df_contract

    def pipeline_id_srnum(self,
                          df_contract) -> pd.DataFrame:  # pragma: no cover
        """
        Identify Serial Numbers for the contract. PDI SalesForce has multiple
        columns where SerialNumber and corresponding Qty is logged.

        :param df_contract: Contracts data from PDI salesforce.
        :type df_contract: pandas DataFrame
        :raises Exception: Raised if unknown data type provided.
        :return: Serial Numbers extracted from PDI SalesForce.
        :rtype: pandas DataFrame

        """
        _step = f"{' ' * 5}Extract Serial Numbers from contract"

        try:


            # Search Serial Numbers based on pattern for SerialNumber
            df_serialnum = self.srnum_ops.search_srnum(df_contract)


            df_serialnum["SerialNumber"] = df_serialnum["SerialNumber"].apply(lambda x : re.sub("\\s+|\\)|unit[s]?",'',str(x).lower()))
            df_serialnum["SerialNumber"] = df_serialnum["SerialNumber"].apply(lambda x: re.sub("&", ',', str(x)))
            df_serialnum["SerialNumber"] = df_serialnum["SerialNumber"].apply(lambda x: re.sub("\\(", '-', str(x)))
            df_serialnum["SerialNumber"] = df_serialnum["SerialNumber"].str.rstrip(",").str.rstrip('-')
            df_serialnum = df_serialnum.drop_duplicates(subset=['ContractNumber', 'SerialNumber'])

            # df_serialnum = df_serialnum[pd.notna(df_serialnum['SerialNumber'])]
            repat_sr_num = "|".join(self.valid_ext_srnum)
            repat_sr_num = re.compile(repat_sr_num)
            df_serialnum = df_serialnum[pd.notna(df_serialnum.SerialNumber)].reset_index(drop=True)
                          
            df_serialnum["SerialNumber"] = df_serialnum['SerialNumber'].apply(
                lambda x: re.sub(repat_sr_num, "", str(x)))
            df_serialnum["SerialNumber"] = df_serialnum["SerialNumber"].str.strip(" -")
            del repat_sr_num

            ref_sr_num = IO.read_csv(
               self.mode,
               {
                   "file_dir": self.config["file"]["dir_ref"],
                   "file_name": self.config["file"]["Reference"]["decode_sr_num"],
                   "adls_config": self.config["adls"]["Reference"]["adls_credentials"],
                   "adls_dir": self.config["adls"]["Reference"]["decode_sr_num"],
               },
            )

            ref_sr_num['SerialNumberPattern'] = ref_sr_num['SerialNumberPattern'].str.lower()
            ls_eligible_4_exp = ref_sr_num.loc[
                ref_sr_num['eligible_for_expand'] == "TRUE", "SerialNumberPattern"]

            df_serialnum["SerialNumber"] = df_serialnum["SerialNumber"].str.lower()
            df_serialnum = df_serialnum[df_serialnum["SerialNumber"] != ""]

            df_serialnum["hypen_present"] = df_serialnum["SerialNumber"].apply(lambda x: '-' in x)
            df_serialnum["hypen_present"] = df_serialnum["hypen_present"].fillna(False)
            temp1 = df_serialnum.loc[df_serialnum["hypen_present"] == True, :]
            temp1["SerialNumber"] = temp1["SerialNumber"].apply(
                lambda x: "" if len(x.split('-')[1]) < 3 and x.strip().lower().startswith(tuple(ls_eligible_4_exp)) else x)
            temp1 = temp1[temp1["SerialNumber"] != ""]

            temp2 = df_serialnum.loc[df_serialnum["hypen_present"] == False, :]
            # temp2["SerialNumber"] = temp2["SerialNumber"].apply(
            #     lambda x: x if x.strip().lower().startswith(tuple(ls_eligible_4_exp)) else "")
            # temp2 = temp2[temp2["SerialNumber"] != ""]
            df_serialnum = pd.concat([temp1, temp2])
            df_serialnum["no_of_sep"] = df_serialnum['SerialNumber'].apply(
                lambda x: len(re.findall("-", x)))
            df_serialnum["has_range_sep"] = df_serialnum['SerialNumber'].apply(
                lambda x: len(re.findall(",|&|\(", x)) > 0)

            # has range sep: (110-b-1,2,3) or (110-b (124, 125)) or (110-b-1&2) will be expanded
            # x[1] > 2
            #   : 110-b will not be expanded as quantity is not reliable
            #   : 110-b-c is unique
            #   : 110-b-c-e will be expanded
            # x[0].lower().startswith(tuple(ls_eligible_4_exp))
            #   : t-us-.. will not be expanded as its not eligible for expansion

            df_serialnum['is_srnum_range'] = df_serialnum[['SerialNumber', 'no_of_sep', 'has_range_sep', 'Qty']].apply(
                lambda x: (
                    (x[2]) |
                    (
                            (x[1] > 2) &
                            (x[0].lower().startswith(tuple(ls_eligible_4_exp)))
                    )
                ),
                axis=1)
            del ls_eligible_4_exp, ref_sr_num

            # Clean serial number
            df_serialnum['SerialNumber'] = df_serialnum["SerialNumber"].apply(
                lambda x: re.sub(" +", "", str(x)))

            # Filter out non-serial numbers
            IO.write_csv(
                self.mode,
                {
                    "file_dir": self.config["file"]["dir_results"]
                    + self.config["file"]["dir_validation"],
                    "file_name": self.config["file"]["Processed"]["contracts"][
                        "contract_srnum_validation"
                    ],
                    "adls_config": self.config["adls"]["Processed"]["adls_credentials"],
                    "adls_dir": {
                    "container_name": "results",
                    "directory_name": "validation/contracts",
                    "file_name": "contract_check.csv"
                }
                },
                df_serialnum
            )

            ls_cols = ['ContractNumber', 'SerialNumberContract', 'SerialNumber', 'Qty', 'src']
            unique_sr_num = df_serialnum.loc[~df_serialnum['is_srnum_range'], ls_cols]
            range_sr_num = df_serialnum.loc[df_serialnum['is_srnum_range'], ls_cols]
            del df_serialnum

            # Expand serial number
            expanded_sr_num = self.get_range_srum(range_sr_num)
            del range_sr_num

            # Attach single and range serial numbers
            expanded_sr_num = pd.concat([expanded_sr_num, unique_sr_num])
            expanded_sr_num = expanded_sr_num.drop_duplicates(subset=['ContractNumber', 'SerialNumber'])
            del unique_sr_num
            logger.app_success(_step)

        except Exception as excp:
            logger.app_info(" Pipeline id_srnum failed ")
            logger.app_info(_step, f"{traceback.print_exc()}")
            raise Exception('f"{_step}: Failed') from excp

        return expanded_sr_num

    def validate_contract_install_sr_num(self, df_contract):
        """
        Validate contract Serial Numbers.
        :param df_contract: Dataframe with possible startup date
        fields in the sequence if Priority,
        :type df_contract: pandas DataFrame.
        :raises Exception: Raised if unknown data type provided.
        :return: Data Frame with two columns
        :rtype: pandas Data Frame
        """
        _step = "Validate contract Serial Numbers "
        try:

            df_install = self.read_processed_installbase()
            df_install = df_install[pd.notna(df_install["SerialNumber_M2M"])]

            df_install.loc[:, "SerialNumber"] = df_install.SerialNumber_M2M.astype(str)
            # handling single character case in SerialNumber col "111-0000-1a"
            df_install["SerialNumber"] = df_install["SerialNumber"].apply(
                lambda x: re.sub(r"-(\d{1})[a-zA-Z]$", r"-\1", x)
            )
            #Replace leading zeros in contract as well as in installbase before performing exact and partial match
            #If serial numbers begins only with zero i.e. 0000-111-12 then do not replace leading zeros else replace leading zeros otherwise replace leading zeros.
            pattern = re.compile("^0+(?!$)")
            df_install["SerialNumber"] = df_install["SerialNumber"].apply(lambda x: x if re.sub(pattern, '', x).startswith('-') else re.sub(pattern, '', x))
            df_contract["SerialNumber"] = df_contract.SerialNumber.astype(str)
            df_contract["SerialNumber"] = df_contract["SerialNumber"].apply(lambda x: x if re.sub(pattern, '', x).startswith('-') else re.sub(pattern, '', x))

            df = IO.read_csv(
                self.mode,
                {
                    "file_dir": self.config["file"]["dir_ref"],
                    "file_name": self.config["file"]["Reference"]["decode_sr_num"],
                    "adls_config": self.config["adls"]["Reference"]["adls_credentials"],
                    "adls_dir": self.config["adls"]["Reference"]["decode_sr_num"],
                },
            )

            # List of Customer for which we need only exact match.
            ls_exact_match = self.config["install_base"]["sr_num_validation"][
                "exact_match_filter"
            ]

            # Filter rows where partial_flag is TRUE and extract the values of the Product column
            filtered_products = df.loc[
                df["partial_flag"] == True, "SerialNumberPattern"
            ].tolist()

            # Step 1: Exact Match
            df['SerialNumberPattern'] = df['SerialNumberPattern'].str.lower()
            ls_eligible_4_exp = df.loc[df['eligible_for_expand'] == "TRUE", "SerialNumberPattern"]
            del df

            # Initialize
            df_contract["match_flag"] = False

            time_start = time.time()
            # Step 1: Exact Match

            # Change 20230929: Changed logic from Iterator to lambda function
            # Iterator was not functioning correctly
                                                                   
            df_contract["match_flag"] = df_contract.SerialNumber.apply(
                lambda x: x in df_install["SerialNumber"].values
                          
            )
            time_end = time.time()
            time_delta = round(time_end - time_start, 2)
            logger.app_info(f"Time taken for exact match : {time_delta}secs.", 2)

            # Step 2: Filter df_install based on "StrategicCustomer"
            df_filtered = df_install[
                ~df_install["StrategicCustomer"].isin(ls_exact_match)
            ]
            del df_install

            #pandas_series_srnum_4_parmatch = df_filtered.loc[pd.notna(df_filtered['SerialNumber']), "SerialNumber"]
            df_filtered = df_filtered.loc[pd.notna(df_filtered['SerialNumber'])]
            df_filtered["sn_to_consider"] = df_filtered["SerialNumber"].apply(lambda x : str(x).lower().startswith(tuple(ls_eligible_4_exp)))
            pandas_series_srnum_4_parmatch = df_filtered.loc[df_filtered["sn_to_consider"]==True, "SerialNumber"]
            del df_filtered

            df_contract["no_of_sep"] = df_contract['SerialNumber'].apply(lambda x: len(re.findall("-", str(x))))

            df_contract['sr_num_split'] = df_contract['SerialNumber'].apply(lambda x: str(x).split("-"))
            df_contract['go4partmatch'] = df_contract['sr_num_split'].apply(lambda x: x[0] in filtered_products)

            # if logic of for loop
            time_start = time.time()
            df_contract['is_partial'] = (
                    (df_contract["match_flag"] == False) &
                    (df_contract["no_of_sep"] == 1) &
                    (df_contract['go4partmatch'])
            )

            ls_cols = ['is_partial', 'SerialNumber']
            df_contract['partial_matches'] = df_contract[ls_cols].apply(
                lambda x:
                pandas_series_srnum_4_parmatch[pandas_series_srnum_4_parmatch.str.contains(x[1])].tolist()
                if x[0]
                else "", axis=1)
            del ls_cols

            df_contract.loc[
                df_contract['partial_matches'] != "", "match_flag"] = "Partial_match"

            time_end = time.time()
            time_delta = round(time_end - time_start, 2)

            logger.app_info(f"Time for validate for loop for serial numbers : {time_delta}secs.", 2)

            # Step 5: Change partial_match to True so that we don't filter rows in further process
            # Rename the "match_flag" column to "flag_validinstall"
            df_contract.rename(
                columns={"match_flag": "flag_validinstall"}, inplace=True
            )

            # Change "partial_match" values to True
            df_contract.loc[
                ((df_contract["flag_validinstall"] == "Partial_match") | (df_contract["flag_validinstall"] == True)), "flag_validinstall"
            ] = True

            if "partial_matches" in df_contract.columns:
                df_contract["partial_matches"] = df_contract["partial_matches"].fillna(
                    df_contract["SerialNumber"]
                )
                df_contract = df_contract.explode("partial_matches")
                df_contract = df_contract.rename(
                    columns={"partial_matches": "SerialNumber_Partial","SerialNumber": "SerialNumberExact"}
                )

            else:
                df_contract["SerialNumber_Partial"] = df_contract["SerialNumber"].copy()

            if "is_srnum_range" in df_contract:
                df_contract.drop(["is_srnum_range","no_of_sep",	"sr_num_split","go4partmatch","is_partial"], axis=1, inplace=True)
            else:
                df_contract.drop(["no_of_sep",	"sr_num_split","go4partmatch","is_partial"], axis=1, inplace=True)
            df_contract["SerialNumber"] = df_contract[["SerialNumber_Partial", "SerialNumberExact"]].apply(lambda x: x[0] if pd.notna(x[0]) and x[0] != '' else x[1], axis=1)

            df_contract = df_contract[pd.notna(df_contract["SerialNumber"])]

            IO.write_csv(
                self.mode,
                {
                    "file_dir": self.config["file"]["dir_results"]
                    + self.config["file"]["dir_validation"],
                    "file_name": self.config["file"]["Processed"]["contracts"][
                        "contract_srnum_validation"
                    ],
                    "adls_config": self.config["adls"]["Processed"]["adls_credentials"],
                    "adls_dir": self.config["adls"]["Processed"]["contracts"][
                        "contract_srnum_validation"
                    ],
                },
                df_contract
            )

            logger.app_success(_step)
        except Exception as excp:
            logger.app_info(f"The error message generated inside function validate_contract_install_sr_num inside class_contracts_data.py is {str(excp)} ")
            logger.app_info(_step, f"{traceback.print_exc()}")
            raise Exception from excp

        return df_contract

    def pipeline_renewal(self) -> pd.DataFrame:  # pragma: no cover
        """
         Pipeline to pre-process renewal data.

        :raises Exception: Raised if unknown data type provided.
        :return: processed contracts data with serial numbers.
        :rtype: pandas dataframe.

        """
        try:
            _step = "Read renewal data"
            df_renewal = IO.read_csv(
                self.mode,
                {
                    "file_dir": self.config["file"]["dir_data"],
                    "file_name": self.config["file"]["Raw"]["renewal"]["file_name"],
                    "adls_config": self.config["adls"]["Raw"]["adls_credentials"],
                    "adls_dir": self.config["adls"]["Raw"]["renewal"],
                },
            )
            logger.app_success(_step)
            input_format = self.config["database"]["renewal"]["Dictionary Format"]
            df_renewal = self.format.format_data(df_renewal, input_format)
            df_renewal.reset_index(drop=True, inplace=True)
            _step = "Preprocess data"
            df_renewal["Contract_Amount"] = df_renewal["Contract_Amount"].fillna(0)
            df_renewal["Contract Years"] = (
                pd.to_datetime(df_renewal["Contract_Expiration_Date"])
                - pd.to_datetime(df_renewal["Contract_Start_Date"])
            ).dt.days
            df_renewal["Contract Years"] = df_renewal["Contract Years"] / 365
            df_renewal["Contract Years"] = df_renewal["Contract Years"].round(1)
            IO.write_csv(
                self.mode, {
                    'file_dir': (self.config['file']['dir_results'] +
                                 self.config['file']['dir_validation']),
                    'file_name': 'v4_contract_renewel.csv',
                    "adls_config": self.config["adls"]["Processed"]["adls_credentials"],
                    "adls_dir": self.config["adls"]["Processed"]["contracts"]["contract_renewal"]
                    
                    },
                df_renewal)

            logger.app_success(self.preprocess_renewal)
        except Exception as excp:
            logger.app_info(self.preprocess_renewal, f"{traceback.print_exc()}")
            raise Exception from excp

        return df_renewal

    def pipeline_decode_contract_type(self, df_contract) -> pd.DataFrame:
        """
        Decode Installbase and Contract data.

        :param df_contract: Dataframe with possible startup date fields in the sequence if Priority,
        :type df_contract: pandas DataFrame.
        :raises Exception: Raised if unknown data type provided.
        :return: Decode installbase and contract data.
        :rtype: pandas Data Frame

        """
        _step = "Decode Contract Type"
        try:
            # Decode : Service Plan
            df_contract = self.decode_contract_data(df_contract)

            # Decode : Installbase
            df_install_temp = IO.read_csv(
                self.mode,
                {
                    "file_dir": self.config["file"]["dir_data"],
                    "file_name": self.config["file"]["Raw"]["M2M"]["file_name"],
                    "adls_config": self.config["adls"]["Raw"]["adls_credentials"],
                    "adls_dir": self.config["adls"]["Raw"]["M2M"],
                },
            )
            df_install_temp = df_install_temp[["SO", "Description"]]

            df_contract = self.decode_installbase_data(df_install_temp,
                                                       df_contract)

            logger.app_success(_step)

            return df_contract
        except Exception as excp:
            logger.app_info(" Pipeline contract decode failed ")
            logger.app_info(_step, f"{traceback.print_exc()}")
            raise Exception from excp

    # ***** Support Codes : Contract *****
    def id_startup(self, df_startup) -> pd.DataFrame:
        """
        Identify if EATON started up the product.

        :param df_startup: Dataframe with possible startup date
        fields in the sequence if Priority,
        :type df_startup: pandas DataFrame.
        :raises Exception: Raised if unknown data type provided.
        :return: Data Frame with two columns:
            was_startedup : Flag indicating if product has StartUp.
            startup_date : Date when the product is started up.
        :rtype: pandas Data Frame

        """

        df_startup_org = df_startup.copy()
        del df_startup
        _step = f"{' ' * 5}Identify Start-up"
        try:

            ls_cols_startup = df_startup_org.columns
            for ix_col in range(len(ls_cols_startup)):
                col = ls_cols_startup[ix_col]

                if ix_col == 0:
                    df_startup_org.loc[:, "startup_date"] = df_startup_org[col]
                else:
                    df_startup_org.loc[:, "startup_date"] = df_startup_org[
                        "startup_date"
                    ].fillna(df_startup_org[col])

                logger.app_debug(
                    f"# NAs in Startup: "
                    f"{pd.isna(df_startup_org['startup_date']).value_counts()[True]}"
                )

            df_startup_org["was_startedup"] = pd.notna(df_startup_org["startup_date"])
            df_startup_org = df_startup_org[["was_startedup", "startup_date"]]

            IO.write_csv(
                self.mode,
                {
                    "file_dir": self.config["file"]["dir_results"]
                    + self.config["file"]["dir_validation"],
                    "file_name": "contract_startup_validation.csv",
                    "adls_config": self.config["adls"]["Processed"]["adls_credentials"],
                    "adls_dir": self.config["adls"]["Processed"]["contracts"]["contract_startup_validation"],
                },
                df_startup_org,
            )

        except Exception as excp:
            logger.app_info(_step, f"{traceback.print_exc()}")
            raise Exception('f"{_step}: Failed') from excp

        return df_startup_org

    def decode_contract_data(self, df_contract) -> pd.DataFrame:
        """
        Decode contract Service Plan data.

        :param df_contract: Dataframe with possible service plan data
        :type df_contract: pandas DataFrame.
        :raises Exception: Raised if unknown data type provided.
        :return: Data Frame with decoded service plan
        :rtype: pandas Data Frame

        """
        _step = "Decode Contract Service Data"
        try:
            df_contract.Service_Plan = df_contract.Service_Plan.str.lower()
            df_contract["Eaton_ContractType"] = ""
            for type_ in self.dict_contract:
                # type_ = 'Gold'
                df_contract["flag_ided"] = df_contract["Service_Plan"].str.contains(
                    str.lower(type_)
                )
                df_contract["flag_blank"] = df_contract["Eaton_ContractType"] == ""
                df_contract["flag"] = (
                    df_contract["flag_blank"] & df_contract["flag_ided"]
                )
                df_contract.loc[
                    df_contract["flag"], "Eaton_ContractType"
                ] = self.dict_contract[type_]
            df_contract = df_contract.drop(["flag_ided", "flag_blank", "flag"], axis=1)

            IO.write_csv(
                self.mode,
                {
                    "file_dir": self.config["file"]["dir_results"]
                    + self.config["file"]["dir_validation"],
                    "file_name": "contract_decode_validation.csv",
                    "adls_config": self.config["adls"]["Processed"]["adls_credentials"],
                    "adls_dir": self.config["adls"]["Processed"]["contracts"]["contract_decode_validation"],
                },
                df_contract,
            )

        except KeyError as excp:
            logger.app_info(_step, f"{traceback.print_exc()}")
            raise KeyError('f"{_step}: Failed') from excp

        return df_contract

    def decode_installbase_data(self, df_install_temp,
                                df_contract) -> pd.DataFrame:
        """
        Decode Installbase description data.

        :param df_install_temp: Dataframe with description and SO data
        :type df_install_temp: pandas DataFrame.
        :param df_contract: Dataframe with decoded service plan data
        :type df_contract: pandas DataFrame.
        :raises Exception: Raised if unknown data type provided.
        :return: Data Frame with decoded installbase plan
        :rtype: pandas Data Frame

        """
        _step = "Decode Installbase description data"
        try:
            df_install_temp["Description"] = (
                df_install_temp["Description"]
                .str.replace(" ", "")
                .str.lower()
                .str.replace(")", "")
            )

            # Decode Contracts from
            df_install_temp.loc[:, "Eaton_ContractType_M2M"] = ""

            for type_ in self.dict_decode_contract:
                # type_ = 'gold'
                pat_contract = self.dict_decode_contract[type_]
                df_install_temp["flag_ided"] = df_install_temp["Description"].apply(
                    lambda x: re.search(pat_contract, str(x)) is not None
                )

                df_install_temp["flag_blank"] = (
                    df_install_temp["Eaton_ContractType_M2M"] == ""
                )

                df_install_temp["flag"] = (
                    df_install_temp["flag_blank"] & df_install_temp["flag_ided"]
                )

                df_install_temp.loc[
                    df_install_temp["flag"], "Eaton_ContractType_M2M"
                ] = type_

            df_install_temp = df_install_temp.loc[
                df_install_temp["Eaton_ContractType_M2M"] != "",
                ["SO", "Eaton_ContractType_M2M"],
            ]

            df_contract["Original_Sales_Order__c"] = df_contract[
                "Original_Sales_Order__c"
            ].astype(str)
            df_install_temp["SO"] = df_install_temp["SO"].astype(str)
            df_contract = df_contract.merge(
                df_install_temp,
                left_on="Original_Sales_Order__c",
                right_on="SO",
                how="left",
            )

            df_contract["flag_update"] = df_contract["Eaton_ContractType"] == ""

            df_contract.loc[
                df_contract["flag_update"], "Eaton_ContractType"
            ] = df_contract.loc[df_contract["flag_update"], "Eaton_ContractType_M2M"]

            IO.write_csv(
                self.mode,
                {
                    "file_dir": self.config["file"]["dir_results"]
                    + self.config["file"]["dir_validation"],
                    "file_name": "v4_contract_decode_installbase.csv",
                    "adls_config": self.config["adls"]["Processed"]["adls_credentials"],
                    "adls_dir": self.config["adls"]["Processed"]["contracts"]["contract_decode"],
                },
                df_contract,
            )


        except KeyError as excp:
            logger.app_info(_step, f"{traceback.print_exc()}")
            raise KeyError('f"{_step}: Failed') from excp

        return df_contract

    def read_processed_installbase(self) -> pd.DataFrame:  # pragma: no cover
        """
        Read processed installbase data.

        :raises Exception: Raised if unknown data type provided.
        :return: processed installbase data.
        :rtype: pandas Data Frame

        """
        # Read : Installbase Processed data
        _step = "Read raw data : BOM"
        try:

            df_install = IO.read_csv(
                self.mode,
                {
                    "file_dir": self.config["file"]["dir_results"]
                    + self.config["file"]["dir_intermediate"],
                    "file_name": self.config["file"]["Processed"]["processed_install"][
                        "file_name"
                    ],
                    "adls_config": self.config["adls"]["Processed"]["adls_credentials"],
                    "adls_dir": self.config["adls"]["Processed"]["processed_install"],
                },
            )

        except Exception as excp:
            logger.app_info(_step, f"{traceback.print_exc()}")
            raise Exception('f"{_step}: Failed') from excp


        return df_install

    #  ***** Support Codes : Serial Number *****

    def sep_single_mul_srnum(
        self, df_serialnum, ls_cols
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Separates Single and Multiple Serial number data.

        :param df_serialnum: Dataframe having all serial number data,
        :type df_serialnum: pandas DataFrame.
        :param ls_cols: List of column names ,
        :type ls_cols: List.
        :raises Exception: Raised if unknown data type provided.
        :return df_out_sub_single, df_convert_rge: DataFrame having single
        serial number range,
         DataFrame having multiple serial number range
        :rtype: tuple

        """
        try:
            # Serial Number - Single
            df_out_sub_single = df_serialnum.loc[
                df_serialnum["is_single"], ls_cols
            ].copy()

            # Serial Number - Multiple
            df_convert_rge = df_serialnum.loc[
                ~df_serialnum["is_single"], ls_cols
            ].copy()
            return df_out_sub_single, df_convert_rge
        except Exception as excp:
            raise Exception from excp

    def concat_export_data(self, df_out_sub_single,
                           df_out_sub_multi) -> pd.DataFrame:
        """
        Concat and export Single and Multiple Serial number data.

        :param df_out_sub_single: Dataframe having single serial number data.
        :type df_out_sub_single: pandas DataFrame.
        :param df_out_sub_multi: Dataframe having multiple serial number data
        :type df_out_sub_multi: List.
        :raises Exception: Raised if unknown data type provided.
        :return df_out: Merged Data
        :rtype: pandas DataFrame

        """
        try:
            df_out = pd.concat([df_out_sub_single, df_out_sub_multi])
            df_out["SerialNumber"] = df_out["SerialNumber"].fillna(
                df_out["SerialNumberOrg"]
            )

            #df_out = df_out.drop_duplicates(subset=["SerialNumber"])
            df_out = df_out.drop_duplicates(subset=["ContractNumber", "SerialNumber"])

            return df_out
        except Exception as excp:
            raise Exception from excp

    def get_range_srum(self, df_temp_org, ar_key_serial="contracts") -> pd.DataFrame:
        """
        Clean and Merge expanded serial number to contract data.

        :param df_temp_org: Contract Data
        :type df_temp_org: pandas DataFrame
        :raises Exception: Raised if unknown data type provided.
        :return: Contracts data with extracted SerialNumbers:
        :rtype: pandas DataFrame

        """
        # df_temp_org = df_convert_rge[ls_cols].copy()
        _step = f"{' ' * 5}Identify Start-up"
        
        try:
            # Clean punctuation

            for char in self.dict_char:
                # char = list(dict_char.keys())[0]
                sep = self.dict_char[char]
                df_temp_org.loc[:, "SerialNumber"] = df_temp_org[
                    "SerialNumber"
                ].str.replace(f"{char}", sep, regex=True)

                df_temp_org.loc[:, "SerialNumber"] = df_temp_org["SerialNumber"].apply(
                    lambda x: re.sub(f"{sep}+", sep, str(x))
                )

            # Prep Data
            if "SerialNumberOrg" in df_temp_org.columns:
                df_temp_org.rename({"SerialNumberOrg":"SerialNumberOrg_old"}, axis=1, inplace=True)
            
            df_temp_org = df_temp_org.rename(
                columns={"SerialNumber": "SerialNumberOrg"}
            )
            df_temp_org["SerialNumberOrg"] = (
                df_temp_org["SerialNumberOrg"].astype(str).str.lower()
            )
            df_temp_org["SerialNumberOrg"] = df_temp_org["SerialNumberOrg"].apply(
                lambda x: x.lstrip(punctuation).rstrip(punctuation)
            )

            # Get Range

            df_expanded_srnum, _ = self.srnum.get_serialnumber(
                df_temp_org.SerialNumberOrg, df_temp_org.Qty, ar_key_serial, "contracts"
            )

            df_expanded_srnum["SerialNumberOrg"] = (
                df_expanded_srnum["SerialNumberOrg"].astype(str).str.lower()
            )

            df_temp_org = df_temp_org.merge(
                df_expanded_srnum, how="left", on="SerialNumberOrg"
            )

            logger.app_success(_step)

        except Exception as excp:
            logger.app_info(f"The error message is {str(excp)}")
            logger.app_info(_step, f"{traceback.print_exc()}")
            raise Exception('f"{_step}: Failed') from excp

        return df_temp_org

    def flag_serialnumber_wid_range(self, df_temp_org) -> pd.Series:
        """
        Identify if SerialNumber represents a range eg xxx-xxx-1-5 represents 5 serial numbers.

        :param df_temp_org: Serial number data
        :type df_temp_org: pd.DataFrame
        :raises Exception: Raised if unknown data type provided.
        :return: flag, having single serial number, true means it's single else it has range
        :rtype: pd.Series

        """
        _step = f"{' ' * 5}Identify serial numbers with range"

        try:
            # Based on quantity column from contracts
            df_temp_org.loc[:, "flag_qty"] = df_temp_org.Qty == 1

            # Patterns without range
            df_temp_org.loc[:, "flag_sr_type"] = df_temp_org.SerialNumber.apply(
                lambda x: re.search(self.pat_single_srnum, str(x) + " ") is not None
            )

            # Quantity is zero and has only one separator
            df_temp_org.loc[:, "flag_unknown_qty"] = df_temp_org.SerialNumber.apply(
                lambda x: len(re.findall(r"\-", x)) <= 2
            )

            df_temp_org.loc[:, "flag_unknown_qty"] = df_temp_org["flag_unknown_qty"] & (
                df_temp_org["Qty"] == 0
            )

            # Summarize Flags
            df_temp_org.loc[:, "flag_single"] = (
                df_temp_org["flag_qty"]
                | df_temp_org["flag_sr_type"]
                | df_temp_org["flag_unknown_qty"]
            )

            logger.app_success(_step)

        except Exception as excp:
            logger.app_info(_step, f"{traceback.print_exc()}")
            raise Exception('f"{_step}: Failed') from excp

        return df_temp_org["flag_single"]

    def filter_srnum(self, df_temp_org) -> pd.DataFrame:
        """
        Identify if SerialNumber represents a range eg xxx-xxx-1-5 represents 5 serial numbers.

        :param df_temp_org: DESCRIPTION
        :type df_temp_org: TYPE
        :raises Exception: Raised if unknown data type provided.
        :return: Filtered Serial number
        :rtype: pd.DataFrame

        """
        _step = "Filter Serial Numbers"
        try:

            df_temp_org = df_temp_org[pd.notna(df_temp_org.SerialNumber)]

            # Filter : Limit to DCPD products : STS / RPP / PDU
            df_temp_org.loc[:, "Product"] = self.bus_logic.idetify_product_fr_serial(
                df_temp_org["SerialNumber"]
            )
            df_temp_org = df_temp_org[df_temp_org["Product"] != ""]

            # Filter : Valid Serial Number
            df_temp_org["SerialNumber"] = df_temp_org["SerialNumber"].apply(
                lambda x: x.lstrip(punctuation).rstrip(punctuation)
            )
            df_temp_org.loc[:, "valid_sr"] = self.srnum.validate_srnum(
                df_temp_org["SerialNumber"]
            )
            df_temp_org = df_temp_org.loc[df_temp_org["valid_sr"], :]

            # Filter : Mobile number (as they have similar patterns to Serial Numbers)
            df_temp_org["flag_mob"] = df_temp_org.SerialNumber.apply(
                lambda x: re.search(self.pat_mob, str(x)) is not None
            )
            df_temp_org = df_temp_org[df_temp_org["flag_mob"] == False]

            df_temp_org = df_temp_org[
                [
                    "SerialNumber",
                    "ContractNumber",
                    "SerialNumberContract",
                    "Qty",
                    "src",
                    "Product",
                ]
            ]
            df_temp_org = df_temp_org.reset_index(drop=True)
            logger.app_debug(f"{_step} : SUCCEEDED", 3)

        except Exception as excp:
            logger.app_info(_step, f"{traceback.print_exc()}")
            raise Exception('f"{_step}: Failed') from excp

        return df_temp_org

    #  ***** Data merge *****
    def merge_contract_and_renewal(self, df_contract, df_renewal) -> pd.DataFrame:
        """
         Merge contract data with renewal data.

        :param df_contract: contracts data for PDI from SaleForce.
        :type df_contract: pandas DataFrame.
        :param df_renewal: contracts data for PDI from SaleForce.
        :type df_renewal: pandas DataFrame.
        :raises Exception: Raised if unknown data type provided.
        :return: Merged Data
        :rtype: pd.DataFrame

        """
        try:
            df_contract['Contract']=df_contract['Contract'].astype(str).str.lower()
            df_renewal['Contract'] = df_renewal['Contract'].astype(str).str.lower()

            df_contract = df_contract.merge(df_renewal, on="Contract", how="left")

            logger.app_success(self.merge_data)

            # Drop rows where Contract_Start_Date in future, 2 Jan, 2024
            # df_contract = df_contract[
            #     df_contract['Contract_Start_Date'].dt.date <= datetime.now().date()
            # ]

            # Read Raw M2M Data
            df_raw_m2m = IO.read_csv(
                self.mode,
                {
                    "file_dir": self.config["file"]["dir_data"],
                    "file_name": self.config["file"]["Raw"]["M2M"]["file_name"],
                    "adls_config": self.config["adls"]["Raw"]["adls_credentials"],
                    "adls_dir": self.config["adls"]["Raw"]["M2M"],
                },
            )

            df_contract = self.get_billto_data(df_contract, df_raw_m2m)
            IO.write_csv(
                self.mode, {
                    'file_dir': (self.config['file']['dir_results'] +
                                 self.config['file']['dir_validation']),
                    'file_name': 'v4_contract_merge_contract_renewel_bill_to_data.csv',
                    "adls_config": self.config["adls"]["Processed"]["adls_credentials"],
                    "adls_dir": self.config["adls"]["Processed"]["contracts"]["contract_merge"]
                    },
                df_contract)
        except Exception as excp:
            logger.app_info(f" Pipeline contract  renewal failed {str(excp)} ")
            logger.app_info(self.merge_data, f"{traceback.print_exc()}")
            raise Exception from excp


        return df_contract

    def get_billto_data(self, df_contract, df_raw_m2m):
        """
        Update billto information for the contracts data from M2M data

        :param ref_install: Install base data
        :type ref_install: pandas dataframe
        :return: InstallBase data with updated BillTo information.
        :rtype: pandas dataframe

        """
        # Contract data billto information is not accurate. Therefore
        # Bill to information for quatract is queries from M2M data using "SO"
        # as key
        _step = "Query BillTo data"
        try:

            # M2M data preparation
            dict_rename = {
                "SO": "key_SO",
                "Customer": "BillingCustomer",
                # removed spaces example:  Sold to Street - SoldtoStreet
                "SoldtoStreet": "BillingAddress",
                "SoldtoCity": "BillingCity",
                "SoldtoState": "BillingState",
                "SoldtoZip": "BillingPostalCode",
                "SoldtoCountry": "BillingCountry",
            }

            df_raw_m2m = df_raw_m2m.rename(columns=dict_rename)

            ls_cols = list(dict_rename.values())
            df_raw_m2m = df_raw_m2m.loc[:, ls_cols]

            del ls_cols, dict_rename

            # Rename existing "BillTO" columns from contracts for validation
            ls_cols = df_contract.columns[
                df_contract.columns.str.contains("bill", case=False)
            ]

            dict_rename = {}
            for col in ls_cols:
                if col in df_contract.columns:
                    dict_rename[col] = col + "_old"
            df_contract = df_contract.rename(dict_rename, axis=1)
            del dict_rename

            # Query BillTo Information from M2M data
            obj_filt = Filter()
            df_contract.loc[:, "key_contract"] = obj_filt.prioratized_columns(
                df_contract, ["Contract_Sales_Order__c", "Original_Sales_Order__c"]
            )


            df_raw_m2m["key_SO"] = df_raw_m2m["key_SO"].astype(int).astype(str)
            df_contract = df_contract.loc[((df_contract["key_contract"] != '') & (df_contract["key_contract"] != 'none') & (df_contract["key_contract"] != 'nan')), :]
            df_contract["key_contract"] = df_contract["key_contract"].astype(int).astype(str)

            df_contract = df_contract.merge(
                df_raw_m2m, how="left", left_on="key_contract", right_on="key_SO"
            )

            # If SO are unavailable then use billing info from SalesForce
            ls_cols = df_raw_m2m.columns[
                df_raw_m2m.columns.str.contains("bill", case=False)
            ]
            for col in ls_cols:
                if f"{col}_old" in df_contract.columns:
                    df_contract.loc[:, col] = obj_filt.prioratized_columns(
                        df_contract, [col, f"{col}_old"]
                    )

            logger.app_success(_step)
        except Exception as excp:
            logger.app_info(f'exception raised in bill to {excp}')
            logger.app_info(_step, f"{traceback.print_exc()}")
            raise Exception from excp

        return df_contract

    def merge_contract_and_srnum(self, df_contract, df_contract_srnum) -> pd.DataFrame:
        """
        Merge Contract and Srnum Data.

        :param df_contract: contracts data for PDI from SaleForce.
        :type df_contract: pandas DataFrame.
        :param df_contract_srnum: contracts data for PDI from SaleForce.
        :type df_contract_srnum: pandas DataFrame.
        :raises Exception: Raised if unknown data type provided.
        :return: Merged Contract data with Srnum data.
        :rtype: pd.DataFrame

        """
        _step = "Merge Contract and Srnum Data"
        try:
            df_contract_srnum = df_contract_srnum.loc[
                df_contract_srnum.flag_validinstall, :
            ]

            # Prep Contract Serial Number
            # ls_cols = ["ContractNumber", "Product", "SerialNumber_Partial"]
            ls_cols = ["ContractNumber", "SerialNumber"]
            df_contract_srnum = df_contract_srnum.loc[
                df_contract_srnum.flag_validinstall, ls_cols
            ]

            df_conract = df_contract_srnum.merge(
                df_contract, on="ContractNumber", how="inner"
            )
            #df_conract = df_conract.rename(
            #    columns={"SerialNumber_Partial": "SerialNumber"}
            #)
            logger.app_success(_step)
            return df_conract

        except Exception as excp:
            logger.app_info(_step, f"{traceback.print_exc()}")
            raise Exception from excp

    def merge_contract_install(self, df_contract=None, df_install=None):
        """
        This method summarizes the contract dataframe and then merges this with install base data.

        :param df_contract: contracts data for PDI from SaleForce.
        :type df_contract: pandas DataFrame.
        :param df_install: install base data
        :type df_install: pandas dataframe
        """

        _step = "Merging contract and install base data"
        try:
            if df_install is None:
                df_install = self.read_processed_installbase()
                df_install = df_install[pd.notna(df_install["SerialNumber_M2M"])]
                df_install.loc[:, "SerialNumber"] = df_install.SerialNumber_M2M.astype(
                    str
                )
                # handling single character case in SerialNumber col "111-0000-1a"
                df_install["SerialNumber"] = df_install["SerialNumber"].apply(
                    lambda x: re.sub(r"-(\d{1})[a-zA-Z]$", r"-\1", x)
                )

            df_install["SO"] = df_install["SO"].astype(str)
            # df_contract_with_na_for_sn = df_contract[pd.isna(df_contract["SerialNumber"])]

            #Replace leading zeros in contract as well as in installbase before performing exact and partial match
            #If serial numbers begins only with zero i.e. 0000-111-12 then do not replace leading zeros else replace leading zeros otherwise replace leading zeros
            pattern = re.compile("^0+(?!$)")
            df_install["SerialNumber"] = df_install["SerialNumber"].apply(lambda x: x if re.sub(pattern, '', x).startswith('-') else re.sub(pattern, '', x))
            df_contract["SerialNumber"] = df_contract.SerialNumber.astype(str)
            df_contract["SerialNumber"] = df_contract["SerialNumber"].apply(lambda x: x if re.sub(pattern, '', x).startswith('-') else re.sub(pattern, '', x))
            
            try:
                ls_prep_contract_cols = self.config["contracts"]["config_cols"][
                    "prep_contract_col_install"
                ]
                processed_contract = df_contract.loc[:, ls_prep_contract_cols]
            except KeyError as _:
                processed_contract = df_contract

            processed_contract = processed_contract.drop_duplicates(
                subset=None, keep="first"
            )

            # Convert date columns to datetime format
            date_columns = self.config["contracts"]["config_cols"][
                "contract_date_cols"]
            for column in date_columns:
                processed_contract[column] = pd.to_datetime(
                    processed_contract[column], errors="coerce"
                )

            # Derive the "First_Contract_Start_Date" column
            processed_contract[
                "First_Contract_Start_Date"
            ] = processed_contract.groupby("SerialNumber")[
                "Contract_Start_Date"
            ].transform(
                lambda x: x.min()
            )

            mask = (processed_contract["Contract_Start_Date"].isna()) | (
                processed_contract.groupby("SerialNumber")[
                    "Contract_Start_Date"
                ].transform(max)
                == processed_contract["Contract_Start_Date"]
            )
            df_sorted = processed_contract[mask].reset_index(drop=True)

            # Reset the index of the new DataFrame
            df_sorted = df_sorted.reset_index(drop=True)
            df = df_sorted.drop_duplicates()

            # Derive the "Contract_Conversion" column
            df["Contract_Conversion"] = "No Warranty"

            for i, row in df.iterrows():
                if pd.notnull(row["Warranty_Expiration_Date"]) and pd.notnull(
                    row["Contract_Start_Date"]
                ):
                    diff = (
                        row["First_Contract_Start_Date"]
                        - row["Warranty_Expiration_Date"]
                    )
                    if pd.notnull(diff) and diff.days > 180:
                        df.at[i, "Contract_Conversion"] = "New Business"
                    else:
                        df.at[i, "Contract_Conversion"] = "Warranty Conversion"
                elif pd.notnull(row["Warranty_Expiration_Date"]) and pd.isnull(
                    row["Contract_Start_Date"]
                ):
                    diff = datetime.now() - row["Warranty_Expiration_Date"]
                    if pd.notnull(diff) and diff.days <= 180:
                        df.at[i, "Contract_Conversion"] = "Warranty Due"
                    else:
                        df.at[i, "Contract_Conversion"] = "No Contract"
            
            # Renaming SO from df to SO_contract so that it doesn't clash with SO from df_install, 2 Jan, 2024
            df.rename(columns={"SO": "SO_contract"},inplace=True)

            # df_install = df_install.merge(
            #     df,
            #     right_on="Original_Sales_Order__c",
            #     left_on="SO",
            #     how="left"
            # )
            #
            merge_df = pd.merge(df_install, df, on='SerialNumber', how='left')

            # merge_df = pd.merge(df_install, df, how='left', left_on=['SerialNumber', 'SO'],
            #                   right_on=['SerialNumber', 'Original_Sales_Order__c'])

            merge_df.loc[:, "was_startedup"] = merge_df["was_startedup"].fillna(False)
            merge_df["Contract_Conversion"] = merge_df["Contract_Conversion"].fillna(
                "No Warranty / Contract"
            )

            # Dropped duplicate values for SO and SerialNumber_M2M, 2 Jan, 2024
            merge_df = merge_df.drop_duplicates(subset=[
                'SO', 'SerialNumber_M2M'
            ])
            merge_df = merge_df.replace("\n", " ", regex=True).replace(
                "\r", " ", regex=True)

            logger.app_success(_step)

            return merge_df
        except Exception as excp:
            logger.app_info(" merge contracts failed ")
            logger.app_info(_step, f"{traceback.print_exc()}")
            raise Exception('f"{_step}: Failed') from excp


# %% *** Call ***

if __name__ == "__main__":
    obj = Contract()
    obj.main_contracts()
# %%
