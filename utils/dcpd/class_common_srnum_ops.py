"""@file class_common_srnum_ops.py.

@brief : Common functionalities that can be used between processing of different data source.


@details :
    While Processing data for DCPD for different source there are some common processing
    functionalities that is re-usable for contracts, services and install base data.
    This python file has all those common reusable modules.


@copyright 2023 Eaton Corporation. All Rights Reserved.
@note Eaton Corporation claims proprietary rights to the material disclosed
here on. This technical information may not be reproduced or used without
direct written permission from Eaton Corporation.
"""

# %% ***** Setup Environment *****
# import all required modules
import re
import traceback
from string import punctuation
import numpy as np
import pandas as pd
from utils import IO
import os
import json
from utils import AppLogger

logger = AppLogger(__name__)


class SearchSrnum:
    """Class process and expands Contract Serial NUmber."""

    def __init__(self):
        """Class will extract and process contract data and renewal data."""

        config_dir = os.path.join(os.path.dirname(__file__), "../../config")
        config_file = os.path.join(config_dir, "config_dcpd.json")

        # Read the configuration file
        with open(config_file,'r') as config_file:
            self.config = json.load(config_file)

        self.mode = self.config.get("conf.env", "azure-adls")
        logger.app_info(f'mode fetched in SearchSrnum: {self.mode}')
        self.pat_srnum1 = self.config["contracts"]["srnum_pattern"]["pat_srnum1"]
        self.pat_srnum_services = self.config["contracts"]["srnum_pattern"][
            "pat_srnum_services"
        ]
        self.dict_cols_srnum = self.config["services"]["SerialNumberColumns"]
        self.pat_srnum = self.config["contracts"]["srnum_pattern"]["pat_srnum"]
        self.dict_srnum_cols = self.config["contracts"]["config_cols"][
            "dict_srnum_cols"
        ]
        self.prep_srnum_cols = self.config["contracts"]["config_cols"][
            "prep_srnum_cols"
        ]
        self.prep_srnum_cols_services = self.config["services"]["config_cols"][
            "prep_srnum_cols"
        ]

        self.ls_char = [" ", "-"]

    def clean_serialnum(self, df_srnum) -> pd.DataFrame:
        """
        Clean serial number data.

        :param df_srnum:  Data to be cleaned
        :type: pd.DataFrame

        :raises ValueError: raises error if unknown data type provided.
        :return: cleaned serial number data.
        :rtype: pd.DataFrame.

        """
        try:
            df_srnum["SerialNumberContract"] = df_srnum["SerialNumberContract"].fillna("")
            df_srnum["SerialNumberContract"] = df_srnum[
                "SerialNumberContract"
            ].apply(lambda x: re.sub(r"\(", " (", str(x)))          
            # Format - Punctuation
            for char in self.ls_char:
                df_srnum["SerialNumberContract"] = df_srnum[
                    "SerialNumberContract"
                ].apply(lambda x: re.sub(f"{char}+", "-", str(x)))

            # Format - Punctuation
            df_srnum["SerialNumberContract"] = df_srnum["SerialNumberContract"].apply(
                lambda x: x.lstrip(punctuation).rstrip(punctuation)
            )

            return df_srnum
        except Exception as excp:
            raise ValueError from excp

    def search_srnum(self, df_temp_org) -> pd.DataFrame:
        """
        Extract Serial Number from fields.

        :param df_temp_org: Contract data
        :type df_temp_org: pandas DataFrame
        :raises Exception: Raised if unknown data type provided.
        :return: Contracts data with extracted SerialNumbers:
        :rtype: pandas DataFrame

        """
        _step = "Extract Serial Number from fields"

        try:

            # Input
            sep = " "

            # Initialize Output
            df_serialnum = pd.DataFrame()

            # Prepare Data
            df_temp_org = self.prepare_srnum_data(df_temp_org)

            # PDI Salesforce has 4 fields with SerialNumber data.
            # Extract SerialNumber data from these fields.
            for cur_field in self.dict_srnum_cols:

                cur_qty = self.dict_srnum_cols[cur_field]

                # TODO: Take ContractNumber "key" as an input to this method.
                df_data = df_temp_org[[cur_field, cur_qty, "ContractNumber"]].copy()
                df_data.columns = ["SerialNumberContract", "Qty", "ContractNumber"]

                # Format - Punctuation
                # df_data = self.clean_serialnum(df_data)

                df_data.loc[:, "SerialNumber"] = self.prep_data_services(
                    df_data[["SerialNumberContract"]], sep
                )
                df_data['SerialNumber'] = df_data['SerialNumber'].fillna("")
                df_data['SerialNumber'] = df_data['SerialNumber'].apply(
                    lambda x: re.sub(r"\n|\r", ' ', x))

                df_data = df_data.reset_index(drop=True)
                ls_out_col = []
                ls_pats = self.pat_srnum
                for ix_pat in range(len(ls_pats)):
                    # Inputs
                    n_col_out = f"pat_match_{ix_pat}"
                    ls_out_col.append(n_col_out)
                    repat_sr_num = ls_pats[ix_pat]
                    repat_sr_num = re.compile(repat_sr_num)

                    # Find patterns
                    df_data[n_col_out] = df_data['SerialNumber'].apply(
                        lambda x: re.findall(repat_sr_num, str(x)))

                    # Remove matched strings from text
                    # a-b (c, d) will be matched by both the patterns.
                    # Therefore,  what is found as 1st pattern will be removed from original text
                    df_data['SerialNumber'] = df_data['SerialNumber'].apply(
                        lambda x: re.sub(repat_sr_num, " ", x))

                ix = 0
                for ix in range(len(ls_out_col)):
                    n_col = ls_out_col[ix]
                    if ix == 0:
                        df_data.loc[:, 'ls_sr_num'] = df_data[n_col].copy()
                    else:
                        df_data['ls_sr_num'] = df_data[['ls_sr_num', n_col]].apply(
                            lambda x: x[0] + x[1], axis=1)

                df_data['n_sr_num'] = df_data['ls_sr_num'].apply(
                    lambda x: len(x))

                # Results
                df_ls_collapse = df_data.explode('ls_sr_num')
                df_ls_collapse['len'] = df_ls_collapse['ls_sr_num'].str.len()
                df_ls_collapse["src"] = cur_field

                df_serialnum = pd.concat([df_serialnum, df_ls_collapse])

                                                                                
                del df_data, df_ls_collapse

            df_serialnum = df_serialnum.reset_index(drop=True)

            df_serialnum = df_serialnum.rename(
                {
                'SerialNumber': 'SerialNumber_old',
                'ls_sr_num': 'SerialNumber'}, axis=1)

            logger.app_success(_step)

        except Exception as excp:
            logger.app_fail(_step, f"{traceback.print_exc()}")
            raise Exception('f"{_step}: Failed') from excp

        return df_serialnum

    def prep_data(self, df_temp_org, sep) -> pd.Series:
        """
        Clean serial number fields before individual SerialNumber can be identified.

        :param df_temp_org: contracts data for PDI from SaleForce.
        :type df_temp_org: pandas DataFrame.
        :param sep: character used for separating two serial numbers
        :type sep: string
        :raises Exception: Raised if unknown data type provided.
        :return: Cleaned Serial number
        :rtype: pd.Series

        """
        _step = "Preparing Serial Number for Expansion"
        try:
            df_temp_org.columns = ["SerialNumber"]

            # Clean punctuation
            ls_char = ["\r", "\n"]  # '\.', , '\;', '\\'
            for char in ls_char:
                # char = ls_char[3]
                df_temp_org.loc[:, "SerialNumber"] = df_temp_org[
                    "SerialNumber"
                ].str.replace(char, sep, regex=True)

                df_temp_org.loc[:, "SerialNumber"] = df_temp_org["SerialNumber"].apply(
                    lambda row_data: re.sub(f"\{char}+", sep, row_data)
                )

            # Collapse multiple punctuations
            df_temp_org.loc[:, "SerialNumber"] = df_temp_org["SerialNumber"].apply(
                lambda col_data: re.sub(f"{sep}+", sep, col_data)
            )
            df_temp_org.loc[:, "SerialNumber"] = df_temp_org["SerialNumber"] + sep
        except Exception as excp:
            logger.app_fail(_step, f"{traceback.print_exc()}")
            raise Exception from excp
        return df_temp_org["SerialNumber"]

    def expand_srnum(self, col_data, pat_srnum) -> pd.DataFrame:
        """
        Expand SerialNumbers.

        :param col_data: Serial Number data
        :type col_data: TYPE
        :param pat_srnum: Pattern to identify Serial Number
        :type pat_srnum: String
        :raises Exception: Raised if unknown data type provided.
        :return: dataframe having expanded serial number
        :rtype: pd.DataFrame

        """
        _step = "Expand Serial Number"
        try:
            if not col_data["is_serialnum"]:
                df_srnum = pd.DataFrame(
                    data={
                        "SerialNumber": np.nan,
                        "ContractNumber": col_data["ContractNumber"],
                        "SerialNumberContract": col_data["SerialNumberContract"],
                        "Qty": col_data["Qty"],
                    },
                    index=[0],
                )
                return df_srnum

            sr_num = col_data["SerialNumber"]

            ls_sr_num = []

            for cur_srnum_pat in pat_srnum:
                ls_sr_num_cur = re.findall(cur_srnum_pat, str(sr_num))

                if len(ls_sr_num_cur) > 0:
                    ls_sr_num = ls_sr_num + ls_sr_num_cur
                    sr_num = re.sub(cur_srnum_pat, "", sr_num)

                if len(sr_num) <= 2:
                    break

            df_srnum = pd.DataFrame(data={"SerialNumber": ls_sr_num})
            df_srnum["ContractNumber"] = col_data["ContractNumber"]
            df_srnum["SerialNumberContract"] = col_data["SerialNumberContract"]
            df_srnum["Qty"] = col_data["Qty"]
        except Exception as excp:
            logger.app_fail(_step, f"{traceback.print_exc()}")
            raise Exception from excp

        return df_srnum

    def prepare_srnum_data(self, data):
        """
        Prepare Srnum Data.

        :param data: contracts data for PDI from SaleForce.
        :type data: pandas DataFrame..
        :raises Exception: Raised if unknown data type provided.
        :return: Prepared Srnum data.
        :rtype: pd.DataFrame

        """
        _step = "Prepare Srnum Data"
        try:
            data[self.prep_srnum_cols] = data[self.prep_srnum_cols].fillna(0)
            data["Qty_comment"] = data[self.prep_srnum_cols].apply(
                lambda x: x[3] - (x[0] + x[1] + x[2]), axis=1
            )
            return data
        except Exception as excp:
            logger.app_fail(_step, f"{traceback.print_exc()}")
            raise Exception from excp

    def search_srnum_services(self, df_temp_org) -> pd.DataFrame:
        """
        Extract Serial Number from fields.

        :param df_temp_org: Contract data
        :type df_temp_org: pandas DataFrame
        :raises Exception: Raised if unknown data type provided.
        :return: Contracts data with extracted SerialNumbers:
        :rtype: pandas DataFrame

        """
        _step = "Extract Serial Number from fields"

        try:
            # Input
            sep = " "

            # Initialize Output
            df_serialnum = pd.DataFrame()

            # Prepare Data

            df_temp_org = self.prepare_srnum_data_services(df_temp_org)

            # PDI Salesforce has 4 fields with SerialNumber data.
            # Extract SerialNumber data from these fields.
            for cur_field in self.dict_cols_srnum:

                cur_qty = self.dict_cols_srnum[cur_field]

                df_data = df_temp_org[[cur_field, cur_qty, "ContractNumber"]].copy()
                df_data.columns = ["SerialNumberContract", "Qty", "ContractNumber"]

                # Format - Punctuation
                # df_data = self.clean_serialnum(df_data)

                df_data.loc[:, "SerialNumber"] = self.prep_data_services(
                    df_data[["SerialNumberContract"]], sep
                )

                df_data['SerialNumber'] = df_data['SerialNumber'].fillna("")
                df_data['SerialNumber'] = df_data['SerialNumber'].apply(
                    lambda x: re.sub(r"\n|\r", ' ', x))

                df_data = df_data.reset_index(drop=True)
                ls_out_col = []
                ls_pats = self.pat_srnum_services
                for ix_pat in range(len(ls_pats)):
                    n_col_out = f"pat_match_{ix_pat}"
                    ls_out_col.append(n_col_out)
                    repat_sr_num = ls_pats[ix_pat]
                    repat_sr_num = re.compile(repat_sr_num)

                    # Find patterns
                    df_data[n_col_out] = df_data['SerialNumber'].apply(
                        lambda x: re.findall(repat_sr_num, str(x)))

                    # Remove matched strings from text
                    # a-b (c, d) will be matched by both the patterns.
                    # Therefore,  what is found as 1st pattern will be removed from original text
                    df_data['SerialNumber'] = df_data['SerialNumber'].apply(
                        lambda x: re.sub(repat_sr_num, " ", x))


                ix=0
                for ix in range(len(ls_out_col)):
                    n_col = ls_out_col[ix]
                    if ix == 0:
                        df_data.loc[:, 'ls_sr_num'] = df_data[n_col].copy()
                    else:
                        df_data['ls_sr_num'] = df_data[['ls_sr_num', n_col]].apply(
                             lambda x: x[0] + x[1], axis=1)

                df_data['n_sr_num'] = df_data['ls_sr_num'].apply(
                    lambda x: len(x))

                # Results
                df_ls_collapse = df_data.explode('ls_sr_num')
                df_ls_collapse['len'] = df_ls_collapse['ls_sr_num'].str.len()
                df_ls_collapse["src"] = cur_field

                df_serialnum = pd.concat([df_serialnum, df_ls_collapse])

                                                                                
                del df_data, df_ls_collapse

            df_serialnum = df_serialnum.reset_index(drop=True)

            df_serialnum = df_serialnum.rename(
                {"ContractNumber":"Id",
                 'SerialNumber': 'SerialNumber_old',
                 'ls_sr_num': 'SerialNumber'}, axis=1)

            logger.app_success(_step)

        except Exception as excp:
            logger.app_fail(_step, f"{traceback.print_exc()}")
            raise Exception('f"{_step}: Failed') from excp

        return df_serialnum

    def prepare_srnum_data_services(self, data):
        """
        Prepare Srnum Data for processing services data.

        :param data: contracts data for PDI from SaleForce.
        :type data: pandas DataFrame..
        :raises Exception: Raised if unknown data type provided.
        :return: Prepared Srnum data.
        :rtype: pd.DataFrame

        """
        _step = "Prepare Srnum Data"
        try:
            data[self.prep_srnum_cols_services] = data[
                self.prep_srnum_cols_services
            ].fillna(0)
            data["Qty_comment"] = data[self.prep_srnum_cols_services].apply(
                lambda x: (x[0] + x[1] + x[2]), axis=1
            )
            return data
        except Exception as excp:
            logger.app_fail(_step, f"{traceback.print_exc()}")
            raise Exception from excp

    def prep_data_services(self, df_temp, sep) -> pd.Series:
        """
        Clean serial number fields before individual SerialNumber can be identified.

        :param df_temp_org: contracts data for PDI from SaleForce.
        :type df_temp_org: pandas DataFrame.
        :param sep: character used for separating two serial numbers
        :type sep: string
        :raises Exception: Raised if unknown data type provided.
        :return: Cleaned Serial number
        :rtype: pd.Series

        """
        df_temp_org = df_temp.copy()
        del df_temp
        df_temp_org.columns = ["SerialNumber"]

        # Clean punctuation
        ls_char = ["\\r", "\\n", "\\\\",
                   '\.', '\;', "\\t"]
        for char in ls_char:

            # Changed type of df, casted values to str type
            df_temp_org["SerialNumber"] = df_temp_org["SerialNumber"].astype(str)


            df_temp_org.loc[:, "SerialNumber"] = df_temp_org["SerialNumber"].apply(
                lambda row_data: re.sub(f"{char}+", sep, row_data)
            )
        # Fix for 50046000000rAORAA2 411\x81-0129\x81-1\x81-12
        df_temp_org.loc[:, "SerialNumber"] = df_temp_org["SerialNumber"].apply(
            lambda col_data: re.sub('\\x81', '', col_data)
        )

        # replacing the space with hypen with hypen without space "180-2184 -5" and "180-2184 -2"
        df_temp_org.loc[:, "SerialNumber"] = df_temp_org["SerialNumber"].apply(
            lambda col_data: re.sub("\\s-",'-', col_data)
        )

        # Fix for 50046000000r9pjAAA replacing / with - for serial numbers "120-0229/1" or "120-0229/1 " while
        # retaining/preserving the / for 442-0013/442-0033 and STS/PDU if found.
        df_temp_org.loc[:, "SerialNumber"] = df_temp_org["SerialNumber"].apply(
            lambda col_data: re.sub('/(?=\\d\\s)', '-', col_data)
        )
        df_temp_org.loc[:, "SerialNumber"] = df_temp_org["SerialNumber"].apply(
            lambda col_data: re.sub('/(?=\\d$)', '-', col_data)
        )
        df_temp_org.loc[:, "SerialNumber"] = df_temp_org["SerialNumber"].apply(
            lambda row_data: re.sub("\\/+", sep, row_data)
        )

        # Collapse multiple punctuations
        df_temp_org.loc[:, "SerialNumber"] = df_temp_org["SerialNumber"].apply(
            lambda col_data: re.sub(f"{sep}+", sep, col_data)
        )
        df_temp_org.loc[:, "SerialNumber"] = df_temp_org["SerialNumber"] + sep

        return df_temp_org["SerialNumber"]
