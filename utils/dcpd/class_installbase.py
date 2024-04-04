"""
@file class_installbase.py.

@brief This module process below M2M Data:
    -Shipment Data
    -Serial Data
    -BOM Data

@details Shipment Data process/filter based on below steps:
            - Read Data and apply filters based on configuration:
                -Identify Product by Product Class
                -Filter out Data other than ProductClass
                -Create Foreign key and drops duplicates from data
        Serial Number Data process based on below steps:
            - Validate and Decode Serial Number Data
            - Convert Serial Number to unique Serial Number
            - Create foreign key based on Serial Number
        BOM Data process based on below steps:
            - Convert Range of Serial Number to Unique Serial Number



@copyright 2023 Eaton Corporation. All Rights Reserved.
@note Eaton Corporation claims proprietary rights to the material disclosed
here on. This technical information may not be reproduced or used without
direct written permission from Eaton Corporation.
"""

# %% ***** Setup Environment *****

import os
import re
import traceback
from string import punctuation
from typing import Tuple
import pandas as pd
import json
import numpy as np


config_dir = os.path.join(os.path.dirname(__file__), "../../config")
config_file = os.path.join(config_dir, "config_dcpd.json") 
with open(config_file,'r') as config_file:
    config = json.load(config_file)
mode = config.get("conf.env", "azure-adls")
if mode == "local":
    path = os.getcwd()
    path = os.path.join(path.split('ileads_lead_generation')[0], 'ileads_lead_generation')
    os.chdir(path)


from utils.dcpd.class_business_logic import BusinessLogic
from utils.dcpd.class_serial_number import SerialNumber
from utils.strategic_customer import StrategicCustomer
from utils import IO
from utils import AppLogger
logger = AppLogger(__name__)



from utils import Format
from utils import Filter

obj_srnum = SerialNumber()

obj_bus_logic = BusinessLogic()
obj_filters = Filter()
obj_format = Format()


# %%
class InstallBase:
    """This module process the M2M:Shipment Data, M2M:Serial Data, M2M BOM Data."""

    def __init__(self) -> pd.DataFrame:
        """Initialise environment variables, class instance and
        variables used throughout the modules."""

        # Insatance of class
        config_dir = os.path.join(os.path.dirname(__file__), "../../config")
        config_file = os.path.join(config_dir, "config_dcpd.json")
        with open(config_file, 'r') as config_file:
            self.config = json.load(config_file) 
        self.mode = self.config.get("conf.env", "azure-adls")
       
        # Steps
        self.step_main_install = "main install"
        self.step_install_base = "Process Install Base"
        self.step_serial_number = "Query Serial Number"
        self.step_bom_data = "Process BOM data"
        self.step_customer_number = "Query Customer Number"
        self.step_identify_strategic_customer = "Identify strategic customer"
        self.step_export_data = "Export data"
        self.unsuccessful = "unsuccessful !"

        # Variable
        self.ls_char = [" ", "-"]

        self.ls_priority = ["ShipTo_Country", "SoldTo_Country"]
        self.ls_cols_out = ["key_serial", "SerialNumber", "Product"]
        self.ls_cols_ref = ["ProductClass", "product_type", "product_prodclass"]

        # data_install = self.main_install()

        # return data_install

    def main_install(self) -> None:  # pragma: no cover
        """
        Process Data by running the M2M, Serial Number, BOM pipline to for M2M data.

        :return: processed and filtered DataFrame
        :rtype: CSV file

        """
        try:

            # Install Base
            df_install = self.pipeline_m2m()

            # Serial Number : M2M
            df_install = self.pipeline_serialnum(df_install, merge_type="inner")

            # BOM
            df_install = self.pipeline_bom(df_install, merge_type='left')

            # Customer Name
            df_install = self.pipeline_customer(df_install)

            # Filtering data based on product type, country
            filtered_data = self.filter_mtmdata(df_install)

            # Export
            IO.write_csv(
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
                filtered_data,
            )
            logger.app_success(self.step_export_data)

        except Exception as excp:
            logger.app_fail(self.step_main_install, f"{traceback.print_exc()}")
            raise ValueError from excp

    #  ******************* Support Pipelines *********************
    def filter_mtmdata(self, df_install):
        """
        Filter SO status, Country for output from df_install

        :return: processed and filtered DataFrame
        :rtype: Pandas df

        """
        p_validation = (self.config['file']['dir_results'] +
                        self.config['file']['dir_validation'])

        # Filter out SO where status is not closed.
        listcol_filter = ["closed"]
        df_install = df_install.loc[(df_install.SOStatus.isin(listcol_filter))]


        # Flag column values where flag is not in USA
        IO.write_csv(
            self.mode, {
                'file_dir': p_validation,
                'file_name': 'v4_install_outside_usa.csv',
                "adls_config": self.config["adls"]["Processed"]["adls_credentials"],
                "adls_dir": self.config["adls"]["Processed"]["processed_install"]["v4_install_outside"]},
            df_install.loc[~df_install.is_in_usa])

        IO.write_csv(
            self.mode, {
                'file_dir': p_validation,
                'file_name': 'v4_install_all_countries.csv',
                "adls_config": self.config["adls"]["Processed"]["adls_credentials"],
                "adls_dir": self.config["adls"]["Processed"]["processed_install"]["v4_install_all_countries"]
                },
            df_install)
        # 20231214: Commented as CTQ is based on all countries.
        if self.config['Country'] != "All":
            df_install = df_install[df_install.is_in_usa]

        # Drop Duplicate SO and Sr_num_m2m pairs: 8 Dec,23
        ls_cols4dup = ['SO', 'SerialNumber_M2M']
        df_validation = df_install.loc[df_install.duplicated(ls_cols4dup)]

        IO.write_csv(
            self.mode, {
                'file_dir': p_validation,
                'file_name': 'v4_install_dup_srnum_by_SO.csv',
                "adls_config": self.config["adls"]["Processed"]["adls_credentials"],
                "adls_dir": self.config["adls"]["Processed"]["processed_install"]["v4_install_dup_srnum_by_SO"]
                },
            df_validation)
        df_install = df_install.drop_duplicates(subset=ls_cols4dup)
        df_install = df_install.replace("\n", " ", regex=True).replace("\r", " ", regex=True)

        return df_install

    def pipeline_m2m(self) -> pd.DataFrame:  # pragma: no cover
        """
        Read Shipment Data and apply filters.

        raises Exception: None
        :return: df_data_install
        :rtype: pd.DataFrame
        """
        try:

            # This method will read csv data into pandas DataFrame
            df_data_install = IO.read_csv(
                self.mode,
                {
                    "file_dir": self.config["file"]["dir_data"],
                    "file_name": self.config["file"]["Raw"]["M2M"]["file_name"],
                    "adls_config": self.config["adls"]["Raw"]["adls_credentials"],
                    "adls_dir": self.config["adls"]["Raw"]["M2M"],
                },
            )

            input_format = self.config["database"]["M2M"]["Dictionary Format"]

            df_data_install = obj_format.format_data(df_data_install, input_format)

            df_data_install = self.get_metadata(df_data_install)

            df_data_install.reset_index(drop=True, inplace=True)

            # This block will prioritise 'ShipTo_Country' and 'SoldTo_Country'
            # into 'Country' column based on Null value
            dict_states_us = self.config['output_contacts_lead']["usa_states"]
            ls_state = list(dict_states_us.keys()) + list(
                dict_states_us.values())
            ls_state = [entry.lower() for entry in ls_state]
            df_data_install['us_states'] = (
                    (df_data_install['ShipTo_State'].isin(ls_state)) |
                    (df_data_install['SoldTo_State'].isin(ls_state))
            )
            df_data_install['Country'] = obj_filters.prioratized_columns(
                df_data_install[self.ls_priority], self.ls_priority)
            IO.write_csv(
                self.mode, {
                    'file_dir': (self.config['file']['dir_results'] +
                                 self.config['file']['dir_validation']),
                    'file_name': 'v4_install_metadata_and_country.csv',
                    "adls_config": self.config["adls"]["Processed"]["adls_credentials"],
                    "adls_dir": self.config["adls"]["Processed"]["processed_install"]["v4_install_metadata_and_country"]},
                df_data_install)

            # If country is blank, then use state to identify if asset is installed in us
            df_data_install['Country'] = df_data_install[
                ['Country', 'us_states']].apply(
                lambda row: "us" if ((row[0] == "") & row[1])
                else row[0], axis=1
            )
            ls_cols = df_data_install.columns.tolist()

            # Database level filters are applied as per configurations[config_database.json]
            df_data_install = obj_filters.filter_data(
                df_data_install, self.config["database"]["M2M"]["Filters"]
            )

            df_data_install = df_data_install.rename(
                columns={"flag_Country": "is_in_usa"}
            )

            # Decode product
            ref_prod = IO.read_csv(
                self.mode,
                {
                    "file_dir": self.config["file"]["dir_ref"],
                    "file_name": self.config["file"]["Reference"]["product_class"],
                    "adls_config": self.config["adls"]["Reference"]["adls_credentials"],
                    "adls_dir": self.config["adls"]["Reference"]["product_class"],
                },
            )


            # filters product class as per configurations[config_database.json]
            df_data_install, ls_cols = self.filter_product_class(
                ref_prod, df_data_install, ls_cols
            )


            # filters key_serial column
            df_data_install = self.drop_duplicate_key_serial(df_data_install, ls_cols)


            # Export processed file
            IO.write_csv(
                self.mode,
                {
                    "file_dir": self.config["file"]["dir_results"]
                    + self.config["file"]["dir_validation"],
                    "file_name": self.config["file"]["Processed"][
                        "processed_m2m_shipment"
                    ]["file_name"],
                    "adls_config": self.config["adls"]["Processed"]["adls_credentials"],
                    "adls_dir": self.config["adls"]["Processed"][
                        "processed_m2m_shipment"
                    ],
                },
                df_data_install,
            )

            logger.app_success(self.step_install_base)

            return df_data_install

        except Exception as excp:
            logger.app_fail(self.step_install_base, f"{traceback.print_exc()}")
            raise ValueError from excp

    def pipeline_serialnum(
        self, df_data_install: pd.DataFrame, merge_type: str
    ) -> pd.DataFrame:  # pragma: no cover
        """
        Validate and Decode the Serial Numbers from data.

        :param df_data_install: Data to be filtered
        :type: pd.DataFrame
        :param merge_type: It is custom based on requirement, Default is "inner":
            "left": use only keys from left frame,
            "right": use only keys from right frame,
            "outer": use union of keys from both frames,
            "inner": use intersection of keys from both frames.
            "cross": creates the cartesian product from both frames.
        :type: str
        :raises None: None
        :return df_data_install: Data with Validated and Decoded Serial number
        :rtype: pd.DataFrame

        """
        try:
            # Read and Filter Serial Number
            df_srnum_all = self.pipeline_process_serialnum(df_data_install)

            df_srnum = df_data_install[["key_serial"]].copy()

            # preprocess serial number before expanding
            df_srnum_range, df_srnum = self.preprocess_expand_range(
                df_srnum_all, df_srnum)


            """
            sort df_srnum_range by sr_num. e.g. we have serial numbers and qty as
            {a-b-5-7, 3) , {a-b-1-2, 2} , {a-b, 2}
            Then a-b should be assigned missing index 3 and 4.
            To achieve this; datais sorted by length of serial numbers to ensure that 
            serial numbers with index are covered first.
            """
            df_srnum_range['len'] = df_srnum_range['SerialNumber'].str.len()
            df_srnum_range = df_srnum_range.sort_values(
                by=['len', 'SerialNumber'], ascending=False
            )

            del df_srnum_all
            # edit later ** suguna - 2023-11
            # gets unique/repeated serial number data
            df_out, df_couldnot = obj_srnum.get_serialnumber(
                df_srnum_range["SerialNumber"],
                df_srnum_range["Shipper_Qty"],
                df_srnum_range["key_serial"],
            )

            # combined expanded serial number data with original data
            df_data_install = self.combine_serialnum_data(
                df_srnum_range,
                df_srnum,
                df_data_install,
                df_out,
                df_couldnot,
                merge_type,
            )


            logger.app_success(self.step_serial_number)
            return df_data_install

        except Exception as excp:
            logger.app_fail(self.step_serial_number, f"{traceback.print_exc()}")
            raise ValueError from excp

    def pipeline_process_serialnum(self, df_data_install) -> pd.DataFrame:  # pragma: no cover
        """
        Read and filter Serial Number Data.

        :raises Exception: Raised if unknown data type provided.
        :return df_srnum: Validated Serial number data
        :rtype: pd.DataFrame

        """
        try:
            # Read SerialNumber data

            df_srnum = IO.read_csv(
                self.mode,
                {
                    "file_dir": self.config["file"]["dir_data"],
                    "file_name": self.config["file"]["Raw"]["SerialNumber"][
                        "file_name"
                    ],
                    "adls_config": self.config["adls"]["Raw"]["adls_credentials"],
                    "adls_dir": self.config["adls"]["Raw"]["SerialNumber"],
                },
            )
            df_srnum["flag"] = df_srnum["Serial"].apply(lambda x: len(re.findall("[a-zA-Z0-9]+", str(x))) > 0)
            df_srnum = df_srnum.loc[df_srnum["flag"] == True, :]
            df_srnum.drop(["flag"], axis=1, inplace=True)

            # Format Data
            input_format = self.config["database"]["SerialNumber"]["Dictionary Format"]
            df_srnum = obj_format.format_data(df_srnum, input_format)
            df_srnum.reset_index(drop=True, inplace=True)

            # Format - Punctuation
            df_srnum = self.clean_serialnum(df_srnum)

            # foreign / parent keys : Serial Number
            df_srnum = self.create_foreignkey(df_srnum)

            # merge df_data_install['SOLine'] and sr_num on foriegn key
            # We drop duplicates for SO Line and sr_num pairs, 11 Dec, 23
            df_srnum = df_srnum.merge(
                df_data_install[['SOLine', 'key_serial']], on='key_serial', how='inner'
            )

            # Drop Duplicate SOLine and Sr_num_m2m pairs: 8 Dec,23
            df_srnum = df_srnum.drop_duplicates(subset=[
                'SOLine', 'SerialNumber'
            ])
            df_srnum = df_srnum.drop(columns=['SOLine'])

            # Serial (Filter : Product)
            df_srnum["Product"] = obj_bus_logic.idetify_product_fr_serial(
                df_srnum["SerialNumber"]
            )

            # Filter : Valid Serial Number
            df_srnum.loc[:, 'valid_sr'] = obj_srnum.validate_srnum(
                df_srnum['SerialNumber'])

            IO.write_csv(
                self.mode, {
                    'file_dir': (self.config['file']['dir_results'] +
                                 self.config['file']['dir_validation']),
                    'file_name': 'v4_install_droppedby_validate_srnum.csv',
                    "adls_config": self.config["adls"]["Processed"]["adls_credentials"],
                    "adls_dir": self.config["adls"]["Processed"]["processed_install"]["v4_install_droppedby_validate_srnum"]},
                df_srnum[~df_srnum['valid_sr']])

            df_srnum = df_srnum.loc[df_srnum['valid_sr'], :]

            # Export to csv
            IO.write_csv(
                self.mode,
                {
                    "file_dir": self.config["file"]["dir_results"]
                    + self.config["file"]["dir_validation"],
                    "file_name": self.config["file"]["Processed"][
                        "processed_serialnum"
                    ]["file_name"],
                    "adls_config": self.config["adls"]["Processed"]["adls_credentials"],
                    "adls_dir": self.config["adls"]["Processed"]["processed_serialnum"],
                },
                df_srnum,
            )

            return df_srnum
        except Exception as excp:
            logger.app_info(f'exception {excp}')
            logger.app_fail("filter product class", f"{traceback.print_exc()}")
            raise ValueError from excp

    def pipeline_customer(
        self, df_data_install: pd.DataFrame
    ) -> pd.DataFrame:  # pragma: no cover
        """
        Identify strategic customer from the data.

        :param df_data_install: Data to identify strategic customers
        :type:  pd.DataFrame
        :raises None: None
        :return df_install_data: Data with strategic customers
        :rtype:  pd.DataFrame

        """

        obj_sc = StrategicCustomer()

        df_customer = obj_sc.main_customer_list(df_leads=df_data_install)

        # Merge customer data with shipment, serial number and BOM data
        df_install_data = self.merge_customdata(df_customer, df_data_install)


        return df_install_data

    def id_metadata(self, df_bom):  # pragma: no cover
        """
        append rating/types column to the df_bom.

        :raises Exception: Raised if unknown data type provided.
        :return df_part_rating: Added rating column to the df_bom corresponding to part number
        :rtype: pd.DataFrame

        """
        try:
            df_ref_pdi = IO.read_csv(
                self.mode,
                {
                    "file_dir": self.config["file"]["dir_ref"],
                    "file_name": self.config["file"]["Reference"]["ref_sheet_pdi"],
                    "adls_config": self.config["adls"]["Reference"]["adls_credentials"],
                    "adls_dir": self.config["adls"]["Reference"]["ref_sheet_pdi"],
                },
            )

            # Drop duplicates in reference file
            df_ref_pdi = df_ref_pdi.drop_duplicates(
                subset="PartNumber_TLN_BOM", keep="first"
            )

            # Merge df_bom and df_ref_pdi
            df_part_rating = pd.merge(
                df_bom, df_ref_pdi, how="left", on="PartNumber_TLN_BOM"
            )

            logger.app_success(self.step_bom_data)
            return df_part_rating

        except Exception as excp:
            logger.app_fail(self.step_bom_data, f"{traceback.print_exc()}")
            raise ValueError from excp

    def id_main_breaker(self, df_data_org):
        """
        Append main breaker details to existing install base data.

        :raises Exception: Raised if unknown data type provided.
        :return df_part_rating: Added circuit breaker column to the df_bom corresponding to part number
        :rtype: pd.DataFrame

        """
        df_data = df_data_org.copy()
        del df_data_org
        ref_main_breaker = IO.read_csv(
            self.mode,
            {
                "file_dir": self.config["file"]["dir_ref"],
                "file_name": self.config["file"]["Reference"]["lead_opportunities"],
                "adls_config": self.config["adls"]["Reference"]["adls_credentials"],
                "adls_dir": self.config["adls"]["Reference"]["lead_opportunities"],
            },
        )

        export_cols = self.config["install_base"]["main_breaker_cols"][
            "export_col_data"
        ]

        ref_main_breaker = ref_main_breaker.loc[
            pd.notna(ref_main_breaker["Input CB"]), export_cols
        ]

        ref_main_breaker["PartNumber_BOM_BOM"] = ref_main_breaker[
            "PartNumber_BOM_BOM"
        ].str.lower()

        df_data = df_data[["Job_Index", "PartNumber_BOM_BOM"]].merge(
            ref_main_breaker, on="PartNumber_BOM_BOM", how="inner"
        )
        df_data = df_data.rename(columns={"PartNumber_BOM_BOM": "pn_main_braker"})
        return df_data

    def pipeline_bom(
        self, df_install: pd.DataFrame, merge_type: str
    ) -> pd.DataFrame:  # pragma: no cover
        """
        Convert Range of Serial number to Unique Serial number.

        :param df_install: Data to be filtered
        :type: pd.DataFrame
        :param merge_type: It is custom based on requirement, Default is "inner":
            "left": use only keys from left frame,
            "right": use only keys from right frame,
            "outer": use union of keys from both frames,
            "inner": use intersection of keys from both frames.
            "cross": creates the cartesian product from both frames.
        :type: str
        :raises None: None
        :return df_install: Data with unique Serial number
        :rtype: pd.DataFrame:

        """
        try:
            # Read SerialNumber data
            df_bom = IO.read_csv(
                self.mode,
                {
                    "file_dir": self.config["file"]["dir_data"],
                    "file_name": self.config["file"]["Raw"]["bom"]["file_name"],
                    "adls_config": self.config["adls"]["Raw"]["adls_credentials"],
                    "adls_dir": self.config["adls"]["Raw"]["bom"],
                },
            )
            # Format Data

            input_format = self.config["database"]["bom"]["Dictionary Format"]
            df_bom = obj_format.format_data(df_bom, input_format)

            df_bom.reset_index(drop=True, inplace=True)

            # Display Part Numbers
            df_display_parts = self.id_display_parts(df_bom)

            # Main Breakers
            df_main_breaker = self.id_main_breaker(df_bom)


            # merge BOM data with shipment and serial number data
            # df_install = self.merge_bomdata(df_bom, df_install, merge_type)

            # Drop Duplicates
            # df_bom = df_bom[ls_cols]
            df_bom = df_bom[["Job_Index", "PartNumber_TLN_BOM"]]
            df_bom = df_bom.drop_duplicates(
                subset=["Job_Index", "PartNumber_TLN_BOM"]
            ).reset_index(drop=True)

            df_bom = self.id_metadata(df_bom)


            # Merge main breaker data and display part number data
            df_bom = df_bom.merge(df_main_breaker, on="Job_Index", how="left")

            df_bom = df_bom.merge(df_display_parts, on="Job_Index", how="left")

            # df_bom = self.id_metadata(df_bom)

            # Merge Data
            df_install = df_install.merge(df_bom, on="Job_Index", how=merge_type)
            df_install.loc[:, "PartNumber_TLN_BOM"] = df_install[
                "PartNumber_TLN_BOM"
            ].fillna("")

            logger.app_success(self.step_bom_data)
            return df_install

        except Exception as excp:
            logger.app_fail(self.step_bom_data, f"{traceback.print_exc()}")
            raise ValueError from excp

    def filter_product_class(
        self, ref_prod: pd.DataFrame, df_data_install: pd.DataFrame, ls_cols
    ) -> Tuple[pd.DataFrame, list]:
        """
        Filter data based on Product Class.

        :param ref_prod: Used to filter Product Class
        :type: pd.DataFrame
        :param df_data_install: Data to be filtered
        :type: pd.DataFrame
        :param ls_cols: list of columns used for filter
        :type: list
        :raises Exception: Raised if unknown data type provided.
        :return  df_data_install, ls_cols: Filtered Data, list of column name
        :rtype: tuple

        """
        try:

            ref_prod = ref_prod[self.ls_cols_ref]
            for col in self.ls_cols_ref:
                ref_prod.loc[:, col] = ref_prod[col].str.lower()

            df_data_install = df_data_install.merge(
                ref_prod, on="ProductClass", how="left"
            )

            # Filter based on product type
            df_data_install = df_data_install[pd.notna(
                df_data_install.product_prodclass)]
            product_types = self.config['install_base']['product_type']
            df_data_install = df_data_install[df_data_install.product_prodclass
            .isin(product_types)
            ]
            ls_cols = ls_cols + ['product_type', 'product_prodclass', 'is_in_usa']

            return df_data_install, ls_cols
        except Exception as excp:
            logger.app_info(f"exception: 685: {excp}")
            logger.app_fail("filter product class", f"{traceback.print_exc()}")
            raise ValueError from excp

    def drop_duplicate_key_serial(self, df_data_install, ls_cols) -> pd.DataFrame:
        """
        Create Foreign key and drops duplicate rows.

        :param df_data_install:  Data to be filtered
        :type: pd.DataFrame
        :param ls_cols: list of columns of interest
        :type: list
        :raises ValueError: raises error if unknown data type provided.
        :return: filtered data based on ls_cols.
        :rtype: pd.DataFrame.

        """
        try:
            
            

            IO.write_csv(
            self.mode, {
                'file_dir': (self.config['file']['dir_results'] +
                self.config['file']['dir_validation']),
                'file_name': 'v4_install_filter_database.csv',
                "adls_config": self.config["adls"]["Processed"]["adls_credentials"],
                "adls_dir": self.config["adls"]["Processed"]["processed_install"]["v4_install_filter_database"]
                },
            df_data_install.loc[~df_data_install.f_all])

            # Filter to columns of interest
            df_data_install = df_data_install.loc[df_data_install["f_all"], ls_cols]

            # Key: Foreign / parent
            # M2M Shipment Data
            df_data_install["key_serial"] = df_data_install[
                ["Shipper_Index", "ShipperItem_Index"]
            ].apply(lambda x: str(int(x[0])) + ":" + str(int(x[1])), axis=1)

            # M2M BOM Data
            df_data_install["key_bom"] = df_data_install["Job_Index"].str.lower()

            # Sharepoint Serial Number
            df_data_install["key_serial_shapepoint"] = df_data_install["SO"].copy()
            df_data_install["key_serial_shapepoint"] = df_data_install[
                "key_serial_shapepoint"
            ].astype(str)
            # Drop duplicates
            df_data_install = df_data_install.drop_duplicates(subset=["key_serial"])
            return df_data_install
        except Exception as excp:
            logger.app_fail("filter product class", f"{traceback.print_exc()}")
            raise ValueError from excp

    def preprocess_expand_range(
        self, df_srnum_all, df_srnum
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Expand serial number data based on serial key.

        :param df_srnum_all:  Data to be merged
        :type: pd.DataFrame
        :param df_srnum: Serial key data
        :type: pd.Series
        :raises ValueError: raises error if unknown data type provided.
        :return: serial number range.
        :rtype: pd.DataFrame.

        """
        try:
            df_srnum = df_srnum.merge(df_srnum_all, on="key_serial", how="inner")


            # Serial number have different patterns. Old patterns were manually generated and allowed ranges e,g, a-b-1-60.
            # New SrNum are system generated and donot allow range of serial numbers t-us-..
            # 'eligible_for_expand' identifies serial number patterns which could have ranges.
            ref_sr_num = IO.read_csv(
                self.mode,
                {'file_dir': self.config['file']['dir_ref'],
                 'file_name': self.config['file']['Reference']['decode_sr_num'],
                  "adls_config": self.config["adls"]["Reference"]["adls_credentials"],
                  "adls_dir": self.config["adls"]["Reference"]["decode_sr_num"], 
                 })

            ref_sr_num['SerialNumberPattern'] = ref_sr_num['SerialNumberPattern'].str.lower()
            ls_eligible_4_exp = ref_sr_num.loc[
                ref_sr_num['eligible_for_expand']=="TRUE", "SerialNumberPattern"]

            df_srnum["no_of_sep"] = df_srnum['SerialNumber'].apply(
                lambda x: len(re.findall("-", x)))
            df_srnum["has_range_sep"] = df_srnum['SerialNumber'].apply(
                lambda x: len(re.findall(",|&|\(", x)) > 0)

            # has range sep: (110-b-1,2,3) or (110-b (124, 125)) or (110-b-1&2) will be expanded
            # x[1] > 2
            #   : 110-b will not be expanded as quantity is not reliable
            #   : 110-b-c is unique
            #   : 110-b-c-e will be expanded
            # x[0].lower().startswith(tuple(ls_eligible_4_exp))
            #   : t-us-.. will not be expanded as its not eligible for expansion

            df_srnum['is_srnum_range'] = df_srnum[
                ['SerialNumber', 'no_of_sep', 'has_range_sep', 'Shipper_Qty']].apply(
                lambda x: (
                        (x[2]) |
                        (x[3] > 1) |
                        (
                                ((x[1] > 2) | (x[1] == 1)) &
                                (x[0].lower().startswith(
                                    tuple(ls_eligible_4_exp)))
                        )
                ),
                axis=1)

            # df_srnum['is_srnum_range'] = df_srnum[['SerialNumber', 'Shipper_Qty']].apply(
            #     lambda x: True if x[1] > 1
            #     else ( (len(re.findall("-", x[0])) == 1) & (x[0].lower().startswith(tuple(ls_eligible_4_exp)))),
            #     axis=1)

            df_srnum_range = df_srnum[df_srnum['is_srnum_range']]

            return df_srnum_range, df_srnum
        except Exception as excp:
            logger.app_fail("expand serial number range", f"{traceback.print_exc()}")
            raise ValueError from excp

    def combine_serialnum_data(
        self, df_srnum_range, df_srnum, df_data_install, df_out, df_couldnot, merge_type
    ) -> pd.DataFrame:
        """
        Create Foreign key and drops duplicate rows.

        :param df_srnum_range:  Data having complete serial number range
        :type: pd.DataFrame
        :param df_srnum:  Complete Serial Number Data
        :type: pd.DataFrame
        :param df_data_install:  Original Data
        :type: pd.DataFrame
        :param df_out:  Data to be filtered
        :type: pd.DataFrame
        :param df_couldnot:  Data to be filtered
        :type: pd.DataFrame
        :param merge_type: It is custom based on requirement, Default is "inner":
            "left": use only keys from left frame,
            "right": use only keys from right frame,
            "outer": use union of keys from both frames,
            "inner": use intersection of keys from both frames.
            "cross": creates the cartesian product from both frames.
        :type: str
        :raises ValueError: raises error if unknown data type provided.
        :return: serial number process data.
        :rtype: pd.DataFrame.

        """
        try:
            df_srnum_range = df_srnum_range.rename(
                columns={"SerialNumber": "SerialNumberOrg"}
            )
            df_out = df_out.rename(columns={"KeySerial": "key_serial"})

            df_srnum_range = df_srnum_range.merge(
                df_out, on=["SerialNumberOrg", "key_serial"], how="inner"
            )

            df_srnum_range = df_srnum_range.drop_duplicates()

            df_srnum_range = df_srnum_range.loc[:, self.ls_cols_out]

            # Club data
            df_srnum = df_srnum.loc[~df_srnum['is_srnum_range'], self.ls_cols_out]

            df_srnum = pd.concat([df_srnum, df_srnum_range])

            df_srnum = df_srnum.rename(
                {"SerialNumber": "SerialNumber_M2M", "Product": "Product_M2M"}, axis=1
            )

            # Merge two tbls
            df_data_install = df_data_install.merge(
                df_srnum, on="key_serial", how=merge_type
            )

            df_data_install = df_data_install.drop_duplicates(
                ["Shipper_Index", "SerialNumber_M2M"]
            )
            return df_data_install
        except Exception as excp:
            logger.app_fail("filter product class", f"{traceback.print_exc()}")
            raise ValueError from excp

    def merge_bomdata(self, df_bom, df_install, merge_type) -> pd.DataFrame:
        """
        Merge BOM data to shipment data.

        :param df_bom:  Data to be merged
        :type: pd.DataFrame
        :param df_install: Data to be merged
        :type: pd.DataFrame
        :param merge_type: It is custom based on requirement, Default is "inner":
            "left": use only keys from left frame,
            "right": use only keys from right frame,
            "outer": use union of keys from both frames,
            "inner": use intersection of keys from both frames.
            "cross": creates the cartesian product from both frames.
        :type: str
        :raises ValueError: raises error if unknown data type provided.
        :return: Merged bom data with shipment data.
        :rtype: pd.DataFrame.

        """
        try:
            df_bom = df_bom[["Job_Index", "PartNumber_TLN_BOM"]]
            df_bom = df_bom.drop_duplicates()

            # Merge Data
            df_install = df_install.merge(df_bom, on="Job_Index", how=merge_type)
            df_install.loc[:, "PartNumber_TLN_BOM"] = df_install[
                "PartNumber_TLN_BOM"
            ].fillna("")
            return df_install
        except Exception as excp:
            logger.app_fail("filter product class", f"{traceback.print_exc()}")
            raise ValueError from excp

    def merge_customdata(self, df_custom, df_data_install) -> pd.DataFrame:
        """
        Merge custom data to shipment data.

        :param df_custom:  Data to be merged
        :type: pd.DataFrame
        :param df_data_install: Data to be merged
        :type: pd.DataFrame
        :raises ValueError: raises error if unknown data type provided.
        :return: Merged custom data with shipment data.
        :rtype: pd.DataFrame.

        """
        try:

            df_custom = df_custom.drop_duplicates(subset=["Serial_Number"])
            df_install_data = df_data_install.merge(
                df_custom[["Serial_Number", "StrategicCustomer"]],
                left_on="SerialNumber_M2M",
                right_on="Serial_Number",
                how="left",
            )

            logger.app_success(self.step_identify_strategic_customer)
            return df_install_data
        except Exception as excp:
            logger.app_fail(
                self.step_identify_strategic_customer, f"{traceback.print_exc()}"
            )
            raise ValueError from excp

    def clean_serialnum(self, df_srnum) -> pd.DataFrame:
        """
        Format/clean serial number data .

        :param df_srnum:  Data to be cleaned
        :type: pd.DataFrame

        :raises ValueError: raises error if unknown data type provided.
        :return: cleaned serial number data.
        :rtype: pd.DataFrame.

        """
        try:
            # Format - Duplicate Characters e.g. -- / ---
            for char in self.ls_char:
                df_srnum["SerialNumber"] = df_srnum["SerialNumber"].apply(
                    lambda x: re.sub(f"{char}+", "-", x)
                )

            # Format - Punctuation
            df_srnum["SerialNumber"] = df_srnum["SerialNumber"].apply(
                lambda x: x.lstrip(punctuation).rstrip(punctuation)
            )

            return df_srnum
        except Exception as excp:
            raise ValueError from excp

    def create_foreignkey(self, df_srnum) -> pd.DataFrame:
        """
        Create foreign key for serial number data.

        :param df_srnum:  Data to create foreign key
        :type: pd.DataFrame

        :raises ValueError: raises error if unknown data type provided.
        :return: serial number data with foreign key.
        :rtype: pd.DataFrame.

        """
        try:
            # foreign / parent keys : Serial Number
            df_srnum.loc[:, ["key_serial"]] = df_srnum[
                ["Shipper_Index", "ShipperItem_Index"]
            ].apply(lambda x: str(int(x[0])) + ":" + str(int(x[1])), axis=1)
            return df_srnum
        except Exception as excp:
            raise ValueError from excp

    def id_display_parts(self, df_data_org):
        """
        Identify display part numbers from bom.

        :param df_data_org: dataframe containing org data
        :type df_data_org: Pandas df

        """
        try:
            df_data = df_data_org.copy()
            del df_data_org

            # delete   suguna
            dict_display_parts = self.config["install_base"]["dict_display_parts"]

            df_out = df_data[["Job_Index"]].drop_duplicates()

            for col_name_out in dict_display_parts:
                # col_name_out =  list(dict_display_parts.keys())[0]

                if "txt_search" in dict_display_parts[col_name_out].keys():
                    part_num_keys = dict_display_parts[col_name_out]["txt_search"]
                    part_num_keys = [item.lower() for item in part_num_keys]

                    df_data["flag_part"] = df_data.PartNumber_BOM_BOM.apply(
                        lambda x: x.startswith(tuple(part_num_keys))
                    )

                    # List Models
                    df_sub = df_data[df_data["flag_part"]].copy()
                else:
                    df_sub = df_data


                ls_parts_of_interest = dict_display_parts[col_name_out][
                    "PartsOfInterest"
                ]
                ls_parts_of_interest = [txt.lower() for txt in ls_parts_of_interest]

                df_sub_1 = df_sub.copy()
                df_sub_1["can_raise_lead"] = df_sub["PartNumber_BOM_BOM"].isin(
                    ls_parts_of_interest
                )
                df_sub_1 = (
                    df_sub_1.groupby("Job_Index")["can_raise_lead"]
                    .apply(sum)
                    .reset_index()
                )
                df_sub_1 = df_sub_1.rename(
                    columns={
                        "can_raise_lead": f'is_valid_{col_name_out.replace("pn_", "")}_lead'
                    }
                )

                df_sub["can_raise_lead"] = df_sub["PartNumber_BOM_BOM"].isin(
                    ls_parts_of_interest
                )

                df_sub = (
                    df_sub.groupby("Job_Index")["PartNumber_BOM_BOM"]
                    .apply(lambda x: self.summarize_part_num(x, ls_parts_of_interest))
                    .reset_index()
                )

                df_sub = df_sub.rename(columns={"PartNumber_BOM_BOM": col_name_out})

                df_sub = df_sub.merge(df_sub_1, on="Job_Index", how="left")

                df_out = df_out.merge(df_sub, on="Job_Index", how="left")

            return df_out
        except Exception as e:
            logger.app_info(f"id_display :{e}")
            raise e

    def summarize_part_num(self, part_list, list_of_interest):
        """
        Summarize part numbers for a JonIndex.

        :param part_list: list of PartNumbers in a product.
        :type part_list: list
        :param list_of_interest: list of part number of interest.
        :type list_of_interest: list
        :return: Output format PartNumber of interest (other PartNumbers)
        :rtype: string

        """
        if type(list_of_interest) != list:
            raise TypeError(
                "InstallBase class summarize_part_num method argument" " is not a list"
            )
        part_list = list(np.unique(part_list))

        ls_present = [str.upper(pn) for pn in part_list if (pn in list_of_interest)]
        other_parts = len(part_list) - len(ls_present)

        out = "" if len(ls_present) == 0 else (", ".join(ls_present))
        sep = "" if len(out) == 0 else " "
        out += (
            ""
            if other_parts == 0
            else (sep + "(# Other Parts: " + str(other_parts) + ")")
        )

        return out

    def get_metadata(self, df_data_install):
        """
        This method calculates kva, amp, voltage metadata from description
        field.
        @param df_data_install: Dataframe containing shipment data
        @return: Updated dataframe containing metadata
        """
        try:
            kva_search_pattern = self.config["install_base"]["kva_search_pattern"]
            amp_search_pattern = self.config["install_base"]["amp_search_pattern"]
            voltage_serach_pattern = self.config["install_base"][
                "voltage_serach_pattern"
            ]
        except ValueError as err:
            logger.app_info(
                "Failed to get required config files in get_metadata() class "
                "InstallBase"
            )
            raise Exception from err

        try:
            df_data_install["kva"] = df_data_install.Description.apply(
                lambda x: re.findall(
                    kva_search_pattern, str(x)
                    , flags=re.IGNORECASE
                ))
            df_data_install["kva"] = df_data_install["kva"].apply(
                self.list_to_string
            )
            df_data_install["amp"] = df_data_install.Description.apply(
                lambda x: re.findall(
                    amp_search_pattern, str(x)
                    ,flags=re.IGNORECASE
                ))
            df_data_install["amp"] = df_data_install["amp"].apply(
                self.list_to_string)
            df_data_install["voltage"] = df_data_install.Description.apply(
                lambda x: re.findall(
                    voltage_serach_pattern, str(x)
                    ,flags=re.IGNORECASE
                ))
            df_data_install["voltage"] = df_data_install["voltage"].apply(
                self.list_to_string)
        except Exception as err:
            logger.app_info("failed in get_metadata() class InstallBase")
            raise Exception from err

        return df_data_install

    def list_to_string(self, metadata):
        """
        Coverting the metadata list to string
        :param metadata: metadata list
        :return: comma seperated metadata values
        """
        if len(metadata) == 0:
            return None
        if len(metadata) == 1:
            return re.findall("\d+", metadata[0])[0]

        # Extract Numeric value
        for i in range(len(metadata)):
            metadata[i] = re.findall("\d+", metadata[i])[0]
        # Find unique entries in the list
        metadata = set(metadata)
        metadata = list(metadata)

        return ', '.join(metadata)

# %% *** Call ***


# %%
if __name__ == "__main__":
    obj = InstallBase()
    obj.main_install()
