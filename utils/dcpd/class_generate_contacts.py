"""
@file class_generate_contacts.py.

@brief : For DCPD business; analyze contracts data and generate contacts.


@details :
    Code generates contacts from the contracts data which again consists of
    three data sources

    1. Contracts: has all columns except SerialNumber
    2. processed_contract: has a few cols with SerialNumber


@copyright 2023 Eaton Corporation. All Rights Reserved.
@note Eaton Corporation claims proprietary rights to the material disclosed
here on. This technical information may not be reproduced or used without
direct written permission from Eaton Corporation.
"""

# %% ***** Setup Environment *****
import os
import re
import json

config_dir = os.path.join(os.path.dirname(__file__), "../../config")
config_file = os.path.join(config_dir, "config_dcpd.json") 
with open(config_file,'r') as config_file:
    config = json.load(config_file)
mode = config.get("conf.env", "azure-adls")
if mode == "local":
    path = os.getcwd()
    path = os.path.join(path.split('ileads_lead_generation')[0], 'ileads_lead_generation')
    os.chdir(path)

import numpy as np
import pandas as pd
import traceback
import time
import sys
from utils import IO
from utils import AppLogger
from utils.format_data import Format
from utils.dcpd.class_contracts_data import Contract
from utils.class_iLead_contact import ilead_contact
from utils.filter_data import Filter
from utils.contacts_fr_events_data import DataExtraction

# contractObj = Contract()
filter_ = Filter()
logger = AppLogger(__name__)


# %% Generate Contacts


class Contacts:
    """Class will extract and process contract data and processed data."""

    def __init__(self):
        """
        Initialise environment variables, class instance and variables.

        :param mode: DESCRIPTION, defaults to 'local'
        :type mode: sttring, optional
        """
        # class instance
        self.contractObj = Contract()
        self.format = Format()
        config_dir = os.path.join(os.path.dirname(__file__), "../../config")
        config_file = os.path.join(config_dir, "config_dcpd.json")
        with open(config_file, "r") as config_file:
            self.config = json.load(config_file)
        # logging.info(f"config file: {config}")
        self.mode = self.config.get("conf.env", "azure-adls")
        if self.mode == "local":
            path = os.getcwd()
            path = os.path.join(path.split('ileads_lead_generation')[0], 'ileads_lead_generation')
            os.chdir(path)
        # steps
        self.contact_contracts = "generate contract"

        self.TH_DATE = pd.to_datetime("01/01/1980")
        self.gc = ilead_contact(self.TH_DATE)

        # Read reference data
        _step = "Read reference files"

        # file_dir = {
        #     'file_dir': self.config['file']['dir_ref'],
        #     'file_name': self.config['file']['Reference']['contact_type']}
        file_dir = {
            "file_dir": self.config["file"]["dir_ref"],
            "file_name": self.config["file"]["Reference"]["contact_type"],
            "adls_config": self.config["adls"]["Reference"]["adls_credentials"],
            "adls_dir": self.config["adls"]["Reference"]["contact_type"],
        }

        ref_df = IO.read_csv(self.mode, file_dir)

        self.ref_df = self.gc.format_reference_file(ref_df)

        logger.app_success(_step)
        self.start_msg = ": STARTED"
        self.dir_raw = {
            "file_dir": self.config["file"]["dir_data"],
            "file_name": "",
            "adls_config": self.config["adls"]["Raw"]["adls_credentials"],
            "adls_dir": "",
        }
        self.dir_inter = {
            "file_dir": (
                    self.config["file"]["dir_results"] +
                    self.config["file"]["dir_intermediate"]
            ),
            "file_name": "",
            "adls_config": self.config["adls"]["Processed"][
                "adls_credentials"
            ],
            "adls_dir": ""
        }

    def main_contact(self):  # pragma: no cover
        """
        Pipeline for extracting contacts from all sources listed in config.

        :return: Pipeline status. Successful vs Failed.
        :rtype: string

        """
        try:
            # Generate Contact:
            _step = "Generate contact"
            df_con = self.deploy_across_sources()
            logger.app_success(_step)

            # PostProcess kdata
            df_con = self.post_process(df_con)

            # Export results
            _step = "Export results"

            # file_dir = {
            #     'file_dir': self.config['file']['dir_results'],
            #     'file_name': self.config['file']['Processed']['output_iLead_contact']['file_name']
            # }
            file_dir = {
                "file_dir": self.config["file"]["dir_results"],
                "file_name": self.config["file"]["Processed"]["output_iLead_contact"][
                    "file_name"
                ],
                "adls_config": self.config["adls"]["Processed"]["adls_credentials"],
                "adls_dir": self.config["adls"]["Processed"]["output_iLead_contact"],
            }
            IO.write_csv(self.mode, file_dir, df_con)

            logger.app_success(_step)

        except Exception as excp:
            logger.app_fail(self.contact_contracts, f"{traceback.print_exc()}")
            logger.app_info(excp)
            raise excp

        return df_con  # "Successful !"

    def deploy_across_sources(self):  # pragma: no cover
        """
        Deploy generate_contacts across data bases listed in config.

        :return: Contacts from all the databases.
        :rtype: pandas DataFrame.

        """
        # Read list of databases to be used for generating contacts
        _step = "Read confirguration for contacts"
        try:
            dict_sources = self.config["output_contacts_lead"]["dict_dbs"]
        except Exception as e:
            logger.app_fail(_step, f"{traceback.print_exc()}")
            raise ValueError from e

        # Generate contacts from all databases from config
        df_out = pd.DataFrame()
        for src in dict_sources:
            # src = list(dict_sources.keys())[0]
            f_analyze = dict_sources[src]

            if f_analyze == 1:
                _step = f"Generate contacts: {src}"

                try:
                    df_data = self.generate_contacts(src)
                    time.sleep(5)
                except Exception as e:
                    logger.app_fail(_step, f"{traceback.print_exc()}")
                    logger.app_info(e)
                    df_data = pd.DataFrame()

            elif f_analyze == 0:
                _step = f"Read old processed data: {src}"

                try:
                    file_dir = {
                        "file_dir": self.config["file"]["dir_results"]
                        + self.config["file"]["dir_intermediate"],
                        "file_name": ("processed_contacts" + src + ".csv"),
                        "adls_config": self.config["adls"]["Processed"][
                            "adls_credentials"
                        ],
                        "adls_dir": self.config["adls"]["Processed"]["contact"][src],
                    }


                    df_data = IO.read_csv(self.mode, file_dir)


                except:
                    logger.app_fail(_step, f"{traceback.print_exc()}")
                    df_data = pd.DataFrame()

            elif f_analyze == -1:
                logger.app_info(f"Generate contact method not implemented for {src}")
                df_data = pd.DataFrame()

            else:
                logger.app_info(f"Unknown analyze method {f_analyze}")
                df_data = pd.DataFrame()

            # Concatenate output from all sources
            df_out = pd.concat([df_out, df_data])
            del df_data

        return df_out

    def generate_contacts(self, src):  # pragma: no cover
        """
        Generate contacts for individual database.

        Steps:
            - Read raw data
            - Read columns mapping from config
            - Preprocess data (concat data if multiple columns are mapped and exceptions)
            - Generate contact
            - Export data to intermediate folder

        :param src: database name for which contacts are to be generated.
        :type src: string
        :return: Contacts for databases provided in input
        :rtype: pandas dataframe.

        """
        _step = f"Generate contact for {src}"

        try:

            logger.app_info(_step + self.start_msg)
            # Read raw data
            file_dir = {
                "file_dir": self.config["file"]["dir_data"],
                "file_name": self.config["file"]["Raw"][src]["file_name"],
                "adls_config": self.config["adls"]["Raw"]["adls_credentials"],
                "adls_dir": self.config["adls"]["Raw"][src],
            }

            df_data = IO.read_csv(self.mode, file_dir)

            del file_dir


            ls_dict = [src]
            ls_dict += ["PM", "Renewal"] if src == "contracts" else []

            df_results = pd.DataFrame()
            for dict_src in ls_dict:
                #  dict_src = ls_dict[0]

                # dict_contact
                dict_contact = self.config["output_contacts_lead"][dict_src]

                df_data, dict_updated = self.exception_src(
                    dict_src, df_data, dict_contact
                )

                df_data, dict_updated = self.prep_data(df_data, dict_contact)
                del dict_contact

                # Generate contact
                df_contact = self.gc.create_contact(
                    df_data,
                    dict_updated,
                    dict_src,
                    f_form_date=False,
                    ref_df_all=self.ref_df,
                )

                # Concat outputs
                df_results = pd.concat([df_results, df_contact])
                del df_contact

                # Postprocess data

                ls_col_drop = [
                    (col) for col in df_data.columns if col.startswith("nc_")
                ]


                if len(ls_col_drop) > 0:
                    df_data = df_data.drop(columns=ls_col_drop)

            file_dir = {
                "file_dir": self.config["file"]["dir_results"]
                + self.config["file"]["dir_intermediate"],
                "file_name": ("processed_contacts" + src + ".csv"),
                "adls_config": self.config["adls"]["Processed"]["adls_credentials"],
                "adls_dir": self.config["adls"]["Processed"]["contact"][src],
            }
            status = IO.write_csv(self.mode, file_dir, df_results)

            del file_dir, status

            logger.app_success(_step)

        except Exception as e:
            logger.app_info(f"Exception : {e}")
            logger.app_fail(_step, f"{traceback.print_exc()}")
            raise Exception from e


        return df_results

    def prep_data(self, df_data, dict_in):
        """
        Concatenate contact data if field is mapped to multiple columns.

        Corner scenarios:
            Input: ["a", "b", ""]   Output: "a, b"
            Input: ["a", "a", ""]   Output: "a"

        :param df_data: Input data frame.
        :type df_data: pandas DataFrame
        :param dict_in: dictionary mapping columns from database to
         contacts field expected in output
        :type dict_in: dictionary
        :return: Processed data. concatenated column name:
            "nc_" + output field name
        :rtype: pandas DataFrame

        """
        _step = "Pre-Process data"
        if type(dict_in) != dict:
            raise TypeError(
                "Contacts class prep_data method argument dict_in is not a "
                "dictionary"
            )
        try:
            for key in dict_in:
                # Filter out Company_Phone for minimum length
                if key == "Company_Phone":
                    # standard for us
                    min_len = 10
                else:
                    min_len = 0

                ls_col = dict_in[key]

                if isinstance(ls_col, list):
                    n_col = "nc_" + key
                    df_data[ls_col] = df_data[ls_col].fillna("").astype(str)

                    df_data.loc[:, n_col] = df_data[ls_col].apply(
                        lambda x: "; ".join(
                            y
                            for y in np.unique(x)
                            if (len(str(y)) > min_len) & pd.notna(y)
                        ),
                        axis=1,
                    )

                    dict_in[key] = n_col
                else:
                    dict_in[key] = ls_col
        except Exception as e:
            logger.app_fail(_step, f"{traceback.print_exc()}")
            raise Exception from e

        return df_data, dict_in

    def exception_src(self, src, df_data, dict_contact):
        """
        Process specific to individual database.

        :param src: database name
        :type src: string
        :param df_data: Data for selected database
        :type df_data: pandas DataFrame.
        :param dict_contact: Mapping columns to output field names.
        :type dict_contact: dictionary
        :return: Processed data.
        :rtype: pandas DataFrame.

        """
        if type(src) != str:
            raise TypeError(
                "Contacts class exception_src method argument src " "is not a string"
            )
        if not isinstance(df_data, pd.DataFrame):
            raise TypeError(
                "Contacts class exception_src method argument df_data "
                "is not a Pandas Dataframe"
            )
        if type(dict_contact) != dict:
            raise TypeError(
                "Contacts class exception_src method argument dict_contact "
                "is not a Dictionary"
            )
        try:
            match src:
                case  "contact":
                    # Read raw services data and get account Id to SO
                    file_dir = self.dir_raw
                    file_dir["file_name"] = self.config["file"]["Raw"]["services"]["file_name"]
                    file_dir["adls_dir"] = self.config["adls"]["Raw"]["services"]

                    df_data_serv = IO.read_csv(self.mode, file_dir)
                    del file_dir

                    df_data_serv = df_data_serv[['AccountId', 'Original_Sales_Order_del__c']]
                    df_data_serv = df_data_serv.rename(columns={'Original_Sales_Order_del__c': "SO"})

                    # Read raw contracts data and get account Id to SO
                    file_dir = self.dir_raw
                    file_dir["file_name"] = self.config["file"]["Raw"]["contracts"]["file_name"]
                    file_dir["adls_dir"] = self.config["adls"]["Raw"]["contracts"]

                    df_data_con = IO.read_csv(self.mode, file_dir)
                    del file_dir

                    df_data_con = df_data_con[['AccountId', 'Original_Sales_Order__c']]
                    df_data_con = df_data_con.rename(columns={'Original_Sales_Order__c': "SO"})

                    # Clean SO (a/b) >> two rows for a and b
                    df_data_map = pd.concat([df_data_serv, df_data_con])
                    del df_data_serv, df_data_con

                    # Keep unique
                    df_data_map = df_data_map.drop_duplicates()
                    df_data_map = df_data_map.dropna()

                    df_data_map['SO_org'] = df_data_map['SO'].copy()
                    # df_data_map['SO'] = df_data_map['SO'].astype(str).str.replace(" ", "")
                    df_data_map.loc[:, 'ls_SO'] = df_data_map['SO'].astype(str).str.split(' |/|,| |&|\(|-')
                    df_data_map = df_data_map[['AccountId', 'SO_org', 'ls_SO']].explode('ls_SO')
                    df_data_map = df_data_map.rename(columns={"ls_SO": "SO"})
                    df_data_map['SO'] = pd.to_numeric(df_data_map['SO'], errors="coerce")
                    df_data_map = df_data_map[pd.notna(df_data_map['SO'])]
                    df_data_map['SO'] = df_data_map['SO'].astype(int)
                    df_data_map = df_data_map.drop_duplicates(
                        subset=['SO', 'AccountId']
                    )

                    # Read processed install data for mapping (SO to SerialNumber)
                    file_dir = self.dir_inter
                    file_dir['file_name'] = (
                        self.config["file"]["Processed"]["processed_install"]['file_name'])
                    file_dir['adls_dir'] = (
                        self.config["adls"]["Processed"]["processed_install"])

                    df_sr_num = IO.read_csv(self.mode, file_dir)
                    del file_dir

                    df_sr_num = df_sr_num[["SO", 'SerialNumber_M2M']]
                    df_sr_num = df_sr_num.rename(columns={"SerialNumber_M2M": "SerialNumber"})
                    df_sr_num = df_sr_num.merge(df_data_map, on="SO", how="inner")
                    # TODO : save mapping
                    del df_data_map

                    # Map serial numbers to account
                    df_data = df_data.merge(df_sr_num, on="AccountId", how="left")
                    del df_sr_num
                    df_data = df_data \
                        .sort_values(by="LastActivityDate") \
                        .drop_duplicates("SerialNumber")

                    df_data['Company'] = "blank"
                    df_data = df_data.rename(columns={'Email': 'Email_org'})
                    dict_contact["Email"] = ["Email_org", "Other_Email__c"]
                    dict_contact["Serial Number"] = "SerialNumber"
                    dict_contact["Company"] = "Company"

                case "services":
                    # Read serial numbers
                    file_dir = {
                        "file_dir": (
                            self.config["file"]["dir_results"]
                            + self.config["file"]["dir_intermediate"]
                        ),
                        "file_name": self.config["file"]["Processed"]["services"][
                            "validated_sr_num"
                        ],
                        "adls_config": self.config["adls"]["Processed"][
                            "adls_credentials"
                        ],
                        "adls_dir": self.config["adls"]["Processed"]["services"][
                            "validated_sr_num"
                        ],
                    }

                    df_sr_num = IO.read_csv(self.mode, file_dir)

                    del file_dir

                    # Merge Data
                    df_data = df_data.merge(df_sr_num, on="Id", how="left")

                    # Update contact dictionary
                    dict_contact["Serial Number"] = "SerialNumber"

                case "events":
                    # extract_data

                    df_data = self.extract_data(src, df_data)

                    dict_contact["Serial Number"] = "SerialNumber"

                case "contracts":
                    # Read serial numbers
                    file_dir = {
                        "file_dir": self.config["file"]["dir_results"]
                        + self.config["file"]["dir_intermediate"],
                        "file_name": self.config["file"]["Processed"][src]["file_name"],
                        "adls_config": self.config["adls"]["Processed"][
                            "adls_credentials"
                        ],
                        "adls_dir": self.config["adls"]["Processed"][src],
                    }


                    df_sr_num = IO.read_csv(self.mode, file_dir)

                    df_sr_num = df_sr_num[["ContractNumber", "SerialNumber"]]

                    del file_dir

                    # Merge Data
                    df_data["ContractNumber"] = (
                        df_data["ContractNumber"].fillna(0).astype("int64").astype(str)
                    )
                    df_sr_num["ContractNumber"] = (
                        df_sr_num["ContractNumber"]
                        .fillna(0)
                        .astype("int64")
                        .astype(str)
                    )

                    df_data["ContractNumber"] = df_data["ContractNumber"].replace(
                        "0", ""
                    )
                    df_sr_num["ContractNumber"] = df_sr_num["ContractNumber"].replace(
                        "0", ""
                    )

                    # changed from merge to concat
                    df_data = df_data.merge(df_sr_num, on="ContractNumber", how="left")

                    # Data prep
                    df_data.Zipcode__c = pd.to_numeric(
                        df_data.Zipcode__c, errors="coerce"
                    )

                    # Update contact dictionary
                    dict_contact["Serial Number"] = "SerialNumber"

                case "Renewal":
                    # Update contact dictionary
                    dict_contact["Serial Number"] = "SerialNumber"
                case "PM":
                    # Update contact dictionary
                    dict_contact["Serial Number"] = "SerialNumber"
                case _:
                    logger.app_info(f"No exception for {src}", 1)

            return df_data, dict_contact
        except Exception as e:
            logger.app_info(f"exception in exception_src {e} in {src}")
            logger.app_fail(
                f"Exception for {src} in exception_src:", f"{traceback.print_exc()}"
            )
            logger.app_info(f"FAILED:{e}")
            raise Exception from e

    def extract_data(self, src, df_data):
        """
        This function extracts the Contact details from Events data
        """
        try:
            _step = "Extract contact details from Events Description"
            if type(src) != str:
                raise TypeError(
                    "Contacts class extract_data method argument dict_src "
                    "is not a string"
                )
            match src:
                case "events":
                    try:
                        usa_states = self.config["output_contacts_lead"]["usa_states"]

                    except Exception as e:
                        logger.app_fail(
                            "usa_states not available in config",
                            f"{traceback.print_exc()}",
                        )
                        raise ValueError from e

                    pat_state_short = " " + " | ".join(list(usa_states.keys())) + " "
                    pat_state_long = " " + " | ".join(list(usa_states.values())) + " "

                    pat_address = (
                        "(" + pat_state_short + "|" + pat_state_long + ")"
                    ).lower()

                    data_extractor = DataExtraction()


                    df_data.Description = df_data.Description.fillna("")

                    df_data.loc[:, "contact_name"] = df_data.Description.apply(
                        lambda x: data_extractor.extract_contact_name(x)
                    )
                    df_data.loc[:, "contact"] = df_data.Description.apply(
                        lambda x: data_extractor.extract_contact_no(x)
                    )
                    df_data.loc[:, "email"] = df_data.Description.apply(
                        lambda x: data_extractor.extract_email(x)
                    )
                    df_data.loc[:, "address"] = df_data.Description.apply(
                        lambda x: data_extractor.extract_address(x, pat_address)
                    )
                    df_data.loc[:, "SerialNumber"] = df_data["Description"].apply(
                        lambda x: self.serial_num(str(x))
                    )

                    df_data = df_data.explode("SerialNumber").astype(str)

                    output_dir = {
                        "file_dir": self.config["file"]["dir_results"]
                        + self.config["file"]["dir_validation"],
                        "file_name": self.config["file"]["Processed"]["contact"][
                            "events_sr_num"
                        ],
                        "adls_config": self.config["adls"]["Processed"][
                            "adls_credentials"
                        ],
                        "adls_dir": self.config["adls"]["Processed"]["contact"][
                            "events_sr_num"
                        ],
                    }

                    IO.write_csv(self.mode, output_dir, df_data)
                    df_data = df_data.loc[
                        (df_data["contact"] != df_data["SerialNumber"])
                    ]
                    df_data = self.contractObj.validate_contract_install_sr_num(
                        df_data)
                    df_data = df_data[df_data.flag_validinstall]
                    df_data = df_data.drop(columns=[
                        'flag_validinstall', 'SerialNumberExact',
                        'SerialNumber_Partial'
                    ])

        except Exception as e:
            # logger.app_fail(_step, f"{traceback.print_exc()}")
            logger.app_info(f"FAILED:{e}")
            raise e

        return df_data

    def post_process(self, df_con):
        """
        Postprocess contacts which includes following steps.

            1. Drop rows if contacts are invalid ()
            2. Drop duplicate for serial number keeping latest contact

        :param df_con: DESCRIPTION
        :type df_con: TYPE
        :return: DESCRIPTION
        :rtype: TYPE

        """
        _step = "Post process contacts"
        try:

            df_con = self.validate_op(df_con)

            # Keep latest
            df_con = self.filter_latest(df_con)

        except Exception as e:
            logger.app_fail(_step, f"{traceback.print_exc()}")
            raise Exception from e

        return df_con

    def validate_op(self, df_con):
        """
        Drop invalid rows, Valid row would have atleast one non empty
        or nan Name / email / phone.
        :param df_con: Data from all contacts.
        :type df_con: pandas DataFrame.
        :return: df_con, Validated contact details
        :rtype: pandas DataFrame.

        """
        ls_cols_must = ["Name", "Email", "Company_Phone"]
        ls_flags = []
        df_con.loc[:, "flag_include"] = False

        min_length = {
            "Name": 2,
            "Email": 4,
            "Company_Phone": 10
        }

        # If the name, email, contact number all are null, drop the entry
        # Following code checks whether if the said columns are null or blank
        for col in ls_cols_must:
            # Clean column
            df_con.loc[:, col] = df_con[col].fillna("")
            df_con.loc[:, col] = df_con[col].apply(
                lambda x: x.strip("_-* ")
            )

            # Identiy Valid entries
            n_col = f"f_{col}"
            ls_flags += [n_col]

            flag1 = pd.notna(df_con[col])
            flag2 = df_con[col] != ""
            flag = flag1 & flag2
            df_con.loc[:, n_col] = flag

            df_con.loc[:, "flag_include"] = (
                df_con["flag_include"] | df_con.loc[:, n_col]
            )

        df_con = df_con.loc[
            (df_con["Name"].str.len() >= min_length["Name"]) |
            (df_con["Email"].str.len() >= min_length["Email"]) |
            (df_con["Company_Phone"].str.len() >= min_length["Company_Phone"])
        ]
        df_con.rename(
            columns={'Serial Number': 'SerialNumber'},
            inplace=True
        )
        return df_con

    def filter_latest(self, df_contact):
        """
        Drop duplicate contact keeping latest contact.

        :param df_contact: Data from all contacts.
        :type df_contact: pandas DataFrame.
        :return: DESCRIPTION
        :rtype: pandas DataFrame.

        """
        _step = "Filter to latest data"

        try:
            df_contact = df_contact.sort_values(
                by=["SerialNumber", "Source", "Contact_Type", "Date"], ascending=False
            ).drop_duplicates(
                subset=["SerialNumber", "Source", "Contact_Type"], keep="first"
            )
            df_contact = df_contact.replace("\n", " ", regex=True).replace(
                "\r", " ", regex=True)
        except Exception as e:
            logger.app_fail(_step, f"{traceback.print_exc()}")
            raise Exception from e

        return df_contact

    def serial_num(self, col):
        """
        Method to extract serial number details from events description
        @param col: Events data description column
        @return: list of extracted patterns
        """
        pattern = self.config["output_contacts_lead"]["pat_srnum_event"]
        return re.findall(pattern, col)


# %% *** Call ***
if __name__ == "__main__": # pragma: no cover
    obj = Contacts()
    out = obj.main_contact()

# s

# %%
