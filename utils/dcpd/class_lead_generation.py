"""@file class_lead_generation.py

@brief: for DCPD business, generate the leads from reference leads data and also combine meta data
from different other sources of data i.e contract, install, services.


@details
Lead generation module works on bom data, contract data , install base data, services data
and majorly on a reference leads data as input and generates the leads for customer impact.


@copyright 2023 Eaton Corporation. All Rights Reserved.
@note Eaton Corporation claims proprietary rights to the material disclosed
here on. This technical information may not be reproduced or used without
direct written permission from Eaton Corporation.
"""
# %% Setup Environment

import os
import traceback
from string import punctuation
import re
from datetime import timedelta
import numpy as np
import pandas as pd
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

from utils.dcpd.class_business_logic import BusinessLogic
from utils.dcpd.class_serial_number import SerialNumber
from utils.strategic_customer import StrategicCustomer
from utils.format_data import Format
from utils import AppLogger
from utils import IO
from utils import Filter
import json


obj_filt = Filter()
logger = AppLogger(__name__)
punctuation = punctuation + " "



# %% Lead Generation


class LeadGeneration:
    def __init__(self):
        
        self.srnum = SerialNumber()
        self.bus_logic = BusinessLogic()
        self.format = Format()
        config_dir = os.path.join(os.path.dirname(__file__), "../../config")
        config_file = os.path.join(config_dir, "config_dcpd.json")
        with open(config_file, 'r') as config_file:
            self.config = json.load(config_file) 
        self.mode = self.config.get("conf.env", "azure-adls")
        if self.mode == "local":
            path = os.getcwd()
            path = os.path.join(path.split('ileads_lead_generation')[0], 'ileads_lead_generation')
            os.chdir(path)
        self.msg_start = "STARTED"
        self.msg_end = "COMPLETED"
        self.msg_success = "successful! "

    def main_lead_generation(self):  # pragma: no cover
        """
        This is the main method or entry point for the lead generation module.
        """
        _step = "Read Merged Contracts and Install Base data"
        try:
            # Read Instal + contracts data
            logger.app_info(f"{_step} - {self.msg_start}")
            df_install = self.pipeline_contract_install()
            #logger.app_success(f"***** {df_install.SerialNumber_M2M.nunique()} *****")
            logger.app_success(_step)


            # ***** PreProcess BOM data *****
            # Read BOM Data
            _step = "Process BOM data and identify leads"
            logger.app_info(f"{_step} - {self.msg_start}")
            df_leads = self.pipeline_bom_identify_lead(df_install)

            #logger.app_success(f"***** {df_leads.SerialNumber_M2M.nunique()} *****")
            logger.app_success(_step)


            # Merge BOM & Install data
            _step = "Merge data: Install and BOM"
            logger.app_info(f"{_step} - {self.msg_start}")
            df_leads = self.pipeline_merge(df_leads, df_install, "meta_data")
            #logger.app_success(f"***** {df_leads.SerialNumber_M2M.nunique()} *****")
            logger.app_success(_step)


            # Merge Service data
            _step = "Adding JCOMM and Sidecar Fields to Lead Generation Data"
            logger.app_info(f"{_step} - {self.msg_start}")
            df_leads = self.pipeline_add_jcomm_sidecar(df_leads)
            logger.app_success(_step)

            # Post Process : Leads
            _step = (
                "Post Process output before formatting to calculate standard offering."
            )
            logger.app_info(f"{_step} - {self.msg_start}")
            df_leads = self.post_proecess_leads(df_leads)
            logger.app_success(_step)

            _step = "Post Processing and Deriving columns on output iLeads"
            logger.app_info(f"{_step} - {self.msg_start}")
            df_leads = self.post_process_output_ilead(df_leads)
            logger.app_success(_step)

            # Post Process : InstallBase
            _step = "Post Processing and Deriving columns on reference install leads"
            logger.app_info(f"{_step} - {self.msg_start}")
            df_leads = self.post_process_ref_install(df_leads)
            logger.app_success(_step)


            address_cols = [
                "End_Customer_Address",
                "ShipTo_Street",
                "BillingAddress",
                "StartupAddress",
            ]
            for col in address_cols:
                if not df_leads[col].isnull().values.all():
                    df_leads[col] = df_leads[col].str.replace("\n", " ")
                    df_leads[col] = df_leads[col].str.replace("\r", " ")

            df_leads = df_leads.drop(
                columns=["temp_column", "component", "ClosedDate"]
            ).reset_index(drop=True)

            _step = "Write output lead to result directory"
            IO.write_csv(
                self.mode,
                {
                    "file_dir": self.config["file"]["dir_results"] +
                                self.config["file"]["dir_validation"],
                    "file_name": self.config["file"]["Processed"]["output_iLead"][
                        "validation"
                    ],
                    "adls_config": self.config["adls"]["Processed"]["adls_credentials"],
                    "adls_dir": self.config["adls"]["Processed"]["output_iLead"][
                        "validation"
                    ],
                },
                df_leads,
            )
            logger.app_success(_step)

            _step = "Formatting Output"
            logger.app_info(f"{_step} - {self.msg_start}")
            df_leads = df_leads.replace("\n", " ", regex=True).replace(
                "\r", " ", regex=True)
            ref_install_output_format = self.config["output_format"]["ref_install_base"]
            ref_install = self.format.format_output(df_leads, ref_install_output_format)

            ilead_output_format = self.config["output_format"]["output_iLead"]
            output_ilead = self.format.format_output(df_leads, ilead_output_format)

            lead_type = output_ilead[["Serial_Number", "Lead_Type"]]
            lead_type["EOSL_reached"] = lead_type["Lead_Type"] == "EOSL"
            lead_type = lead_type.groupby("Serial_Number")["EOSL_reached"].any()
            ref_install = ref_install.merge(lead_type, on="Serial_Number", how="left")
            logger.app_info(f"{_step} - {self.msg_end}")

            _step = "Exporting reference install file"

            ref_install = ref_install.drop_duplicates(
                subset=["Serial_Number"]
            ).reset_index(drop=True)

            IO.write_csv(
                self.mode,
                {
                    "file_dir": self.config["file"]["dir_results"],
                    "file_name": self.config["file"]["Processed"]["output_iLead"][
                        "ref_install"
                    ],
                    "adls_config": self.config["adls"]["Processed"]["adls_credentials"],
                    "adls_dir": self.config["adls"]["Processed"]["output_iLead"][
                        "ref_install"
                    ],
                },
                ref_install,
            )
            logger.app_info(f"{_step} - {self.msg_end}")

            ref_install_map_level = self.output_ref_install_map_level_reformat(
                ref_install)
            ref_install_unit_summary = self.ref_install_unit_summary_reformat(
                ref_install)

            IO.write_csv(self.mode,
                         {'file_dir': self.config['file']['dir_results'],
                          'file_name':
                              self.config['file']['Processed']['output_iLead'][
                                  'ref_install_map_level'],
                          "adls_config": self.config["adls"]["Processed"][
                              "adls_credentials"],
                          "adls_dir":
                              self.config["adls"]["Processed"]["output_iLead"][
                                  "ref_install_map_level"
                              ],
                          },
                         ref_install_map_level)

            IO.write_csv(self.mode,
                         {'file_dir': self.config['file']['dir_results'],
                          'file_name':
                              self.config['file']['Processed']['output_iLead'][
                                  'ref_install_unit_summary'],
                          "adls_config": self.config["adls"]["Processed"][
                              "adls_credentials"],
                          "adls_dir":
                              self.config["adls"]["Processed"]["output_iLead"][
                                  "ref_install_unit_summary"
                              ],
                          },
                         ref_install_unit_summary)

            logger.app_success(_step)

            _step = "Exporting output iLead file"
            logger.app_info(f"{_step} - {self.msg_start}")

            IO.write_csv(
                self.mode,
                {
                    "file_dir": self.config["file"]["dir_results"],
                    "file_name": self.config["file"]["Processed"]["output_iLead"][
                        "file_name"
                    ],
                    "adls_config": self.config["adls"]["Processed"]["adls_credentials"],
                    "adls_dir": self.config["adls"]["Processed"]["output_iLead"],
                },
                output_ilead,
            )
            logger.app_info(f"{_step} - {self.msg_end}")

            output_ilead_component_summary = self.output_ilead_component_summary_reformat(
                output_ilead)
            IO.write_csv(self.mode,
                         {'file_dir': self.config['file']['dir_results'],
                          'file_name':
                              self.config['file']['Processed']['output_iLead'][
                                  'output_ilead_component_summary'],
                          "adls_config": self.config["adls"]["Processed"][
                              "adls_credentials"],
                          "adls_dir":
                              self.config["adls"]["Processed"]["output_iLead"][
                                  "output_ilead_component_summary"
                              ],
                          },
                         output_ilead_component_summary)

            logger.app_success(_step)

        except Exception as e:
            logger.app_fail(f"{_step} : {traceback.print_exc()}", e)
            raise Exception from e

        return "successfully !"

    #  ***** Pipelines ****
    def post_proecess_leads(self, df_leads):
        # All other displays: Invalid
        df_leads["is_standard_offering"] = False

        # PDU + ( M4 display  + 8212)
        ls_comp_type = ["M4 Display", "8212 Display"]
        f_std_offer = (
            (
                df_leads.Product_M2M_Org.str.upper().isin(
                    ["PDU", "PDU - PRIMARY", "PDU - SECONDARY"]
                )
            )
            & df_leads.Component.isin(ls_comp_type)
            & df_leads.is_valid_logic_tray_lead
            & df_leads.is_valid_door_assembly_lead
            & df_leads.is_valid_input_breaker_panel_lead
        )

        df_leads.loc[f_std_offer, "is_standard_offering"] = True
        # PDU + ( 'Monochrome Display', 'Color Display')
        ls_comp_type = ["Monochrome Display", "Color Display"]
        f_std_offer = (
            (
                df_leads.Product_M2M_Org.str.upper().isin(
                    ["PDU", "PDU - PRIMARY", "PDU - SECONDARY"]
                )
            )
            & df_leads.Component.isin(ls_comp_type)
            & df_leads.is_valid_door_assembly_lead
            & df_leads.is_valid_input_breaker_panel_lead
        )

        df_leads.loc[f_std_offer, "is_standard_offering"] = True

        # RPP + ( M4 display  + 8212)
        ls_comp_type = [
            "Monochrome Display",
            "Color Display",
            "M4 Display",
            "8212 Display",
        ]
        f_std_offer = (
            (df_leads.Product_M2M_Org.str.upper() == "RPP")
            & df_leads.Component.isin(ls_comp_type)
            & df_leads.is_valid_chasis_lead
            & (pd.to_datetime(df_leads.ShipmentDate) >= pd.to_datetime("2008-01-01"))
        )

        df_leads.loc[f_std_offer, "is_standard_offering"] = True
        # All other components: are valid
        ls_comp_type = ["BCMS", "PCB", "SPD", "Fan", "PDU", "RPP", "STS"]
        f_std_offer = df_leads.Component.isin(ls_comp_type)
        df_leads.loc[f_std_offer, 'is_standard_offering'] = True
        IO.write_csv(
            self.mode, {
                'file_dir': (self.config['file']['dir_results'] +
                             self.config['file']['dir_validation']),
                'file_name': 'v4_lead_post_process_leads.csv',
                "adls_config": self.config["adls"]["Processed"]["adls_credentials"],
                "adls_dir": self.config["adls"]["Processed"]["output_iLead"]["v4_lead_post_process_leads"]
                },
        df_leads)

        return df_leads

    def post_process_ref_install(self, ref_install):
        """
        This module derives new columns for the blank columns after formatting the output.
        @param ref_install: ref_install output after formatting.
        @type ref_install: pd.Dataframe.
        """
        _step = "Deriving columns for output_iLead final data."
        try:
            _step = "Deriving Product Age column using install date column"

            # TODO: for calculation if startup date has values used that else use shipment date.

            ref_install["Product_Age"] = (
                pd.Timestamp.now().normalize()
                - pd.to_datetime(ref_install["ShipmentDate"])
            ) / np.timedelta64(365, "D")

            ref_install["Product_Age"] = ref_install["Product_Age"].astype(int)

            logger.app_success(_step)

            _step = "Deriving is_under_contract column values based on contract end date & today"

            # Get the current date
            current_date = pd.Timestamp.now().normalize()

            # Create the 'is_under_contract' column based on conditions
            ref_install["is_under_contract"] = (
                pd.to_datetime(ref_install["Contract_Expiration_Date"], errors="coerce")
                >= current_date
            ) & ~(ref_install["Contract_Expiration_Date"].isna())

            logger.app_success(_step)

            # TODO: These columns needs to be derived later as of now values a populated False
            # TODO: mapping in config is set to empty that should be assigned later to column
            # TODO: from where the column values will be formatted.

            ref_install.loc[:, "flag_decommissioned"] = False
            ref_install.loc[:, "flag_prior_lead"] = False
            ref_install.loc[:, "flag_prior_service_lead"] = False

            # Area
            ref_area = IO.read_csv(
                self.mode,
                {
                    "file_dir": self.config["file"]["dir_ref"],
                    "file_name": self.config["file"]["Reference"][
                        "area_region"],
                    "adls_config": self.config["adls"]["Reference"][
                        "adls_credentials"],
                    "adls_dir": self.config["adls"]["Reference"][
                        "area_region"],
                },
            )

            ls_priority = ['StartupState', 'ShipTo_State', 'SoldTo_State',
                           "BillingState", "BillingState_old"]
            dict_states_us = self.config['output_contacts_lead']["usa_states"]
            dict_abbr = dict()
            for val in dict_states_us.keys():
                dict_abbr[dict_states_us[val].lower()] = val
            ls_state = list(dict_states_us.keys()) + list(
                dict_states_us.values())
            ls_state = [entry.lower() for entry in ls_state]

            # ref_install.to_csv("ref_install.csv")
            for state in ls_priority:
                ref_install[state] = ref_install[state].apply(
                    lambda state: "" if (str.lower(
                        str(state)) not in ls_state) else state
                )

            for state in ls_priority:
                ref_install[state] = ref_install[state].apply(
                    lambda state: dict_abbr[state] if (
                            str.upper(str(state)) not in dict_states_us.keys()
                            and state != ''
                    )
                    else state
                )

            ref_install['Key_region'] = obj_filt.prioratized_columns(
                ref_install[ls_priority], ls_priority)
            ref_install['Key_region'] = ref_install['Key_region'].str.lower()
            ref_area.Abreviation = ref_area.Abreviation.str.lower()
            ref_install = ref_install.merge(
                ref_area[["Abreviation", "Region", "CSE Area"]],
                left_on="Key_region",
                right_on="Abreviation",
                how="left",
            )
            del ref_install["Key_region"]
            ref_install["Brand"] = "PDI"
            ref_install["Clean_Model_ID"] = (
                ref_install["Brand"] + "-" + ref_install["product_prodclass"]
            )

            # Upgraded Monitor check, 10 Nov, 23
            df_service = IO.read_csv(
                self.mode,
                {
                    "file_dir": self.config["file"]["dir_results"]
                    + self.config["file"]["dir_intermediate"],
                    "file_name": self.config["file"]["Processed"]["services"][
                        "file_name"
                    ],
                    "adls_config": self.config["adls"]["Processed"]["adls_credentials"],
                    "adls_dir": self.config["adls"]["Processed"]["services"],
                },
            )
            df_service = df_service[df_service.component == "Display"]
            df_service = df_service.rename(columns={"SerialNumber": "SerialNumber_M2M"})
            ref_install = ref_install.merge(
                df_service[["type", "SerialNumber_M2M"]],
                on="SerialNumber_M2M",
                how="left",
            )
            ref_install = ref_install.rename(columns={"type": "Upgrade_Type"})
            ref_install.loc[ref_install.Upgrade_Type.notna(), "Upgraded_Monitor"] = True
            ref_install.loc[ref_install.Upgrade_Type.isna(), "Upgraded_Monitor"] = False

            # *** Strategic account ***
            ref_install = self.update_strategic_account(ref_install)

            ref_install = self.add_raise_lead_on(ref_install)

            IO.write_csv(
                self.mode, {
                    'file_dir': (self.config['file']['dir_results'] +
                                 self.config['file']['dir_validation']),
                    'file_name': 'v4_lead_post_process_ref_install.csv',
                    "adls_config": self.config["adls"]["Processed"]["adls_credentials"],
                    "adls_dir": self.config["adls"]["Processed"]["output_iLead"]["v4_lead_post_process_ref_install"]},
            ref_install)

            # Reformat Zip code
            ls_cols = ["End_Customer_Zip", "ShipTo_Zip", "SoldTo_Zip"]
            # ref_install = self.format_zip_code(ref_install, ls_cols)

            return ref_install
        except Exception as e:
            logger.app_fail(f"{_step} : {traceback.print_exc()}", e)
            raise Exception from e

    def update_strategic_account(self, ref_install):
        """

        :param ref_install: DESCRIPTION
        :type ref_install: TYPE
        :return: DESCRIPTION
        :rtype: pandas dataframe

        """
        # Update customer name
        ref_install["Customer_old"] = ref_install["Customer"].copy()
        ref_install["Customer"] = obj_filt.prioratized_columns(
            ref_install, ["StartupCustomer", "ShipTo_Customer", "BillingCustomer"]
        )
        ref_install = ref_install.rename(
            columns={"StrategicCustomer": "StrategicCustomer_old"}
        )

        # ref_install = ref_install.rename({"Customer_old": "Customer", "Customer": "End_Customer"})

        # Update strategic account logic
        obj_sc = StrategicCustomer()
        df_customer = obj_sc.main_customer_list(df_leads=ref_install)
        df_customer = df_customer.drop_duplicates(subset=["Serial_Number"])

        ref_install = ref_install.merge(
            df_customer[["Serial_Number", "StrategicCustomer"]],
            left_on="SerialNumber_M2M",
            right_on="Serial_Number",
            how="left",
        )

        # End To Custoimer details
        ref_install["EndCustomer"] = ref_install["Customer"].copy()
        dict_cols = {
            "End_Customer_Address": ["StartupAddress", "ShipTo_Street"],
            "End_Customer_City": ["StartupCity", "ShipTo_City"],
            "End_Customer_State": ["StartupState", "ShipTo_State"],
            "End_Customer_Zip": ["StartupPostalCode", "ShipTo_Zip"],
        }
        for col in dict_cols:
            ls_cols = ["was_startedup"] + dict_cols[col]
            # Startup columns
            ref_install.loc[:, col] = ref_install[ls_cols].apply(
                lambda x: x[1] if x[0] else x[2], axis=1
            )
        return ref_install

    def post_process_output_ilead(self, output_ilead_df):
        """
        This module derives new columns for the blank columns after formatting the output.
        @param output_ilead_df: iLead output after formatting.
        @type output_ilead_df: pd.Dataframe.
        """
        _step = "Deriving columns for output_iLead final data."

        try:
            output_ilead_df.dropna(subset=["EOSL", "Life__Years"], inplace=True, how='all')
            # Calculate the current year
            current_year = pd.Timestamp.now().year

            _step = "Add lead revenue"
            output_ilead_df = self.add_prices(output_ilead_df)

            _step = "Derive column Component_Due_Date based on Lead_Type, Component_Date_Code"
            # Apply the custom function to create the 'Component_Due_Date' column
            output_ilead_df["Component_Due_Date"] = output_ilead_df.apply(
                self.calculate_component_due_date, axis=1
            )

            output_ilead_df["EOSL"] = output_ilead_df.apply(self.update_eosl, axis=1)


            logger.app_success(_step)

            _step = "Calculating Component_Due_in (years) using Component_Due_Date and current year"

            # Calculate the difference in years between 'Component_Due_Date' and the current year
            output_ilead_df["Component_Due_in (years)"] = (
                pd.to_datetime(output_ilead_df["Component_Due_Date"]).dt.year
                - current_year
            )

            # Convert the 'year_difference' to integer
            output_ilead_df["Component_Due_in (years)"] = output_ilead_df[
                "Component_Due_in (years)"
            ].astype(int)

            logger.app_success(_step)

            _step = "Deriving Component_Due_in (Category) based on Due years"

            # Apply the function to create the 'Component_Due_in (Category)' column
            output_ilead_df["Component_Due_in (Category)"] = output_ilead_df[
                "Component_Due_in (years)"
            ].apply(self.categorize_due_in_category)

            # Add prod meta data
            output_ilead_df = self.prod_meta_data(output_ilead_df)

            IO.write_csv(
                self.mode, {
                    'file_dir': (self.config['file']['dir_results'] +
                                 self.config['file']['dir_validation']),
                    'file_name': 'v4_lead_post_process_output_ilead.csv',
                    "adls_config": self.config["adls"]["Processed"]["adls_credentials"],
                    "adls_dir": self.config["adls"]["Processed"]["output_iLead"]["v4_lead_post_process_output_ilead"]},
            output_ilead_df)
            return output_ilead_df
        except Exception as e:
            logger.app_fail(f"{_step} : {traceback.print_exc()}", e)
            raise Exception from e

    def add_prices(self, df_leads):
        """
        Estimate revenue form generated leads ided based on component.

        :param df_leads: generated leads
        :return: Leads with revue.
        """
        _step = "Read pricing model"
        try:
            # Price of lead is defined by the component / asset for which lead is generated.
            # This reference file has the pricing data.
            ref_price = IO.read_csv(
                self.mode,
                {
                    "file_dir": self.config["file"]["dir_ref"],
                    "file_name": self.config["file"]["Reference"]["price"],
                    "adls_config": self.config["adls"]["Reference"]["adls_credentials"],
                    "adls_dir": self.config["adls"]["Reference"]["price"],
                },
            )
            ref_price = ref_price[['Component', "AvgListPrice"]]
            logger.app_success(_step)
        except Exception as e:
            logger.app_fail(f"{_step} : {traceback.print_exc()}", e)
            raise Exception from e

        _step = "Estimate lead revenue"
        try:
            df_leads = df_leads.merge(ref_price, on="Component", how="left")

            # Report if the pricing model does not contains specific components.
            ls_comp_no_price = df_leads.Component[pd.isna(df_leads.AvgListPrice)].unique()
            if len(ls_comp_no_price) > 0:
                logger.app_info(f"Reference file does NOT contain prices \
                                for following models: {', '.join(ls_comp_no_price)}")

            logger.app_success(_step)
            return df_leads
        except Exception as e:
            logger.app_fail(f"{_step} : {traceback.print_exc()}", e)
            raise Exception from e

    def prod_meta_data(self, output_ilead_df):
        # Partnumber for chasis decides the axle
        _step = "Product meta data"
        try:
            # Read reference data
            ref_chasis = IO.read_csv(
                self.mode,
                {
                    "file_dir": self.config["file"]["dir_ref"],
                    "file_name": self.config["file"]["Reference"]["chasis"],
                    "adls_config": self.config["adls"]["Reference"]["adls_credentials"],
                    "adls_dir": self.config["adls"]["Reference"]["chasis"],
                },
            )

            ref_chasis = ref_chasis.drop_duplicates(subset=["key_chasis"])

            # Get part numbers
            output_ilead_df["key_chasis"] = output_ilead_df["pn_chasis"].copy()

            output_ilead_df["key_chasis"] = output_ilead_df["key_chasis"].fillna("(")

            output_ilead_df.loc[:, "key_chasis"] = output_ilead_df["key_chasis"].apply(
                lambda x: re.split(", | \(", x)[0] if x[0] != "(" else ""
            )

            # Attach data
            output_ilead_df = output_ilead_df.merge(
                ref_chasis, on="key_chasis", how="left"
            )

            return output_ilead_df
        except Exception as e:
            logger.app_fail(f"{_step} : {traceback.print_exc()}", e)
            raise Exception from e

    def pipeline_add_jcomm_sidecar(self, df_leads: object, service_df=None):
        """
        This module read's services data and joins with the lead data and add jcomm, sidecar fields.
        @param service_df: intermediate generated services data.
        @type df_leads: object : leads data after identifying leads.
        """
        _step = (
            "Read Services data and append has_jcomm, has_sidecar field to lead data"
        )

        try:
            if service_df is not None:
                df_service_jcomm_sidecar = service_df

            else:
                df_service_jcomm_sidecar = IO.read_csv(
                    self.mode,
                    {
                        "file_dir": self.config["file"]["dir_results"]
                        + self.config["file"]["dir_intermediate"],
                        "file_name": self.config["file"]["Processed"]["services"][
                            "intermediate"
                        ],
                        "adls_config": self.config["adls"]["Processed"][
                            "adls_credentials"
                        ],
                        "adls_dir": self.config["adls"]["Processed"]["services"][
                            "intermediate"
                        ],
                    },
                )

            df_service_jcomm_sidecar = df_service_jcomm_sidecar[
                ["SerialNumber", "Has_JCOMM", "Has_Sidecar"]
            ]

            df_service_jcomm_sidecar.rename(
                columns={"SerialNumber": "SerialNumber_M2M"}, inplace=True
            )

            df_leads = df_leads.merge(
                df_service_jcomm_sidecar,
                left_on="SerialNumber_M2M",
                right_on="SerialNumber_M2M",
                how="left",
            )

            IO.write_csv(
                self.mode, {
                    'file_dir': (self.config['file']['dir_results'] +
                                 self.config['file']['dir_validation']),
                    'file_name': 'v4_lead_add_jcom_sidecar.csv',
                    "adls_config": self.config["adls"]["Processed"]["adls_credentials"],
                    "adls_dir": self.config["adls"]["Processed"]["output_iLead"]["v4_lead_add_jcom_sidecar"]
                    },
            df_leads)
            logger.app_success(_step)
            return df_leads
        except Exception as e:
            logger.app_fail(f"{_step} : {traceback.print_exc()}", e)
            raise Exception from e

    def pipeline_bom_identify_lead(self, df_install):  # pragma: no cover
        """
        This method reads the bom data joins it with install data and then uses the input
        reference lead file to generate the leads.
        @param df_install: processed install base data coming from contract pipeline
        @return: return a df_lead after identifying the leads.
        """
        # Read : Reference lead opportunities
        _step = "Read : Reference lead opportunities"
        try:
            ref_lead_opp = IO.read_csv(
                self.mode,
                {
                    "file_dir": self.config["file"]["dir_ref"],
                    "file_name": self.config["file"]["Reference"]["lead_opportunities"],
                    "adls_config": self.config["adls"]["Reference"]["adls_credentials"],
                    "adls_dir": self.config["adls"]["Reference"]["lead_opportunities"],
                },
            )
            logger.app_info(f"ref_lead_opp shape: {ref_lead_opp.shape}")

            # Read : Raw BOM data
            _step = "Read raw data : BOM"
            df_bom = IO.read_csv(
                self.mode,
                {
                    "file_dir": self.config["file"]["dir_data"],
                    "file_name": self.config["file"]["Raw"]["bom"]["file_name"],
                    "adls_config": self.config["adls"]["Raw"]["adls_credentials"],
                    "adls_dir": self.config["adls"]["Raw"]["bom"],
                },
            )
            logger.app_info(f"df_bom shape: {df_bom.shape}")

            df_bom[["Job#", "blank"]] = df_bom["Job#"].str.split("-", expand=True)

            input_format = self.config["database"]["bom"]["Dictionary Format"]
            df_bom = self.format.format_data(df_bom, input_format)

            # Merge raw bom data with processed_merge_contract_install dataframe
            _step = "Merge data: Install and BOM"

            df_install[["Job_Index", "blank"]] = df_install["Job_Index"].str.split(
                "-", expand=True
            )
            logger.app_info(f"df_install shape: {df_install.shape}")

            df_bom = self.pipeline_merge(df_bom, df_install, type_="lead_id")

            logger.app_success(_step)

            ref_lead_opp = ref_lead_opp.dropna(
                subset=["EOSL", "Life__Years"], how="all"
            ).reset_index(drop=True)

            # Identify Lead from Part Number TLN and BOM
            _step = "Identify Lead for BOM"
            df_leads = self.identify_leads(df_bom, ref_lead_opp)

            logger.app_success(_step)
        except Exception as e:
            logger.app_fail(f"{_step} : {traceback.print_exc()}", e)
            raise Exception from e

        return df_leads

    def update_sts_leads(self, df_leads):
        """
        Update component life for STS
        :param df_leads: Generated leads
        :type df_leads: pandas DataFrame
        :raises Exception: captures all
        :return: DESCRIPTION
        :rtype: pandas data frame
        """

        _step = 'Update component life for STS'
        try:
            # 20240105: Removed this logic STS will have same age as that of RPP and STS
            # df_leads['flag_update_sts_leads'] = (
            #             (df_leads['Product_M2M_Org'].str.lower() == 'sts') &
            #             (df_leads['Component'].str.lower().isin(
            #                 ['color display',
            #                  'monochrome display', 'm4 display',
            #                  '8212 display'
            #                  #'spd', 'pcb'
            #                  ])))
            # df_leads.loc[df_leads.flag_update_sts_leads, 'Life__Years'] = 7
            ls_partnums = ["pba08555", "pcb08553"]
            # should_update = ((df_leads.Product_M2M_Org == "sts") &
            #                  ~(df_leads.key.isin(ls_partnums)))
            should_update = ((df_leads.Product_M2M_Org == "sts") &
                             (df_leads.Component.str.contains("display", case=False)) &
                             (~ df_leads.key.isin(ls_partnums)))
            df_leads.loc[should_update, 'Component'] = "SBR or LKO Display"

            should_update = ((df_leads.Product_M2M_Org == "sts") &
                             (df_leads.Component.str.contains("display", case=False)) &
                             (df_leads.key.isin(ls_partnums)))
            df_leads.loc[should_update, 'Component'] = "Wavestar Display"

            return df_leads
        except Exception as e:
            logger.app_fail(f"{_step} : {traceback.print_exc()}", e)
            raise Exception from e

    def pipeline_merge_lead_services(self, df_leads, df_services=None):
        """
        This functions reads the services intermediate data and join with the leads data to
        generate a date code column.
        @param df_services: this is adding for testing purpose (processed services data)
        @param df_leads: intermediate leads dataframe.
        @return: return a df_lead merging services with leads dataframe.
        """
        _step = (
            "Merging leads and services data to extract date code at component level"
        )
        try:
            if df_services is None:
                df_services = IO.read_csv(
                    self.mode,
                    {
                        "file_dir": self.config["file"]["dir_results"]
                        + self.config["file"]["dir_intermediate"],
                        "file_name": self.config["file"]["Processed"]["services"][
                            "file_name"
                        ],
                        "adls_config": self.config["adls"]["Processed"][
                            "adls_credentials"
                        ],
                        "adls_dir": self.config["adls"]["Processed"]["services"],
                    },
                )

            # Convert to correct date format
            df_services["ClosedDate"] = pd.to_datetime(
                df_services["ClosedDate"], errors="coerce"
            ).dt.strftime("%Y-%m-%d")

            df_services.sort_values(by="ClosedDate", ascending=False, inplace=True)

            # Identify the duplicate rows based on the two columns and keep the last occurrence
            # using this because when there is no duplicate drop_duplicate can result none
            # when we use keep last.
            duplicates_mask = df_services.duplicated(
                subset=["SerialNumber", "component"], keep="last"
            )

            # Invert the mask to keep the non-duplicate rows
            df_services = df_services[~duplicates_mask]

            unique_component = []
            for i in list(df_services["component"].unique()):
                if "fan" in i.lower():
                    unique_component.append(i.replace("s", "").strip().lower())
                else:
                    unique_component.append(i.strip().lower())

            df_services = df_services[["SerialNumber", "component", "ClosedDate"]]

            df_services["component"] = df_services["component"].str.lower()

            # Convert to correct date format
            df_leads["InstallDate"] = pd.to_datetime(
                df_leads["InstallDate"], errors="coerce"
            ).dt.strftime("%Y-%m-%d")

            df_leads["Component"] = df_leads["Component"].fillna("")
            # Create a new column based on the list values
            df_leads["temp_column"] = df_leads["Component"].apply(
                lambda x: next((val for val in unique_component if val in x.lower()), x)
            )

            df_services.rename(
                columns={"SerialNumber": "SerialNumber_M2M"}, inplace=True
            )

            df_leads = df_leads.merge(
                df_services,
                left_on=["SerialNumber_M2M", "temp_column"],
                right_on=["SerialNumber_M2M", "component"],
                how="left",
            )

            # Replace NaN with empty string
            df_leads = df_leads.fillna("")
            df_leads = df_leads.reset_index(drop=True)

            # Use np.where to create the new column
            df_leads["date_code"] = np.where(
                df_leads["ClosedDate"] != "",
                df_leads["ClosedDate"],
                df_leads["InstallDate"],
            )

            # Use np.where to create the new column
            df_leads["source"] = np.where(
                df_leads["ClosedDate"] != "", "Services", "InstallBase"
            )

            df_leads = df_leads.drop_duplicates().reset_index(drop=True)

            logger.app_success(_step)
            return df_leads
        except Exception as e:
            logger.app_fail(f"{_step} : {traceback.print_exc()}", e)
            raise Exception from e

    def pipeline_merge(self, df_bom, df_install, type_):
        """
        This method merges the bom data and processed install base data on two conditions.
        Either we want to join for purpose of lead generation or for purpose of adding metadata.
        @param df_bom: this is raw bom data.
        @param df_install: A pandas dataframe processed from contracts pipeline.
        @param type_: pd.Dataframe
        @return: pd.Dataframe
        """
        _step = f"Query install data ({type_}"
        try:

            ls_cols = [
                "Job_Index",
                "InstallDate",
                "Product_M2M",
                "SerialNumber_M2M",
                "Revision",
                "PartNumber_TLN_Shipment",
            ]

            if type_ == "lead_id":
                key = "Job_Index"

                if "InstallDate" not in df_install.columns:
                    df_install["InstallDate"] = df_install["startup_date"].fillna(
                        df_install["ShipmentDate"]
                    )
                else:
                    df_install["InstallDate"] = df_install["startup_date"].fillna(
                        df_install["InstallDate"]
                    )

                # Product_M2M : change request by stephen on 17 July, 23.
                df_install["Product_M2M"] = df_install["product_prodclass"].str.lower()

                # Divide the install base data into made to order(mto) and made
                # to stock(mts) category. The entries with job_index are made
                # to batch, entries without job_index is made to stock
                df_install_mto = df_install.loc[df_install["Job_Index"].notna()]

                df_install_mts = df_install.loc[df_install["Job_Index"].isna()]

                # Changed join from "inner" to "right" on 19th July 23 (Bug CIPILEADS-533)
                # Joined  made to batch data with bom data on 23rd Oct, 23
                df_out_mto = df_bom.merge(df_install_mto[ls_cols], on=key, how="right")


                # Added component part numbers for made to stock data on 23rd Oct, 23
                df_out_mts = self.add_data_mts(
                    df_install_mts[ls_cols], merge_type="left"
                )

                # Append the custom data, batch empty data and batch non_empty data
                df_out = df_out_mto._append(df_out_mts, ignore_index=True)

            elif type_ == "meta_data":
                key = "SerialNumber_M2M"
                ls_cols = [col for col in df_install.columns if col not in ls_cols]
                ls_cols = ls_cols + ["SerialNumber_M2M"]
                ls_cols.remove("SerialNumber")

                # Changed join from "inner" to "right" on 19th July 23 (Bug CIPILEADS-533)
                df_out = df_bom.merge(df_install[ls_cols], on=key, how="right")

                IO.write_csv(
                    self.mode, {
                        'file_dir': (self.config['file']['dir_results'] +
                                     self.config['file']['dir_validation']),
                        'file_name': 'v4_lead_merge_bom_install.csv',
                        "adls_config": self.config["adls"]["Processed"]["adls_credentials"],
                        "adls_dir": self.config["adls"]["Processed"]["output_iLead"]["v4_lead_merge_bom_install"]
                        },
                df_out)

        except Exception as e:
            logger.app_fail(f"{_step} : {traceback.print_exc()}", e)
            raise Exception from e
        return df_out

    def pipeline_contract_install(self):  # pragma: no cover
        """
        This returns the processed install base data from contracts pipeline.
        @return: pd.Dataframe
        """
        # Read : Contract Processed data
        _step = "Read processed contract data"
        try:
            df_contract = IO.read_csv(
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
            )

            df_contract = df_contract.drop_duplicates(
                subset=["SerialNumber_M2M"]
            ).reset_index(drop=True)

            df_contract = df_contract.dropna(subset=["product_prodclass"])

        except Exception as e:
            logger.app_fail(f"{_step} : {traceback.print_exc()}", e)
            raise Exception from e

        return df_contract

    # ***** Lead Identification *****

    def identify_leads(self, df_bom, ref_lead_opp):
        """
        These methods identify leads based on ProductM2M and PartNumber_BOM_BOM
        @param df_bom: Raw Bom data.
        @param ref_lead_opp: reference leads data on which leads are generated.
        @return: pd.Dataframe
        """

        df_bom["Product_M2M_Org"] = df_bom["Product_M2M"].copy()
        ls_cols_ref = [
            "",
            "Match",
            "Component",
            "Component_Description",
            "End of Prod",
            "Status",
            "Life__Years",
            "EOSL",
            "flag_raise_in_gp",
        ]
        ls_cols = [
            "Job_Index",
            "Total_Quantity",
            "",
            "InstallDate",
            "Product_M2M_Org",
            "SerialNumber_M2M",
            "key_value",
        ]

        # Identify Leads: TLN
        _step = "Identify Leads: TLN"
        try:
            # Lead_id_bases is changed from PartNumber_TLN_BOM
            lead_id_basedon = "Product_M2M"

            # Subset data : Ref
            ls_cols_ref[0] = lead_id_basedon

            # filter reference lead dataframe where value in Product_M2M is empty or na
            ref_lead = ref_lead_opp.loc[
                pd.notna(ref_lead_opp[ls_cols_ref[0]]), ls_cols_ref
            ]

            # filter rows from reference lead dataframe where Product_M2M value is "blank"
            ref_lead = ref_lead[~ref_lead[lead_id_basedon].isin(["blank", ""])]

            # Subset data : BOM
            ls_cols[2] = lead_id_basedon

            # dropping duplicates on reference lead and bom_install dataframe
            # df_bom = df_bom[ls_cols[:-1]].drop_duplicates()
            ref_lead = ref_lead.drop_duplicates(
                subset=[lead_id_basedon, "Component"]
            ).reset_index(drop=True)
            df_data = df_bom.drop_duplicates(
                subset=[lead_id_basedon, "SerialNumber_M2M"]
            ).reset_index(drop=True)
            df_data["key_value"] = df_data[lead_id_basedon]

            # Id Leads
            df_leads_tln = self.id_leads_for_partno(df_data, ref_lead, lead_id_basedon)
            del df_data, ref_lead
            logger.app_info(
                f"Leads based on {lead_id_basedon}:" + str(df_leads_tln.shape))
        except Exception as e:
            logger.app_fail(f"{_step} : {traceback.print_exc()}", e)
            raise Exception from e

        # Identify Leads: BOM
        try:
            _step = "Identify Leads: BOM"
            lead_id_basedon = "PartNumber_BOM_BOM"

            # Subset data : Ref
            ls_cols_ref[0] = lead_id_basedon
            ref_lead = ref_lead_opp.loc[
                pd.notna(ref_lead_opp[ls_cols_ref[0]]), ls_cols_ref
            ]

            # Subset data : BOM
            ls_cols[2] = lead_id_basedon
            df_data = df_bom[ls_cols[:-1]].drop_duplicates()
            df_data["key_value"] = df_data[lead_id_basedon]

            df_data["PartNumber_BOM_BOM"] = df_data["PartNumber_BOM_BOM"].fillna("")
            # Id Leads
            df_leads_bom = self.id_leads_for_partno(df_data, ref_lead, lead_id_basedon)

            del df_data, ref_lead
            logger.app_info(
                f"Leads based on {lead_id_basedon}:" + str(df_leads_tln.shape))
        except Exception as e:
            logger.app_fail(f"{_step} : {traceback.print_exc()}", e)
            raise Exception from e

        # Merge leads
        try:
            _step = "Merge Leads from TLN and BOM"

            df_leads_out = pd.concat([df_leads_tln, df_leads_bom])
            del df_leads_tln, df_leads_bom

            _step = "Drop Duplicate leads"
            df_leads_out = df_leads_out.drop_duplicates(
                subset=['SerialNumber_M2M', "Component", "key"]
            )
        except Exception as e:
            logger.app_fail(f"{_step} : {traceback.print_exc()}", e)
            raise Exception from e

        # Leads for PDU
        try:
            _step = "Identify leads for PDU"
            lead_id_basedon = 'PartNumber_TLN_BOM'

            # Identify Monitors based on TLN
            # Subset data : Ref
            ls_cols_ref[0] = lead_id_basedon

            ref_pdu_leads = IO.read_csv(
                self.mode, {
                    'file_dir': self.config['file']['dir_ref'],
                    'file_name': "display_4_pdu.csv",
                    'adls_config': self.config['adls']['Reference']['adls_credentials'],
                    'adls_dir': self.config['adls']['Reference']['display_pdu']
                })

            ref_pdu_leads[lead_id_basedon] = ref_pdu_leads[lead_id_basedon].str.lower()
            ref_pdu_leads.loc[
                (ref_pdu_leads['Component'] == 'SQD Monochrome Display'),
                'Component'] = 'Monochrome Display'

            ref_lead = ref_lead_opp.drop_duplicates(subset=['Component'])
            ref_lead = ref_lead.drop(
                columns=['Index', 'Product_M2M', 'PartNum_Comp', 'PartNumber_BOM_BOM', 'Match'])

            ref_lead = ref_lead.merge(ref_pdu_leads, on="Component", how="inner")
            #ref_lead = ref_lead[ls_cols_ref]
            ref_lead['key'] = ref_lead[lead_id_basedon]
            ref_lead = ref_lead[pd.notna(ref_lead.key)]

            _step = "Summarize leads"
            df_leads_sum = df_leads_out.copy()
            df_leads_sum['is_display_lead'] = df_leads_sum.Component.str.contains("display", case=False)
            df_leads_sum = pd.pivot_table(
                df_leads_sum,
                index=['SerialNumber_M2M', 'Product_M2M_Org'], columns=['is_display_lead'],
                values='Component',  aggfunc=len).reset_index().fillna(0)
            if len(df_leads_sum.columns) == 3:
                df_leads_sum.columns = [
                    'SerialNumber_M2M', 'Product_M2M_Org', 'n_display']
            else:
                df_leads_sum.columns = [
                    'SerialNumber_M2M', 'Product_M2M_Org', 'n_non_display', 'n_display']

            ls_srnum_no_dis = df_leads_sum.SerialNumber_M2M[
                (df_leads_sum['n_display'] == 0) &
                (df_leads_sum['Product_M2M_Org'] == 'pdu')]

            # Identify leads
            # All PDUs have display. If a PDU does not have a display lead ,
            # For such PDUs, display leads will be generated using TLN Part Number



            # Subset data : BOM
            ls_cols[2] = lead_id_basedon
            df_data = df_bom[ls_cols[:-1]].drop_duplicates(
                subset=["SerialNumber_M2M"]
            ).reset_index(drop=True)

            # Identify PDUs with no display
            df_data = df_data[df_data.SerialNumber_M2M.isin(ls_srnum_no_dis)]

            df_data["key_value"] = df_data[lead_id_basedon]
            df_lead_pdu = df_data.merge(
                ref_lead, on=lead_id_basedon, how="inner")
            del df_data

            ls_col_out = [
                "Job_Index", "Total_Quantity", "Component", "Component_Description",
                "key", "End of Prod", "Status", "Life__Years", "EOSL",
                "flag_raise_in_gp", "SerialNumber_M2M", "Product_M2M_Org",
                "InstallDate", "key_value"]
            df_lead_pdu = df_lead_pdu[ls_col_out]
            df_leads_out = pd.concat([df_leads_out, df_lead_pdu])

            del df_lead_pdu, df_bom

        except Exception as e:
            logger.app_fail(f"{_step} : {traceback.print_exc()}", e)
            raise Exception from e

        try:
            _step = "Update lead for STS"
            df_leads_out = self.update_sts_leads(df_leads_out)

            IO.write_csv(
                self.mode,
                {
                    "file_dir": self.config["file"]["dir_results"]
                    + self.config["file"]["dir_validation"],
                    "file_name": self.config["file"]["Processed"]["output_iLead"][
                        "before_classify"
                    ],
                    "adls_config": self.config["adls"]["Processed"]["adls_credentials"],
                    "adls_dir": self.config["adls"]["Processed"]["output_iLead"][
                        "before_classify"
                    ],
                },
                df_leads_out,
            )
            logger.app_debug(f"No of leads b4 classify: {df_leads_out.shape[0]}")


            ref_drop = ref_lead_opp[[
                "PartNumber_BOM_BOM", "date_greater_than",
                "change_component_to", "change_status_to",
                "change_life_years_to", "change_eosl_to"
            ]]
            ref_drop = ref_drop.applymap(lambda x: None if pd.isna(x) else x)
            ref_drop = ref_drop.applymap(lambda x: None if pd.isna(x) else x)
            ref_drop["change_life_years_to"] = ref_drop[
                "change_life_years_to"].replace(np.nan, None)
            ref_drop = ref_drop.dropna(
                subset=['date_greater_than'], how='all')
            ref_drop.date_greater_than = pd.to_datetime(
                ref_drop.date_greater_than)
            ref_drop.date_greater_than = ref_drop.date_greater_than.dt.strftime(
                '%Y-%m-%d').astype(str)
            ref_drop["date_greater_than"] = ref_drop[
                "date_greater_than"].replace('nan', None)
            update_cols = {
                "change_component_to": "Component",
                "change_status_to": "Status",
                "change_life_years_to": "Life__Years",
                "change_eosl_to": "EOSL"
            }
            for _, row in ref_drop.iterrows():
                condition = (
                    (
                        df_leads_out.key == row.PartNumber_BOM_BOM.lower()
                    ) &
                    (
                        df_leads_out.InstallDate.astype(str)
                        >= row.date_greater_than
                    )
                )
                update = dict()
                for col in update_cols.keys():
                    if row.loc[col]:
                        update[col] = row.loc[col]
                for col in update.keys():
                    df_leads_out.loc[condition, update_cols[col]] = update[col]

            df_leads_out = self.classify_lead(df_leads_out)
            logger.app_debug(f"No of leads after classify: {df_leads_out.shape[0]}")

            IO.write_csv(
                self.mode,
                {
                    "file_dir": self.config["file"]["dir_results"]
                    + self.config["file"]["dir_validation"],
                    "file_name": self.config["file"]["Processed"]["output_iLead"][
                        "after_classify"
                    ],
                    "adls_config": self.config["adls"]["Processed"]["adls_credentials"],
                    "adls_dir": self.config["adls"]["Processed"]["output_iLead"][
                        "after_classify"
                    ],
                },
                df_leads_out,
            )
        except Exception as e:
            logger.app_fail(f"{_step} : {traceback.print_exc()}", e)
            raise Exception from e

        return df_leads_out

    def id_leads_for_partno(self, df_data, ref_lead, lead_id_basedon):
        """

        Reference file identifies the leads based on TLN i.e. assembly level parts. Only one
        pattern exists i.e. bigns_with. In future  if new patterns are identified,
        code would require changes.
        @param df_data: This the processed bom and install data
        @type df_data: pd.Dataframe
        @param ref_lead: this input reference lead data.
        @type ref_lead: pd.Dataframe
        @param lead_id_basedon: this is a column based on which leads is generated.
        @type lead_id_basedon: str

        """
        _step = f"Identify lead based on {lead_id_basedon}"
        try:
            # Input
            types_patterns = ref_lead.Match.unique()

            # Initialize Output
            df_out = pd.DataFrame()

            # total 12 columnns as output
            ls_col_out = [
                "Job_Index",
                "Total_Quantity",
                "Component",
                "Component_Description",
                "key",
                "End of Prod",
                "Status",
                "Life__Years",
                "EOSL",
                "flag_raise_in_gp",
                "SerialNumber_M2M",
                "Product_M2M_Org",
            ]

            # Prep reference file
            # rename 'Product_M2M' or "PartNumber_BOM_BOM" column to "key"
            ref_lead = ref_lead.rename(columns={ref_lead.columns[0]: "key"})

            # find the length on values in key column and store them in a new col called "len_key"
            ref_lead["len_key"] = ref_lead.key.str.len()

            # sort the values in descending order i.e 3,2,1 based on values of "len_key" column.
            ref_lead = ref_lead.sort_values(by=["len_key"], ascending=False)

            # convert all the values in "key" column to lowercase
            ref_lead["key"] = ref_lead["key"].str.lower()

            # Add two more column names to output list of columns after adding total 14 columns
            ls_col_out += ["InstallDate", "key_value"]

            # Identify leads where PartNumber matches exactly.
            # if lead is identified for PartNumber;
            # then it will be dropped from further processing

            if "exact" in types_patterns:
                # filter reference lead data based on "match" columns values
                df_ref_sub = ref_lead[ref_lead.Match == "exact"]
                df_cur_out, df_data = self.lead4_exact_match(
                    df_data, df_ref_sub, lead_id_basedon, ls_col_out
                )

                # if the current output has more than 0 rows then add a new column "lead_match"
                # assign the values as "exact" in this column
                if df_cur_out.shape[0] > 0:
                    df_cur_out.loc[:, "lead_match"] = "exact"

                    df_out = pd.concat([df_out, df_cur_out])
                del df_ref_sub, df_cur_out

            if "contains" in types_patterns:
                # filter reference lead data based on "match" columns values
                df_ref_sub = ref_lead[ref_lead.Match == "contains"]
                df_cur_out, df_data = self.lead4_contains(
                    df_data, df_ref_sub, lead_id_basedon, ls_col_out
                )

                # if the current output has more than 0 rows then add a new column "lead_match"
                # assign the values as "contains" in this column
                if df_cur_out.shape[0] > 0:
                    df_cur_out.loc[:, "lead_match"] = "contains"
                    df_out = pd.concat([df_out, df_cur_out])
                del df_ref_sub, df_cur_out

            if "begins_with" in types_patterns:
                # filter reference lead data based on "match" columns values
                df_ref_sub = ref_lead[ref_lead.Match == "begins_with"]
                df_cur_out, df_data = self.lead4_begins_with(
                    df_data, df_ref_sub, lead_id_basedon, ls_col_out
                )

                # if the current output has more than 0 rows then add a new column "lead_match"
                # assign the values as "begins_with" in this column
                if df_cur_out.shape[0] > 0:
                    df_cur_out.loc[:, "lead_match"] = "begins_with"
                    df_out = pd.concat([df_out, df_cur_out])
                del df_ref_sub, df_cur_out

            # Metadata,
            # add a column called as "lead_id_basedon" which indicate the lead was calculated
            # on what type of column either PartNumber_TLN_BOM or PartNumber_BOM_BOM
            df_out["lead_id_basedon"] = lead_id_basedon
            logger.app_debug(f"{_step} : SUCCEEDED", 1)
            return df_out
        except Exception as e:
            logger.app_fail(f"{_step} : {traceback.print_exc()}", e)
            raise Exception from e


    # ***** Match ref data for lead identification *****
    def lead4_exact_match(self, df_temp_data, df_ref_sub, lead_id_basedon,
                          ls_col_out):
        """
        This method runs when there is an exact keyword in Match column in reference leads
        @param df_temp_data: processed bom and install data
        @param df_ref_sub: reference lead data after filtering
        @param lead_id_basedon: column based on which leads will be generated
        @param ls_col_out: list of output columns
        @return: pd.Dataframe.
        """
        _step = f"identify leads key: {lead_id_basedon}, match: exact"

        try:
            logger.app_info(f"{_step}: STARTED")
            org_size = df_temp_data.shape[0]
            ls_col_in = df_temp_data.columns
            logger.app_debug(f"Data Size Original : {df_temp_data.shape[0]}; ")

            df_temp_data["key"] = df_temp_data[lead_id_basedon].copy()
            df_temp_data = df_temp_data.drop_duplicates()
            df_temp_data["key"] = df_temp_data["key"].str.lower()

            df_ref_sub = df_ref_sub.drop_duplicates(subset=["key", "Component"])

            df_decoded = df_temp_data.merge(df_ref_sub, on="key", how="left")

            # Consolidated leads. For TLN where lead has been identified,
            # will be added to output

            if any(pd.notna(df_decoded.Component)):
                df_out_sub = df_decoded.loc[pd.notna(df_decoded.Component), ls_col_out]
            else:
                df_out_sub = pd.DataFrame()

            # df_temp_data: Remove keys with identified lead from further processing

            df_temp_data = df_decoded.loc[pd.isna(df_decoded.Component), ls_col_in]

            # Cross-checking
            new_size = df_temp_data.shape[0]

            del df_ref_sub, df_decoded

            logger.app_debug(
                f"New Size: {new_size}; " f"Size Drop : {org_size - new_size}"
            )

            logger.app_debug(f"{_step} : SUCCEEDED", 1)

        except Exception as e:
            logger.app_fail(f"{_step} : {traceback.print_exc()}", e)
            raise Exception from e
        return df_out_sub, df_temp_data

    def lead4_begins_with(self, df_temp_data, df_ref_sub, lead_id_basedon,
                          ls_col_out):
        """
        This method runs when there is a begin_with keyword in Match column in reference leads
        @param df_temp_data: processed bom and install data
        @param df_ref_sub: reference lead data after filtering
        @param lead_id_basedon: column based on which leads will be generated
        @param ls_col_out: list of output columns
        @return: pd.Dataframe.
        """

        _step = f"identify leads key: {lead_id_basedon}, match: being_with"
        try:
            logger.app_info(f"{_step}: STARTED")
            org_size = df_temp_data.shape[0]
            df_ref_sub = df_ref_sub.drop_duplicates(subset=["key", "Component"])

            df_out_sub = pd.DataFrame()
            ls_key_len = df_ref_sub.len_key.unique()
            ls_col_in = df_temp_data.columns

            logger.app_debug(f"Data Size Original : {df_temp_data.shape[0]}; ")

            for key_len in ls_key_len:
                # key_len = ls_key_len[0]
                org_size = df_temp_data.shape[0]

                df_temp_data["key"] = df_temp_data[lead_id_basedon].apply(
                    lambda x: x[:key_len].lower()
                )

                df_decoded = df_temp_data.merge(df_ref_sub, on="key", how="left")

                # Consolidated leads. For TLN where lead has been identified,
                # will be added to output

                if any(pd.notna(df_decoded.Component)):
                    df_cur_out = df_decoded.loc[
                        pd.notna(df_decoded.Component), ls_col_out
                    ]
                else:
                    df_cur_out = pd.DataFrame()
                df_out_sub = pd.concat([df_out_sub, df_cur_out])

                # Filter data from further processing for keys with lead identified

                df_temp_data = df_decoded.loc[pd.isna(df_decoded.Component), ls_col_in]

                # Cross-checking
                new_size = df_temp_data.shape[0]

                # logger.app_debug(
                #     f"For keylen: {key_len}; "
                #     f"New Size: {new_size}; "
                #     f"Size Drop : {org_size - new_size}"
                # )

                del df_cur_out, df_decoded

            logger.app_debug(f"{_step} : SUCCEEDED", 1)

        except Exception as e:
            logger.app_fail(f"{_step} : {traceback.print_exc()}", e)
            raise Exception from e

        return df_out_sub, df_temp_data

    def lead4_contains(self, df_temp_data, df_ref_sub, lead_id_basedon,
                       ls_col_out):
        """
        This method runs when there is a contains keyword in Match column in reference leads
        @param df_temp_data: processed bom and install data
        @param df_ref_sub: reference lead data after filtering
        @param lead_id_basedon: column based on which leads will be generated
        @param ls_col_out: list of output columns
        @return: pd.Dataframe.
        """
        _step = f"identify leads key: {lead_id_basedon}, match: contains"
        try:
            ls_col_in = df_temp_data.columns
            df_out_sub = pd.DataFrame()
            ls_key = df_ref_sub["key"].unique()
            ls_col_in = df_temp_data.columns

            logger.app_info(f"{_step}: STARTED")
            logger.app_debug(f"Data Size Original : {df_temp_data.shape[0]}; ")

            # Develop regex
            df_ref_sub = df_ref_sub.drop_duplicates(subset=["key", "Component"])
            df_ref_sub["len_key"] = df_ref_sub.key.str.len()
            df_ref_sub = df_ref_sub.sort_values(by=["len_key"], ascending=False)
            re_pat = "|".join(df_ref_sub['key'])
            re_pat = re.compile(re_pat, flags=re.IGNORECASE)

            # Extract patterns
            df_temp_data[lead_id_basedon] = df_temp_data[lead_id_basedon].fillna("")
            df_temp_data['ls_BomParts'] = df_temp_data[lead_id_basedon].str.findall(
                re_pat)
            df_temp_data['n_BomParts'] = df_temp_data['ls_BomParts'].apply(lambda x: len(x))

            if max(df_temp_data['n_BomParts']) > 1:
                logger.app_info("!!! Needs investigation !!!")

            # Merge data
            df_temp_data["BomPart"] = df_temp_data["ls_BomParts"].apply(
                lambda ls_bom: ls_bom[0] if len(ls_bom) > 0 else "")
            df_temp_data["flag_valid"] = (df_temp_data["BomPart"] != "")
            df_temp_data["left_key"] = df_temp_data["BomPart"].copy()

            # Create Output
            if any(df_temp_data["flag_valid"]):
                df_out_sub = df_temp_data.merge(
                    df_ref_sub, how="left", left_on="left_key", right_on="key"
                )
                df_out_sub = df_out_sub[ls_col_out]
            df_temp_data = df_temp_data.loc[
                df_temp_data.flag_valid == False, ls_col_in
            ]
            logger.app_debug(f"n_leads: {df_out_sub.shape[0]}, n_raw: {df_temp_data.shape[0]}")
            logger.app_debug(f"{_step} : SUCCEEDED", 1)

        except Exception as e:
            logger.app_fail(f"{_step} : {traceback.print_exc()}", e)
            raise Exception from e

        return df_out_sub, df_temp_data

    # ***** Classify leads *****
    def classify_lead(self, df_leads_wn_class, test_services=None):
        """
        There are 2 types of component replacement leads for DCPD:
            1. EOSL: if a component reaches its End of Service Life
            2. Life: based on age of component if it has reached its design life
                if Life of component > design life, then only lead will be raised.

        :param df_leads_wn_class: BOM data mapped with lead opportunities
        :type df_leads_wn_class: pandas dataframe
        :raises Exception: DESCRIPTION
        :return: leads classified as EOSL or Life.
        :rtype: pandas dataframe
        :param test_services: this is added for unit testing purpose.

        """
        _step = "Lead classification"
        try:
            df_leads = pd.DataFrame()

            # Merge generated leads and services data
            try:
                _step = "Merge lead and services data"
                if test_services is None:
                    df_leads_wn_class = self.pipeline_merge_lead_services(
                        df_leads_wn_class
                    )

                logger.app_success(_step)  ##Changes made of trial shree
            except Exception as e:
                logger.app_fail(f"{_step} : {traceback.print_exc()}", e)
                raise Exception from e


            # Derive fields
            df_leads_wn_class.loc[:, "date_code"] = pd.to_datetime(
                df_leads_wn_class["date_code"]
            )
            df_leads_wn_class.loc[:, "today"] = pd.to_datetime(pd.Timestamp.now())

            df_leads_wn_class["age"] = (
                (
                    (df_leads_wn_class["today"] - df_leads_wn_class["date_code"])
                    / np.timedelta64(365, "D")
                )
                .round()
                .astype(int)
            )


            # EOSL Leads
            df_leads_wn_class.EOSL = df_leads_wn_class.EOSL.fillna("")
            flag_lead_eosl = df_leads_wn_class.EOSL != ""

            if any(flag_lead_eosl):
                df_leads_sub = df_leads_wn_class[flag_lead_eosl].copy()
                df_leads_sub.loc[:, "lead_type"] = "EOSL"
                df_leads = pd.concat([df_leads, df_leads_sub])
                del df_leads_sub
            del flag_lead_eosl

            df_leads_wn_class = df_leads_wn_class[df_leads_wn_class["EOSL"] == ""]
            # For DCPD leads due this year will be processed
            df_leads_wn_class.Life__Years = pd.to_numeric(
                df_leads_wn_class.Life__Years)
            flag_lead_life = pd.notna(df_leads_wn_class.Life__Years)
            if any(flag_lead_life):
                df_leads_sub = df_leads_wn_class[flag_lead_life].copy()
                df_leads_sub["Life__Years"] = df_leads_sub["Life__Years"].replace(
                    "", pd.NA
                )
                df_leads_sub["Life__Years"] = (
                    pd.to_numeric(df_leads_sub["Life__Years"], errors="coerce")
                    .fillna(0)
                    .astype(int)
                )
                df_leads_sub.loc[:, "flag_include"] = (
                    df_leads_sub["age"] > df_leads_sub["Life__Years"]
                )

                # if any(df_leads_sub.flag_include):
                df_leads_sub.loc[:, "lead_type"] = "Life"
                # df_leads_sub = df_leads_sub[df_leads_sub['flag_include']]
                df_leads = pd.concat([df_leads, df_leads_sub])

                del flag_lead_life

            logger.app_debug(f"{_step} : SUCCEEDED", 1)


        except Exception as e:
            logger.app_fail(f"{_step} : {traceback.print_exc()}", e)
            raise Exception from e

        return df_leads

    def calculate_component_due_date(self, row):
        """
        Define a custom function to calculate the 'Component_Due_Date'
        """
        if row["lead_type"] == "EOSL":
            eosl_year = int(row["EOSL"])
            first_day_of_year = pd.to_datetime(f"01/01/{eosl_year}", format="%m/%d/%Y")
            return first_day_of_year.strftime("%m/%d/%Y")

        component_date_code = pd.to_datetime(row["date_code"], format="%m/%d/%Y")

        component_life_years = pd.DateOffset(years=row["Life__Years"])
  
        return (component_date_code + component_life_years).strftime("%m/%d/%Y")

    def update_eosl(self, row):
        """
        Changes the format of EOSL column from year to date
        """
        if row["lead_type"] == "EOSL":
            return row["Component_Due_Date"]
        return row["EOSL"]

    def add_data_mts(self, df_install_mts, merge_type):
        """
        Method to join made to stock data with standard bom data
        :param df_install_mts: Made to stock install data
        :param merge_type: merge type
        :return: Merged data
        """
        # Read Standard BOM data and preprocess standard bom data
        df_bom_data_default = IO.read_csv(
            self.mode,
            {
                "file_dir": self.config["file"]["dir_data"],
                "file_name": self.config["file"]["Raw"]["bomdata_deafault"][
                    "file_name"
                ],
                "adls_config": self.config["adls"]["Raw"]["adls_credentials"],
                "adls_dir": self.config["adls"]["Raw"]["bomdata_deafault"],
            },
        )
        df_bom_sisc = IO.read_csv(
            self.mode,
            {
                "file_dir": self.config["file"]["dir_data"],
                "file_name": self.config["file"]["Raw"]["bomdata_sisc"]["file_name"],
                "adls_config": self.config["adls"]["Raw"]["adls_credentials"],
                "adls_dir": self.config["adls"]["Raw"]["bomdata_sisc"],
            },
        )
        df_standard_bom = df_bom_sisc._append(df_bom_data_default, ignore_index=True)

        df_standard_bom.rename(
            columns={
                "fparent": "PartNumber_TLN_Shipment",
                "fcomponent": "PartNumber_BOM_BOM",
                "fparentrev": "Revision",
                "fqty": "Total_Quantity",
            },
            inplace=True,
        )


        df_standard_bom["PartNumber_TLN_Shipment"] = (
            df_standard_bom["PartNumber_TLN_Shipment"].str.lstrip().str.rstrip()
        )

        df_standard_bom["PartNumber_BOM_BOM"] = (
            df_standard_bom["PartNumber_BOM_BOM"].str.lstrip().str.rstrip()
        )

        df_standard_bom = df_standard_bom[
            [
                "PartNumber_TLN_Shipment",
                "PartNumber_BOM_BOM",
                "Revision",
                "Total_Quantity",
            ]
        ]

        # Preprocess made to stock data
        df_install_mts["PartNumber_TLN_Shipment"] = (
            df_install_mts["PartNumber_TLN_Shipment"]
            .astype(str)
            .str.lstrip()
            .str.rstrip()
        )


        # Merge made to stock data with standard bom data based on shipment
        # number and revision
        df_install_mts = df_install_mts.merge(
            df_standard_bom, on=["PartNumber_TLN_Shipment", "Revision"], how=merge_type
        )

        # Further divide the mts data based on if the join was successful i.e.
        # obtained BOM number is null or not.
        # For the batch data where join fails, it doesn't seem to have a proper
        # revision, hence we merge it with the first revision available
        df_install_mts_joined = df_install_mts.loc[
            df_install_mts["PartNumber_BOM_BOM"].notna()
        ]
        df_install_mts_not_joined = df_install_mts.loc[
            df_install_mts["PartNumber_BOM_BOM"].isna()
        ]

        # Preprocess standard bom data to select the first revision for every shipment
        df_standard_bom[["PartNumber_TLN_Shipment", "Revision"]].fillna(
            "", inplace=True
        )
        df_standard_bom["key"] = (
            df_standard_bom["PartNumber_TLN_Shipment"]
            + ":"
            + df_standard_bom["Revision"]
        )
        df_standard_bom.drop_duplicates(
            ["PartNumber_TLN_Shipment", "key"], inplace=True
        )
        df_standard_bom.sort_values(by=["key"])
        df_standard_bom.drop_duplicates(
            ["PartNumber_TLN_Shipment"], keep="first", inplace=True
        )
        df_standard_bom.drop(["key"], axis=1, inplace=True)

        # Merge the not joined data with the first revision available
        df_install_mts_not_joined.drop(
            ["PartNumber_BOM_BOM", "Revision", "Total_Quantity"], axis=1, inplace=True
        )
        df_install_mts_not_joined = df_install_mts_not_joined.merge(
            df_standard_bom, on=["PartNumber_TLN_Shipment"], how=merge_type
        )

        # Append joined data and unjoined data to get complete made to stock data
        df_install_mts = df_install_mts_joined._append(
            df_install_mts_not_joined, ignore_index=True
        )

        IO.write_csv(
            self.mode, {
                'file_dir': (self.config['file']['dir_results'] +
                             self.config['file']['dir_validation']),
                'file_name': 'v4_lead_made_to_stock.csv',
                "adls_config": self.config["adls"]["Processed"]["adls_credentials"],
                "adls_dir": self.config["adls"]["Processed"]["output_iLead"]["v4_lead_made_to_stock"]},
            df_install_mts)

        return df_install_mts

    def categorize_due_in_category(self, component_due_in_years):
        """
        Method categorises component due date
        :param component_due_in_years: Due date in years
        :return: Due category
        """
        if component_due_in_years < 0:
            return "Past Due"
        if 0 <= component_due_in_years <= 1:
            return "Due this year"
        if 1 < component_due_in_years <= 3:
            return "Due in 2-3 years"
        if 3 < component_due_in_years < 100:
            return "Due after 3 years"
        return "Unknown"  # Or any other default category you want to assign

    def add_raise_lead_on(self, ref_install):
        """
        Method to calculate the date to raise the lead on
        :param ref_install: Reference install dataframe
        :return: Updated reference install dataframe
        """
        raise_lead_in = self.config["lead_generation"]["raise_lead_in"]
        ref_install["Raise_Lead_In"] = raise_lead_in
        ref_install["Raise_Lead_On"] = (
            pd.to_datetime(ref_install["Contract_Expiration_Date"], errors="coerce")
            - timedelta(raise_lead_in)
        ).dt.date

        ref_install.loc[
            (
                (
                    (ref_install.Contract_Conversion == "Warranty Due")
                    | (ref_install.Contract_Conversion == "Warranty Conversion")
                    | (ref_install.Contract_Conversion == "No Contract")
                )
            ),
            "Lead_type",
        ] = "Warranty Conversion"
        ref_install.loc[
            (
                (
                    (ref_install.Contract_Conversion == "New Business")
                    | (ref_install.Contract_Conversion == "No Warranty / Contract")
                    | (ref_install.Contract_Conversion == "No Warranty")
                )
            ),
            "Lead_type",
        ] = "Contract Leads"

        return ref_install

    def output_ref_install_map_level_reformat(self, df_input):
        """
        Sumamrize location data by serial number for UI UX.

        param: Ref install base data
        type: pandas data frame
        return: status
        type: str
        """
        ls_usa_states = (
            self.config['output_contacts_lead']["usa_states"]
        )
        df_input = df_input[
            ["Serial_Number", 'End_Customer_Address', 'End_Customer_City',
             'End_Customer_State']]  # 'EndCustomer',

        dict_rename = {"End_Customer_Address": "Site",
                       "End_Customer_City": "City",
                       'End_Customer_State': " State"}
        df_input = df_input.rename(columns=dict_rename)

        ls_usa_states = {v: k for k, v in ls_usa_states.items()}

        # Ensure state names are in standard format
        df_input[" State"] = df_input[" State"].apply(
            lambda txt: ls_usa_states[txt] if txt in list(
                ls_usa_states.keys()) else txt)

        # Creating entire address
        df_input[' State'] = df_input[' State'] + ", USA"
        df_input['City'] = df_input['City'] + ", " + df_input[' State']
        df_input['Site'] = df_input['Site'] + ", " + df_input['City']

        df_form = pd.melt(df_input, id_vars=['Serial_Number'],
                          value_vars=[' State', 'City', 'Site']).sort_values(
            by=["Serial_Number"])

        return df_form

    def ref_install_unit_summary_reformat(self, df_leads):
        """
        Aim : This Function has been developed to provide the unit details (Install Base),
         according to the UI/UX design requirements for Customer_report Page.

        par : df_leads
        type : data frame
        return:
        type: data frame
        """
        _step = "Formatting ref_install_base for UI/UX"
        try:
            # *** Constants ***

            ls_cols_compsum = ['Serial_Number', 'Txt_Party_Site_Number',
                               'Product_Name', 'Model_Id', 'Product_Age',
                               'Contract_End_Date']  # 'Site_Description', 'Contract_Line_Status'

            dict_rename = {'Serial_Number': 'Serial no.',
                           'Txt_Party_Site_Number': 'Site name',
                           'Product_Name': 'Unit type', 'Model_Id': 'Model',
                           'Product_Age': 'Age',
                           'Contract_End_Date': 'Contract expiry date'}

            # Formating in form of Long format from wide, Provide the list that you want to keep in the column
            ls_wide_to_long = ['Serial no.', 'Site name', 'Unit type', 'Model',
                               'Age',
                               'Contract expiry date']  # 'Contract status'

            # Dict to provide Order of details
            dict_index = {"Serial no.": 0, "Site name": 1, "Unit type": 2,
                          "Model": 3, "Age": 4,
                          "Contract expiry date": 5}  # 'Site description':2,

            # *** Data Formating ***
            df_leads = df_leads[ls_cols_compsum]  # Select columns of interest
            df_leads.rename(dict_rename, axis=1,
                            inplace=True)  # Rename columns per UI/UX  design requirements

            df_leads.loc[:, 'Contract expiry date'] = df_leads[
                'Contract expiry date'].fillna("No Contract")
            df_leads["Sr_Num"] = df_leads["Serial no."]

            df_leads_melted = pd.melt(df_leads, id_vars=['Sr_Num'],
                                      value_vars=ls_wide_to_long)
            df_leads_melted = df_leads_melted.sort_values(by='Sr_Num')

            df_leads_melted["Unit summary"] = df_leads_melted[
                ["variable", "value"]].apply(lambda x: x[0] + ": " + str(x[1]),
                                             axis=1)  # Metadta addition

            df_leads_melted['Index'] = df_leads_melted.variable.apply(
                lambda x: dict_index[x])

            df_leads_melted['Key'] = df_leads_melted['Sr_Num'] + ":" + \
                                     df_leads_melted['Index'].astype(str)
            df_leads_melted = df_leads_melted.sort_values(['Key'])

            df_leads_melted["Highlight Cell"] = df_leads_melted[
                ["variable", "value"]].apply(
                lambda x: 3 if x[0] != "Contract expiry date"
                else (2 if x[1] == "No Contract"
                      else 1 * (
                            pd.to_datetime(x[1]) >= pd.to_datetime("today"))),
                axis=1)
        except Exception as e:
            logger.app_fail(f"{_step} : {traceback.print_exc()}", e)
            raise Exception from e

        return df_leads_melted

    def output_ilead_component_summary_reformat(self, df_leads):
        """
        Aim : This Function has been developed to provide the components details(Output_iLeads), according to the UI/UX design requirements for Customer_report Page.

        par : df_leads
        type : data frame
        return:
        type: data frame
        """
        _step = "Formatting output iLeads for UI/UX"
        try:
            # *** Constants ***
            ls_cols_compsum = [
                'Serial_Number', 'Component', 'Component_Due_in (Category)',
                'Component_Due_in (years)', 'Component_Date_Code',
                'Component_Due_Date', 'Lead_Type']

            dict_rename = {'Component_Due_in (Category)': 'Comp. status',
                           'Component_Due_in (years)': 'Est. life',
                           'Component_Date_Code': 'Date code',
                           'Component_Due_Date': 'Due date',
                           'Lead_Type': 'Service status'}

            # As per UI/UX  design requirements, 1st row is for serial no (blank) and 2nd row is for headings of component summary. ( For matching Key-key values with ref install base)
            n_empty_header_rows = 2

            # *** Data Formating ***
            df_form_leads = df_leads[
                ls_cols_compsum].copy()  # Select columns of interest
            df_form_leads.rename(dict_rename, axis=1,
                                 inplace=True)  # Rename columns per UI/UX  design requirements

            df_form_leads = df_form_leads.sort_values(
                by=['Serial_Number', 'Due date'], ascending=True)
            df_form_leads['Index'] = df_form_leads.groupby(
                ['Serial_Number']).cumcount() + n_empty_header_rows

            # We need Unique number for components of a Unique Serial number, SO all components of each serial number has been ranked as "KEY" --> Serial Number:Index.
            df_form_leads.loc[:, 'Key'] = df_form_leads[
                                              'Serial_Number'] + ":" + \
                                          df_form_leads['Index'].astype(str)

            # Adding Headings of the each component of a unique serial no

            df = pd.DataFrame()  # an Empty data frame

            for col in df_form_leads.columns:
                if col == "Serial_Number":
                    df["Serial_Number"] = df_form_leads[
                        "Serial_Number"].unique()
                elif col == "Index":
                    df["Index"] = 1
                else:
                    df[col] = col

            df.loc[:, 'Key'] = df['Serial_Number'] + ":" + df['Index'].astype(
                str)
            # print(f"The headers data frame is {df}.")

            df_form_leads = pd.concat([df_form_leads, df])
            df_form_leads = df_form_leads.sort_values(['Key'])
            # print(f"The headers data frame is {df_form_leads}.")`

            logger.app_success(_step)
        except Exception as e:
            logger.app_fail(f"{_step} : {traceback.print_exc()}", e)
            raise Exception from e
        return df_form_leads

    def format_zip_code(self, df_data, ls_cols):
        """
        Reformat the zip code to American Standards
        :param df_data: Input data
        :param ls_cols: columns for transformation
        :return: Transformed data
        """
        _step = "Format zip codes"
        try:
            for col in ls_cols:
                if col in df_data.columns:
                    df_data[col].fillna(0, inplace=True)
                    df_data[col] = df_data[col].astype(float).astype(int)
                    df_data[col] = df_data[col].fillna("-----").astype(
                        str).str.zfill(5)
                else:
                    logger.app_info(f"Missing column in zip: {col}")
            return df_data
        except Exception as e:
            logger.app_fail(f"{_step} : {traceback.print_exc()}", e)
            raise Exception from e

# %%
if __name__ == "__main__":
    obj = LeadGeneration()
    obj.main_lead_generation()
