"""@file class_services_data

@brief : For DCPD business, analyze services data to check if a component was replaced or upgraded during service.
Results to be consumed by lead generation to get accurate component age.

@details : This method implements services class data which serves as an input
to the lead generation data file. Code identifies the updated date code/install date of components based on services data.
So that current age can be calculated. Component type filtering is performed based on key filtering components.
There are two conditions which are considered for component type.
1. Only Display components can be Replaced and Upgraded.
2. Rest of all component such as BCMS, Breaker, Fans, SPD, PCB support only
replacement.

@copyright 2023 Eaton Corporation. All Rights Reserved.
@note Eaton Corporation claims proprietary rights to the material disclosed
here on. This technical information may not be reproduced or used without
direct written permission from Eaton Corporation.
"""

# %% Load Libraries
from string import punctuation
import traceback
import os
import pandas as pd
import numpy as np
import sys
import json
import re
import time

# %% Setup env
config_dir = os.path.join(os.path.dirname(__file__), "../../config")
config_file = os.path.join(config_dir, "config_dcpd.json")
with open(config_file,'r') as config_file:
    config = json.load(config_file)

mode = config.get("conf.env", "azure-adls")
if mode == "local":
    path = os.getcwd()
    path = os.path.join(path.split('ileads_lead_generation')[0], 'ileads_lead_generation')
    os.chdir(path)

# %% Initialize packages
from utils import AppLogger
from utils import IO
from utils import Filter
from utils import Format
from utils.dcpd.class_common_srnum_ops import SearchSrnum
from utils.dcpd.class_business_logic import BusinessLogic
import utils.dcpd.class_contracts_data as ccd
# from utils.get_env_var_size import *


# Create instance of the class
formatObj = Format()
filterObj = Filter()
contractObj = ccd.Contract()
busLogObj = BusinessLogic()
srnumObj = SearchSrnum()
loggerObj = AppLogger(__name__)
punctuation = punctuation + ' '
# %% Class definition
class ProcessServiceIncidents:
    """
    Class implements the method for extracting serial numbers
    from the raw services data based on various conditions. It considers the
    component type as Replace / Upgrade for various parameters including
    Display, PCB, SPD, Fans, Breaker.

    """

    def __init__(self):
        """The __init__ function sets the config
           variable by reading the json file containing
           information about the storage accounts, file names etc.
           and initializes the mode variable to either local or azure-adls.
        """

        self.mode = config.get("conf.env")
        self.config = config
        self.msg_done = ": DONE"
        self.msg_start = ": STARTED"
        self.e_flag = self.config.get("e_flag", "noescape")
        
        self.ls_cols = self.config['ls_cols']

        self.valid_ext_srnum = self.config['valid_ext_srnum']

    def main_services(self):  # pragma: no cover
        """Main function for processing services data.
           Firstly raw services data is read
           and then it is filtered and passed ahead for
           extracting and expanding serial numbers.
           The serial numbers considered for expansion are
           those which do not have empty product class and range
           separators. Serial numbers which are unique are then
           combined with the ones that are expanded.
           Raw services data after applying the filter
           is considered for determining hardware changes.
           Data frames returned after serial number expansion
           and hardware changes is merged and content is saved
           into processed_services.csv file
           Filtered raw services data is considered for identfying
           sidecar or jcomm replacements.

        Raises:
            Exception: A generic exception for any error reported/thrown
                       during execution.

        Returns:
            str: Successful message is returned indicating
                 the function has completed its intended functionality
                 properly
        """

        # Read configuration
        _step = ""
        try:
            # Read raw services data
            _step = 'Read raw services data'
            loggerObj.app_info(f"{_step}: {self.msg_start}")

            # file_dir = self.dict_file_raw.copy()
            # file_dir['file_name'] = self.config['file']['Raw']['services']['file_name']
            # file_dir['adls_dir'] = self.config['adls']['Raw']['services']
            file_dir = {'file_dir': self.config['file']['dir_data'],
                        'file_name': self.config['file']['Raw']['services']['file_name'],
                        'adls_config': self.config['adls']['Raw']['adls_credentials'],
                        'adls_dir': self.config['adls']['Raw']['services']}
            df_services_raw = IO.read_csv(self.mode, file_dir)
            del file_dir
            loggerObj.app_success(_step)

            # Filter services data (filter level: database)
            _step = 'Read filter config'
            loggerObj.app_info(f"{_step}: {self.msg_start}")
            dict_config_params = self.config['services']['services_data_overall']
            loggerObj.app_success(_step)

            _step = 'Filter raw services data'
            # Considering the entry with the latest value for modified date with an
            # assumption that the latest/most recent value for modified date will have updated details
            loggerObj.app_info(f"{_step}: {self.msg_start}")
            df_services_raw = df_services_raw \
                .sort_values(['Id', 'LastModifiedDate'], ascending=False) \
                .drop_duplicates(subset=['Id'])
            df_services_raw = filterObj.filter_data(df_services_raw, dict_config_params)
            df_services_raw = df_services_raw.loc[df_services_raw.f_all, self.ls_cols]
            del dict_config_params
            loggerObj.app_success(_step)


            # Identify Serial Numbers
            _step = 'Identify Serial Numbers'
            loggerObj.app_info(f"{_step}: {self.msg_start}")
            df_sr_num = self.pipeline_serial_number(
                df_services_raw)
            loggerObj.app_success(_step)

            # Export intermediate results data
            _step = 'Export results of serial number identification'
            loggerObj.app_info(f"{_step}: {self.msg_start}")


            output_dir = {'file_dir': self.config['file']['dir_results'] +
                                      self.config['file'][
                                          'dir_validation'],
                          'file_name':
                              self.config['file']['Processed']['services'][
                                  'validation'],
                        'adls_config': self.config['adls']['Processed']['adls_credentials'],
                          'adls_dir':self.config['adls']['Processed']['services']['validation']
                          }
            IO.write_csv(self.mode, output_dir, df_sr_num)
            del output_dir
            loggerObj.app_success(_step)

            # Keep Services data where Serial Number are available
            _step = 'Filter services data using Serial Numbers'
            loggerObj.app_info(f"{_step}: {self.msg_start}")
            df_services_raw = df_services_raw[df_services_raw['Id'].isin(df_sr_num['Id'])]
            loggerObj.app_success(_step)


            # Identify Hardware Changes
            _step = 'Identify hardware replacements'
            loggerObj.app_info(f"{_step}: {self.msg_start}")
            upgrade_component = self.config['services']['UpgradeComponents']['ComponentName']
            df_hardware_changes, df_out_validation, df_for_discarded_rows = self.pipeline_id_hardwarechanges(
                df_services_raw,
                self.config['services']['Component_replacement'],
                upgrade_component)
            del upgrade_component
            loggerObj.app_success(_step)
            # check_var_size(list(locals().items()), log=self.config['log_var_size'])

            output_dir = {'file_dir': self.config['file']['dir_results'] +
                                      self.config['file'][
                                          'dir_validation'],
                          'file_name':self.config['file']['Processed']['services']['hardware_changes'],
                          'adls_config': self.config['adls']['Processed']['adls_credentials'],
                          'adls_dir': self.config['adls']['Processed']['services']["hardware_changes"]
                          }
            IO.write_csv(self.mode, output_dir, df_out_validation)

            output_dir = {'file_dir': self.config['file']['dir_results'] +
                                      self.config['file'][
                                          'dir_validation'],
                          'file_name':self.config['file']['Processed']['services']['dropped_rows_for_hardware_changes'],
                          'adls_config': self.config['adls']['Processed']['adls_credentials'],
                          'adls_dir': self.config['adls']['Processed']['services']["dropped_rows_for_hardware_changes"]
                          }
            IO.write_csv(self.mode, output_dir, df_for_discarded_rows)

            # Keep Services data where Serial Number are available
            _step = 'Add serial numbers to hardware changes'
            loggerObj.app_info(f"{_step}: {self.msg_start}")
            df_hardware_and_sr_merged = self.merge_data(df_hardware_changes, df_sr_num)
            del df_hardware_changes
            loggerObj.app_success(_step)

            df_hardware_and_sr_merged["KeySerial"] = df_hardware_and_sr_merged["KeySerial"].fillna("services")
            # Export   results data
            _step = 'Export results of hardware replacements'
            loggerObj.app_info(f"{_step}: {self.msg_start}")

            output_dir = {'file_dir': self.config['file']['dir_results'] +
                                      self.config['file'][
                                          'dir_intermediate'],
                          'file_name':
                              self.config['file']['Processed']['services'][
                                  'file_name'],
                          'adls_config': self.config['adls']['Processed']['adls_credentials'],
                            'adls_dir': self.config['adls']['Processed']['services']
                          }
            IO.write_csv(self.mode, output_dir, df_hardware_and_sr_merged)
            del output_dir, df_hardware_and_sr_merged
            loggerObj.app_success(_step)

            # Identify if sidecar or jcomm comp is present and save results to an intermediate file.
            _step = 'Identify JCOMM and Sidecar'
            loggerObj.app_info(f"{_step}: {self.msg_start}")
            self.pipeline_component_identify(df_services_raw, df_sr_num)
            loggerObj.app_success(_step)

        except Exception as excep:
            loggerObj.app_info(_step, f'{traceback.print_exc()}')
            raise Exception(f"{_step}: Failed") from excep

        return 'successfull !'

    # *** Support Code ***
    def merge_data(self, df_hardware_changes, df_sr_num):
        """Function merges two data frames based on the common column Id using inner join.

        Args:
            df_hardware_changes (pd.DataFrame):  DF containing components as
                                                     well as its type viz. Replace / Upgrade.
            df_sr_num (pd.DataFrame): DataFrame containing serial numbers expanded and
                                          validated.

        Raises:
            Exception: A generic exception for any error reported/thrown
                       during execution.

        Returns:
            df_out (pd.DataFrame) : Merged DataFrame containing both components along with
                                        its type and serial numbers which are validated and
                                        expanded.
        """

        _step = 'Merge data'
        try:
            df_hardware_changes = df_hardware_changes.drop_duplicates(subset=['Id'])
            df_out = df_hardware_changes.merge(df_sr_num, on='Id', how='inner')

            loggerObj.app_success(_step)
        except Exception as excep:

            loggerObj.app_info(_step + f'{traceback.print_exc()}')
            raise Exception('f"{_step}: Failed') from excep
        return df_out

    def pipeline_serial_number(self, df_data_org): # pragma: no cover
        """Function extracts and expands serial number from raw services data.

        Args:
            df_data (pd.DataFrame): filtered raw services data
            dict_cols_srnum (Python Dictionary): Dictionary containing the serial number column
                                      based on the input file received for processing

        Raises:
            Exception: A generic exception for any error reported/thrown
                       during execution.

        Returns:
            validated_sr_num (pd.DataFrame): DataFrame containing values under serial
                                                 number which is expanded and validated
                                                 with respect to installbase data.
        """

        df_data = df_data_org.copy()
        del df_data_org

        _step = 'pipeline_serial_number'
        try:
            loggerObj.app_info(f"{_step} : {self.msg_start}", 1)

            # %% Initialize
            _sub_step = "Initialize variables"
            df_data['empty_qty'] = 0
            df_data.rename({'Id': "ContractNumber"}, axis=1, inplace=True)
            loggerObj.app_info(f"{_sub_step} : {self.msg_done}", 1)

            # %% Extract serial number from columns and comments
            _sub_step = "Extract serial numbers"
            time_start = time.time()
            df_out = srnumObj.search_srnum_services(df_data)
            time_end = time.time()
            time_delta = round(time_end - time_start, 2)
            loggerObj.app_info(f"Time for {_sub_step} : {time_delta}secs.", 2)
            del time_start, time_end, time_delta, df_data

            df_out["SerialNumber"] = df_out["SerialNumber"].apply(lambda x : re.sub("\\s+|\\)|unit[s]?",'',str(x).lower()))
            df_out["SerialNumber"] = df_out["SerialNumber"].apply(lambda x: re.sub("&", ',', str(x)))
            df_out["SerialNumber"] = df_out["SerialNumber"].apply(lambda x: re.sub("\\(", '-', str(x)))
            df_out["SerialNumber"] = df_out["SerialNumber"].str.rstrip(",").str.rstrip('-')

            # Export data for validation
            df_out = df_out.drop_duplicates(subset=['Id', 'SerialNumber'])


            output_dir = {'file_dir': self.config['file']['dir_results'] +
                                      self.config['file'][
                                          'dir_validation'],
                          'file_name': self.config['file']['Processed']['services']['serial_number_for_services_with_na'],
                          'adls_config': self.config['adls']['Processed']['adls_credentials'],
                          'adls_dir': self.config['adls']['Processed']['services']["serial_number_for_services_with_na"]
            }
            IO.write_csv(self.mode, output_dir, df_out[pd.isna(df_out['SerialNumber'])])

            df_out = df_out[pd.notna(df_out['SerialNumber'])]
            # del file_dir
            output_dir = {'file_dir': self.config['file']['dir_results'] +
                                      self.config['file'][
                                          'dir_validation'],
                          'file_name': self.config['file']['Processed']['services']['serial_number_for_services_with_notna'],
                          'adls_config': self.config['adls']['Processed']['adls_credentials'],
                          'adls_dir': self.config['adls']['Processed']['services']["serial_number_for_services_with_notna"]
            }
            IO.write_csv(self.mode, output_dir, df_out)
            loggerObj.app_info(f"{_sub_step} : {self.msg_done}", 1)

            # %% Validate serial number based blank or na etc or not PDU/RPP/STS
            """
            Some "-" separated serial number are also identified. This process ensures
            that such serial number are not added to the output.
            """
            _sub_step = "Validate product type"

            # Identify product type based on serial number
            df_out["SerialNumber"] = df_out["SerialNumber"].fillna("").str.lower()
            df_out = df_out[df_out['SerialNumber'] != ""].reset_index(drop=True)
            df_out["product"] = busLogObj.idetify_product_fr_serial(df_out["SerialNumber"])

            # Validation

            output_dir = {'file_dir': self.config['file']['dir_results'] +
                                      self.config['file'][
                                          'dir_validation'],
                          'file_name': self.config['file']['Processed']['services']['serial_num_empty_prod_class'],
                          'adls_config': self.config['adls']['Processed']['adls_credentials'],
                          'adls_dir': self.config['adls']['Processed']['services']["serial_num_empty_prod_class"]
            }
            IO.write_csv(self.mode, output_dir, df_out[(df_out["product"] == "")])

            # Filter dataframe based on product type
            df_out = df_out[df_out["product"] != ""].reset_index(drop=True)
            df_out.drop(columns={"product"}, inplace=True)

            loggerObj.app_info(f"{_sub_step} : {self.msg_done}", 1)

            # Clean Serial Numbers
            """
            Sometimes components of an asset are given unique serial number with the following pattern:
            <serialNumber>-<component> 
            e.g. 110-1234-1-bcb is bcb of 110-1234-1
            bcb: branch circuit breaker
            fs: floor stand 
            """
            repat_sr_num = "|".join(self.valid_ext_srnum)
            repat_sr_num = re.compile(repat_sr_num)
            df_out = df_out[pd.notna(df_out.SerialNumber)].reset_index(drop=True)

            df_out["SerialNumber"] = df_out['SerialNumber'].apply(
                lambda x: re.sub(repat_sr_num, "", str(x)))
            df_out["SerialNumber"] = df_out["SerialNumber"].str.strip(" -")
            del repat_sr_num

            # %% Identify if serial number is range vs  unique
            _sub_step = "Id range vs unique"

            # Serial number have different patterns. Old patterns were manually generated and allowed ranges
            # e,g, a-b-1-60.
            # New SrNum are system generated and do NOT allow range of serial numbers t-us-..
            # 'eligible_for_expand' identifies serial number patterns which could have ranges.

            # Read reference data

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


            df_out["no_of_sep"] = df_out['SerialNumber'].apply(
                lambda x: len(re.findall("-", x)))
            df_out["has_range_sep"] = df_out['SerialNumber'].apply(
                lambda x: len(re.findall(",|&|\(", x)) > 0)

            # has range sep: (110-b-1,2,3) or (110-b (124, 125)) or (110-b-1&2) will be expanded
            # x[1] > 2
            #   : 110-b will not be expanded as quantity is not reliable
            #   : 110-b-c is unique
            #   : 110-b-c-e will be expanded
            # x[0].lower().startswith(tuple(ls_eligible_4_exp))
            #   : t-us-.. will not be expanded as its not eligible for expansion

            df_out['is_srnum_range'] = df_out[['SerialNumber', 'no_of_sep', 'has_range_sep', 'Qty']].apply(
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
            df_out['SerialNumber'] = df_out["SerialNumber"].apply(
                lambda x: re.sub(" +", "", str(x)))

            loggerObj.app_info(f"{_sub_step} : {self.msg_done}", 1)

            # Split data unique vs range of serial numbers
            _sub_step = "Split Serial Numbers unique vs range"
            ls_cols = ['Id', 'SerialNumberContract', 'SerialNumber', 'Qty', 'SerialNumber_old', 'src', 'is_srnum_range']
            unique_sr_num = df_out.loc[~df_out['is_srnum_range'], ls_cols]
            range_sr_num = df_out.loc[df_out['is_srnum_range'], ls_cols]
            del df_out
            loggerObj.app_info(f"{_sub_step} : {self.msg_done}", 1)

            # Expand serial number
            expanded_sr_num = contractObj.get_range_srum(range_sr_num, ar_key_serial="services")
            del range_sr_num

            # Attach single and range serial numbers
            expanded_sr_num = pd.concat([expanded_sr_num, unique_sr_num])
            expanded_sr_num = expanded_sr_num.drop_duplicates(subset=['Id', 'SerialNumber'])
            del unique_sr_num

            output_dir = {'file_dir': self.config['file']['dir_results'] +
                                      self.config['file'][
                                          'dir_intermediate'],
                          'file_name':
                              self.config['file']['Processed']['services'][
                                  'serial_number_services'],
                        'adls_config': self.config['adls']['Processed']['adls_credentials'],
                            'adls_dir': self.config['adls']['Processed']['services'][
                                  'serial_number_services']
                          }
            IO.write_csv(self.mode, output_dir, expanded_sr_num)
            del output_dir

            # Validate serial numbers
            _sub_step = "Validate serial numbers with install"
            time_start = time.time()
            validated_sr_num = contractObj.validate_contract_install_sr_num(
                expanded_sr_num[pd.notna(expanded_sr_num.SerialNumber)]
            )
            time_end = time.time()
            time_delta = round(time_end - time_start, 2)
            loggerObj.app_info(f"Time for {_sub_step} : {time_delta}secs.", 2)
            del time_start, time_end, time_delta

            validated_sr_num.rename({
                "ContractNumber": 'Id'}, axis=1, inplace=True)
            validated_sr_num = validated_sr_num.loc[
                validated_sr_num.flag_validinstall
            ]
            # Drop flag_valid column
            del validated_sr_num['flag_validinstall']


            validated_sr_num.drop(["SerialNumber_old"], axis=1, inplace=True)

            IO.write_csv(
                self.mode,
                {'file_dir': (
                        self.config['file']['dir_results']
                        + self.config['file']['dir_intermediate']),
                    'file_name': self.config['file']['Processed']['services'][
                        'validated_sr_num'],
                    'adls_config': self.config['adls']['Processed']['adls_credentials'],
                        'adls_dir': self.config['adls']['Processed']['services'][
                        'validated_sr_num']
                }, validated_sr_num)

            loggerObj.app_success(_step)
            return validated_sr_num

        except Exception as excep:
            loggerObj.app_info(_step, f'{traceback.print_exc()}')
            raise Exception('f"{_step}: Failed') from excep

    def pipeline_id_hardwarechanges(
            self, df_data, dict_filt, upgrade_component):
        """Function identifies and segregates the component, its type
           based on the params present in the dictionary. A regex is applied
           on every line of summary column to determine whether a
           component was actually replaced/upgraded during its
           servicing.

        Args:
            df_data (pd.DataFrame): filtered raw services data
            dict_filt (dict): Dictionary containing various parameters
                                           required for filtering rows
            upgrade_component (list): list of component which can be upgraded.

        Raises:
            Exception: A generic exception for any error reported/thrown
                       during execution.

        Returns:
            df_out (pd.DataFrame): Dataframe containing component and type for serial numbers.
        """

        _step = 'Identify hardware replacements'
        try:

            # Initialize output for accumulating all hardware replacements.
            # Field names as per input file.
            ls_cols_interest = [
                'Id', 'Status', 'ClosedDate', 'Customer_Issue_Summary__c', 'Customer_Issue__c',
                'Resolution_Summary__c', 'Resolution__c', "component", "type"]
            ls_cols_interest_validation = [
                'Id', 'Status', 'ClosedDate', 'Customer_Issue_Summary__c', 'Customer_Issue__c',
                'Resolution_Summary__c', 'Resolution__c', "component", "type",
                'supporting_sentence_for_upgrade', 'supporting_sentence_for_replace']

            df_out = pd.DataFrame()
            df_out_validation = pd.DataFrame()
            df_for_discarded_rows = pd.DataFrame()

            # Added on 2023-12-08
            # Split problem summary into a list of strings based on the multiple deliiters
            # (currently only period and new line character are used) in order to prevent the
            # code to generate false positives (i.e. if a component is installed and neither upgraded nor replaced then such a
            # row containing summary shall be discarded)

            # e.g Display replaces. BCMS installed.
            # If summary are not split based on delimiter, then it would add entries with type as replace for both the components display and BCMS.
            # This update would ensure that only display is recorded.

            df_data["list_of_strings_for_summary"] = df_data["Customer_Issue_Summary__c"].apply(
                lambda x: re.sub('\.|:|,|;', ' ', str(x)))
            df_data["list_of_strings_for_summary"] = df_data["list_of_strings_for_summary"].apply(
                lambda x: re.sub(' +', ' ', str(x)))
            df_data["list_of_strings_for_summary"] = df_data["list_of_strings_for_summary"].apply(
                lambda x: re.split('\r|\n', str(x)))

            # Component for replacement
            for component in dict_filt:
                # component = list(dict_filt.keys())[0]

                # Check if any case for component was recorded (this will include upgrade, replace, maintain)
                comp_filters = dict_filt[component]

                # This check was needed because components such as BCMS, Breaker, Display have two keys namely Customer_Issue_Summary__c and
                # Customer_Issue__c, where as components such as PCB, SPD, Fan do not have key Customer_Issue__c. Hence, only if the component
                # has the key Customer_Issue__c a call to the function filter_data is made otherwise value for f_all column is set to True.
                if "Customer_Issue__c" in comp_filters.keys():
                    df_data = filterObj.filter_data(df_data, {"Customer_Issue__c": comp_filters['Customer_Issue__c']})
                else:
                    df_data["f_all"] = True

                # Create regex pattern from config For identifying component replacement
                # the ?= in the regex would ensure that sequence of keywords "replace" and "component_name"
                # when interchanged would still perform the pattern matching.
                # for e.g. "Today BCMS was replaced" or "After the replacement of BCMS in the unit." the regex would return true.
                # Also the regex would return true only if the component_name and "replace" words are found. When either of these keywords
                # are present the regex would return false.
                filt_regex = ("(" + "|".join(
                    comp_filters['Customer_Issue_Summary__c']['text_match_pattern'].split(',')) + ")")
                pat_mod = "replace"
                pat = f"(?=.*{filt_regex})(?=.*({pat_mod}))"
                # Boolean value for f_replace would be present based on the result of pattern matching with the regex.
                # The elements of the list (sentences in the summary spanning over more than one line) would be considered one-by-one
                # and the line would be matched against the regex, if a match is found then length of the result would be greater than zero otherwise
                # it will be equal to zero. any() would return true if for at least one sentence in the summary matches with the regex.
                df_data['f_replace'] = df_data["list_of_strings_for_summary"].apply(
                    lambda lines: any([len(re.findall(pat, " " + line + " ", re.IGNORECASE)) > 0 for line in lines]))

                df_data['supporting_sentence_for_replace'] = df_data["list_of_strings_for_summary"].apply(
                    lambda lines: [(line) for line in lines if
                                   (len(re.findall(pat, " " + line + " ", re.IGNORECASE)) > 0)])

                # Identify if component was upgraded (only if applicable else initialize to False)
                if component in upgrade_component:
                    # Create regex pattern from confirg For identifying component replacement
                    pat_mod = "upgrade"
                    pat = f"(?=.*{filt_regex})(?=.*({pat_mod}))"
                    # Explanation for f_replace holds for populating the column f_upgrade
                    df_data['f_upgrade'] = df_data["list_of_strings_for_summary"].apply(lambda lines: any(
                        [len(re.findall(pat, " " + line + " ", re.IGNORECASE)) > 0 for line in lines]))

                    df_data['supporting_sentence_for_upgrade'] = df_data["list_of_strings_for_summary"].apply(
                        lambda lines: [(line) for line in lines if
                                       (len(re.findall(pat, " " + line + " ", re.IGNORECASE)) > 0)])

                else:
                    df_data['f_upgrade'] = False
                    df_data['supporting_sentence_for_upgrade'] = ''

                # Update the column f_all in order to prevent considering the rows where the summary does not contain upgrade or replace.
                df_data["f_all"] = df_data["f_all"] & (df_data["f_replace"] | df_data["f_upgrade"])

                # Update output. Consider the row only if it has a summary where a component is found to be replaced/upgraded.
                if any(df_data.f_all):
                    # Select only those rows where f_all is 1/True
                    df_data_comp = df_data.loc[df_data.f_all, :]
                    # Set the value for all rows in component column which have f_all as 1/True
                    df_data_comp.loc[:, 'component'] = component
                    # Set the type for the row to upgrade if the row has 1/True for f_upgrade else set the type to replace (when f_upgrade is 0/false)
                    df_data_comp.loc[:, 'type'] = df_data_comp["f_upgrade"].apply(
                        lambda x: "upgrade" if x is True else "replace")

                    df_out_validation = pd.concat([df_out_validation, df_data_comp[ls_cols_interest_validation]])
                    df_out = pd.concat([df_out, df_data_comp[ls_cols_interest]])
                    del df_data_comp

                # Drop the columns f_all, f_upgrade, f_replace in order to proceed with the next iteration.
                df_data.drop(['f_all', "f_upgrade", "f_replace", "supporting_sentence_for_upgrade",
                              'supporting_sentence_for_replace'], axis=1, inplace=True)
                # loggerObj.app_debug(f"{_step}: {component}: SUCCEEDED", 1)
        except Exception as excep:
            loggerObj.app_info(
                f"The exception message reported from pipeline_id_hardwarechanges function defined inside class_services_data.py is {str(excep)}")
            loggerObj.app_info(_step, f'{traceback.print_exc()}')
            raise Exception('f"{_step}: Failed') from excep

        ids_considered = list(df_out_validation.Id.to_dict().values())
        tempdf = df_data[~df_data["Id"].isin(ids_considered)]
        tempdf["component"] = ''
        tempdf["type"] = ''
        df_for_discarded_rows = tempdf[ls_cols_interest]

        return df_out, df_out_validation, df_for_discarded_rows


    def pipeline_component_identify(self, df_services_raw=None,
                                   df_services_serialnum=None):
        """Function identifies if JCOMM and Sidecar fields are present in the
           filtered raw services data and saves the result into an
           intermediate file

        Args:
            df_services_raw (pd.DataFrame): filtered raw services data
            df_services_serialnum (pd.DataFrame): DataFrame containing serial numbers
                                                      which are expanded and validated.

        Raises:
            Exception: A generic exception for any error reported/thrown
                       during execution.

        """
        try:
            ls_cols = ['Id', 'Customer_Issue_Summary__c']
            df_services_raw_merged = df_services_raw[ls_cols]
            del df_services_raw

            # JCOMM field processing
            _step = "Identify JCOMM"
            loggerObj.app_info(f"{_step}: {self.msg_start}")
            df_services_raw_merged['Has_JCOMM'] = df_services_raw_merged.Customer_Issue_Summary__c.str.contains(
                r'\bJCOMM\b', na=False, case=False, regex=True)
            loggerObj.app_success(_step)

            # Sidecar field processing
            _step = "Identify Sidecar"
            loggerObj.app_info(f"{_step}: {self.msg_start}")
            df_services_raw_merged['Has_Sidecar'] = df_services_raw_merged.Customer_Issue_Summary__c.str.contains(
                r'(\bSidecar\b|\bSide car\b)', na=False, case=False, regex=True)
            loggerObj.app_success(_step)

            # JCOMM or Side car
            _step = "Identify if summary has JCOMM or Sidecar"
            loggerObj.app_info(f"{_step} : {self.msg_start}")
            df_services_raw_merged["f_keep"] = (df_services_raw_merged['Has_JCOMM'] | df_services_raw_merged['Has_Sidecar'])
            df_services_raw_merged = df_services_raw_merged[df_services_raw_merged.f_keep]
            loggerObj.app_success(_step)

            # Add serial number
            _step = "Adding serial number of JCOMM and Sidecar"
            loggerObj.app_info(f"{_step} : {self.msg_start}")
            df_services_raw_merged = df_services_raw_merged.merge(
                df_services_serialnum, on='Id', how='left')
            loggerObj.app_success(_step)

            df_services_raw_merged["KeySerial"] = df_services_raw_merged["KeySerial"].fillna("services")
            df_services_raw_merged.drop(["f_keep"], axis=1, inplace=True)
            # Export results
            _step = "Exporting JCOMM and Sidecar data"

            output_dir = {'file_dir': self.config['file']['dir_results'] +
                                      self.config[
                                          'file']['dir_intermediate'],
                          'file_name':
                              self.config['file']['Processed']['services'][
                                  'intermediate'],
                          'adls_config': self.config['adls']['Processed']['adls_credentials'],
                          'adls_dir': self.config['adls']['Processed']['services'][
                              'intermediate']
                          }
            IO.write_csv(self.mode, output_dir, df_services_raw_merged)
            del output_dir

            loggerObj.app_success(_step)

        except Exception as excep:
            loggerObj.app_info(_step, f'{traceback.print_exc()}')
            raise Exception('f"{_step}: Failed') from excep


# %% *** Call ***
if __name__ == "__main__":# pragma: no cover
    services_obj = ProcessServiceIncidents()
    services_obj.main_services()

# %%

    # df = pd.DataFrame ({"SerialNumber":["a-b-1-10","a-b-1b-2b","a-b-1,2,3","a-b-1&2,3","a-b-1-5,10"],
    #                     "Id":["50046000002TVVmAAO","50046000002TSYKAA4","50046000005jr7HAAQ","50046000005jraZAAQ","50046000004N0BFAA0"],
    #                     "SerialNumberContract":["a-b-1-10","a-b-1b-2b","a-b-1,2,3","a-b-1&2,3","a-b-1-5,10"],
    #                     "Qty":[3,5,8,2,10],
    #                     "src":["Product_1__c","Product_1__c","Product_1__c","Product_1__c","Product_1__c"]
    #                   })

    # df1 = get_range_srum(df)
