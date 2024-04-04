# -*- coding: utf-8 -*-
"""
Created on Mon Sep 26 17:24:38 2022

@author: E9780837
"""
# %% Set Environment
import random
import json
import time
import datetime
import traceback

import requests
import pandas as pd
import numpy as np
import os, math, sys
import base64

config_dir = os.path.join(os.path.dirname(__file__))
config_file = os.path.join(config_dir, "config_blssrm.json")
with open(config_file, 'r') as config_file:
    config = json.load(config_file)

mode = config.get("mode", "azure")
if mode == "local":
    path = os.getcwd()
    path = os.path.dirname(path)
    os.chdir(path)

from utils import AppLogger
from utils.format_data import Format
from utils.io_adopter.class_adlsFunc import adlsFunc
from utils.io import IO
from src.column_names_and_dtypes import column_data_types

logger = AppLogger(__name__)


# %%
class blssrm:
    """This class implements methods for creating a json from a data frame 
       as per the schema provided in requirements and attempts to push the
       json data using a post request to API with a retry mechanism and wait time
       interval between attempts. Loggers have been added with status codes and response
       obtained for the corresponding request.
    """

    def __init__(self):
        # os.environ["HTTP_PROXY"] = "http://proxy.apac.etn.com:8080"
        # os.environ["HTTPS_PROXY"] = "http://proxy.apac.etn.com:8080"

        self.last_update = None
        self.config = config
        self.mode = self.config["mode"]
        self.format = Format()
        self.msg_done = ": DONE"
        self.msg_start = ": STARTED"
        self.df = pd.DataFrame(columns=["SerialNumber", "RequestId"])

        if self.config["mode"] == "local":
            test_dir = os.path.join(os.path.dirname(__file__), "../results")
            test_file_path = os.path.join(test_dir, self.config["file"]["Processed"]["processed_assets"]["file_name"])
            self.test_df = pd.read_csv(test_file_path, dtype=column_data_types, sep=',')
            self.test_df = self.format.format_data(self.test_df, self.config["processed_assets"]["Dictionary Format"])

            api_creds_test_dir = os.path.join(os.path.dirname(__file__), "../reference_files")
            api_creds_test_file_path = os.path.join(api_creds_test_dir, "set_config_adls.csv")
            self.api_creds_df = pd.read_csv(api_creds_test_file_path, sep=',')

            self.client_id = self.api_creds_df.loc[self.api_creds_df["name"] == "blssrm_clientid", "value"].tolist()[0]
            self.client_secret = self.api_creds_df.loc[self.api_creds_df["name"] ==
                                                       "blssrm_clientsecret", "value"].tolist()[0]
            self.url_for_token = self.api_creds_df.loc[self.api_creds_df["name"] ==
                                                       "blssrm_url_token", "value"].tolist()[0]
            self.url_for_api = self.api_creds_df.loc[self.api_creds_df["name"] == "blssrm_api_url", "value"].tolist()[0]

        elif self.config["mode"] == "azure":
            self.adls = adlsFunc()
            logger.app_info("Reading secrets from key-vault")
            self.dict_cred = self.adls.read_credentials(ls_cred=["blssrm-client-id", "blssrm-client-secret",
                                                                 "blssrm-token-url", "blssrm-api-url"])

            logger.app_info("Completed reading secrets from key-vault")
            self.client_id = self.dict_cred.get("blssrm_client_id")
            self.client_secret = self.dict_cred.get("blssrm_client_secret")
            self.url_for_token = self.dict_cred.get("blssrm_token_url")
            self.url_for_api = self.dict_cred.get("blssrm_api_url")

    def main_blssrm_pipeline(self, df_output_ilead=None):
        """Main function which performs the following tasks
           1. Receives a data frame as input which has values under the required columns as per the format specified by business
           2. Checks are performed to ensure that the data frame is not empty.
           3. A check is performed to determine whether the file in ADLS exists or not, if found create a DF for identifying the updates to find the delta/incremental data 
           4. If the file is not found then consider the data frame received as input as the delta/incremental data.
           5. Calls the respective function which shall prepare the data in JSON format required by business
           6. If the data is prepared successfully then the data is pushed to API using a post request method by calling post_json method

        Args:
            df_output_ilead (pd.DataFrame, optional): Data frame containing output_ilead and assets information.
            Defaults to None. While running locally.

        Raises:
            TypeError: When input supplied is not a dataframe
            ValueError: When input supplied is a empty data frame
            ValueError: When input dataframe does not have all required columns
            Exception: When an unpredictable error occurs.

        Returns:
            bool_: status of the cuurent execution
        """

        if not isinstance(df_output_ilead, pd.DataFrame):
            raise TypeError("The input supplied is not a data frame")

        if df_output_ilead.empty:
            raise ValueError("The input supplied is an empty dataframe")

        ls_cols_required = self.config["required_columns"]

        if not set(ls_cols_required).issubset(set(df_output_ilead.columns)):
            ls_missing = set(ls_cols_required).difference(df_output_ilead.columns)
            ls_missing = ' , '.join(ls_missing)
            raise ValueError(
                f"{ls_missing} columns are not available in the dataframe")

        try:
            return_value = self.check_for_file(df_output_ilead)
            if return_value is True:
                return return_value

            df_blssrm = return_value.copy(deep=True)
            del return_value

            _step = 'Check for updates'
            logger.app_info(f"{_step}: {self.msg_start}")
            df_with_updates, df_upsert, df_with_sn_no_change = self.identify_updates(df_blssrm, df_output_ilead)

            logger.app_success(_step)
            if df_with_updates.empty:
                logger.app_info("No incremental data found.")
                logger.app_info("File will not be overwritten.")
                logger.app_info("Avoiding API call.")
                return True

            list_of_success_sn, list_of_failed_sn, batch_tracking_df, request_id = \
                self.push_data_in_batches(df_with_updates)

            _step = 'Saving the data frame for tracking status for batches'
            logger.app_info(f"{_step}: {self.msg_start}")
            filename = self.config['adls']['Processed']['batch_status_df']["file_name"]

            filename = "%s_%s.csv" % (filename, datetime.datetime.now().strftime("%Y%m%d_%H%M%S"))
            temp_dict = self.config['adls']['Processed']['batch_status_df']
            batch_tracking_dict = temp_dict.copy()
            del temp_dict
            batch_tracking_dict["file_name"] = filename

            output_dir = {"file_dir": self.config['file']['dir_results'],
                          "file_name": filename,
                          "adls_config": self.config['adls']['Processed']['adls_credentials'],
                          "adls_dir": batch_tracking_dict
                          }

            IO.write_csv(self.mode, output_dir, batch_tracking_df)
            del batch_tracking_df, batch_tracking_dict, output_dir, filename
            logger.app_success(_step)

            _step = 'Saving the csv'
            logger.app_info(f"{_step}: {self.msg_start}")
            df_upsert = df_upsert[df_upsert.SerialNumber.isin(list_of_success_sn +
                                                              df_with_sn_no_change["SerialNumber"].tolist())]
            df_upsert = pd.concat([df_upsert, df_blssrm[df_blssrm.SerialNumber.isin(list_of_failed_sn)]])
            # df_upsert = self.format_int_type_columns(df_upsert)
            df_upsert[self.config["integer_type_columns"]] = (df_upsert[self.config["integer_type_columns"]]
                                                              .replace('', 0).astype(float).astype("Int64"))

            output_dir = {"file_dir": self.config['file']['dir_results'],
                          "file_name": self.config["file"]["Processed"]["processed_delta_csv"]["file_name"],
                          "adls_config": self.config['adls']['Processed']['adls_credentials'],
                          "adls_dir": self.config['adls']['Processed']['processed_delta_csv']
                          }
            IO.write_csv(self.mode, output_dir, df_upsert)
            del df_upsert, output_dir
            logger.app_success(_step)

            df_save_to_json = df_with_updates[df_with_updates.SerialNumber.isin(list_of_success_sn)]
            del df_with_updates

            if not df_save_to_json.empty:
                _step = 'Saving the JSON for serial numbers pushed successfully to API'
                logger.app_info(f"{_step}: {self.msg_start}")
                output_dir = {"file_dir": self.config['file']['dir_results'],
                              "file_name": self.config["file"]["Processed"]["processed_delta_json"]["file_name"],
                              "adls_config": self.config['adls']['Processed']['adls_credentials'],
                              "adls_dir": self.config['adls']['Processed']['processed_delta_json']
                              }
                IO.write_json(self.mode, output_dir, self.prepare_data_in_exp_json_format(df_save_to_json,
                                                                                          request_id)[1])
                logger.app_success(_step)
            else:
                logger.app_info("Push operation to API was not successful for any batch. "
                                "JSON data will not be saved.")
            return True

        except Exception as excp:
            logger.app_fail(f"Inside main : {traceback.print_exc()}", excp)
            raise excp

    def check_for_file(self, df_output_ilead):
        """This function checks whether the file is available in ADLS or not. If the file is found, then a dataframe is created by reading the file using the data type for the columns same as the data frame received as input. If the file is not found, then the data frame received as input is saved to ADLS.

        Args:
            df_output_ilead (pd.DataFrame): Data frame coming from class_assets.py

        Returns:
            bool: True once for DF is created after reading the file or when the file in ALDS is not found but the input DF is saved on ADLS. (which is the condition for day 1)
        """
        _step = 'Check for csv file in ADLS'
        logger.app_info(f"{_step}: {self.msg_start}")
        file_name_to_lookup = self.config["file"]["Processed"]["processed_delta_csv"]["file_name"]
        df_blssrm = None
        try:
            col_dtypes_in_output_ilead = df_output_ilead.dtypes.to_dict()
            df_blssrm = IO.read_csv(
                self.mode,
                {
                    "file_dir": self.config["file"]["dir_results"],
                    "file_name": self.config["file"]["Processed"]["processed_delta_csv"]["file_name"],
                    "adls_config": self.config['adls']['Processed']['adls_credentials'],
                    "adls_dir": self.config['adls']['Processed']['processed_delta_csv']
                },
                column_data_types=col_dtypes_in_output_ilead
            )
            logger.app_info(f"Found the file with name {file_name_to_lookup}")
            logger.app_info(f"Data frame created from the csv file {file_name_to_lookup} "
                            f"has dimensions {df_blssrm.shape}")
        except Exception as excp:
            logger.app_info(f"File with name {file_name_to_lookup} was not found.")
            logger.app_fail(f"{_step} : {traceback.print_exc()}", excp)
        logger.app_success(_step)

        if not isinstance(df_blssrm, pd.DataFrame):
            # df_output_ilead = self.format_int_type_columns(df_output_ilead)
            output_dir = {"file_dir": self.config['file']['dir_results'],
                          "file_name": self.config["file"]["Processed"]["processed_delta_csv"]["file_name"],
                          "adls_config": self.config['adls']['Processed']['adls_credentials'],
                          "adls_dir": self.config['adls']['Processed']['processed_delta_csv']
                          }
            IO.write_csv(self.mode, output_dir, df_output_ilead)
            logger.app_info(f"Since file name {file_name_to_lookup} was not found. Saved input DF.")
            return True

        input_format = self.config["processed_assets"]["Dictionary Format"]
        df_blssrm = self.format.format_data(df_blssrm, input_format)
        return df_blssrm

    def push_data_in_batches(self, df_containing_updates):

        _step = 'Beginning with batch processing'
        try:
            logger.app_info(f"{_step}: {self.msg_start}")
            ls_pass_sn, ls_fail_sn = [], []

            batch_status_df = pd.DataFrame(columns=self.config["columns_for_tracking_batches"])
            batch_size = self.config["batch_size"]

            number_of_rows = df_containing_updates.shape[0]
            ix_beg, ix_end, counter_for_batches, request_id = 0, 0, 0, 0

            while ix_end < number_of_rows:
                ix_end = min(ix_beg + batch_size, number_of_rows)
                df_subset = df_containing_updates.iloc[ix_beg: ix_end]
                logger.app_info(f"Beginning with batch number {counter_for_batches}")

                flag_proceed, json_data_to_push, request_id = self.prepare_data_in_exp_json_format(df_subset)

                batch_status_df.loc[counter_for_batches,] = [
                    counter_for_batches, ix_beg, ix_end, len(df_subset),
                    -1,
                    round(sys.getsizeof(df_subset) / 1024, 2),
                    round(sys.getsizeof(json.dumps(json_data_to_push)) / 1024, 2),
                    request_id
                ]

                if not isinstance(json_data_to_push, dict):
                    logger.app_info(f"Batch {counter_for_batches} failed with error: {str(json_data_to_push)}")
                    counter_for_batches = counter_for_batches + 1
                    ix_beg = ix_end
                    continue

                status_code_for_req = self.post_json(json_data_to_push)

                if status_code_for_req in self.config["success_status_code"]:
                    logger.app_info(
                        f"For batch number {counter_for_batches} data was pushed to API successfully.")
                    ls_pass_sn = ls_pass_sn + df_subset["SerialNumber"].unique().tolist()
                else:
                    logger.app_info(f"For batch number {counter_for_batches} "
                                    f"data was not pushed to API successfully.")
                    ls_fail_sn = ls_fail_sn + df_subset["SerialNumber"].unique().tolist()

                batch_status_df.loc[counter_for_batches, "StatusCode"] = status_code_for_req

                counter_for_batches = counter_for_batches + 1
                ix_beg = ix_end
                time.sleep(self.config["wait_time_in_secs_for_batches"])

            logger.app_success(_step)
            return ls_pass_sn, ls_fail_sn, batch_status_df, request_id

        except Exception as excp:
            logger.app_info(f"The error message generated while processing data in batches is {str(excp)}")
            logger.app_fail(f"{_step} : {traceback.print_exc()}", excp)
            raise Exception from excp

    def identify_updates(self, df_blssrm, df_output_ilead):
        """This method will try to find out for which fields/columns the value is changed for each serial number.
        There will be one row per serial number and serial number will be a primary key.

        Args:
            df_blssrm (pd.DataFrame): Data Frame created from csv file which was saved yesterday (a day prior)
            df_output_ilead (pd.DataFrame): Data frame containing output_ilead and assets information.

        Raises:
            TypeError: When input supplied as an argument/parameter is not a data frame
            ValueError: When either of the input data frame supplied as a parameter/argument is empty.
            Exception: When an unpredictable error occurs.

        Returns:
            pd.DataFrame: Data frame containing delta/incremental data (or updates).
            pd.DataFrame: Data frame with serial numbers where not updates were found.
            pd.DataFrame: Data frame which needs to be saved back to ADLS with updated values for fields.
        """

        if not isinstance(df_blssrm, pd.DataFrame):
            raise TypeError('Input is not a dataframe')

        if not isinstance(df_output_ilead, pd.DataFrame):
            raise TypeError('Input is not a dataframe')

        if df_output_ilead.empty or df_blssrm.empty:
            raise ValueError("Either output_ilead_df or data frame created from csv file is empty.")

        _step = "Now initiating comparison between two dataframes for finding updates"
        logger.app_info(f"{_step}: {self.msg_start}")
        try:

            df_new = df_output_ilead.copy(deep=True)
            df_old = df_blssrm.copy(deep=True)
            del df_output_ilead, df_blssrm

            df_new = df_new.astype(str)
            df_old = df_old.astype(str)

            df_new = (df_new.replace('', np.nan).replace("nan", np.nan).
                      replace("<NA>", np.nan))
            df_old = (df_old.replace('', np.nan).replace("nan", np.nan).
                      replace("<NA>", np.nan))

            df_sn_only_in_blssrm = df_old[~df_old.SerialNumber.isin(df_new.SerialNumber)]
            df_new_sn_in_output_ilead = df_new[~df_new.SerialNumber.isin(df_old.SerialNumber)]

            # Find common serial numbers
            df_old = df_old[~df_old.SerialNumber.isin(df_sn_only_in_blssrm.SerialNumber)]
            df_new = df_new[~df_new.SerialNumber.isin(df_new_sn_in_output_ilead.SerialNumber)]

            # Keep the serial number as primary key as index (since there will be one row per primary key)
            columns_order = self.config["column_order"]
            df_old = df_old.sort_values(by="SerialNumber", ascending=False).set_index("SerialNumber")
            df_new = df_new.sort_values(by="SerialNumber", ascending=False).set_index("SerialNumber")

            df_new = df_new[columns_order]
            df_old = df_old[columns_order]

            df_temp = df_new.copy(deep=True)
            df_upsert = df_new.copy(deep=True)
            for col in columns_order:
                # when there is a mismatch between the old value and new value for a field/column, consider the new
                # value. But when a match is found, this implied value for the respective field is not updated.
                df_temp[col] = np.where(df_old[col] != df_new[col], df_new[col], np.nan)
                # When value for field was not null yesterday, but today it is found to be null or np.nan in this case
                # NaN should be sent. Discussed with Salesforce team on 27/2/2024
                # "NULL" is a placeholder which shall be replaced while preparing JSON, with either '' or 0 depending
                # upon the data type.
                df_temp["flag_for_null"] = (df_old[col].notna() & df_new[col].isna())
                df_temp.loc[df_temp["flag_for_null"] == True, col] = 'NULL'
                df_temp.drop(["flag_for_null"], axis=1, inplace=True)

                # when there is a mismatch between the old value and new value for a field/column, consider the new
                # value. But when a match is found, the value for the field is not updated, hence consider either the
                # old or new value for saving.
                df_upsert[col] = np.where(df_old[col] != df_new[col], df_new[col], df_old[col])  # or df_new[col]
                df_upsert["flag_for_null"] = (df_old[col].notna() & df_new[col].isna())

                # "NULL" is a placeholder which shall be replaced while preparing JSON, with either '' or 0 depending
                # upon the data type.
                df_upsert.loc[df_upsert["flag_for_null"] == True, col] = 'NULL'
                df_upsert.drop(["flag_for_null"], axis=1, inplace=True)

            # Replace np.nan with empty string to initiate a check for finding the rows which contain updates.
            df_temp = df_temp.replace(np.nan, '', regex=True)

            del df_new, df_old
            df_temp = df_temp.fillna("").astype(str)
            df_temp['has_updates'] = df_temp[columns_order].apply(lambda x: len(''.join(x)), axis=1)

            # if length for a row is found to be greater than zero, this implies that the row has values for fields
            # which are updated.
            df_temp_len_gtr_zero = df_temp.loc[
                df_temp['has_updates'] > 0, columns_order]

            # if length for a row is found to be equal to zero, this implies that the row does not have any field with
            # updates.(all fields will be set to '' from np.nan)
            df_temp_len_eq_zero = df_temp.loc[
                df_temp['has_updates'] == 0, columns_order]

            del columns_order, df_temp
            df_temp_len_gtr_zero = df_temp_len_gtr_zero.reset_index()
            df_temp_len_eq_zero = df_temp_len_eq_zero.reset_index()
            df_upsert = df_upsert.reset_index()

            # serial numbers which were present yesterday, but are not found today. In this case, all the fields would
            # be set to "NULL" (a placeholder) which shall be further replaced by a zero or empty string depending
            # upon data type.
            df_sn_only_in_blssrm = (df_sn_only_in_blssrm.set_index("SerialNumber").
                                    applymap(lambda x: 'NULL').reset_index())

            # serial numbers with updates, new serial numbers and serial numbers
            # not found today but where pushed to BLSSRM yesterday are set to "NULL".
            df_updates = pd.concat([df_temp_len_gtr_zero, df_new_sn_in_output_ilead, df_sn_only_in_blssrm])
            df_upsert = pd.concat([df_sn_only_in_blssrm, df_upsert, df_new_sn_in_output_ilead])
            df_upsert = (df_upsert.sort_values(by="SerialNumber", ascending=True).
                         reset_index(drop=True).replace("NULL", ''))

            if not df_sn_only_in_blssrm.empty:
                logger.app_info(
                    f"Updates have not been identified for the entries corresponding to the serial numbers "
                    f"{df_sn_only_in_blssrm['SerialNumber'].unique()}")

            logger.app_success(_step)
            df_updates = df_updates.sort_values(by="SerialNumber", ascending=True).reset_index(drop=True)
            return df_updates, df_upsert, df_temp_len_eq_zero
        except Exception as excp:
            logger.app_info(f"The error message generated while identifying updates is {str(excp)}")
            logger.app_fail(f"{_step} : {traceback.print_exc()}", excp)
            raise Exception from excp

    def generate_token(self):
        '''
        Generate a token required for pushing data to API using
        post request method. The client-id and client-secret are used
        for authenticating the user/entity sending the data. The request once
        authenticated would return a response with a header and body with body
        containing the token type and actual token which would grant the
        entity authorization/permission to send/receive from the API thereby
        removing the need for providing password. The code will retry to generate the token
        for certain number of times if token is not generated within specified number of
        attempts then the program raises an exception and stops execution.

        Raises
        ------
        Exception: If the request sent reports an error or when maximum number of attempts are exceeded.

        Returns
        -------
        str. A bearer token.

        '''

        attempts = 1
        header_config = self.config["header_config"]
        payload_config = self.config["payload"]
        auth_response = None

        auth_creds = base64.b64encode(bytes(self.client_id + ":" + self.client_secret, 'utf-8'))
        header_config["Authorization"] = "{} {}".format(self.config["creds_type"], auth_creds.decode('utf-8'))

        while attempts <= self.config["attempts_for_token"]:
            logger.app_info("Attempt {} for generating token.".format(attempts))
            try:
                auth_response = requests.post(self.url_for_token, headers=header_config,
                                              data=payload_config, verify=True, allow_redirects=False
                                              )
            except Exception as excp:
                logger.app_info(f"An error occurred while acquiring token using post method.")
                logger.app_info(f"The error message generated is {str(excp)}")

            if auth_response is not None:
                if str(auth_response.status_code) in self.config["success_status_code"]:
                    logger.app_info(f"Token got generated in attempt number {attempts}.")
                    response_body = auth_response.json()
                    if len(response_body["access_token"]) > 0:
                        self.last_update = datetime.datetime.now()
                        return response_body["token_type"] + " " + response_body["access_token"]

                logger.app_info(f"The status code in the response received is {auth_response.status_code}")
                logger.app_info(f"The message present in the response body is {auth_response.content.decode('utf-8')}")
            else:
                logger.app_info("Response received from API is empty.")

            if attempts + 1 > self.config["attempts_for_token"]:
                logger.app_info("Maximum number of attempts for generating token have exceeded.")
                # raise Exception("Maximum number of attempts for generating token have exceeded.")
                return self.config["token_message"]

            logger.app_info(f'Will begin with the next attempt for generating a new token'
                            f' after {self.config["wait_time_in_secs"]} seconds.')
            time.sleep(self.config["wait_time_in_secs"])
            attempts = attempts + 1

    def format_int_type_columns(self, df_input_org):
        df_input = df_input_org.copy(deep=True)
        del df_input_org
        # Int64 is used to keep missing values as NaN and change floating-point values to int
        for col in self.config["integer_type_columns"]:
            # fix for CIPILEADS-1345
            df_input[col] = df_input[col].apply(lambda x:
                                                np.nan if pd.isna(x) else '0'
                                                if len(str(x)) == 0 else x)
            df_input[col] = pd.to_numeric(df_input[col], errors='coerce')
            df_input[col] = df_input[col].astype(float).astype("Int64")

        return df_input

    def prepare_data_in_exp_json_format(self, df_output_ilead, req_id=None):
        '''
        Prepare the JSON from the input data frame as per the format
        shared by BLSSRM team. Few columns need to be mapped into dictionaries
        and lists and the details for the same are taken from the config file.

        Parameters
        ----------
        df_output_ilead : pandas DataFrame. Serial number with updates.

        Raises
        ------
        TypeError: if input is not a pandas dataframe.
        ValueError: if input dataframe does not have all required columns.
        Exception: If an unpredictable error occurs during execution

        Returns
        -------
        bool. indicates if data has any changes.
        json. Updates for BLSSRM with mandatory columns and changes.

        '''

        if not isinstance(df_output_ilead, pd.DataFrame):
            raise TypeError('Input is not a dataframe')

        if df_output_ilead.empty:
            raise ValueError("Output iLead is empty.")

        try:
            df_input = df_output_ilead.copy(deep=True)
            del df_output_ilead
            columns_to_be_grouped = self.config["config_sets"]

            # When value for field was not null yesterday, but today it is found to be null or np.nan in this case
            # NaN should be sent. Discussed with Salesforce team on 27/2/2024. Hence "NULL" is replaced with ''
            # afterwards
            df_input = df_input.replace(r'^\s*$', np.nan, regex=True).replace("NULL", '')
            # df_input = self.format_int_type_columns(df_input)
            df_input[self.config["integer_type_columns"]] = df_input[self.config["integer_type_columns"]].replace('', 0).astype(float).astype("Int64")

            # There are new columns which are required to be added which are sets of four or two columns.
            # The new columns are either contain a dictionary inside a list or a dictionary. Below for loop
            # has the logic to address this requirement.The fields holding a NULL/NaN values will be
            # removed from the list
            for key in columns_to_be_grouped:
                type_ = columns_to_be_grouped[key][0]
                ls_cols = columns_to_be_grouped[key][1]

                if type_ == "list":
                    df_input[key] = df_input[ls_cols].apply(
                        lambda x: np.nan if all(pd.isna(x)) else [x[pd.notna(x)].to_dict()], axis=1)
                else:
                    df_input[key] = df_input[ls_cols].apply(
                        lambda x: np.nan if all(pd.isna(x)) else x[pd.notna(x)].to_dict(), axis=1)

            df_input = df_input.drop(
                columns=self.config["columns_to_drop"], axis=1)

            cur_date = datetime.datetime.now().strftime(self.config["DICT_FORM_MANDATE"]['LastUpdateDate'])

            df_input['CreationDate'] = cur_date
            df_input['CreatedBy'] = self.config["RECORD_ID"]
            df_input['LastUpdateDate'] = cur_date
            df_input['LastUpdatedBy'] = self.config["RECORD_ID"]
            df_input["Status"] = self.config["STATUS"]

            ls_random = None
            if req_id is None:
                ls_random = [
                    (random.randrange(self.config["RGE_REQ_ID"][0], self.config["RGE_REQ_ID"][1]))
                    for i in range(df_input.shape[0])]
            else:
                # ls_random = req_id
                ls_random = self.df.loc[self.df["SerialNumber"].isin(df_input.SerialNumber), "RequestId"]

            df_input['RequestId'] = ls_random
            df_input['RequestId'] = df_input['RequestId'].astype(self.config["DICT_FORM_MANDATE"]["RequestId"])

            # The keys in the dictionary can appear in any specific order. There is not constraint for
            # preserving the order of keys while preparing the JSON.
            json_str = json.dumps(
                [row.dropna().to_dict() for index, row in df_input.iterrows()])
            json_data = json.loads(json_str)

            json_4_api = {"ERPSummarys": json_data}

            self.df = pd.concat([self.df, df_input[["SerialNumber", "RequestId"]]])

            return True, json_4_api, ls_random
        except Exception as excp:
            logger.app_info("An error occurred while preparing data in specified JSON format.")
            logger.app_info(f"The message in the exception raised is {str(excp)}.")
            # raise Exception from excp
            return False, excp

    def post_json(self, json_input):
        '''
        Push Json data to BLSSRM api by sending a request using the post method.
        Along with the token acquired.

        Parameters
        ----------
        json_input : pandas DataFrame in json format. Serial number with updates.

        Raises
        ------
        TypeError: if input is not a python dictionary.
        Exception: When maximum number of attempts are exceeded for generating the token.

        Returns
        -------
        bool: When the response for the request contains status code as 200 along with a valid body.
        '''

        if not isinstance(json_input, dict):
            raise TypeError('Input is not a dictionary')

        if "ERPSummarys" not in json_input.keys():
            raise ValueError('The key "ERPSummarys" was not found.')

        attempts = 1
        response = None
        msg = ""
        status_and_response_dict = self.config["status_code_and_content"]

        while attempts <= self.config["number_of_attempts"]:
            logger.app_info("Generating a new token.")
            bearer_token = self.generate_token()
            if bearer_token != self.config["token_message"]:  # fix for CIPILEADS-1346
                logger.app_info("New token has been generated.")

                logger.app_info(f"Beginning with attempt {attempts} for pushing data to API.")
                time_start = time.time()
                try:
                    response = requests.post(
                        self.url_for_api,
                        headers={'Authorization': f"{bearer_token}",
                                 "Content-Type": "application/json",
                                 "Accept": "application/json",
                                 "Cache-control": "no-cache"
                                 },
                        timeout=(self.config["connection_time_out_in_secs"],
                                 self.config["response_time_out_in_secs"]),
                        json=json_input,
                        verify=True,
                        allow_redirects=False
                    )
                except Exception as excp:
                    logger.app_info(f"An error occurred while sending data using post method.")
                    logger.app_info(f"The error message generated is {str(excp)}")

                time_end = time.time()
                time_delta = round(time_end - time_start, 2)
                logger.app_info(f"Time for post method to complete : {time_delta}secs.")
                del time_start, time_end, time_delta
                if response is not None:
                    if len(response.content.decode('utf-8')) == 0:
                        msg = "not available/is empty"
                    else:
                        msg = response.content.decode('utf-8')

                    if str(response.status_code) in status_and_response_dict.keys():
                        if str(response.status_code) in self.config["success_status_code"]:
                            logger.app_info(f"The status code for response received is {response.status_code} and the "
                                            f"content in response is {msg}")

                            temp_json = json.loads(msg)
                            if (temp_json["success"] is True and
                                    len(temp_json["data"]["batchID"]) > 0 and
                                    len(temp_json["requestId"]) > 0 and
                                    temp_json["requestId"].isalnum()):
                                logger.app_info(f'success = {temp_json["success"]}, batchID = '
                                                f'{temp_json["data"]["batchID"]}, requestId = {temp_json["requestId"]}')
                                logger.app_info(f"{status_and_response_dict[str(response.status_code)]}")
                                return str(response.status_code)
                        else:
                            logger.app_info(f"The status code for response received is {response.status_code} and the "
                                            f"content in response is {msg}")
                            logger.app_info(f"{status_and_response_dict.get(str(response.status_code))}")
                    else:
                        logger.app_info(f"The status code for response received is {response.status_code} and the"
                                        f" content in response is {msg}")
                        logger.app_info("Unknown status code received.")
            else:
                logger.app_info("New token was not generated.")
                logger.app_info("Discarding further attempts since token was not generated.")  # fix for CIPILEADS-1346
                return self.config["st_code"]

            if attempts + 1 > self.config["number_of_attempts"]:
                logger.app_info("Maximum number of attempts exceeded for pushing data to API.")
                return self.config["st_code"] if response is None else self.config["st_code"] \
                    if not json.loads(msg)["success"] else str(response.status_code)

            logger.app_info(f'Will begin with the next attempt for pushing data '
                            f'after {self.config["wait_time_in_secs"]} seconds.')
            time.sleep(self.config["wait_time_in_secs"])
            attempts = attempts + 1


if __name__ == '__main__':
    blssrm_obj = blssrm()
    status = blssrm_obj.main_blssrm_pipeline(blssrm_obj.test_df)
