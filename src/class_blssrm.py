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
import requests
import pandas as pd
import numpy as np
import os
import base64

config_dir = os.path.join(os.path.dirname(__file__))
config_file = os.path.join(config_dir, "config_blssrm.json")
with open(config_file, 'r') as config_file:
    config = json.load(config_file)

mode = config.get("mode", "azure")
if mode == "local":
    path = os.getcwd()
    path = os.path.join(path.split('blssrm')[0], 'blssrm')
    os.chdir(path)

from utils import AppLogger
from utils.io_adopter.class_adlsFunc import adlsFunc
from utils.io import IO

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
        self.msg_done = ": DONE"
        self.msg_start = ": STARTED"

        if self.config["mode"] == "local":
            test_dir = os.path.join(os.path.dirname(__file__), "../results")
            test_file = os.path.join(test_dir, self.config["file"]["Processed"]["processed_assets"]["file_name"])
            with open(test_file, 'r') as tf:
                self.test_df = pd.read_csv(tf, sep=',')

            api_creds_test_dir = os.path.join(os.path.dirname(__file__), "../reference_files")
            api_creds_test_file = os.path.join(api_creds_test_dir, "set_config_adls.csv")
            with open(api_creds_test_file, 'r') as ctf:
                self.api_creds_df = pd.read_csv(ctf, sep=',')

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

            _step = 'Check for csv file in ADLS'
            logger.app_info(f"{_step}: {self.msg_start}")
            df_with_updates, df_blssrm = None, None
            df_with_sn_only_in_blssrm = None
            file_name_to_lookup = self.config["file"]["Processed"]["processed_delta_csv"]["file_name"]
            try:
                df_blssrm = IO.read_csv(
                    self.mode,
                    {
                        "file_dir": self.config["file"]["dir_results"],
                        "file_name": self.config["file"]["Processed"]["processed_delta_csv"]["file_name"],
                        "adls_config": self.config['adls']['Processed']['adls_credentials'],
                        "adls_dir": self.config['adls']['Processed']['processed_delta_csv']
                    }
                )
                logger.app_info(f"Found the file with name {file_name_to_lookup}")
                logger.app_info(f"Data frame created from the csv file {file_name_to_lookup} "
                                f"has dimensions {df_blssrm.shape}")
            except Exception as excp:
                logger.app_info(f"File with name {file_name_to_lookup} was not found.")
            logger.app_success(_step)

            if not isinstance(df_blssrm, pd.DataFrame):
                df_blssrm = None

            _step = 'Check for updates'
            logger.app_info(f"{_step}: {self.msg_start}")
            if isinstance(df_blssrm, pd.DataFrame) and not df_blssrm.empty:
                df_with_updates, df_upsert, df_with_sn_only_in_blssrm = self.identify_updates(df_blssrm, df_output_ilead)
            else:
                logger.app_info(f"Skipping the step for identifying updates.")
            logger.app_success(_step)

            _step = 'Prepare data in JSON format'
            logger.app_info(f"{_step}: {self.msg_start}")
            if df_blssrm is None:
                flag_proceed, json_data_to_push = self.prepare_data_in_exp_json_format(df_output_ilead)
            elif df_with_updates.empty:
                flag_proceed, json_data_to_push = True, {}
            else:
                flag_proceed, json_data_to_push = self.prepare_data_in_exp_json_format(df_with_updates)
                if not isinstance(json_data_to_push, dict):
                    raise Exception from json_data_to_push
            logger.app_success(_step)

            if flag_proceed and json_data_to_push == {}:
                logger.app_info("No incremental data found.")
                logger.app_info("File will not be overwritten.")
                logger.app_info(f"Avoiding API call.")
                return True
            elif flag_proceed is False:
                raise Exception from json_data_to_push

            _step = 'Push updates to API'
            logger.app_info(f"{_step}: {self.msg_start}")
            flag_success = self.post_json(json_data_to_push)
            if flag_success is True:
                logger.app_info("Data was pushed to API successfully.")
                logger.app_success(_step)
                del flag_proceed

                _step = 'Saving processed results in csv format'
                logger.app_info(f"{_step}: {self.msg_start}")

                if df_blssrm is None:
                    df_to_save = df_output_ilead
                else:
                    df_to_save = df_upsert

                output_dir = {"file_dir": self.config['file']['dir_results'],
                              "file_name": self.config["file"]["Processed"]["processed_delta_csv"]["file_name"],
                              "adls_config": self.config['adls']['Processed']['adls_credentials'],
                              "adls_dir": self.config['adls']['Processed']['processed_delta_csv']
                              }
                IO.write_csv(self.mode, output_dir, df_to_save)
                logger.app_success(_step)
                del df_output_ilead

                _step = 'Saving processed results in JSON format'
                logger.app_info(f"{_step}: {self.msg_start}")
                output_dir = {"file_dir": self.config['file']['dir_results'],
                              "file_name": self.config["file"]["Processed"]["processed_delta_json"]["file_name"],
                              "adls_config": self.config['adls']['Processed']['adls_credentials'],
                              "adls_dir": self.config['adls']['Processed']['processed_delta_json']
                              }
                IO.write_json(self.mode, output_dir, json_data_to_push)
                logger.app_success(_step)
                del json_data_to_push

                if isinstance(df_with_sn_only_in_blssrm, pd.DataFrame) and not df_with_sn_only_in_blssrm.empty:
                    logger.app_info(f"Number of serial numbers which are not present today are : "
                                    f"{len(df_with_sn_only_in_blssrm['SerialNumber'].unique())}")
                    logger.app_info(f"Serial numbers not present today are "
                                    f"{df_with_sn_only_in_blssrm['SerialNumber'].unique()}")
                    logger.app_info(
                        f"Updates have not been identified for the entries corresponding to the serial numbers "
                        f"{df_with_sn_only_in_blssrm['SerialNumber'].unique()}")
                    del df_with_sn_only_in_blssrm

                return True
            else:
                return False
        except Exception as excp:
            raise excp


    def identify_updates(self, df_blssrm, df_output_ilead):
        """This method will try to find out for which fields/columns the value is changed for each serial number. There will be one row per serial number and serial number will be a primary key.
         1. The method will first find out whether there are any new rows in the data frame. If there are new rows then the method will filter out the new rows and keep the rest of the data frame for further processing.
         2. The method will then sort the data frame based serial number in descending order and keep the primary key as index.
         3. The method will select value under the required columns in a specific order for each row.
         4. The method will then compare the old and new data frame and find out the fields for which the value is changed. The method will use np.where to find out the fields for which the value is changed. The method will then create a new data frame with the updated values for each serial number. Whenever a mismatch is found value for the field from the new data frame would be taken (even though it is NaN).
         5. NaN values if any are replaced by empty string and value under all columns are converted to string type to check whether there are any updates. If there are no updates then an empty data frame will be returned, otherwise, the index would be reset and data frame containing delta data would be returned along with entries for serial numbers only in
          BLSSRM and output_ilead

        Args:
            df_blssrm (pd.DataFrame): Data Frame created from csv file which was saved yesterday (a day prior)
            df_output_ilead (pd.DataFrame): Data frame containing output_ilead and assets information.

        Raises:
            TypeError: When input supplied as an argument/parameter is not a data frame
            ValueError: When either of the input data frame supplied as a parameter/argument is empty.
            Exception: When an unpredictable error occurs.

        Returns:
            pd.DataFrame: Data frame containing delta/incremental data (or updates).
            pd.DataFrame: Data frame with serial numbers not found in output_ilead today but where pushed to BLSSRM
            pd.DataFrame: Data frame with serial numbers present only in output_ilead (new serial numbers)
        """

        if not isinstance(df_blssrm, pd.DataFrame):
            raise TypeError('Input is not a dataframe')

        if not isinstance(df_output_ilead, pd.DataFrame):
            raise TypeError('Input is not a dataframe')

        if df_output_ilead.empty or df_blssrm.empty:
            raise ValueError("Either output_ilead_df or data frame created from csv file is empty.")

        try:
            df_new = df_output_ilead.copy()
            df_old = df_blssrm.copy()
            del df_output_ilead, df_blssrm

            # check whether there are serial numbers which are not present only in (BLSSRM) and only in output_ilead
            # If common serial numbers are found, then updates will be identified for those serial numbers.
            # If not found, then all serial numbers present in output_ilead are also available in
            # BLSSRM hence, the updates DF would be become empty.
            df_sn_only_in_blssrm = df_old[~df_old.SerialNumber.isin(df_new.SerialNumber)]
            df_new_sn_in_output_ilead = df_new[~df_new.SerialNumber.isin(df_old.SerialNumber)]

            # Find common serial numbers
            df_old = df_old[~df_old.SerialNumber.isin(df_sn_only_in_blssrm.SerialNumber)]
            df_new = df_new[~df_new.SerialNumber.isin(df_new_sn_in_output_ilead.SerialNumber)]

            # Keep the primary key as index (since there will be one row per primary key)
            columns_order = self.config["column_order"]
            df_old = df_old.sort_values(by="SerialNumber", ascending=False).set_index("SerialNumber")
            df_new = df_new.sort_values(by="SerialNumber", ascending=False).set_index("SerialNumber")

            df_new = df_new[columns_order]
            df_old = df_old[columns_order]

            # Use np.where when old == new then NULL, (old is NULL and new is not NULL, then new), (old is not NULL and
            # new is NULL then new), (old != new then new)
            df_temp = df_new.copy(deep=True)
            df_upsert = df_new.copy(deep=True)
            for col in columns_order:
                df_temp[col] = np.where(df_old[col] != df_new[col], df_new[col], np.nan)  # when new != old then select
                # from new, when match is found keep is NaN
                df_upsert[col] = np.where(df_old[col] != df_new[col], df_new[col], df_old[col])  # or df_new[col]

            df_temp = df_temp.replace(np.nan, '', regex=True)

            df_temp = df_temp.fillna("").astype(str)
            df_temp['has_updates'] = df_temp[columns_order].apply(
                lambda x: len(''.join(x)), axis=1)
            df_temp = df_temp.loc[df_temp['has_updates'] > 0, columns_order]

            del columns_order, df_new, df_old
            df_temp = df_temp.reset_index()
            df_upsert = df_upsert.reset_index()

            df_temp = pd.concat([df_temp, df_new_sn_in_output_ilead])
            df_upsert = pd.concat([df_sn_only_in_blssrm, df_upsert, df_new_sn_in_output_ilead])
            df_upsert = df_upsert.sort_values(by="SerialNumber", ascending=True).reset_index(drop=True)
            df_temp = df_temp.reset_index(drop=True)

            return df_temp, df_upsert, df_sn_only_in_blssrm
        except Exception as excp:
            logger.app_info(f"The error message generated while identifying updates is {str(excp)}")
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
                                              data=payload_config, verify=True, allow_redirects=False,
                                              timeout=(self.config["connection_time_out_in_secs"],
                                                       self.config["response_time_out_in_secs"])
                                              )
            except Exception as excp:
                logger.app_info(f"An error occurred while acquiring token using post method.")
                logger.app_info(f"The error message generated is {str(excp)}")
                if "timeout" in str(excp).lower():
                    logger.app_info("Time in seconds specified for establishing a connection or waiting for a "
                                    "response from API is causing Timeout error")

            if auth_response is not None:
                if str(auth_response.status_code) == "200":
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
                raise Exception("Maximum number of attempts for generating token have exceeded.")

            logger.app_info(f'Will begin with the next attempt after {self.config["wait_time_in_secs"]} seconds.')
            time.sleep(self.config["wait_time_in_secs"])
            attempts = attempts + 1

    def prepare_data_in_exp_json_format(self, df_output_ilead):
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
            df_input = df_output_ilead.copy()
            del df_output_ilead
            columns_to_be_grouped = self.config["config_sets"]

            df_input = df_input.replace(r'^\s*$', np.nan, regex=True)
            # Int64 is used to keep missing values as NaN and change floating-point values to int
            for col in self.config["integer_type_columns"]:
                df_input[col] = df_input[col].astype(float).astype("Int64")

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

            cur_date = datetime.datetime.now()

            df_input['CreationDate'] = cur_date.strftime(self.config["DICT_FORM_MANDATE"]['CreationDate'])
            df_input['CreatedBy'] = self.config["DICT_FORM_MANDATE"]["CreatedBy"]
            df_input['LastUpdateDate'] = cur_date.strftime(self.config["DICT_FORM_MANDATE"]['LastUpdateDate'])
            df_input['LastUpdatedBy'] = self.config["DICT_FORM_MANDATE"]["LastUpdatedBy"]
            df_input["Status"] = self.config["STATUS"]
            df_input["RecordId"] = self.config["RECORD_ID"]

            ls_random = [
                (random.randrange(self.config["RGE_REQ_ID"][0], self.config["RGE_REQ_ID"][1]))
                for i in range(df_input.shape[0])]
            df_input['RequestId'] = ls_random
            df_input['RequestId'] = df_input['RequestId'].astype(self.config["DICT_FORM_MANDATE"]["RequestId"])

            # The keys in the dictionary can appear in any specific order. There is not constraint for
            # preserving the order of keys while preparing the JSON.
            json_str = json.dumps(
                [row.dropna().to_dict() for index, row in df_input.iterrows()])
            json_data = json.loads(json_str)

            json_4_api = {"ERPSummarys": json_data}

            # logger.app_info(f"The JSON prepared to be pushed is \n{json_4_api}")
            return True, json_4_api
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
        bearer_token = None

        while attempts <= self.config["number_of_attempts"]:
            logger.app_info("Generating a new token.")
            bearer_token = self.generate_token()
            logger.app_info("New token has been generated.")

            logger.app_info(f"Beginning with attempt {attempts} for pushing data to API.")
            try:
                response = requests.post(
                    self.url_for_api,
                    headers={'Authorization': f"{bearer_token}",
                             "Content-Type": "application/json",
                             "Accept": "application/json",
                             "Cache-control": "no-cache"
                             },
                    json=json_input,
                    timeout=(self.config["connection_time_out_in_secs"],
                             self.config["response_time_out_in_secs"]),
                    verify=True,
                    allow_redirects=False
                )
            except Exception as excp:
                logger.app_info(f"An error occurred while sending data using post method.")
                logger.app_info(f"The error message generated is {str(excp)}")
                if "timeout" in str(excp).lower():
                    logger.app_info("Time in seconds specified for establishing a connection or waiting for a "
                                    "response from API is causing Timeout error")

            if response is not None:
                if len(response.content.decode('utf-8')) == 0:
                    msg = "not available/is empty"
                else:
                    msg = response.content.decode('utf-8')

                if str(response.status_code) in status_and_response_dict.keys():
                    if str(response.status_code) in ["200", "201"]:
                        logger.app_info(f"The status code for response received is {response.status_code} and the "
                                        f"content in response is {msg}")

                        if "error" in msg:
                            logger.app_info("The data was not saved successfully on target server.")
                        else:
                            temp_json = json.loads(msg)
                            if (temp_json["success"] is True and
                                    len(temp_json["data"]["batchID"]) > 0 and
                                    len(temp_json["requestId"]) > 0 and
                                    temp_json["requestId"].isalnum()):
                                logger.app_info(f'success = {temp_json["success"]}, batchID = '
                                                f'{temp_json["data"]["batchID"]}, requestId = {temp_json["requestId"]}')
                                logger.app_info(f"{status_and_response_dict[str(response.status_code)]}")
                                return True
                    else:
                        logger.app_info(f"The status code for response received is {response.status_code} and the "
                                        f"content in response is {msg}")
                        logger.app_info(f"{status_and_response_dict.get(str(response.status_code))}")
                else:
                    logger.app_info("The status code for response received is {response.status_code} and the"
                                    f" content in response is {msg}")
                    logger.app_info("Unknown status code received.")

            if attempts + 1 > self.config["number_of_attempts"]:
                logger.app_info(f'Status code for the response is neither 200 nor 201 for '
                                f'{self.config["number_of_attempts"]} attempts. Halting execution.')
                raise Exception("Maximum number of attempts exceeded for pushing data to API.")

            logger.app_info(f'Will begin with the next attempt after {self.config["wait_time_in_secs"]} seconds.')
            time.sleep(self.config["wait_time_in_secs"])
            attempts = attempts + 1


if __name__ == '__main__':
    blssrm_obj = blssrm()
    status = blssrm_obj.main_blssrm_pipeline(blssrm_obj.test_df)
