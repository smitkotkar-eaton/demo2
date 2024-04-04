# -*- coding: utf-8 -*-
"""
Created on Mon Sep 26 10:34:43 2022

@author: E9780837
"""

# !pytest ./test/test_class_blssrm.py

# !pytest --cov=.\src --cov-report html:.\coverage\ .\test\

# %% Set Environment
import os

import mock
import numpy as np
import pandas as pd
import pytest
import json

from mock import (Mock, sentinel, patch, MagicMock)
from pandas._testing import assert_frame_equal
from numpy import dtype

from pathlib import Path
import sys

sys.path.append(str(Path(__file__).parent.parent))

# import src.config as CONF
from src.class_blssrm import blssrm
from src.column_names_and_dtypes import column_data_types

blssrm = blssrm()
# blssrm.config['file']['dir_data'] = ".\\test\\ip"
# blssrm.config['file']['dir_results'] = ".\\test\\out"
blssrm.config['file']['dir_data'] = r"C:\Users\E0778583\OneDrive - Eaton\Documents\predictpulse\blssrm\test\ip"
blssrm.config['file']['dir_results'] = r"C:\Users\E0778583\OneDrive - Eaton\Documents\predictpulse\blssrm\test\out"


class TestMainBlssrmPipeline:
    @pytest.mark.parametrize(
        "df_output_ilead",
        [None, {10, 20}, [], "string1", 123, True, 45.84])
    def test_1_typeerror(self, df_output_ilead):
        with pytest.raises(Exception) as excinfo:
            flag = blssrm.main_blssrm_pipeline(df_output_ilead)
        assert excinfo.type == TypeError

    def test_1_valueerror(self):
        with pytest.raises(Exception) as excinfo:
            flag = blssrm.main_blssrm_pipeline(pd.DataFrame())
        assert excinfo.type == ValueError

    def test_2_valueerror_missing_columns(self):
        df_output_iead = pd.read_csv(os.path.join(blssrm.config['file']['dir_data'], "missing_columns.csv"))
        with pytest.raises(Exception) as excinfo:
            flag = blssrm.main_blssrm_pipeline(df_output_iead)
        assert excinfo.type == ValueError

    def test_3_missing_columns_in_blssrmfile(self):
        df_output_ilead = pd.read_csv(os.path.join(blssrm.config['file']['dir_data'], "test_case1_new.csv"),
                                      low_memory=False)

        blssrm.config["file"]["Processed"]["processed_delta_csv"]["file_name"] = "test_case1_few_cols.csv"
        with pytest.raises(Exception) as excinfo:
            flag = blssrm.main_blssrm_pipeline(df_output_ilead)
        assert excinfo.type == KeyError

    def test_4_no_updates(self):
        df_output_ilead = pd.read_csv(os.path.join(blssrm.config['file']['dir_data'], "test_no_updates1.csv"),
                                      low_memory=False)
        blssrm.config["file"]["Processed"]["processed_delta_csv"]["file_name"] = "test_no_updates2.csv"
        flag = blssrm.main_blssrm_pipeline(df_output_ilead)
        assert flag

    def test_4_json_not_generated(self):
        df_output_ilead = pd.read_csv(
            os.path.join(blssrm.config['file']['dir_results'], "test_case1_empty_json_msg.csv"),
            low_memory=False)

        blssrm.config["file"]["Processed"]["processed_delta_csv"]["file_name"] = "test_case1_empty_json_msg_copy.csv"
        blssrm.config["file"]["Processed"]["processed_delta_json"]["file_name"] = "no_json_created.json"
        blssrm.config["payload"] = {"grant_type": "client_creds", "scope": "client_creds"}
        flag = blssrm.main_blssrm_pipeline(df_output_ilead)
        assert flag
        assert not os.path.isfile(os.path.join(blssrm.config['file']['dir_results'], "no_json_created.json"))
        blssrm.config["payload"] = {"grant_type": "client_credentials", "scope": "client_cred"}

    def end_to_end(self):
        df_output_ilead = pd.read_csv(os.path.join(blssrm.config['file']['dir_results'], "batchtest.csv"),
                                      low_memory=False)
        blssrm.config["file"]["Processed"]["processed_delta_csv"]["file_name"] = "end_to_end.csv"
        blssrm.config["file"]["Processed"]["processed_delta_json"]["file_name"] = "test_case_created.json"
        assert blssrm.main_blssrm_pipeline(df_output_ilead)


class TestPushDataInBatches:
    @patch("src.class_blssrm.blssrm.post_json")
    def test_success_for_all_batches(self, mock_post_request):
        df_test = pd.read_csv(os.path.join(blssrm.config['file']['dir_data'], "push_data_in_batch.csv"),
                              low_memory=False)
        blssrm.config["batch_size"] = 2
        blssrm.config["wait_time_in_secs_for_batches"] = 1
        mock_response_1 = MagicMock()
        mock_response_1.status_code_for_req = "200"
        mock_post_request.return_value = mock_response_1
        ls_pass, ls_fail, df_batch, req_id = blssrm.push_data_in_batches(df_test)
        assert (len(ls_pass) == df_test.shape[0] and len(ls_fail) == 0 and (df_batch["StatusCode"] == 200).all()
                and len(df_test) == len(df_batch)) and req_id != 0

    def test_success_1_for_all_batches(self):
        df_test = pd.read_csv(os.path.join(blssrm.config['file']['dir_data'], "push_data_in_batch.csv"),
                              low_memory=False)
        blssrm.config["batch_size"] = 2
        blssrm.config["wait_time_in_secs_for_batches"] = 3
        ls_pass, ls_fail, df_batch, req_id = blssrm.push_data_in_batches(df_test)
        assert (len(ls_pass) == df_test.shape[0] and len(ls_fail) == 0 and
                (df_batch["StatusCode"] == blssrm.config["success_status_code"][0]).all()
                and len(df_test) // blssrm.config["batch_size"] == len(df_batch)) and req_id != 0

    @patch("src.class_blssrm.blssrm.post_json")
    def test_failure_for_all_batches(self, mock_post_request):
        df_test = pd.read_csv(os.path.join(blssrm.config['file']['dir_data'], "push_data_in_batch.csv"),
                              low_memory=False)
        blssrm.config["batch_size"] = 2
        blssrm.config["wait_time_in_secs_for_batches"] = 1
        mock_response_1 = MagicMock()
        mock_response_1.status_code_for_req = "401"
        mock_post_request.post_json.return_value = mock_response_1
        ls_pass, ls_fail, df_batch, req_id = blssrm.push_data_in_batches(df_test)
        assert (len(ls_pass) == 0 and len(ls_fail) == df_test.shape[0] and (df_batch["StatusCode"] != 200).all()
                and len(df_test) // blssrm.config["batch_size"] == len(df_batch)) and req_id != 0

    @patch("src.class_blssrm.blssrm.post_json")
    def test_excpetion_for_all_batches(self, mock_post_request):
        df_test = pd.read_csv(os.path.join(blssrm.config['file']['dir_data'], "push_data_in_batch.csv"),
                              low_memory=False)
        blssrm.config["batch_size"] = 2
        blssrm.config["wait_time_in_secs_for_batches"] = -1
        mock_response_1 = MagicMock()
        mock_response_1.status_code_for_req = "401"
        mock_post_request.post_json.return_value = mock_response_1
        with pytest.raises(Exception) as excinfo:
            _, _, _, _ = blssrm.push_data_in_batches(df_test)
        assert excinfo.type == Exception
        blssrm.config["wait_time_in_secs_for_batches"] = 5
        blssrm.config["batch_size"] = 200


class TestPrepareDataInJsonFormat:

    @pytest.mark.parametrize(
        "df_output_ilead",
        [None,
         {10, 20},
         [],
         "string1",
         123,
         True,
         45.84
         ])
    def test_1_typeerror(self, df_output_ilead):
        with pytest.raises(Exception) as excinfo:
            flag = blssrm.prepare_data_in_exp_json_format(df_output_ilead)
        assert excinfo.type == TypeError

    def test_2_valueerror(self):
        with pytest.raises(Exception) as excinfo:
            flag = blssrm.prepare_data_in_exp_json_format(pd.DataFrame())
        assert excinfo.type == ValueError

    def test_3_all_fields_present(self):
        df = pd.read_csv(os.path.join(blssrm.config['file']['dir_data'], "test_case1_prepdata.csv"), low_memory=False)
        flag_proceed, json_data_to_push, reqid = blssrm.prepare_data_in_exp_json_format(df)

        assert list(json_data_to_push.keys())[0] == "ERPSummarys"

        for item in json_data_to_push["ERPSummarys"]:
            assert len(item.items()) == len(blssrm.config["required_columns"]) - len(
                blssrm.config["columns_to_drop"]) + 7

        for item in json_data_to_push["ERPSummarys"]:
            for _ in item.items():
                if _[0] == "ERPContracts":
                    assert isinstance(_[1], list)
                    assert isinstance(_[1][0], dict) and _[1][0] != {}
        with open(os.path.join(blssrm.config['file']['dir_data'], "test_blssrm.json"), 'r') as f:
            exp_json = json.load(f)

        for item in zip(json_data_to_push["ERPSummarys"], exp_json["ERPSummarys"]):
            for y in blssrm.config["required_columns"]:
                if y not in blssrm.config["columns_to_drop"]:
                    assert item[0][y] == item[1][y]
                else:
                    assert item[0]["ERPContracts"][0][y] == item[1]["ERPContracts"][0][y]

    def test_for_reqid(self):
        df = pd.read_csv(os.path.join(blssrm.config['file']['dir_data'], "test_case2_prepdata.csv"), low_memory=False)
        myredid = ["100022", "41311212", "5565656"]
        sns = df["SerialNumber"].tolist()
        blssrm.df = pd.DataFrame({"SerialNumber": sns, "RequestId": myredid})
        flag_proceed, json_data_to_push, reqid = blssrm.prepare_data_in_exp_json_format(df, myredid)

        assert len(set(myredid).difference(set(reqid))) == 0

        for x in json_data_to_push["ERPSummarys"]:
            for y in x.items():
                if y[0] == "RequestId":
                    assert str(y[1]) in myredid

    def test_4_exception(self):
        df = pd.read_csv(os.path.join(blssrm.config['file']['dir_data'], "test_case1_prepdata.csv"), low_memory=False)
        blssrm.config["columns_to_drop"] = ["contractStartDate", "contractEndDate", "predictPulseContractType"]
        flag_proceed, json_data_to_push = blssrm.prepare_data_in_exp_json_format(df)
        assert not flag_proceed and not isinstance(json_data_to_push, dict)
        blssrm.config["columns_to_drop"] = ["ContractStartDate", "ContractEndDate", "PredictPulseContractType"]


class TestForIntTypeColumns:
    def test_format_int_type_columns(self):
        # Create a sample DataFrame
        df_input_org = pd.DataFrame({
            "col1": [1.0, '', np.nan, 4],
            "col2": [5.0, 6, '', 8.0],
            "col3": [9, np.nan, 11.0, 12]
        })

        blssrm.config["integer_type_columns"] = ["col1", "col2", "col3"]
        df_output = blssrm.format_int_type_columns(df_input_org)

        expected_output = pd.DataFrame({
            "col1": [1, 0, np.nan, 4],
            "col2": [5, 6, 0, 8],
            "col3": [9, np.nan, 11, 12]
        }, dtype="Int64")

        # Compare the actual and expected output DataFrames
        assert df_output.equals(expected_output)
        assert_frame_equal(df_output, expected_output,
                           check_dtype=True, check_exact=True,
                           check_names=True)

    def test_for_integer_column(self):
        df = pd.DataFrame(data={"A": [4.0, 5.0, np.nan, '', '', np.nan, 55, 1144001]})
        # df = pd.DataFrame(data={"A": [4.0, 5.0, np.nan, '', '', np.nan, 55, 1144001]}, dtype=object)
        expected_df = pd.DataFrame(data={"A": [4, 5, np.nan, 0, 0, np.nan, 55, 1144001]}, dtype="Int64")
        blssrm.config["integer_type_columns"] = ["A"]
        df = blssrm.format_int_type_columns(df)
        assert df.equals(expected_df)
        assert_frame_equal(df, expected_df,
                           check_dtype=True, check_exact=True,
                           check_names=True)


class TestCheckForFile:
    def test_check_for_file_not_exists(self):
        try:
            os.remove(os.path.join(blssrm.config['file']['dir_results'], "processed_assets_sample.csv"))
        except:
            pass
        blssrm.config["file"]["Processed"]["processed_delta_csv"]["file_name"] = "processed_assets_sample.csv"
        df_output_ilead = pd.read_csv(
            os.path.join(blssrm.config['file']['dir_results'], "merged_asset_ilead_sample.csv"))
        flag = blssrm.check_for_file(df_output_ilead)
        assert flag
        assert os.path.isfile(os.path.join(blssrm.config['file']['dir_results'], "processed_assets_sample.csv"))

    def test_check_for_file_when_exists(self):
        blssrm.config["file"]["Processed"]["processed_delta_csv"]["file_name"] = "processed_assets_sample.csv"
        output_ilead = pd.read_csv(os.path.join(blssrm.config['file']['dir_results'], "merged_asset_ilead_sample.csv"))
        df_returned = blssrm.check_for_file(df_output_ilead=output_ilead)
        assert not df_returned.empty and len(df_returned.columns) == 39 and isinstance(df_returned, pd.DataFrame)


class TestGenerateToken:
    @patch('src.class_blssrm.requests')
    def test_1_for_successful_response(self, mock_post_request):
        mock_response_1 = MagicMock()
        mock_response_1.status_code = 200
        mock_response_1.json.return_value = {"token_type": "Bearer", "expires_in": 3600,
                                             "access_token": "a1b2c3d4e5", "scope": "client_cred"}
        mock_post_request.post.return_value = mock_response_1

        token_value = blssrm.generate_token()

        expected = "Bearer a1b2c3d4e5"
        assert expected == token_value

    @patch('src.class_blssrm.requests')
    @pytest.mark.parametrize("client_id, client_secret",
                             [("abcd", "somesecret"),
                              ("", "somesecret"),
                              ("abcd", ""),
                              ("", ""),
                              ("Basic abcd", "somesecret"),
                              ("Bearer abcd", "eklkpfdfa")
                              ])
    def test_1_for_unsuccessful_response(self, mock_post_request, client_id, client_secret):
        mock_response_1 = MagicMock()
        mock_response_1.status_code = 401
        mock_response_1.content.decode.return_value = {"errorCode": "invalid_client",
                                                       "errorSummary": "Invalid value for 'client_id' parameter.",
                                                       "errorLink": "invalid_client",
                                                       "errorId": "oae3cMMLYtKT-Ca54ncvt_LlA",
                                                       "errorCauses": []}
        mock_post_request.post.return_value = mock_response_1

        blssrm.client_secret = client_secret
        blssrm.client_id = client_id
        # with pytest.raises(Exception) as excinfo:
        #     token_value = blssrm.generate_token()
        # assert excinfo.type == Exception
        assert blssrm.generate_token() == "token not received"


class TestPostJson:

    @pytest.mark.parametrize(
        "example_json_data",
        [None,
         {10, 20},
         [],
         "string1",
         123,
         True,
         45.84])
    def test_1_typeerror(self, example_json_data):
        with pytest.raises(Exception) as excinfo:
            flag = blssrm.post_json(example_json_data)
        assert excinfo.type == TypeError

    @pytest.mark.parametrize(
        "example_json_data",
        [{},
         {"eRPSummarys": [{}]},
         {"A": 10}
         ])
    def test_1_valueerror(self, example_json_data):
        with pytest.raises(Exception) as excinfo:
            flag = blssrm.post_json(example_json_data)
        assert excinfo.type == ValueError

    def test_timeout(self):
        with open(os.path.join(blssrm.config['file']['dir_results'], "one_k_json.json")) as f:
            example_json_data = json.load(f)

        json_data_to_push = {"ERPSummarys": example_json_data}
        blssrm.config['connection_time_out_in_secs'] = 2
        blssrm.config['response_time_out_in_secs'] = 2
        flag = blssrm.post_json(json_data_to_push)
        assert flag in blssrm.config["st_code"]
        blssrm.config['connection_time_out_in_secs'] = 40
        blssrm.config['response_time_out_in_secs'] = 40

    @patch('src.class_blssrm.requests.post')
    @patch('src.class_blssrm.blssrm.generate_token')
    def test_1_for_successful_response(self, mock_generate_token, mock_post_request):
        successful_response = {"success": "true", "data": {"batchID": "12345"}, "requestId": "a1b2c3d4e5"}
        mock_post_request.return_value = mock.Mock(name="mock response for post method",
                                                   **{"status_code": 200, "content.decode.return_value":
                                                       json.dumps(successful_response)})
        mock_generate_token.return_value = mock.Mock(name="mock response for generate_token method",
                                                     **{"bearer_token": "eKLK1212OOa"})

        json_data = {"ERPSummarys": [{"SerialNumber": "Auto_11",
                                      "ERPContracts": [
                                          {
                                              "ContractEndDate": "2023-09-10",
                                              "ContractStartDate": "2013-09-10",
                                              "PredictPulseContractType": "Basic"
                                          }],
                                      "ElectricRoomCleanliness": 4,
                                      "Upm4FanServDate": "2021-08-07"
                                      }]}
        flag_for_success = blssrm.post_json(json_data)
        assert flag_for_success in blssrm.config["success_status_code"]

    @patch('src.class_blssrm.requests.post')
    @patch('src.class_blssrm.blssrm.generate_token')
    def test_2_for_successful_response(self, mock_generate_token, mock_post_request):
        mock_response_1 = MagicMock()
        mock_response_1.status_code = 200
        mock_response_1.content.decode.return_value = json.dumps({"success": "true", "data":
            {"batchID": "12345"}, "requestId": "a1b2c3d4e5"})
        mock_post_request.return_value = mock_response_1

        mock_response_2 = MagicMock()
        mock_response_2.bearer_token = "eKLK1212OOa"
        mock_generate_token.return_value = mock_response_2

        json_data = {"ERPSummarys": [{}]}
        flag_for_success = blssrm.post_json(json_data)

        assert flag_for_success in blssrm.config["success_status_code"]

    @patch('src.class_blssrm.requests.post')
    @patch('src.class_blssrm.blssrm.generate_token')
    def test_3_for_successful_response(self, mock_generate_token, mock_post_request):
        mock_response_1 = MagicMock()
        mock_response_1.status_code = 201
        # mock_response_1.content.decode.return_value = {"success": "true", "data": {"batchID": "12345"},
        #                                                "requestId": "a1b2c3d4e5"}
        mock_response_1.content.decode.return_value = json.dumps({"success": "true", "data": {"batchID": "12345"},
                                                                  "requestId": "a1b2c3d4e5"}
                                                                 )
        mock_post_request.return_value = mock_response_1

        mock_response_2 = MagicMock()
        mock_response_2.bearer_token = "eKLK1212OOa"
        mock_generate_token.return_value = mock_response_2

        json_data = {"ERPSummarys": [{"SerialNumber": "asbv323", "ElectricRoomCleanliness": 4}]}
        flag_for_success = blssrm.post_json(json_data)
        print(f"Line number 306 {flag_for_success}")
        assert flag_for_success not in blssrm.config["success_status_code"]

    @patch('src.class_blssrm.requests.post')
    @patch('src.class_blssrm.blssrm.generate_token')
    def test_for_empty_strings_as_dates(self, mock_generate_token, mock_post_request):
        mock_response_1 = MagicMock()
        mock_response_1.status_code = 200
        # mock_response_1.content.decode.return_value = {"success": "true", "data": {"batchID": "12345"},
        #                                                "requestId": "a1b2c3d4e5"}
        mock_response_1.content.decode.return_value = json.dumps({"success": False, "errorCode":1001, "errorMessage":["com.bssrm.app.exceptions.ERPSError: Date:  not valid.Text '' could not be parsed at index 0"],"requestId":"2c49d5da826d4e52b9e0f8695d97eb92"}
                                                                 )
        mock_post_request.return_value = mock_response_1

        mock_response_2 = MagicMock()
        mock_response_2.bearer_token = "eKLK1212OOa"
        mock_generate_token.return_value = mock_response_2

        json_data = {"ERPSummarys": [{"SerialNumber": "asbv323", "ElectricRoomCleanliness": 4}]}
        flag_for_success = blssrm.post_json(json_data)
        print(f"Line number 432 {flag_for_success}")
        assert flag_for_success not in blssrm.config["success_status_code"]

    @patch('src.class_blssrm.requests')
    @patch('src.class_blssrm.blssrm.generate_token')
    def test_4_for_invalid_request_body(self, mock_generate_token, mock_post_request):
        mock_response_1 = MagicMock()
        mock_response_1.status_code = 400
        mock_response_1.content.decode.return_value = json.dumps({"success": "false", "errorCode": 0, "errorMessage":
            ["Invalid request body or few fields expected by the API are blank"],
                                                       "requestId": "6f469dd9844c4d42b58bbf83bb8504ae"})
        mock_post_request.post.return_value = mock_response_1

        mock_response_2 = MagicMock()
        mock_response_2.bearer_token = "eKLK1212OOa"
        mock_generate_token.return_value = mock_response_2

        # missing serial number column which is mandatory field
        json_data = {"ERPSummarys": [{"": ""}, {"": "12345"}]}

        # with pytest.raises(Exception) as excinfo:
        #     blssrm.post_json(json_data)
        # assert excinfo.type == Exception
        st_code_returned = blssrm.post_json(json_data)
        print(f"Line number 330 {st_code_returned}")
        assert st_code_returned not in blssrm.config["success_status_code"]

    @patch('src.class_blssrm.requests')
    @patch('src.class_blssrm.blssrm.generate_token')
    def test_1_for_authorization_failure(self, mock_generate_token, mock_post_request):
        mock_response_1 = MagicMock()
        mock_response_1.status_code = 401
        mock_response_1.content.decode.return_value = json.dumps({"success": "false", "errorCode": 0, "errorMessage":
            ["Invalid credentials. Authentication has failed."],
                                                       "requestId": "6f469dd9844c4d42b58bbf83bb8504ae"})
        mock_post_request.post.return_value = mock_response_1

        mock_response_2 = MagicMock()
        mock_response_2.bearer_token = ""  # empty token
        mock_generate_token.generate_token.return_value = mock_response_2

        json_data = {"ERPSummarys": [{"SerialNumber": "Auto_13",
                                      "ERPContracts": [
                                          {
                                              "ContractEndDate": "2023-10-10",
                                              "ContractNumber": "90811",
                                              "ContractStartDate": "2019-01-01",
                                              "PredictPulseContractType": "type_1"
                                          }
                                      ],
                                      "Country": "USA"
                                      }]}

        st_code_returned = blssrm.post_json(json_data)
        print(f"Line number 360 {st_code_returned}")
        assert st_code_returned not in blssrm.config["success_status_code"]

    @patch('src.class_blssrm.requests')
    @patch('src.class_blssrm.blssrm.generate_token')
    def test_1_for_forbidden_access(self, mock_generate_token, mock_post_request):
        mock_response_1 = MagicMock()
        mock_response_1.status_code = 403
        mock_response_1.content.decode.return_value = json.dumps({"success": "false", "errorCode": 0, "errorMessage":
            ["Access has been forbidden"],
                                                       "requestId": "6f469dd9844c4d42b58bbf83bb8504ae"})

        mock_response_2 = MagicMock()
        mock_response_2.bearer_token = "erel99asaKK"
        mock_generate_token.generate_token.return_value = mock_response_2

        json_data = {"ERPSummarys": [{"SerialNumber": "Auto_13",
                                      "ERPContracts": [
                                          {
                                              "contractEndDate": "2023-10-10",
                                              "contractNumber": "90811",
                                              "contractStartDate": "2019-01-01",
                                              "predictPulseContractType": "type_1"
                                          }
                                      ],
                                      "country": "USA"
                                      }]}

        mock_post_request.get.return_value = mock_response_1  # get instead of post
        print(f"Line number 389 {blssrm.post_json(json_data)}")
        assert blssrm.post_json(json_data) not in blssrm.config["success_status_code"]

    @patch('src.class_blssrm.requests')
    @patch('src.class_blssrm.blssrm.generate_token')
    def test_1_for_internal_server_error(self, mock_generate_token, mock_post_request):
        mock_response_1 = MagicMock()
        mock_response_1.status_code = 500
        mock_response_1.content.decode.return_value = json.dumps({"success": "false", "errorCode": 0, "errorMessage":
            ["Internal Server Error"],
                                                       "requestId": "7f834dd1111c4d42b99bbf83bb3214ae"})
        mock_post_request.post.return_value = mock_response_1

        mock_response_2 = MagicMock()
        mock_response_2.bearer_token = "erel99asaKK"
        mock_generate_token.generate_token.return_value = mock_response_2

        json_data = {"ERPSummarys": [{"numberOfCabinets": 4,
                                      "serialNumber": "Auto_11",
                                      "state": "state1"
                                      }]}

        st_code_returned = blssrm.post_json(json_data)
        print(f"Line number 412 {st_code_returned}")
        assert st_code_returned not in blssrm.config["success_status_code"]

    @patch('src.class_blssrm.requests')
    @patch('src.class_blssrm.blssrm.generate_token')
    def test_1_for_gateway_issue(self, mock_generate_token, mock_post_request):
        mock_response = MagicMock()
        mock_response.status_code = 502
        mock_response.content.decode.return_value = json.dumps({"success": "false", "errorCode": 0, "errorMessage":
            ["Gateway Problem"],
                                                     "requestId": "21asqqwq9983131vkkvbbf83bb3214ae"})
        mock_post_request.post.return_value = mock_response

        mock_response_2 = MagicMock()
        mock_response_2.bearer_token = "erel99asaKK"
        mock_generate_token.generate_token.return_value = mock_response_2

        json_data = {"ERPSummarys": [{"numberOfCabinets": 4,
                                      "serialNumber": "Auto_11",
                                      "state": "state1"
                                      }]}

        st_code_returned = blssrm.post_json(json_data)
        print(f"Line number 435 {st_code_returned}")
        assert st_code_returned not in blssrm.config["success_status_code"]

    @patch('src.class_blssrm.requests')
    @patch('src.class_blssrm.blssrm.generate_token')
    def test_1_for_service_unavailable(self, mock_generate_token, mock_post_request):
        mock_response_1 = MagicMock()
        mock_response_1.status_code = 503
        mock_response_1.content.decode.return_value = json.dumps({"success": "false", "errorCode": 0, "errorMessage":
            ["Service Unavailable"],
                                                       "requestId": "823pp13135163aa22eef83bb3214ae"})
        mock_post_request.post.return_value = mock_response_1

        mock_response_2 = MagicMock()
        mock_response_2.bearer_token = "erel99asaKK"
        mock_generate_token.generate_token.return_value = mock_response_2

        json_data = {"ERPSummarys": [{"serialNumber": "Auto_11", "installedDate": "2018-07-07"}]}

        st_code_returned = blssrm.post_json(json_data)
        print(f"line number 455 {st_code_returned}")
        assert st_code_returned not in blssrm.config["success_status_code"]

    @patch('src.class_blssrm.requests.post')
    @patch('src.class_blssrm.blssrm.generate_token')
    def test_1_for_unknown_status_code(self, mock_generate_token, mock_post_request):
        successful_response = {"success": "false", "errorCode": 0, "errorMessage":
            ["Unknown status code"],
                               "requestId": "222qq36985274bb22kkd83bb3214ae"}
        mock_post_request.return_value = mock.Mock(name="mock response for post method",
                                                   **{"status_code": '3XX', "content.decode.return_value":
                                                       json.dumps(successful_response)})

        mock_response = MagicMock()
        mock_response.bearer_token = "erel99asaKK"
        mock_generate_token.generate_token.return_value = mock_response

        json_data = {"ERPSummarys": [{"SerialNumber": "Auto_11",
                                      "BatteryDateCode": "2020-02-02",
                                      "BatteryInstallDate": "2021-04-04"
                                      }]}

        st_code_returned = blssrm.post_json(json_data)
        print(f"line number 477 {st_code_returned}")
        assert st_code_returned not in blssrm.config["success_status_code"]

    @patch('src.class_blssrm.requests.post')
    @patch('src.class_blssrm.blssrm.generate_token')
    def test_1_for_unknown_status_code(self, mock_generate_token, mock_post_request):
        successful_response = {"success": "false", "errorCode": 0, "errorMessage":
            ["Unknown status code"],
                               "requestId": "222qq36985274bb22kkd83bb3214ae"}
        mock_post_request.return_value = mock.Mock(name="mock response for post method",
                                                   **{"status_code": '900', "content.decode.return_value":
                                                       json.dumps(successful_response)})

        mock_response = MagicMock()
        mock_response.bearer_token = blssrm.config["token_message"]
        mock_generate_token.generate_token.return_value = mock_response

        json_data = {"ERPSummarys": [{"SerialNumber": "Auto_11",
                                      "BatteryDateCode": "2020-02-02",
                                      "BatteryInstallDate": "2021-04-04"
                                      }]}

        st_code_returned = blssrm.post_json(json_data)
        assert st_code_returned in blssrm.config["st_code"]


class TestIdentifyUpdates:

    @pytest.mark.parametrize(
        "df_blssrm, df_output_ilead",
        [(None, pd.DataFrame()),
         ({10, 20}, pd.DataFrame()),
         ([], pd.DataFrame()),
         ("string1", pd.DataFrame()),
         (123, pd.DataFrame()),
         (True, pd.DataFrame()),
         (45.84, pd.DataFrame())
         ])
    def test_1_typeerror(self, df_blssrm, df_output_ilead):
        with pytest.raises(Exception) as excinfo:
            _, _, _ = blssrm.identify_updates(df_blssrm, df_output_ilead)
        assert excinfo.type == TypeError

    @pytest.mark.parametrize(
        "df_blssrm, df_output_ilead",
        [(pd.DataFrame(), None),
         (pd.DataFrame(), {10, 20}),
         (pd.DataFrame(), []),
         (pd.DataFrame(), "string1"),
         (pd.DataFrame(), 123),
         (pd.DataFrame(), True),
         (pd.DataFrame(), 45.84)
         ])
    def test_2_typeerror(self, df_blssrm, df_output_ilead):
        with pytest.raises(Exception) as excinfo:
            _, _, _ = blssrm.identify_updates(df_blssrm, df_output_ilead)
        assert excinfo.type == TypeError

    @pytest.mark.parametrize(
        "df_blssrm, df_output_ilead",
        [(pd.DataFrame(data={"A": 10}, index=[0]), pd.DataFrame()),
         (pd.DataFrame(), pd.DataFrame(data={"A": 10}, index=[0])),
         (pd.DataFrame(), pd.DataFrame())
         ])
    def test_1_valueerror(self, df_blssrm, df_output_ilead):
        with pytest.raises(Exception) as excinfo:
            _, _, _ = blssrm.identify_updates(df_blssrm, df_output_ilead)
        assert excinfo.type == ValueError

    def test_identify_updates_no_updates_with_no_new_rows(self):
        first_df = pd.read_csv(os.path.join(blssrm.config['file']['dir_data'], "test_case1_old.csv"), dtype=column_data_types)
        first_df = blssrm.format.format_data(first_df, blssrm.config["processed_assets"]["Dictionary Format"])
        second_df = pd.read_csv(os.path.join(blssrm.config['file']['dir_data'], "test_case1_new.csv"), dtype=column_data_types)
        second_df = blssrm.format.format_data(second_df, blssrm.config["processed_assets"]["Dictionary Format"])

        df_with_updates, df_with_upsert, df_sn_with_no_updates = blssrm.identify_updates(first_df, second_df)
        assert (df_with_updates.empty and df_with_upsert.shape == first_df.shape and
                df_sn_with_no_updates.shape == first_df.shape)
        first_df = first_df.sort_values(by="SerialNumber", ascending=False).reset_index(drop=True)
        df_with_upsert = df_with_upsert.sort_values(by="SerialNumber", ascending=False).reset_index(drop=True)
        df_with_upsert = blssrm.format.format_data(df_with_upsert, blssrm.config["processed_assets"]["Dictionary Format"])
        assert_frame_equal(first_df, df_with_upsert,
                           check_dtype=False, check_exact=False,
                           check_names=True)

    def test_identify_updates_for_all_columns_with_no_new_rows(self):
        first_df = pd.read_csv(os.path.join(blssrm.config['file']['dir_data'], "test_case3_old.csv"), dtype=column_data_types)
        first_df = blssrm.format.format_data(first_df, blssrm.config["processed_assets"]["Dictionary Format"])
        second_df = pd.read_csv(os.path.join(blssrm.config['file']['dir_data'], "test_case3_new.csv"), dtype=column_data_types)
        second_df = blssrm.format.format_data(second_df, blssrm.config["processed_assets"]["Dictionary Format"])
        df_with_updates, df_with_upsert, df_sn_with_no_updates = blssrm.identify_updates(first_df, second_df)
        assert df_sn_with_no_updates.empty

        second_df = second_df.astype(str)
        second_df = second_df.replace("<NA>|nan", '', regex=True)
        second_df = second_df.sort_values(by="SerialNumber", ascending=False).reset_index(drop=True)
        df_with_updates = df_with_updates.sort_values(by="SerialNumber", ascending=False).reset_index(drop=True)

        assert_frame_equal(second_df, df_with_updates,
                           check_dtype=False, check_exact=False,
                           check_names=True)

    def test_identify_1_updates_for_old_and_new_rows(self):
        first_df = pd.read_csv(os.path.join(blssrm.config['file']['dir_data'], "test_case5_old_2.csv"),
                               dtype=column_data_types)
        first_df = blssrm.format.format_data(first_df, blssrm.config["processed_assets"]["Dictionary Format"])
        second_df = pd.read_csv(os.path.join(blssrm.config['file']['dir_data'], "test_case5_new_2.csv"),
                                dtype=column_data_types)
        second_df = blssrm.format.format_data(second_df, blssrm.config["processed_assets"]["Dictionary Format"])
        expected_df = pd.read_csv(os.path.join(blssrm.config['file']['dir_data'], "test_case5_check.csv"),
                                  dtype=column_data_types)
        updated_exp_df = pd.read_csv(os.path.join(blssrm.config['file']['dir_data'], "test_case5_updates_check.csv"),
                                     dtype=column_data_types)

        df_with_updates, df_with_upsert, df_sn_no_updates = blssrm.identify_updates(first_df, second_df)
        assert not df_sn_no_updates.empty

        expected_df = expected_df.astype(str)
        expected_df = expected_df.replace('', np.nan).replace("nan", np.nan).replace("<NA>", np.nan).replace("BLANK",'')
        expected_df = expected_df.sort_values(by="SerialNumber", ascending=False).reset_index(drop=True)
        expected_df = blssrm.format_int_type_columns(expected_df)
        df_with_upsert = df_with_upsert.sort_values(by="SerialNumber", ascending=False).reset_index(drop=True)
        df_with_upsert = blssrm.format_int_type_columns(df_with_upsert)
        assert_frame_equal(expected_df, df_with_upsert,
                           check_dtype=False, check_exact=False,
                           check_names=True)

        updated_exp_df = updated_exp_df.astype(str)
        updated_exp_df = updated_exp_df.replace('', np.nan).replace("nan", np.nan).replace("<NA>", np.nan).replace("BLANK",'')
        updated_exp_df = updated_exp_df.sort_values(by="SerialNumber", ascending=False).reset_index(drop=True)
        updated_exp_df = blssrm.format_int_type_columns(updated_exp_df)

        df_with_updates = df_with_updates.sort_values(by="SerialNumber", ascending=False).reset_index(drop=True)
        df_with_updates = df_with_updates.replace('', np.nan).replace("NULL", '')
        df_with_updates = blssrm.format_int_type_columns(df_with_updates)
        assert_frame_equal(updated_exp_df, df_with_updates,
                           check_dtype=False, check_exact=False,
                           check_names=True)

    def test_identify_updates_throws_excpetion(self):
        first_df = pd.read_csv(os.path.join(blssrm.config['file']['dir_data'], "test_case5_old_1.csv"),
                               low_memory=False)
        second_df = pd.read_csv(os.path.join(blssrm.config['file']['dir_data'], "test_case5_new_1.csv"),
                                low_memory=False)
        with pytest.raises(Exception) as excinfo:
            _, _, _ = blssrm.identify_updates(first_df, second_df)
        assert excinfo.type == Exception
