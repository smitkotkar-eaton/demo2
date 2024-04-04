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
import pandas as pd
import pytest
import json
from mock import (Mock, sentinel, patch, MagicMock)
import requests

from pathlib import Path
import sys

sys.path.append(str(Path(__file__).parent.parent))

# import src.config as CONF
from src.class_blssrm import blssrm


# erp = ERP()
blssrm = blssrm()
blssrm.config['file']['dir_data'] = "./test/"
blssrm.config['file']['dir_results'] = "./test/"


# class TestMainBlssrmPipeline:
#     @pytest.mark.parametrize(
#         "df_output_ilead",
#         [None, {10, 20}, [], "string1", 123, True, 45.84])
#     def test_1_typeerror(self, df_output_ilead):
#         with pytest.raises(Exception) as excinfo:
#             flag = blssrm.main_blssrm_pipeline(df_output_ilead)
#         assert excinfo.type == TypeError
#
#     def test_1_valueerror(self):
#         with pytest.raises(Exception) as excinfo:
#             flag = blssrm.main_blssrm_pipeline(pd.DataFrame())
#         assert excinfo.type == ValueError
#
#     def test_2_valueerror_missing_columns(self):
#         df_output_iead = pd.DataFrame(data={"SerialNumber": "Auto_11", "ContractStartDate": "2017-03-14T00:00:00",
#                                             "ContractEndDate": "2019-03-13T00:00:00", "PredictPulseContractType":
#                                                 "Basic", "Current": 1, "SystemModstamp": "2023-10-11T04:45:37.000+0000",
#                                             "BatteryJarsPerTray": 2, "BatteryTraysPerString": 4}, index=[0])
#         with pytest.raises(Exception) as excinfo:
#             flag = blssrm.main_blssrm_pipeline(df_output_iead)
#         assert excinfo.type == ValueError
#
#
#     def test_3_missing_columns_in_blssrmfile(self):
#         df_output_ilead = pd.read_csv(r"C:\Users\E0778583\OneDrive - "
#                                       r"Eaton\Documents\predictpulse\blssrm\test\test_case1_new.csv", low_memory=False)
#
#         blssrm.config["file"]["Processed"]["processed_delta_csv"]["file_name"] = "test_case1_few_cols.csv"
#         with pytest.raises(Exception) as excinfo:
#             flag = blssrm.main_blssrm_pipeline(df_output_ilead)
#         assert excinfo.type == Exception
#
#     def test_4_for_empty_dict(self):
#         df_output_ilead = pd.read_csv(r"C:\Users\E0778583\OneDrive - "
#                                       r"Eaton\Documents\predictpulse\blssrm\test\test_case1_new.csv", low_memory=False)
#
#         blssrm.config["file"]["Processed"]["processed_delta_csv"]["file_name"] = "test_case1_old.csv"
#         flag = blssrm.main_blssrm_pipeline(df_output_ilead)
#         assert flag is True
#
#     def test_5_full_load(self):
#         df_output_ilead = pd.read_csv(r"C:\Users\E0778583\OneDrive - "
#                                       r"Eaton\Documents\predictpulse\blssrm\test\test_case1_new.csv", low_memory=False)
#
#         flag = blssrm.main_blssrm_pipeline(df_output_ilead)
#         assert flag is True
#
#     def test_5_push_delta(self):
#         df_output_ilead = pd.read_csv(r"C:\Users\E0778583\OneDrive - "
#                                       r"Eaton\Documents\predictpulse\blssrm\test\test_case5_new_1.csv", low_memory=False)
#
#         blssrm.config["file"]["Processed"]["processed_delta_csv"]["file_name"] = "test_case5_old_1.csv"
#
#         flag = blssrm.main_blssrm_pipeline(df_output_ilead)
#         assert flag is True


# class TestPrepareDataInJsonFormat:
#
#     @pytest.mark.parametrize(
#         "df_output_ilead",
#         [None,
#          {10, 20},
#          [],
#          "string1",
#          123,
#          True,
#          45.84
#          ])
#     def test_1_typeerror(self, df_output_ilead):
#         with pytest.raises(Exception) as excinfo:
#             flag = blssrm.prepare_data_in_exp_json_format(df_output_ilead)
#         assert excinfo.type == TypeError
#
#     def test_2_valueerror(self):
#         with pytest.raises(Exception) as excinfo:
#             flag = blssrm.prepare_data_in_exp_json_format(pd.DataFrame())
#         assert excinfo.type == ValueError
#
#     def test_3_all_fields_present(self):
#         df = pd.read_csv(
#             r"C:\Users\E0778583\OneDrive - Eaton\Documents\predictpulse\blssrm\test\test_case1_prepdata.csv",
#             low_memory=False)
#         flag_proceed, json_data_to_push = blssrm.prepare_data_in_exp_json_format(df)
#
#         assert list(json_data_to_push.keys())[0] == "ERPSummarys"
#
#         for item in json_data_to_push["ERPSummarys"]:
#             assert len(item.items()) == 22
#
#         for item in json_data_to_push["ERPSummarys"]:
#             for _ in item.items():
#                 if _[0] == "ERPContracts":
#                     assert isinstance(_[1], list)
#                     assert isinstance(_[1][0], dict)
#
#     def test_4_few_fields_blank(self):
#         df = pd.read_csv(
#             r"C:\Users\E0778583\OneDrive - Eaton\Documents\predictpulse\blssrm\test\test_case2_prepdata.csv",
#             low_memory=False)
#         flag_proceed, json_data_to_push = blssrm.prepare_data_in_exp_json_format(df)
#
#         assert list(json_data_to_push.keys())[0] == "ERPSummarys"
#
#         for item in json_data_to_push["ERPSummarys"]:
#             assert len(item.items()) <= 22
#
#         for item in json_data_to_push["ERPSummarys"]:
#             for _ in item.items():
#                 if _[0] == "ERPContracts":
#                     assert isinstance(_[1], list)
#                     assert isinstance(_[1][0], dict)


# class TestGenerateToken:
#     @patch('src.class_blssrm.requests')
#     def test_1_for_successful_response(self, mock_post_request):
#         mock_response_1 = MagicMock()
#         mock_response_1.status_code = 200
#         mock_response_1.json.return_value = {"token_type": "Bearer", "expires_in": 3600,
#                                              "access_token": "a1b2c3d4e5", "scope": "client_cred"}
#         mock_post_request.post.return_value = mock_response_1
#
#         token_value = blssrm.generate_token()
#
#         expected = "Bearer a1b2c3d4e5"
#         assert expected == token_value
#
#     @patch('src.class_blssrm.requests')
#     @pytest.mark.parametrize("client_id, client_secret",
#                              [("abcd", "somesecret"),
#                               ("", "somesecret"),
#                               ("abcd", ""),
#                               ("", ""),
#                               ("Basic abcd", "somesecret"),
#                               ("Bearer abcd", "eklkpfdfa")
#                               ])
#     def test_1_for_unsuccessful_response(self, mock_post_request, client_id, client_secret):
#         mock_response_1 = MagicMock()
#         mock_response_1.status_code = 401
#         mock_response_1.json.return_value = {"errorCode": "invalid_client",
#                                              "errorSummary": "Invalid value for 'client_id' parameter.",
#                                              "errorLink": "invalid_client", "errorId": "oae3cMMLYtKT-Ca54ncvt_LlA",
#                                              "errorCauses": []}
#         mock_post_request.post.return_value = mock_response_1
#
#         blssrm.client_secret = client_secret
#         blssrm.client_id = client_id
#         with pytest.raises(Exception) as excinfo:
#             token_value = blssrm.generate_token()
#         assert excinfo.type == Exception


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



    @patch('src.class_blssrm.requests.post')
    @patch('src.class_blssrm')
    def test_1_for_successful_response(self, mock_post_request, mock_generate_token):
        successful_response = {"success": "true", "data": {"batchID": "12345"}, "requestId": "a1b2c3d4e5"}
        mock_post_request.return_value = mock.Mock(name="mock response for post method",
                                                   **{"status_code": 200, "content.decode.return_value":
                                                       successful_response})
        mock_generate_token.return_value = mock.Mock(name="mock response for generate_token method",
                                                     **{"bearer_token": "eKLK1212OOa"})

        json_data = {"ERPSummarys": [{"SerialNumber": "Auto_11",
                                      "batteryInstallDate": "2021-04-04",
                                      "batteryMfr": "xyz",
                                      "batteryModel": "model1",
                                      "ERPServiceRequests": [
                                          {
                                              "contractEndDate": "2023-09-10",
                                              "contractNumber": "12345",
                                          }],
                                      "elecRooClean": "clean1",
                                      "fanServDate": "2022-04-06",
                                      "upm4FanServDate": "2021-08-07",
                                      "warrantyEndDate": "2021-08-09",
                                      "zip": "12333"
                                      }]}
        flag_for_success = blssrm.post_json(json_data)

        assert flag_for_success

    @patch('src.class_blssrm.requests')
    @patch('src.class_blssrm')
    def test_2_for_successful_response(self, mock_post_request, mock_generate_token):
        mock_response_1 = MagicMock()
        mock_response_1.status_code = 200
        mock_response_1.content.decode.return_value = {"success": "true", "data": {"batchID": "12345"},
                                                       "requestId": "a1b2c3d4e5"}
        mock_post_request.post.return_value = mock_response_1

        mock_response_2 = MagicMock()
        mock_response_2.bearer_token = "eKLK1212OOa"
        mock_generate_token.generate_token.return_value = mock_response_2

        json_data = {"ERPSummarys": [{}]}
        flag_for_success = blssrm.post_json(json_data)

        assert flag_for_success

    @patch('src.class_blssrm.requests')
    @patch('src.class_blssrm')
    def test_3_for_successful_response(self, mock_post_request, mock_generate_token):
        mock_response_1 = MagicMock()
        mock_response_1.status_code = 200
        mock_response_1.content.decode.return_value = {"success": "true", "data": {"batchID": "12345"},
                                                       "requestId": "a1b2c3d4e5"}
        mock_post_request.post.return_value = mock_response_1

        mock_response_2 = MagicMock()
        mock_response_2.bearer_token = "eKLK1212OOa"
        mock_generate_token.generate_token.return_value = mock_response_2

        json_data = {"ERPSummarys": [{"SerialNumber": "Auto_11",
                                      "ERPContracts": [
                                          {
                                              "contractEndDate": "2023-09-10",
                                              "contractNumber": "12345",
                                              "contractStartDate": "2019-10-09",
                                              "predictPulseContractType": "pp_type_1"
                                          }
                                      ]}
                                     ]}

        flag_for_success = blssrm.post_json(json_data)

        assert flag_for_success

    @patch('src.class_blssrm.requests')
    @patch('src.class_blssrm')
    def test_4_for_successful_response(self, mock_post_request, mock_generate_token):
        mock_response_1 = MagicMock()
        mock_response_1.status_code = 201
        mock_response_1.content.decode.return_value = {"success": "true", "data": {"batchID": "12345"},
                                                       "requestId": "a1b2c3d4e5"}
        mock_post_request.post.return_value = mock_response_1

        mock_response_2 = MagicMock()
        mock_response_2.bearer_token = "eKLK1212OOa"
        mock_generate_token.generate_token.return_value = mock_response_2

        json_data = {"ERPSummarys": [{}]}
        flag_for_success = blssrm.post_json(json_data)

        assert flag_for_success

    @patch('src.class_blssrm')
    @patch('src.class_blssrm.requests')
    def test_4_for_invalid_request_body(self, mock_generate_token, mock_post_request):
        mock_response_1 = MagicMock()
        mock_response_1.status_code = 400
        mock_response_1.content.decode.return_value = {"success": "false", "errorCode": 0, "errorMessage":
            ["Invalid request body or few fields expected by the API are blank"],
                                                       "requestId": "6f469dd9844c4d42b58bbf83bb8504ae"}
        mock_post_request.post.return_value = mock_response_1

        mock_response_2 = MagicMock()
        mock_response_2.bearer_token = "eKLK1212OOa"
        mock_generate_token.generate_token.return_value = mock_response_2

        # missing serial number column which is mandatory field
        json_data = {"ERPSummarys": [{"": ""}, {"": "12345"}]}

        with pytest.raises(Exception) as excinfo:
            blssrm.post_json(json_data)
        assert excinfo.type == Exception

    @patch('src.class_blssrm')
    @patch('src.class_blssrm.requests')
    def test_1_for_authorization_failure(self, mock_generate_token, mock_post_request):
        mock_response_1 = MagicMock()
        mock_response_1.status_code = 401
        mock_response_1.content.decode.return_value = {"success": "false", "errorCode": 0, "errorMessage":
            ["Invalid credentials. Authentication has failed."],
                                                       "requestId": "6f469dd9844c4d42b58bbf83bb8504ae"}
        mock_post_request.return_value = mock_response_1

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

        with pytest.raises(Exception) as excinfo:
            blssrm.post_json(json_data)
        assert excinfo.type == Exception

    @patch('src.class_blssrm')
    @patch('src.class_blssrm.requests')
    def test_1_for_forbidden_access(self, mock_generate_token, mock_post_request):
        mock_response_1 = MagicMock()
        mock_response_1.status_code = 403
        mock_response_1.content.decode.return_value = {"success": "false", "errorCode": 0, "errorMessage":
            ["Access has been forbidden"],
                                                       "requestId": "6f469dd9844c4d42b58bbf83bb8504ae"}

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

        with pytest.raises(Exception) as excinfo:
            # sending a request using get method instead of post
            mock_post_request.get.return_value = mock_response_1
            blssrm.post_json(json_data)
        assert excinfo.type == Exception

    @patch('src.class_blssrm')
    @patch('src.class_blssrm.requests')
    def test_1_for_internal_server_error(self, mock_generate_token, mock_post_request):
        mock_response_1 = MagicMock()
        mock_response_1.status_code = 500
        mock_response_1.content.decode.return_value = {"success": "false", "errorCode": 0, "errorMessage":
            ["Internal Server Error"],
                                                       "requestId": "7f834dd1111c4d42b99bbf83bb3214ae"}
        mock_post_request.post.return_value = mock_response_1

        mock_response_2 = MagicMock()
        mock_response_2.bearer_token = "erel99asaKK"
        mock_generate_token.generate_token.return_value = mock_response_2

        json_data = {"ERPSummarys": [{"numberOfCabinets": 4,
                                      "serialNumber": "Auto_11",
                                      "state": "state1"
                                      }]}

        with pytest.raises(Exception) as excinfo:
            blssrm.post_json(json_data)
        assert excinfo.type == Exception

    @patch('src.class_blssrm')
    @patch('src.class_blssrm.requests.post')
    def test_1_for_gateway_issue(self, mock_generate_token, mock_post_request):
        mock_response = MagicMock()
        mock_response.status_code = 502
        mock_response.content.decode.return_value = {"success": "false", "errorCode": 0, "errorMessage":
            ["Gateway Problem"],
                                                     "requestId": "21asqqwq9983131vkkvbbf83bb3214ae"}
        mock_post_request.post.return_value = mock_response

        mock_response_2 = MagicMock()
        mock_response_2.bearer_token = "erel99asaKK"
        mock_generate_token.generate_token.return_value = mock_response_2

        json_data = {"ERPSummarys": [{"numberOfCabinets": 4,
                                      "serialNumber": "Auto_11",
                                      "state": "state1"
                                      }]}

        with pytest.raises(Exception) as excinfo:
            blssrm.post_json(json_data)
        assert excinfo.type == Exception

    @patch('src.class_blssrm')
    @patch('src.class_blssrm.requests.post')
    def test_1_for_service_unavailable(self, mock_generate_token, mock_post_request):
        mock_response_1 = MagicMock()
        mock_response_1.status_code = 503
        mock_response_1.content.decode.return_value = {"success": "false", "errorCode": 0, "errorMessage":
            ["Service Unavailable"],
                                                       "requestId": "823pp13135163aa22eef83bb3214ae"}
        mock_post_request.post.return_value = mock_response_1

        mock_response_2 = MagicMock()
        mock_response_2.bearer_token = "erel99asaKK"
        mock_generate_token.generate_token.return_value = mock_response_2

        json_data = {"ERPSummarys": [{"serialNumber": "Auto_11", "installedDate": "2018-07-07"}]}

        with pytest.raises(Exception) as excinfo:
            blssrm.post_json(json_data)
        assert excinfo.type == Exception

    @patch('src.class_blssrm')
    @patch('src.class_blssrm.requests.post')
    def test_1_for_unknown_status_code(self, mock_generate_token, mock_post_request):
        successful_response = {"success": "false", "errorCode": 0, "errorMessage":
            ["Unknown status code"],
                               "requestId": "222qq36985274bb22kkd83bb3214ae"}
        mock_post_request.return_value = mock.Mock(name="mock response for post method",
                                                   **{"status_code": '3XX', "content.decode.return_value":
                                                       successful_response})

        mock_response = MagicMock()
        mock_response.bearer_token = "erel99asaKK"
        mock_generate_token.generate_token.return_value = mock_response

        json_data = {"ERPSummarys": [{"SerialNumber": "Auto_11",
                                      "BatteryDateCode": "2020-02-02",
                                      "BatteryInstallDate": "2021-04-04"
                                      }]}

        with pytest.raises(Exception) as excinfo:
            blssrm.post_json(json_data)
        assert excinfo.type == Exception


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
            flag = blssrm.identify_updates(df_blssrm, df_output_ilead)
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
            flag = blssrm.identify_updates(df_blssrm, df_output_ilead)
        assert excinfo.type == TypeError

    @pytest.mark.parametrize(
        "df_blssrm, df_output_ilead",
        [(pd.DataFrame(data={"A": 10}, index=[0]), pd.DataFrame()),
         (pd.DataFrame(), pd.DataFrame(data={"A": 10}, index=[0])),
         (pd.DataFrame(), pd.DataFrame())
         ])
    def test_1_valueerror(self, df_blssrm, df_output_ilead):
        with pytest.raises(Exception) as excinfo:
            flag = blssrm.identify_updates(df_blssrm, df_output_ilead)
        assert excinfo.type == ValueError

    def test_identify_updates_no_updates(self):
        first_df = pd.read_csv(
            r"C:\Users\E0778583\OneDrive - Eaton\Documents\predictpulse\blssrm\test\test_case1_old.csv",
            low_memory=False)
        second_df = pd.read_csv(
            r"C:\Users\E0778583\OneDrive - Eaton\Documents\predictpulse\blssrm\test\test_case1_new.csv",
            low_memory=False)
        temp_df = blssrm.identify_updates(first_df, second_df)
        assert temp_df.empty and temp_df.shape == (0, 0)

    def test_identify_updates_for_all_columns(self):
        first_df = pd.read_csv(
            r"C:\Users\E0778583\OneDrive - Eaton\Documents\predictpulse\blssrm\test\test_case3_old.csv",
            low_memory=False)
        second_df = pd.read_csv(
            r"C:\Users\E0778583\OneDrive - Eaton\Documents\predictpulse\blssrm\test\test_case3_new.csv",
            low_memory=False)
        temp_df = blssrm.identify_updates(first_df, second_df)
        assert not temp_df.empty and temp_df.shape == second_df.shape

    def test_identify_updates_nan_for_all_columns(self):
        first_df = pd.read_csv(
            r"C:\Users\E0778583\OneDrive - Eaton\Documents\predictpulse\blssrm\test\test_case4_old.csv",
            low_memory=False)
        second_df = pd.read_csv(
            r"C:\Users\E0778583\OneDrive - Eaton\Documents\predictpulse\blssrm\test\test_case4_new.csv",
            low_memory=False)
        temp_df = blssrm.identify_updates(first_df, second_df)
        assert temp_df.empty and temp_df.shape == (0, 0)

    def test_identify_updates_for_only_new_rows(self):
        first_df = pd.read_csv(
            r"C:\Users\E0778583\OneDrive - Eaton\Documents\predictpulse\blssrm\test\test_case2_old.csv",
            low_memory=False)
        second_df = pd.read_csv(
            r"C:\Users\E0778583\OneDrive - Eaton\Documents\predictpulse\blssrm\test\test_case2_new.csv",
            low_memory=False)
        temp_df = blssrm.identify_updates(first_df, second_df)
        assert not temp_df.empty and temp_df.shape == (2, 18)

    def test_identify_updates_for_old_and_new_rows(self):
        first_df = pd.read_csv(
            r"C:\Users\E0778583\OneDrive - Eaton\Documents\predictpulse\blssrm\test\test_case5_old_2.csv",
            low_memory=False)
        second_df = pd.read_csv(
            r"C:\Users\E0778583\OneDrive - Eaton\Documents\predictpulse\blssrm\test\test_case5_new_2.csv",
            low_memory=False)
        temp_df = blssrm.identify_updates(first_df, second_df)
        assert not temp_df.empty and temp_df.shape == (5, 18)
