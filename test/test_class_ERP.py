# -*- coding: utf-8 -*-
"""
Created on Mon Sep 26 10:34:43 2022

@author: E9780837
"""

# !pytest ./test/test_class_ERP.py

# !pytest --cov=.\src --cov-report html:.\coverage\ .\test\

# %% Set Environment
import os
import pandas as pd
import pytest
from mock import Mock, sentinel, patch, MagicMock
import requests

from pathlib import Path
import sys
sys.path.append(str(Path(__file__).parent.parent))

import src.config as CONF
from src.class_ERP import ERP

erp = ERP()

# %% Test Json


class TestCreatJson:

    @pytest.mark.parametrize(
        "test_input",
        [(None),
         ('abc'),
         (123),
         ])
    def test_1_typeerror(self, test_input):
        with pytest.raises(Exception) as excinfo:
            f_proceed, json_act = erp.create_json(test_input, CONF.DICT_MAP)
        assert excinfo.type == TypeError

    def test_2_valueerror(self):
        ls_col = list(CONF.DICT_MAP.keys())
        ls_col = ls_col[:2]
        df_in = pd.DataFrame(columns=ls_col)
        df_in['SerialNumber'] = ['Auto_00', 'Auto_01']
        df_in['Model'] = ['Auto_00', 'Auto_01']
        with pytest.raises(Exception) as excinfo:
            f_proceed, json_act = erp.create_json(df_in, CONF.DICT_MAP)
        assert excinfo.type == ValueError

    def test_3_emptydata(self):
        df_in = pd.DataFrame()
        f_proceed, json_act = erp.create_json(df_in, CONF.DICT_MAP)
        assert f_proceed is False

    def test_4_noupdates(self):
        ls_cols = list(CONF.DICT_MAP.keys())
        ls_cols.append('SerialNumber')
        df_in = pd.DataFrame(columns=ls_cols)
        df_in['SerialNumber'] = ['Auto_00', 'Auto_01']
        f_proceed, json_act = erp.create_json(df_in, CONF.DICT_MAP)
        assert f_proceed is False

    def test_5_normaljson(self):
        p_data = os.path.join(CONF.DIR_TEST, 'test_json.csv')
        df_in = pd.read_csv(p_data)
        f_proceed, json_act = erp.create_json(df_in, CONF.DICT_MAP)
        assert f_proceed is True

# %%
class TestPostJson:

    @patch('src.class_ERP.requests')
    def test_1_ideal_scenario(self, mocker):

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {'results': {'values': 1663060619811}}
        mocker.post.return_value = mock_response
        json_data = {}
        status = erp.post_json(json_data, "https://unitestcase.com/", "content")
        assert len(status) == 2

    @patch('src.class_ERP.requests')
    def test_2_ideal_scenario(self, mocker):

        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_response.json.return_value = {'results': {'values': 1663060619811}}
        mocker.post.return_value = mock_response
        json_data = {}
        status = erp.post_json(json_data, "https://unitestcase.com/", "")
        assert len(status) == 2

#%%