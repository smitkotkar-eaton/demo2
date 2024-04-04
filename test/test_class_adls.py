# -*- coding: utf-8 -*-
"""
Created on Mon Oct 10 12:35:10 2022

@author: E9780837
"""

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
from utils.io_adopter.class_adlsFunc import adlsFunc

adls = adlsFunc()

# %% conduct testing

# !pytest ./test/test_class_adls.py
# !pytest --cov=.\src --cov-report html:.\coverage\ .\test\


# %% Test Json

@patch('src.class_adlsFunc.ClientSecretCredential')
@patch('src.class_adlsFunc.SecretClient')
@patch("src.class_adlsFunc")

def test_update_1_normal(mocker_sc, mocker_id, mocker_secrets):

    mm_secrets = MagicMock()

    mm_secrets.secret_client.secrets.return_value = 'key1'
    #mocker_sc = mm_secrets

    mocker_id = ""
    mocker_secrets = ""
    ls_keys = ['sql-user']
    act_out = adls.read_credentials(ls_keys)
    print(act_out)
    assert len(act_out.keys()) == len(ls_keys)

#%%
