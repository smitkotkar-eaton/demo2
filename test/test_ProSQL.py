# -*- coding: utf-8 -*-
"""
Created on Mon Oct 10 12:35:10 2022

@author: E9780837
"""

# %% Set Environment

import pandas as pd
import pytest
from mock import Mock, sentinel, patch, MagicMock
import requests
from pandas.testing import assert_frame_equal

from pathlib import Path
import sys
sys.path.append(str(Path(__file__).parent.parent))

import src.config as CONF


# initializing class
if CONF.ENV == 'local':
    dict_const = CONF.sql_dict_const
else:
    adls_config = CONF.ADLS_CONF
    from utils.io_adopter.class_adlsFunc import adlsFunc
    _adls = adlsFunc()

    connection_string = _adls.read_credentials()["adlsconnectionstring"]
    dict_const = _adls.read_credentials(
        ["sql-pass", "sql-user", "sql-url", "sql-database1", "sql-db"])

from src.class_ProSQL import ProSQL
_sql = ProSQL(dict_const)


#%%

def test_read_1_no_exists():
    act_out = _sql.read_sql_table('random')
    exp_out = pd.DataFrame()
    assert_frame_equal(act_out, exp_out)

def test_read_2_exists():
    act_out = _sql.read_sql_table(CONF.SQL_CONF['table_name'])
    assert isinstance(act_out, pd.DataFrame)


# %% conduct testing

# !pytest ./test/test_ProSQL.py
# !pytest --cov=.\src --cov-report html:.\coverage\ .\test\
# !pytest --cov=.\src\class_ProSQL.py --cov-report html:.\coverage\ .\test\
