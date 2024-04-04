# -*- coding: utf-8 -*-
"""
Created on Wed Sep 28 13:52:52 2022

@author: E0642602
"""
# %% Setting Environment
import os
from pathlib import Path
import sys
sys.path.append(str(Path(__file__).parent.parent))

import pytest
# import sys
from src.class_identify_updates import IdentifyUpdates
import src.config as config

import pandas as pd
import numpy as np
id_ups = IdentifyUpdates()

#id_ups.check_updates_in_date_cols(erp, ileads)
# %%

# !pytest ./test/test_identify_ups_updates.py
# !pytest --cov=.\src --cov-report html:.\coverage\ .\test\

#%%
def test_1to5():
    erp_path = os.path.join(config.DIR_TEST, "test_erp.csv")
    ileads_path = os.path.join(config.DIR_TEST, "test_ilead_1.csv")
    out_path = os.path.join(config.DIR_TEST, "test_output.csv")
    erp = pd.read_csv(erp_path)
    ileads = pd.read_csv(ileads_path)

    out_exp = pd.read_csv(out_path, index_col=(
        "SerialNumber"), keep_default_na=False,  infer_datetime_format=False)
    out_exp = out_exp.sort_index(ascending=False)

    out_act = id_ups.update_values(erp, ileads)

    pd.testing.assert_frame_equal(out_exp[out_act.columns], out_act)

def test_7_cfg_format():
    dict_map = config.DICT_MAP
    format_list = [(dict_map[key][0]) for key in dict_map]
    for val in format_list:
        assert val != ""


def test_8_typeerror():
    erp = "a"
    ileads = pd.DataFrame()

    with pytest.raises(Exception) as excinfo:
        id_ups.update_values(erp, ileads)
    assert excinfo.type == TypeError


def test_9_typeerror():
    erp = pd.DataFrame()
    ileads = "b"

    with pytest.raises(Exception) as excinfo:
        id_ups.update_values(erp, ileads)
    assert excinfo.type == TypeError


def test_10_exp_cols():
    erp_path = os.path.join(config.DIR_TEST, "test_erp.csv")
    erp = pd.read_csv(erp_path)
    ileads_path = os.path.join(config.DIR_TEST, "test_ilead_1.csv")
    ileads = pd.read_csv(ileads_path)

    erp = erp.drop("InstalledDate", axis= 1)
    # ileads = ileads
    with pytest.raises(ValueError) as excinfo:
        id_ups.update_values(erp, ileads)
    assert excinfo.type == ValueError

#%%
