"""@file DCPD.py



@brief


@details


@copyright 2021 Eaton Corporation. All Rights Reserved.
@note Eaton Corporation claims proprietary rights to the material disclosed
here on. This technical information may not be reproduced or used without
direct written permission from Eaton Corporation.
"""
import pandas as pd
import pytest
import json

from datetime import datetime, timedelta
from libraries.test_status import TestStatus
ts = TestStatus()

@pytest.fixture
def _config():
    file_path = '/test_framework/config_test_cpdi.json'
    with open(file_path, "r") as jsonfile:
        json_config = json.load(jsonfile)
    return json_config['Contracts']


@pytest.fixture
def data_cr(_config):
    path = _config['fp_cur_release']

    cn_srnum = _config['dict_col_mapping']['SrNum']
    cn_wc = _config['dict_col_mapping']['Warranty_Conversion']

    data = pd.read_csv(path, low_memory=False)
    data = data[list(_config['dict_col_mapping'].values())]
    data = data.sort_values(
        [cn_srnum, cn_wc]).drop_duplicates(subset=[cn_srnum])
    print(data.shape)
    return data


@pytest.fixture
def data_pr(_config): # data_prev
    path = _config['fp_prev_release']
    if path != "":
        cn_srnum = _config['dict_col_mapping']['SrNum']
        cn_wc = _config['dict_col_mapping']['Warranty_Conversion']

        data = pd.read_csv(path, low_memory=False)
        data = data[list(
            _config['dict_col_mapping'].values())]
        print(data)
        data = data.sort_values(
            [cn_srnum, cn_wc]).drop_duplicates(subset=[cn_srnum])
        print(data)
    else:
        data = pd.DataFrame()
    print(data.shape)
    return data


class TestSuitContract():

    @pytest.fixture(autouse=True)
    def setup_method_fixture(self, _config, data_cr, data_pr):
        self._config = _config
        self.data_cr = data_cr
        self.data_pr = data_pr

    def test_file(self):
        """ Standard testing @ File level """
        from libraries.test_suit_file import TestFileSuit

        tfs = TestFileSuit(self._config['fp_cur_release'])
        ls_collected_out = tfs.main_test()
        assert all(ls_collected_out)

    def test_serial_no_less_output_ilead(self):
        """
        # Serial Number for current release MUST be+ greater than equal to
        previous release
        """
        nc_srnum = self._config['dict_col_mapping']['SrNum']

        # Current Release
        count_cr = self.data_cr[nc_srnum].nunique()

        # Previous Release
        count_pr = self.data_pr[nc_srnum].nunique()

        # Compare
        did_pass = (count_cr >= count_pr)
        msg = (
            "# Serial Number for current release >= previous release "
            if did_pass else
            "# Serial Number for current release < previous release ")
        ts.mark(did_pass, msg)
        assert did_pass
    """
    def test_serial_no_match(self):
        '''
        All Serial Numbers from previous release MUST be present in
        current release.
        '''
        n_col = self._config['dict_col_mapping']['SrNum']
        serial_numbers_in_new = self.data_pr[n_col].isin(self.data_cr[n_col])

        # Compare
        did_pass = all(serial_numbers_in_new)
        # did_pass = serial_numbers_in_new
        # print("did_pass :",did_pass == False)

        sr_missing =self.data_pr.loc[serial_numbers_in_new== False, n_col]
        msg = (
            f"All Serial Numbers from previous release are in current release ,prev: {self.data_pr[n_col].nunique()}, 
            cur: {self.data_cr[n_col].nunique()}"
            if did_pass else
            f"One or more Serial Number from previous release is not in current release  {','.join(sr_missing)}"
        )
        ts.mark(did_pass, msg)
        assert did_pass

    """
    def test_warranty_end_date(self):
        nc_srnum = self._config['dict_col_mapping']['SrNum']
        nc_date = self._config['dict_col_mapping']['Warranty_End_Date']

        # Current Release
        data_cr = self.data_cr.loc[:, [nc_srnum, nc_date]]
        data_cr[nc_date] = pd.to_datetime(data_cr[nc_date], errors="coerce")
        data_cr = data_cr.rename(columns={nc_date: "new_date"})

        # Previous Release
        data_pr = self.data_pr.loc[:, [nc_srnum, nc_date]]
        data_pr[nc_date] = pd.to_datetime(data_pr[nc_date], errors="coerce")
        data_pr = data_pr.rename(columns={nc_date: "old_date"})

        # Compare
        df_compare = data_pr.merge(data_cr, on=nc_srnum, how='inner')
        del data_cr, data_pr
        df_compare['result_date'] = df_compare.apply(
            lambda x: 1 if (pd.isna(x['old_date']) or pd.isna(x['new_date']))
            else x['old_date'] <= x['new_date'], axis=1)

        did_pass = all(df_compare['result_date'])
        msg = (
            f"{nc_date} for current release >= new release"
            if did_pass else
            f"{nc_date} for current release < new release")
        ts.mark(did_pass, msg)
        assert did_pass

    def test_contract_header_end_date(self):
        nc_srnum = self._config['dict_col_mapping']['SrNum']
        nc_date = self._config['dict_col_mapping']['Contract_End_Date']

        # Current Release
        data_cr = self.data_cr.loc[:, [nc_srnum, nc_date]]
        data_cr[nc_date] = pd.to_datetime(data_cr[nc_date], errors="coerce")
        data_cr = data_cr.rename(columns={nc_date: "new_date"})

        # Previous Release
        data_pr = self.data_pr.loc[:, [nc_srnum, nc_date]]
        data_pr[nc_date] = pd.to_datetime(data_pr[nc_date], errors="coerce")
        data_pr = data_pr.rename(columns={nc_date: "old_date"})

        # Compare
        df_compare = data_pr.merge(data_cr, on=nc_srnum, how='inner')
        del data_cr, data_pr
        df_compare['result_date'] = df_compare.apply(
            lambda x: 1 if (pd.isna(x['old_date']) or pd.isna(x['new_date']))
            else x['old_date'] <= x['new_date'], axis=1)

        did_pass = all(df_compare['result_date'])
        msg = (
            f"{nc_date} for current release >= new release"
            if did_pass else
            f"{nc_date} for current release < new release")
        ts.mark(did_pass, msg)
        assert did_pass

    def test_contract_conversion_date(self):
        nc_srnum = self._config['dict_col_mapping']['SrNum']
        nc_wc = self._config['dict_col_mapping']['Warranty_Conversion']

        # Current Release
        data_cr = self.data_cr.loc[:, [nc_srnum, nc_wc]]
        data_cr = data_cr[(data_cr[nc_wc] == 'Warranty Conversion')]

        # Previous Release
        data_pr = self.data_pr.loc[:, [nc_srnum, nc_wc]]
        data_pr = data_pr[(data_pr[nc_wc] == 'Warranty Conversion')]

        # Compare
        did_pass = data_pr[nc_srnum].isin(list(data_cr[nc_srnum]))
        # print("first did pass", did_pass== False)
        ls_missing = data_pr.loc[did_pass == False, nc_srnum]
        did_pass = all(did_pass)

        msg = (
            f"All serialnumber with {nc_wc} in previous release are "
            "identified in new release"
            if did_pass else
            f"One or more serialnumber with {nc_wc} in previous release is "
            f"not tagged as {nc_wc} in new release {','.join(ls_missing)}"
        )
        ts.mark(did_pass, msg)

        assert did_pass

    def test_latest_contract_date(self):
        nc_lcontract = self._config['dict_col_mapping']['Latest_contract_date']
        latest_date = pd.to_datetime(self.data_cr.loc[:, nc_lcontract])

        # check
        did_pass = any((datetime.now() - contract_date).days <= 30 for contract_date in latest_date)

        msg = (f"Contract date is present within 30 days "
            if did_pass else
               "No contract date is within the last 30 days.")
        ts.mark(did_pass, msg)
        assert did_pass

#TODO skip if comment add
# %%

# pytest test_framework\\test_contracts_output_ilead.py --continue-on-collection-errors
# pytest test_framework\\test_contracts_output_ilead.py --continue-on-collection-errors  --html=ileadContractOutputIlead.html

# %%
