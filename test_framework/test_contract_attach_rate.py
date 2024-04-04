import pandas as pd
import pytest
import json


from libraries.test_status import TestStatus
ts = TestStatus()

@pytest.fixture
def _config():
    file_path = 'test_framework\\config_test_cpdi.json'
    with open(file_path, "r") as jsonfile:
        json_config = json.load(jsonfile)
    return json_config['Contracts_attach_rate']


@pytest.fixture
def data_cr(_config):
    path = _config['fp_cur_release']

    data = pd.read_csv(path, low_memory=False)
    data = data[list(_config['dict_col_mapping'].values())]

    print(data.shape)
    return data


@pytest.fixture
def data_pr(_config):
    path = _config['fp_prev_release']
    if path != "":
        data = pd.read_csv(path, low_memory=False)
        data = data[list(_config['dict_col_mapping'].values())]
        print(data)
    else:
        data = pd.DataFrame()
    print(data.shape)
    return data


class TestCpdiContractAttachRate():

    @pytest.fixture(autouse=True)
    def setup_method_fixture(self, _config, data_cr, data_pr):
        self._config = _config
        self.data_cr = data_cr
        self.data_pr = data_pr

        # Identify last report date
        cur_year = int(pd.Timestamp.today().strftime('%Y'))

        check_date = pd.DataFrame(data={"LastReport": [
            pd.to_datetime(f"2-15-{cur_year - 1}"),
            pd.to_datetime(f"7-15-{cur_year - 1}"),
            pd.to_datetime(f"2-15-{cur_year}"),
            pd.to_datetime(f"7-15-{cur_year}"),
            pd.to_datetime(f"2-15-{cur_year + 1}")]})
        check_date['time_delta'] = (check_date['LastReport'] - pd.Timestamp.today()).dt.days
        check_date = check_date[check_date['time_delta'] < 0]

        check_date = check_date.loc[check_date['time_delta'] == max(check_date['time_delta']), :].reset_index()

        self.check_date = check_date.LastReport[0]

    def test_file(self):
        """ Standard testing @ File level """
        from libraries.test_suit_file import TestFileSuit

        tfs = TestFileSuit(self._config['fp_cur_release'])
        ls_collected_out = tfs.main_test()
        assert all(ls_collected_out)

    def test_required_contract_attach_rate(self):
        expected_columns = self._config['expected_columns']

        # Current data (Read + Proces)
        cur_release_path = self._config['fp_cur_release']
        data_cur_release = pd.read_csv(cur_release_path, nrows=2)
        actual_columns = data_cur_release.columns

        # compare
        did_pass_count = len(expected_columns) == len(actual_columns)

        flag_available = [(col in actual_columns) for col in expected_columns]
        did_pass = all(flag_available)
        msg = ("Expected column is  matching with actual column"
               if did_pass else
               "Expected column is Not matching with actual column")
        ts.mark(did_pass, msg)
        assert did_pass_count
        assert did_pass


    def test_serial_no_less_contract_attach_rate(self):
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


    def test_serial_no_match(self):
        """
        All Serial Numbers from previous release MUST be present in
        current release.
        """
        n_col = self._config['dict_col_mapping']['SrNum']
        serial_numbers_in_new = self.data_pr[n_col].isin(self.data_cr[n_col])

        # Compare
        did_pass = all(serial_numbers_in_new)
        msg = (
            f"All Serial Numbers from previous release are in current release ,prev: {self.data_pr[n_col].nunique()}, cur: {self.data_cr[n_col].nunique()}"
            if did_pass else
            "One or more Serial Number from previous release is not in "
            "current release")
        ts.mark(did_pass, msg)
        assert did_pass


    def test_date(self):
        nc_date = self._config['dict_col_mapping']['date']

        # Current Release
        latest_date = pd.to_datetime(self.data_cr[nc_date], errors='coerce')
        latest_date = max(latest_date)

        # Compare
        did_pass = (pd.to_datetime(self.check_date) - latest_date).days
        print(f"Delta: {did_pass}, Check Date: {self.check_date}")
        did_pass = did_pass <= 30

        msg = (f"old release date should exact match with new release Date column"
               if did_pass else
               "old release  date should not  match with new release  Date column")
        ts.mark(did_pass,msg)
        assert did_pass

    def test_latest_contract_beg_date(self):
        nc_contract_beg_date = self._config['dict_col_mapping']['contract_beg_date']
        # Current Release
        contract_beg_date = pd.to_datetime(self.data_cr[nc_contract_beg_date], errors='coerce')
        contract_beg_date = contract_beg_date[pd.notna(contract_beg_date)]
        contract_beg_date = max(contract_beg_date)

        # check july month date
        did_pass = (pd.to_datetime(self.check_date) - contract_beg_date).days
        print(f"Delta: {did_pass}, Check Date: {self.check_date}")
        did_pass = did_pass <= 30
        msg = (f"Dates found in month of July in the 'Contract_Beg' column "
               if did_pass else
               "No dates found in month of July in the 'Contract_Beg' column.")
        assert did_pass

        # Check if the specific month (July) of the current year exists in the DataFrame using assert
        ts.mark(did_pass ,"No dates found in July in the 'Contract_Beg' column.")

# pytest test_framework\\test_contract_attach_rate.py --continue-on-collection-errors
# pytest test_framework\\test_contract_attach_rate.py --continue-on-collection-errors --html=ileadContractAttachRate.html

