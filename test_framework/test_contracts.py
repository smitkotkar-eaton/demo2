from datetime import datetime as dt
import pandas as pd
import numpy as np
import pytest
import json
import logging
import libraries.custom_logger as cl
from libraries.test_status import TestStatus
from utils.dcpd.class_contracts_data import Contract
ts = TestStatus()

@pytest.fixture
def _config():
    file_path = "C:\\ileads\\codes\\ileads_lead_generation\\test_framework\\config_test_dcpd.json"
    with open(file_path, "r") as jsonfile:
        json_config = json.load(jsonfile)
    return json_config['Contract_dcpd']


@pytest.fixture
def data_contracts(_config):
    path = _config['fp_contract_dcpd']
    data = pd.read_csv(path, low_memory=False)
    data = data[list(_config['dict_col_mapping'].values())]
    print(data.shape)
    return data



@pytest.fixture
def _config_installbase():
    file_path = "C:\\ileads\\ileads_lead_generation\\test_framework\\config_test_dcpd.json"
    with open(file_path, "r") as jsonfile:
        json_config = json.load(jsonfile)
    return json_config['Install_Base_Dcpd']


@pytest.fixture
def data_install(_config_installbase):
    path = _config_installbase['fp_installbase_dcpd']
    data = pd.read_csv(path, low_memory=False)
    data = data[list(_config_installbase['dict_col_mapping'].values())]
    print(data.shape)
    return data


class TestContracts():
    log = cl.customLogger(logging.DEBUG)

    @pytest.fixture(autouse=True)
    def setup_method_fixture(self, _config, _config_installbase, data_contracts, data_install):
        self._config = _config
        self._config_installbase = _config_installbase
        self.data_contracts = data_contracts
        self.data_install = data_install
    #
    def test_trigger_generate_contracts(self):
        obj = Contract()
        obj.main_contracts()


    def test_time_size_rowcount_contacts(self, setup_method_fixture):
        """ Standard testing @ File level """

        from libraries.test_suit_file import TestFileSuit
        tfs = TestFileSuit(self._config['fp_contract_dcpd'])
        ls_collected_out = tfs.main_test()
        assert all(ls_collected_out)


    def test_column_contacts(self, setup_method_fixture):
        expected_columns = self._config['expected_columns']
        # current data (Read + Process)
        install_path = self._config['fp_contract_dcpd']
        data_install = pd.read_csv(install_path, nrows=2)
        actual_columns = data_install.columns
        self.log.info("Actual Columns in the generated report are :")
        self.log.info(actual_columns)
        self.log.info("Actual Columns in the generated report are :")
        self.log.info(actual_columns)
        missing_col = set(list(actual_columns)) - set(list(expected_columns))
        self.log.info("Below contracts columns are missing :")
        self.log.info(missing_col)
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

    def test_future_contracts(self, setup_method_fixture, data_contracts):
        Contract_date_column = self._config['dict_col_mapping']['Contract_Date']
        Cnot_date = data_contracts[Contract_date_column]
        self.log.info("Contract_start_date : ")
        self.log.info(Cnot_date)
        df = pd.DataFrame({'Contract_date': Cnot_date})
        # Convert 'Contract_start_date' to datetime format
        df['Contract_date'] = pd.to_datetime(df['Contract_date'])
        future_contracts=df[df['Contract_date'] > dt.now()]
        # self.log.info("total future_contracts : ")
        # self.log.info(len(future_contracts))
        print(len(future_contracts),"total future_contracts")
        if not future_contracts.empty:
            f_cont = 0
        else:
            f_cont = 1

        did_pass = f_cont
        msg = ("No future_contracts present"
               if did_pass else
               "future_contracts exist")
        ts.mark(did_pass, msg)
        assert did_pass


    def test_serialno_instalbase(self,setup_method_fixture,data_contracts,data_install):
        Sno_contract = self._config['dict_col_mapping']['SrNum']
        Sno_install= self._config_installbase['dict_col_mapping']['SrNum']
        Sno_contracts = data_contracts[Sno_contract]
        Sno_instalbase = data_install[Sno_install]
        check_Sno = Sno_contracts.isin(Sno_instalbase).all()
        uncommon_elements = [element for element in Sno_contracts if element not in Sno_instalbase]
        df = pd.DataFrame({'cont': Sno_contracts, 'install': Sno_instalbase})
        missing_col = set(list(Sno_contracts)) - set(list(Sno_instalbase))
        print("uncommon serialno.", missing_col)
        #uncommon_elements_column1 = df[~df['cont'].isin(df['install'])]['cont']
        #uncommon_elements_column1 = df[~df['cont'].isin(df['install'])]['cont']
        #print("Elements in column1 but not in column2:", uncommon_elements_column1.tolist())
        self.log.info('uncommon serial no. serial numbers :')
        self.log.info(missing_col)
        #uncommon_elements_column1 = df[~df['column1'].isin(df['column2'])]['column1']
        if check_Sno:
            flag = 1
        else:
            flag = 0
        did_pass = flag
        msg = ("serial numbers present in contract are in install base"
               if did_pass else
               "serial numbers present in contract are not in install base")
        ts.mark(did_pass, msg)
        assert did_pass

    def test_duplicate_serial_number(self, setup_method_fixture, data_install):
        srnum = self._config['dict_col_mapping']['SrNum']
        Sno_dup = self.data_install[srnum]
        df = pd.DataFrame({'Serialno': Sno_dup})
        duplicates = df[df.duplicated('Serialno')]['Serialno'].unique()
        self.log.info('Duplicate serial numbers :')
        self.log.info(duplicates)
        count_install = len(self.data_install[srnum]) - len(self.data_install[srnum].drop_duplicates())
        did_pass = count_install == 0
        msg = ("duplicate serial numbers Not present in contract report"
               if did_pass else
               "duplicate serial numbers are present in contract report")
        ts.mark(did_pass, msg)
        assert did_pass






