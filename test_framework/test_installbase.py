import datetime
import pandas as pd
import pytest
import json
import logging
import libraries.custom_logger as cl
from libraries.test_status import TestStatus
from ileads_lead_generation.utils.dcpd.class_installbase import InstallBase

ts = TestStatus()


@pytest.fixture
def _config():
    file_path = "C:\\ileads\\codes\\ileads_lead_generation\\test_framework\\config_test_dcpd.json"
    with open(file_path, "r") as jsonfile:
        json_config = json.load(jsonfile)
    return json_config['Install_Base_Dcpd']


@pytest.fixture
def data_install(_config):
    path = _config['fp_installbase_dcpd']

    data = pd.read_csv(path, low_memory=False)
    data = data[list(_config['dict_col_mapping'].values())]
    print(data.shape)
    return data


@pytest.fixture
def _config_output_ref():
    file_path = "C:\\ileads\\ileads_lead_generation\\test_framework\\config_test_dcpd.json"
    with open(file_path, "r") as jsonfile:
        json_config = json.load(jsonfile)
    return json_config['Output_Ref_Install_Data']


@pytest.fixture
def output_ref_install(_config_output_ref):
    path = _config_output_ref['fp_output_ref_install_dcpd']

    data = pd.read_csv(path, low_memory=False)
    data = data[list(_config_output_ref['dict_col_mappings'].values())]
    print(data.shape)
    return data


class TestInstallBase():
    log = cl.customLogger(logging.DEBUG)

    @pytest.fixture(autouse=True)
    def setup_method_fixture(self, _config, data_install, _config_output_ref, output_ref_install):
        self._config = _config
        self.data_install = data_install
        self._config_output_ref = _config_output_ref
        self.output_ref_install = output_ref_install

    def test_trigger_installbase(self):

        obj = InstallBase()
        obj.main_install()


    def test_time_size_rowcount_installlbase(self, setup_method_fixture):
        """ Standard testing @ File level """

        from libraries.test_suit_file import TestFileSuit
        tfs = TestFileSuit(self._config['fp_installbase_dcpd'])
        ls_collected_out = tfs.main_test()
        assert all(ls_collected_out)

    def test_column_installbase(self, setup_method_fixture):
        expected_columns = self._config['expected_columns']
        # current data (Read + Process)
        install_path = self._config['fp_installbase_dcpd']
        data_install = pd.read_csv(install_path, nrows=2)
        actual_columns = data_install.columns
        self.log.info("Actual Columns in the generated report are :")
        self.log.info(actual_columns)
        missing_col = set(list(actual_columns))-set(list(expected_columns))
        self.log.info("Below installbase column are missing :")
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

    def test_duplicate_serial_number(self, setup_method_fixture, data_install):
        srnum = self._config['dict_col_mapping']['SrNum']
        Sno_dup = self.data_install[srnum]
        df = pd.DataFrame({'Serialno': Sno_dup})
        duplicates = df[df.duplicated('Serialno')]['Serialno'].unique()
        # with open("C:\\ileads\\ileads_lead_generation\\results\\Duplicate_serialno_installbase.txt",
        #           "w") as file:
        #     file.write(str( duplicates))
        self.log.info('Duplicate serial numbers :')
        self.log.info(duplicates)
        #print("Duplicate elements in the 'Column':", duplicates)
        count_install = len(self.data_install[srnum]) - len(self.data_install[srnum].drop_duplicates())
        did_pass = count_install == 0
        msg = ("duplicate serial numbers Not present in install base report"
               if did_pass else
               "duplicate serial numbers are present in Install report")
        ts.mark(did_pass, msg)
        assert did_pass


    def test_product_type(self, setup_method_fixture, data_install):
        prcol = self._config['dict_col_mapping']['Product_Type']
        product_type = self.data_install[prcol].unique()
        self.log.info("Unique values of product type in the generated report are ")
        self.log.info(product_type)
        if 'PDU - Secondary' or 'PDU - Primary' in product_type:
            # result5 = len(product_type) <= 5
            did_pass = len(product_type) <= 5
            msg = ("less than 5 product types present in product_M2M columns in install base"
                   if did_pass else
                   "more than 5 product types present in product_M2M columns in install base")
            ts.mark(did_pass, msg)
            assert did_pass

        else:
            did_pass = len(product_type) <= 3
            msg = ("less than 3 product types present in product_M2M columns in install base"
                   if did_pass else
                   "more than 3 product types present in product_M2M columns in install base")
            ts.mark(did_pass, msg)
            assert did_pass

    def test_country_name(self, setup_method_fixture, data_install):
        ctrcol = self._config['dict_col_mapping']['Country']
        country_type = self.data_install[ctrcol].apply(
            lambda x: (1 if x == 'united states of america' or x == 'united states' else 0))
        total_sum = self.data_install[ctrcol].isnull().sum()
        self.log.info("Count of empty row in country columns is ")
        self.log.info(total_sum)
        total = len(country_type)
        did_pass = total == country_type.value_counts()[0] and total_sum == 0
        msg = ("united states or united states of america  country name is present"
               if did_pass else
               "other country names or blank value is present in country column")
        ts.mark(did_pass, msg)
        assert did_pass

    def test_strategic_customer(self, setup_method_fixture, data_install):
        strcol = self._config['dict_col_mapping']['Strategic_Customer']
        total_sum = self.data_install[strcol].isnull().sum()
        self.log.info("Count of blank values in strategy column is : ")
        self.log.info(total_sum)
        did_pass = total_sum == 0
        msg = ("strategic customer column does not contains any blank values in install base"
               if did_pass else
               "strategic customer column contains blank values in install base")
        ts.mark(did_pass, msg)
        assert did_pass

    def test_install_date(self, setup_method_fixture, output_ref_install):
        # install date present in output_ref_install file
        in_date = self._config_output_ref['dict_col_mappings']['InstallDate']
        today_date = datetime.date.today()
        df1 = output_ref_install[in_date].apply(lambda x: (1 if pd.to_datetime(x) > pd.to_datetime(today_date) else 0))
        total_count = len(df1)
        self.log.info("Total no. of records in this file :")
        self.log.info(total_count)
        self.log.info("Count of records having date lesser than today :")
        self.log.info(df1.value_counts()[0])
        did_pass = total_count == df1.value_counts()[0]
        msg = ("future date is not present in install base date column"
               if did_pass else
               "future date is present in install base date column")
        ts.mark(did_pass, msg)
        assert did_pass

    def test_serial_number_count(self, setup_method_fixture, data_install):
        srcol = self._config['dict_col_mapping']['SrNum']
        total_count = len(self.data_install[srcol])
        self.log.info("Total no. of serial numbers in this file :")
        self.log.info(total_count)
        did_pass = total_count >= 56309
        msg = ("serial number count is greater"
               if did_pass else
               "serial number count is less")
        ts.mark(did_pass, msg)
        assert did_pass
