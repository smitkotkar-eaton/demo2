import json
import os
import logging
import pandas as pd
import pytest
import libraries.custom_logger as cl
from libraries.test_status import TestStatus
from utils.dcpd.class_generate_contacts import Contacts
ts = TestStatus()


@pytest.fixture
def _config():
    config_dir = os.path.join(os.path.dirname(__file__), "")
    config_file = os.path.join(config_dir, "config_test_dcpd.json")
    with open(config_file, "r") as config_file:
        json_config = json.load(config_file)
    return config_file['Contacts']


@pytest.fixture
def data_contacts(_config):
    path = _config['fp_contacts_dcpd']
    data = pd.read_csv(path, low_memory=False)
    data = data[list(_config['dict_col_mapping'].values())]
    print(data.shape)
    return data


class TestContacts():
    log = cl.customLogger(logging.DEBUG)

    @pytest.fixture(autouse=True)
    def setup_method_fixture(self, _config, data_contacts):
        self._config = _config
        self.data_contacts = data_contacts

    def test_trigger_generate_contacts(self):
        obj = Contacts()
        obj.generate_contacts()

    def test_time_size_rowcount_contacts(self, setup_method_fixture):
        """ Standard testing @ File level """

        from libraries.test_suit_file import TestFileSuit
        tfs = TestFileSuit(self._config['fp_contacts_dcpd'])
        ls_collected_out = tfs.main_test()
        assert all(ls_collected_out)

    def test_column_contacts(self, setup_method_fixture):
        expected_columns = self._config['expected_columns']
        # current data (Read + Process)
        install_path = self._config['fp_contacts_dcpd']
        data_install = pd.read_csv(install_path, nrows=2)
        actual_columns = data_install.columns
        self.log.info("Actual Columns in the generated report are :")
        self.log.info(actual_columns)
        self.log.info("Actual Columns in the generated report are :")
        self.log.info(actual_columns)
        missing_col = set(list(actual_columns)) - set(list(expected_columns))
        self.log.info("Below contacts column are missing :")
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

    def test_name_email_company_phone(self, setup_method_fixture, data_contacts):
        blank_name = self._config['dict_col_mapping']['Name']
        blank_email = self._config['dict_col_mapping']['email']
        blank_phone = self._config['dict_col_mapping']['company_phone']
        name = data_contacts[blank_name].apply(lambda x: 1 if pd.isnull(x) or x == '' else 0).any()
        email = data_contacts[blank_email].apply(lambda y: 1 if pd.isnull(y) or y == '' else 0).any()
        company_phone = data_contacts[blank_phone].apply(lambda z: 1 if pd.isnull(z) or z == '' else 0).any()
        if name == 0 and email == 0 and company_phone == 0:
            blank_count = 1
        elif name == 0 or email == 0 or company_phone == 0:
            blank_count = 1
        else:
            blank_count = 0
        did_pass = blank_count
        msg = "Name, email, and company_phone columns are blank" if did_pass else "No columns are blank"
        ts.mark(did_pass, msg)
        assert did_pass

    def test_len_company_phone(self, setup_method_fixture, data_contacts):
        company_phone = self._config['dict_col_mapping']['company_phone']
        blank_cphone = data_contacts[company_phone]
        check_phone = blank_cphone.apply(lambda x: (1 if len(str(x)) == 14 else 0)).all()
        did_pass = check_phone
        msg = ("length of phone is valid"
               if did_pass else
               "length of phone is not valid")
        ts.mark(did_pass, msg)
        assert did_pass

    def test_country_name(self, setup_method_fixture, data_contacts):
        country_name = self._config['dict_col_mapping']['country']
        blank_country = data_contacts[country_name].isnull()
        self.log.info("No any country mentioned : ")
        self.log.info(blank_country)
        if country_name in ('Us', 'U.S.', 'Usa', 'United Sta', '/Us'):
            cont_us = 1
        else:
            cont_us = 0
        did_pass = cont_us
        msg = ("country name is USA"
               if did_pass else
               "country name is not USA")
        ts.mark(did_pass, msg)
        assert did_pass

    def test_duplicate_serial_no(self, setup_method_fixture, data_contacts):
        sno_column = self._config['dict_col_mapping']['serial_Number']
        sno = data_contacts[sno_column]
        self.log.info("Serial number : ")
        self.log.info(sno)
        source_column = self._config['dict_col_mapping']['source']
        source_data = data_contacts[source_column]
        self.log.info('Source Data : ')
        self.log.info(source_data)
        df = pd.DataFrame({'Serialno.': sno, 'Source': source_data})
        sets_by_column2 = df.groupby('Source')['Serialno.'].apply(set)
        common_elements = set()
        for i in range(len(sets_by_column2)):
            for j in range(i + 1, len(sets_by_column2)):
                common_elements.update(sets_by_column2.iloc[i].intersection(sets_by_column2.iloc[j]))
        if common_elements:
            self.log.info('Duplicate serial numbers : ')
            self.log.info(common_elements)
        else:
            self.log.info("no common serial number")
        flag = 0 if common_elements else 1
        did_pass = flag
        msg = ("No duplicate serial number with respect to all categories() of source "
               if did_pass else
               "Have duplicate serial number in atleast two categories of source")
        ts.mark(did_pass, msg)
        assert did_pass
