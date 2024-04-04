import pandas as pd
import json
import logging
import libraries.custom_logger as cl
import pytest
from libraries.test_status import TestStatus
from utils.dcpd.class_services_data import ProcessServiceIncidents
ts = TestStatus()


@pytest.fixture
def _config():
    file_path = "C:\\ileads\\codes\\ileads_lead_generation\\test_framework\\config_test_dcpd.json"
    with open(file_path, "r") as jsonfile:
        json_config = json.load(jsonfile)
    return json_config['Services']


@pytest.fixture
def data_service(_config):
    path = _config['fp_services_dcpd']
    data = pd.read_csv(path, low_memory=False)
    data = data[list(_config['dict_col_mapping'].values())]
    print(data.shape)
    return data


class TestServices():
    log = cl.customLogger(logging.DEBUG)
    @pytest.fixture(autouse=True)
    def setup_method_fixture(self, _config, data_service):
        self._config = _config
        self.data_service = data_service

    def test_trigger_services(self):
        obj = ProcessServiceIncidents()
        obj.main_services()

    def test_time_size_rowcount_services(self, setup_method_fixture):
        from libraries.test_suit_file import TestFileSuit
        tfs = TestFileSuit(self._config['fp_services_dcpd'])
        ls_collected_out = tfs.main_test()
        assert all(ls_collected_out)

    def test_column_services(self, setup_method_fixture):
        expected_columns = self._config['expected_columns']
        install_path = self._config['fp_services_dcpd']
        data_install = pd.read_csv(install_path, nrows=2)
        actual_columns = data_install.columns
        self.log.info("Actual Columns in the generated report are :")
        self.log.info(actual_columns)
        self.log.info("Actual Columns in the generated report are :")
        self.log.info(actual_columns)
        missing_col = set(list(actual_columns)) - set(list(expected_columns))
        self.log.info("Below services column are missing :")
        self.log.info(missing_col)
        did_pass_count = len(expected_columns) == len(actual_columns)
        flag_available = [(col in actual_columns) for col in expected_columns]
        did_pass = all(flag_available)
        msg = ("Expected column is  matching with actual column"
               if did_pass else
               "Expected column is Not matching with actual column")
        ts.mark(did_pass, msg)
        assert did_pass_count
        assert did_pass

    def test_type_column(self, setup_method_fixture, data_service):
        type_column = self._config['dict_col_mapping']['Type']
        check_type = data_service[type_column].isin(['replace', 'upgrade']).all()
        if check_type:
            flag = 1
        else:
            flag = 0
        did_pass = flag
        msg = ("Only Replace and Upgrade are present"
               if did_pass else
               "other than  Replace and Upgrade are also present")
        ts.mark(did_pass, msg)
        assert did_pass

    def test_component_value(self, setup_method_fixture, data_service):
        comp_column = self._config['dict_col_mapping']['component']
        component_check = data_service[comp_column].unique()
        check_comp = data_service[comp_column].isin(
            ['Display', 'BCMS', 'Breaker', 'Fans', 'PCB', 'SPD', ' PCB', 'PCB ']).all()
        if check_comp:
            flag = 1
        else:
            flag = 0
        did_pass = flag
        msg = ("component column contains Display, Fans, PCB , Breaker, BCMS , SPD"
               if did_pass else
               "component column may contains some value other than Display, Fans, PCB , Breaker, BCMS , SPD")
        ts.mark(did_pass, msg)
        assert did_pass

    def test_compare_component_type(self, setup_method_fixture, data_service):
        component_column = self._config['dict_col_mapping']['component']
        component_check = data_service[component_column]
        typecolumn = self._config['dict_col_mapping']['Type']
        typecheck = data_service[typecolumn]
        df = pd.DataFrame({'comp': component_check, 'tycheck': typecheck})
        df['Verification'] = 0
        display_mask = df['comp'] == 'Display'
        type_mask = df['tycheck'].isin(['replace', 'upgrade'])
        df.loc[display_mask & type_mask, 'Verification'] = 1
        df['Flag'] = df['Verification']
        if (df['Flag'][display_mask] == 1).all():
            did_pass = 1
        else:
            did_pass = 0
        msg = ("In component column for all value of display it contains upgrade or replace"
               if did_pass else
               "for Display it has other values as well")
        ts.mark(did_pass, msg)
        assert did_pass

    def test_component_type_column(self, setup_method_fixture, data_service):
        component_column = self._config['dict_col_mapping']['component']
        component_check = data_service[component_column]
        typecolumn = self._config['dict_col_mapping']['Type']
        typecheck = data_service[typecolumn]
        df = pd.DataFrame({'comp': component_check, 'tycheck': typecheck})
        check_components = ['Display', 'Fans', 'PCB', 'Breaker']
        df['Flag'] = (df['comp'].isin(check_components) & (df['tycheck'] == 'replace')).astype(int)
        print(df)
        if (df['Flag'].any()) == 1:
            did_pass = 1
        else:
            did_pass = 0
        msg = ("In component column for all value of display it contains upgrade or replace"
               if did_pass else
               "for Display it has other values as well")
        ts.mark(did_pass, msg)
        assert did_pass

    #
    def test_Customer_Issue_c_component_dis_type_rep_up(self, setup_method_fixture, data_service):
        component_column = self._config['dict_col_mapping']['component']
        component_check = data_service[component_column]
        typecolumn = self._config['dict_col_mapping']['Type']
        typecheck = data_service[typecolumn]
        Cust_sum = self._config['dict_col_mapping']['Customer_summary']
        customer_sum = data_service[Cust_sum]
        df = pd.DataFrame({'comp': component_check, 'tycheck': typecheck, 'customer_summary': customer_sum})
        keywords = ['display', 'm4', 'monochrome', 'color']
        keyword_condition = df['customer_summary'].str.lower().str.contains('|'.join(keywords))
        # Verify the condition and set 'Verification' column
        df['Verification'] = 0  # Initialize Verification column with 0s
        df['Flag'] = 0  # Initialize Flag column with 0s
        df.loc[keyword_condition & (df['comp'] == 'Display') & df['tycheck'].isin(
            ['replace', 'upgrade']), 'Verification'] = 1
        df['Flag'] = df['Verification']
        self.log.info('Condition applied :')
        self.log.info(df['Flag'])
        if df['Flag'].any() == 1:
            did_pass = 1
        else:
            did_pass = 0
        msg = ("These keyword (display,m4 ,monochrome,color) when present in Customer_Issue_Summary__c the component "
               "is display and type is replace or upgrade"
               if did_pass else
               "These keyword (display,m4,monochrome,color) are also present in other component like PCB")
        ts.mark(did_pass, msg)
        assert did_pass

    def test_Customer_Issue_c_component_BCMS_dis_type_rep(self, setup_method_fixture, data_service):
        component_column = self._config['dict_col_mapping']['component']
        component_check = data_service[component_column]
        typecolumn = self._config['dict_col_mapping']['Type']
        typecheck = data_service[typecolumn]
        Cust_sum = self._config['dict_col_mapping']['Customer_summary']
        customer_sum = data_service[Cust_sum]
        df = pd.DataFrame({'comp': component_check, 'tycheck': typecheck, 'customer_summary': customer_sum})
        keywords = ['BCMS']
        keyword_condition = df['customer_summary'].str.contains(r'\b(?:' + '|'.join(keywords) + r')\b', case=False)
        df['Verification'] = 0  # Initialize Verification column with 0s
        df['Flag'] = 0  # Initialize Flag column with 0s
        # Set 'Verification' to 1 where both conditions are met
        df.loc[keyword_condition & (df['comp'] == 'BCMS') & df['tycheck'].isin(['replace']), 'Verification'] = 1
        # Set 'Flag' to 1 where 'Verification' is 1
        df['Flag'] = df['Verification']
        self.log.info('Condition applied :')
        self.log.info(df['Flag'])
        if df['Flag'].any() == 1:
            did_pass = 1
        else:
            did_pass = 0
        msg = (
            "These keyword (display,m4 ,monochrome,color) when present in Customer_Issue_Summary__c the component is display and type is replace or upgrade"
            if did_pass else
            "These keyword (display,m4,monochrome,color) are also present in other component like PCB")
        ts.mark(did_pass, msg)
        assert did_pass

    def test_Customer_Issue_c_component_Breaker_dis_type_rep(self, setup_method_fixture, data_service):
        component_column = self._config['dict_col_mapping']['component']
        component_check = data_service[component_column]
        typecolumn = self._config['dict_col_mapping']['Type']
        typecheck = data_service[typecolumn]
        Cust_sum = self._config['dict_col_mapping']['Customer_summary']
        customer_sum = data_service[Cust_sum]
        df = pd.DataFrame({'comp': component_check, 'tycheck': typecheck, 'customer_summary': customer_sum})
        keywords = ['breaker']
        keyword_condition = df['customer_summary'].str.contains(r'\b(?:' + '|'.join(keywords) + r')\b', case=False)
        df['Verification'] = 0  # Initialize Verification column with 0s
        df['Flag'] = 0  # Initialize Flag column with 0s
        df.loc[keyword_condition & (df['comp'] == 'Breaker') & df['tycheck'].isin(['replace']), 'Verification'] = 1
        # Set 'Flag' to 1 where 'Verification' is 1
        df['Flag'] = df['Verification']
        self.log.info('Condition applied :')
        self.log.info(df['Flag'])
        print(df)
        # Print 'Good' if 'Flag' is 1, else print 'Bad'
        if df['Flag'].any() == 1:
            did_pass = 1
        else:
            did_pass = 0
        msg = ("These keyword (breaker) when present in Customer_Issue_Summary__c the component is Breaker and type "
               "is replace or upgrade"
               if did_pass else
               "These keyword (breaker) are also present in other component like PCB and BCMS")
        ts.mark(did_pass, msg)
        assert did_pass

    def test_Customer_Issue_c_component_fan_dis_type_rep(self, setup_method_fixture, data_service):
        component_column = self._config['dict_col_mapping']['component']
        component_check = data_service[component_column]
        typecolumn = self._config['dict_col_mapping']['Type']
        typecheck = data_service[typecolumn]
        Cust_sum = self._config['dict_col_mapping']['Customer_summary']
        customer_sum = data_service[Cust_sum]
        df = pd.DataFrame({'comp': component_check, 'tycheck': typecheck, 'customer_summary': customer_sum})
        keywords = ['Fan']
        keyword_condition = df['customer_summary'].str.contains(r'\b(?:' + '|'.join(keywords) + r')\b', case=False)
        df['Verification'] = 0  # Initialize Verification column with 0s
        df['Flag'] = 0  # Initialize Flag column with 0s
        # Set 'Verification' to 1 where both conditions are met
        df.loc[keyword_condition & (df['comp'] == 'Fans') & df['tycheck'].isin(['replace']), 'Verification'] = 1
        # Set 'Flag' to 1 where 'Verification' is 1
        df['Flag'] = df['Verification']
        self.log.info('Condition applied :')
        self.log.info(df['Flag'])
        if df['Flag'].any() == 1:
            did_pass = 1
        else:
            did_pass = 0
        msg = ('These keyword (fan) when present in Customer_Issue_Summary__c the component is Breaker and type is replace'
            if did_pass else
            "These keyword (fan) are also present in other component like PCB")
        ts.mark(did_pass, msg)
        assert did_pass

    def test_customer_issue_c_component_pcb_pca_pcb_type_rep(self, setup_method_fixture, data_service):
        component_column = self._config['dict_col_mapping']['component']
        component_check = data_service[component_column]
        typecolumn = self._config['dict_col_mapping']['Type']
        typecheck = data_service[typecolumn]
        cust_sum = self._config['dict_col_mapping']['Customer_summary']
        customer_sum = data_service[cust_sum]
        df = pd.DataFrame({'comp': component_check, 'tycheck': typecheck, 'customer_summary': customer_sum})
        keywords = ['PCB', 'PCA']
        keyword_condition = df['customer_summary'].str.contains(r'\b(?:' + '|'.join(keywords) + r')\b', case=False)
        df['Verification'] = 0  # Initialize Verification column with 0s
        df['Flag'] = 0  # Initialize Flag column with 0s
        df.loc[keyword_condition & (df['comp'] == 'PCB') & df['tycheck'].isin(['replace']), 'Verification'] = 1
        df['Flag'] = df['Verification']
        self.log.info('Condition applied :')
        self.log.info(df['Flag'])
        if df['Flag'].any() == 1:
            did_pass = 1
        else:
            did_pass = 0
        msg = ("These keyword (PCB,PCA) when present in Customer_Issue_Summary__c the component is Breaker and type "
               "is replace"
               if did_pass else
               "These keyword (PCB,PCA) are also present in other component like ")
        ts.mark(did_pass, msg)
        assert did_pass

    def test_customer_issue_c_component_spd_type_rep(self, setup_method_fixture, data_service):
        component_column = self._config['dict_col_mapping']['component']
        component_check = data_service[component_column]
        typecolumn = self._config['dict_col_mapping']['Type']
        typecheck = data_service[typecolumn]
        cust_sum = self._config['dict_col_mapping']['Customer_summary']
        customer_sum = data_service[cust_sum]
        df = pd.DataFrame({'comp': component_check, 'tycheck': typecheck, 'customer_summary': customer_sum})
        keywords = ['tvss', 'spd', 'surge']
        keyword_condition = df['customer_summary'].str.contains(r'\b(?:' + '|'.join(keywords) + r')\b', case=False)
        df['Verification'] = 0  # Initialize Verification column with 0s
        df['Flag'] = 0  # Initialize Flag column with 0s
        # Set 'Verification' to 1 where both conditions are met
        df.loc[keyword_condition & (df['comp'] == 'SPD') & df['tycheck'].isin(['replace']), 'Verification'] = 1
        # Set 'Flag' to 1 where 'Verification' is 1
        df['Flag'] = df['Verification']
        self.log.info('Condition applied :')
        self.log.info(df['Flag'])
        if df['Flag'].any() == 1:
            did_pass = 1
        else:
            did_pass = 0
        msg = ("These keyword (tvss,spd,surge) when present in Customer_Issue_Summary__c the component is Breaker and "
               "type is replace"
               if did_pass else
               "These keyword (tvss,spd,surge) are also present in other component like")
        ts.mark(did_pass, msg)
        assert did_pass

    def test_customer_issue_c_both_type(self, setup_method_fixture, data_service):
        customer_issue__c_column = self._config['dict_col_mapping']['Customer_issue']
        customer_issue_check = data_service[customer_issue__c_column]
        component_column = self._config['dict_col_mapping']['component']
        component_check = data_service[component_column]
        type_column = self._config['dict_col_mapping']['Type']
        typecheck = data_service[type_column]
        df = pd.DataFrame({'cust_issue': customer_issue_check, 'comp': component_check, 'tycheck': typecheck})
        keywords = ['PCBA - Software Issue', 'Installation Request', 'PCBA - Hardware Issue', 'PCBA',
                    'Hardware Issue - RPP', 'Hardware Issue - PDU', 'Hardware Issue - STS']
        condition = df['cust_issue'].str.contains('|'.join(keywords))

        df['Verification'] = 0  # Initialize Verification column with 0s
        df['Flag'] = 0  # Initialize Flag column with 0s
        # Set 'Verification' to 1 where both conditions are met
        df.loc[condition & (df['comp'] == 'Display') & df['tycheck'].isin(['replace', 'upgrade']), 'Verification'] = 1
        df['Flag'] = df['Verification']
        self.log.info('Customer_issue applied to findout both replace and upgrade type in display component:')
        self.log.info(df['Flag'])
        if df['Flag'].any() == 1:
            did_pass = 1
        else:
            did_pass = 0
        msg = ("In Customer_issue_c column for all value of display it contains upgrade or replace"
               if did_pass else
               "In Customer_issue_c column for all value of display not contains upgrade or replace")
        ts.mark(did_pass, msg)
        assert did_pass

    def test_customer_issue_c_replace_type(self, setup_method_fixture, data_service):
        customer_issue__c_column = self._config['dict_col_mapping']['Customer_issue']
        customer_issue_check = data_service[customer_issue__c_column]
        component_column = self._config['dict_col_mapping']['component']
        component_check = data_service[component_column]
        type_column = self._config['dict_col_mapping']['Type']
        typecheck = data_service[type_column]
        df = pd.DataFrame({'cust_issue': customer_issue_check, 'comp': component_check, 'tycheck': typecheck})
        keywords = ['PCBA', 'PCBA - Hardware Issue', 'Installation Request', 'Hardware Issue - RPP',
                    'Hardware Issue - PDU']
        condition = df['cust_issue'].str.contains('|'.join(keywords))

        df['Verification'] = 0  # Initialize Verification column with 0s
        df['Flag'] = 0  # Initialize Flag column with 0s
        # Set 'Verification' to 1 where both conditions are met
        df.loc[condition & (df['comp'] == 'Display') & df['tycheck'].isin(['replace']), 'Verification'] = 1

        df['Flag'] = df['Verification']
        self.log.info('Customer_issue applied to findout only replace type in display component:')
        self.log.info(df['Flag'])
        if df['Flag'].any() == 1:
            did_pass = 1
        else:
            did_pass = 0
        msg = ("In Customer_issue_c column for all value of display it contains replace"
               if did_pass else
               "In Customer_issue_c column for all value of display not contains replace")
        ts.mark(did_pass, msg)
        assert did_pass

    def test_customer_issue_c_replace(self, setup_method_fixture, data_service):
        customer_issue__c_column = self._config['dict_col_mapping']['Customer_issue']
        customer_issue_check = data_service[customer_issue__c_column]
        component_column = self._config['dict_col_mapping']['component']
        component_check = data_service[component_column]
        type_column = self._config['dict_col_mapping']['Type']
        typecheck = data_service[type_column]
        df = pd.DataFrame({'cust_issue': customer_issue_check, 'comp': component_check, 'tycheck': typecheck})
        keywords = ['Hardware Issue - RPP', 'Hardware Issue - PDU', 'Hardware Issue - STS', 'Non Functioning',
                    'Installation Request',
                    'Hardware Issue - Physical Damage On Site', 'Hardware Issue - Wrong Hardware']
        condition = df['cust_issue'].str.contains('|'.join(keywords))

        df['Verification'] = 0  # Initialize Verification column with 0s
        df['Flag'] = 0  # Initialize Flag column with 0s
        # Set 'Verification' to 1 where both conditions are met
        df.loc[condition & (df['comp'] == 'Display') & df['tycheck'].isin(['replace']), 'Verification'] = 1

        df['Flag'] = df['Verification']
        self.log.info('Customer_issue applied to findout only replace type in display component:')
        self.log.info(df['Flag'])
        if df['Flag'].any() == 1:
            did_pass = 1
        else:
            did_pass = 0
        msg = ("In Customer_issue_c column for all value of display it contains replace only"
               if did_pass else
               "In Customer_issue_c column for all value of display not contains replace only")
        ts.mark(did_pass, msg)
        assert did_pass
