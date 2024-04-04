"""@file test_class_lead_generation_data.py.

@brief This file used to test code for Lead Generation Class




@copyright 2023 Eaton Corporation. All Rights Reserved.
@note Eaton Corporation claims proprietary rights to the material disclosed
here on. This technical information may not be reproduced or used without
direct written permission from Eaton Corporation.
"""
import os
import pytest
import pandas as pd
from pandas._testing import assert_frame_equal
from utils.dcpd.class_lead_generation import LeadGeneration

if 'tests' in os.getcwd():
    os.chdir("..")

obj_lead = LeadGeneration()
obj_lead.config['file']['dir_data'] = "./tests/ip"
obj_lead.config['file']['dir_ref'] = "./tests/ip"
obj_lead.config['file']['dir_results'] = "./tests/"
obj_lead.config['file']['dir_validation'] = "ip"
obj_lead.config['file']['dir_intermediate'] = "ip"

class TestAddDataMTS:
    @pytest.mark.parametrize(
        "df_install_mts",
        [None,
         (pd.DataFrame()),
         'dcacac',
         [123, 'aeda'],
         1432,
         (pd.DataFrame(data={"test_col": ['new', 'new', 'existing']})),
         ])
    def test_add_data_mts_err(self, df_install_mts):
        with pytest.raises(Exception) as _:
            df_out_mts = obj_lead.add_data_mts(df_install_mts, merge_type='left')

    def test_add_data_mts_valid_scenario(self):
        df_install_mts = pd.read_csv("tests/ip/df_install_mts.csv")
        df_out_mts_ac = obj_lead.add_data_mts(df_install_mts, merge_type='left')
        df_out_mts_exp = pd.read_csv("tests/ip/df_out_mts_exp.csv")
        df_out_mts_exp = df_out_mts_exp.fillna("")
        df_out_mts_ac = df_out_mts_ac.fillna("")
        assert_frame_equal(df_out_mts_ac, df_out_mts_exp)

class TestTestContractInstall:
    def test_contract_install_valid_scenario(self):
        contract_install = obj_lead.pipeline_contract_install()
        contract_install_exp = pd.read_csv("tests/ip/contract_install_exp.csv")
        contract_install_exp = contract_install_exp.fillna("")
        contract_install = contract_install.fillna("")
        assert_frame_equal(contract_install_exp, contract_install)

class TestPostProcessLeads:
    @pytest.mark.parametrize(
        "df_leads",
        [None,
         (pd.DataFrame()),
         'dcacac',
         [123, 'aeda'],
         1432,
         (pd.DataFrame(data={"test_col": ['new', 'new', 'existing']})),
         ])
    def test_post_proecess_leads_err(self, df_leads):
        with pytest.raises(Exception) as _:
            filtered_data = obj_lead.post_proecess_leads(df_leads)

class TestPostProcessRefInstall:
    @pytest.mark.parametrize(
        "ref_install",
        [None,
         (pd.DataFrame()),
         'dcacac',
         [123, 'aeda'],
         1432,
         (pd.DataFrame(data={"test_col": ['new', 'new', 'existing']})),
         ])
    def test_post_process_ref_install_err(self, ref_install):
        with pytest.raises(Exception) as _:
            ref_install = obj_lead.post_process_ref_install(ref_install)

    def test_post_process_ref_install_valid_scenario(self):
        ref_install = pd.read_csv("tests/ip/df_leads_post_process_ref_install.csv")
        ref_install_ac = obj_lead.post_process_ref_install(ref_install)
        ref_install_ex = pd.read_csv("tests/ip/df_leads_post_process_ref_install_op.csv")
        ref_install_ex["Product_Age"] = ref_install_ex["Product_Age"].astype(str)
        ref_install_ac["Product_Age"] = ref_install_ac["Product_Age"].astype(
            str)
        ref_install_ex["Upgrade_Type"] = ref_install_ex["Upgrade_Type"].astype(
            str)
        ref_install_ac["Upgrade_Type"] = ref_install_ac["Upgrade_Type"].astype(
            str)
        ref_install_ex["Upgraded_Monitor"] = ref_install_ex["Upgraded_Monitor"].astype(bool)
        ref_install_ac["Upgraded_Monitor"] = ref_install_ac[
            "Upgraded_Monitor"].astype(bool)
        ref_install_ac["Raise_Lead_On"] = pd.to_datetime(ref_install_ac["Raise_Lead_On"])
        ref_install_ex["Raise_Lead_On"] = pd.to_datetime(
            ref_install_ex["Raise_Lead_On"])
        ref_install_ex["StartupState"] = ref_install_ex["StartupState"].fillna("").astype(
            str)
        ref_install_ac["StartupState"] = ref_install_ac["StartupState"].fillna("").astype(
            str)
        ref_install_ex["SoldTo_State"] = ref_install_ex["SoldTo_State"].fillna(
            "").astype(
            str)
        ref_install_ac["SoldTo_State"] = ref_install_ac["SoldTo_State"].fillna(
            "").astype(
            str)
        ref_install_ex["BillingState"] = ref_install_ex["BillingState"].fillna(
            "").astype(
            str)
        ref_install_ac["BillingState"] = ref_install_ac["BillingState"].fillna(
            "").astype(
            str)
        ref_install_ex["BillingState_old"] = ref_install_ex["BillingState_old"].fillna(
            "").astype(
            str)
        ref_install_ac["BillingState_old"] = ref_install_ac["BillingState_old"].fillna(
            "").astype(
            str)
        assert_frame_equal(ref_install_ac, ref_install_ex)

class TestAddRaiseLeadOn:
    @pytest.mark.parametrize(
        "ref_install",
        [None,
         (pd.DataFrame()),
         'dcacac',
         [123, 'aeda'],
         1432,
         (pd.DataFrame(data={"test_col": ['new', 'new', 'existing']})),
         ])
    def test_add_raise_lead_on_err(self, ref_install):
        with pytest.raises(Exception) as _:
            ref_install = obj_lead.add_raise_lead_on(ref_install)

class TestUpdateStretegicAccount:
    @pytest.mark.parametrize(
        "ref_install",
        [None,
         (pd.DataFrame()),
         'dcacac',
         [123, 'aeda'],
         1432,
         (pd.DataFrame(data={"test_col": ['new', 'new', 'existing']})),
         ])
    def test_update_strategic_account_err(self, ref_install):
        with pytest.raises(Exception) as _:
            ref_install = obj_lead.update_strategic_account(ref_install)

    # def test_update_strategic_account_valid_scenario(self):
    #     ref_install = pd.read_csv("tests/ip/ref_install_update_strategic_account.csv")
    #     ref_install_ac = obj_lead.update_strategic_account(ref_install)
    #     ref_install_ex = pd.read_csv("tests/ip/ref_install_update_strategic_account_op.csv")
    #     assert_frame_equal(ref_install_ac, ref_install_ex)

class TestPostProcessOutputiLead:
    @pytest.mark.parametrize(
        "output_ilead_df",
        [None,
         (pd.DataFrame()),
         'dcacac',
         [123, 'aeda'],
         1432,
         (pd.DataFrame(data={"test_col": ['new', 'new', 'existing']})),
         ])
    def test_post_process_output_ilead_err(self, output_ilead_df):
        with pytest.raises(Exception) as _:
            ref_install = obj_lead.post_process_output_ilead(output_ilead_df)

class TestProdMetaData:
    @pytest.mark.parametrize(
        "output_ilead_df",
        [None,
         (pd.DataFrame()),
         'dcacac',
         [123, 'aeda'],
         1432,
         (pd.DataFrame(data={"test_col": ['new', 'new', 'existing']})),
         ])
    def test_prod_meta_data_err(self, output_ilead_df):
        with pytest.raises(Exception) as _:
            ref_install = obj_lead.prod_meta_data(output_ilead_df)

class TestUpdateSTSLead:
    @pytest.mark.parametrize(
        "output_ilead_df",
        [None,
         (pd.DataFrame()),
         'dcacac',
         [123, 'aeda'],
         1432,
         (pd.DataFrame(data={"test_col": ['new', 'new', 'existing']})),
         ])
    def test_update_sts_leads_err(self, output_ilead_df):
        with pytest.raises(Exception) as _:
            ref_install = obj_lead.update_sts_leads(output_ilead_df)

class TestIdentifyLeads:
    @pytest.mark.parametrize(
        "df_bom, ref_lead_opp",
        [(None, pd.DataFrame()),
         (pd.DataFrame(), pd.DataFrame()),
         ('dcacac', pd.DataFrame()),
         ([123, 'aeda'], pd.DataFrame()),
         (1432, pd.DataFrame()),
         (pd.DataFrame(data={"test_col": ['new', 'new', 'existing']}), pd.DataFrame()),
         ])
    def test_identify_leads_err1(self, df_bom, ref_lead_opp):
        with pytest.raises(Exception) as _:
            df_leads = obj_lead.identify_leads(df_bom, ref_lead_opp)

    @pytest.mark.parametrize(
        "df_bom, ref_lead_opp",
        [(pd.DataFrame(), None),
         (pd.DataFrame(), pd.DataFrame()),
         (pd.DataFrame(), 'dcacac'),
         (pd.DataFrame(), [123, 'aeda']),
         (pd.DataFrame(), 1432),
         (pd.DataFrame(), pd.DataFrame(data={"test_col": ['new', 'new', 'existing']})),
         ])
    def test_identify_leads_err1(self, df_bom, ref_lead_opp):
        with pytest.raises(Exception) as _:
            df_leads = obj_lead.identify_leads(df_bom, ref_lead_opp)

    def test_idnetify_leads_valid_scenario(self):
        obj_lead.config['file']['Processed']['services']['file_name'] = "processed_services_identify_leads.csv"
        df_bom = pd.read_csv("tests/ip/df_bom_identify_leads.csv")
        ref_lead_opp = pd.read_csv("tests/ip/red_lead_opp_identify_leads.csv")
        df_leads_ac = obj_lead.identify_leads(df_bom, ref_lead_opp)
        # print(df_leads_ac)
        df_leads_exp = pd.read_csv("tests/ip/idnetify_leads_valid_scenario_exp.csv")
        df_leads_ac.reset_index(inplace=True)
        df_leads_exp.reset_index(inplace=True)
        df_leads_ac.drop(columns=['index', 'today'], inplace=True)
        df_leads_exp.drop(columns=['index', 'today'], inplace=True)
        df_leads_ac[["Job_Index", "Total_Quantity"]] = df_leads_ac[["Job_Index", "Total_Quantity"]].astype(int).astype(str)
        df_leads_exp[["Job_Index", "Total_Quantity"]] = df_leads_exp[["Job_Index", "Total_Quantity"]].astype(int).astype(
            str)
        ls_null = ["Component_Description", "End of Prod", "EOSL", "flag_raise_in_gp", "component", "ClosedDate"]
        df_leads_ac[ls_null] = df_leads_ac[ls_null].fillna("").astype(str)
        df_leads_exp[ls_null] = df_leads_exp[ls_null].fillna("").astype(str)
        df_leads_ac = df_leads_ac.astype(str)
        df_leads_exp = df_leads_exp.astype(str)
        assert_frame_equal(df_leads_ac, df_leads_exp)

class TestClassifyLeads:
    @pytest.mark.parametrize(
        "df_leads_out",
        [None,
         (pd.DataFrame()),
         'dcacac',
         [123, 'aeda'],
         1432,
         (pd.DataFrame(data={"test_col": ['new', 'new', 'existing']})),
         ])
    def test_classify_leads_err(self, df_leads_out):
        with pytest.raises(Exception) as _:
            df_leads_out = obj_lead.classify_lead(df_leads_out)

    def test_classify_leads_ideal_case(self):
        obj_lead.config['file']['Processed']['services']['file_name'] = "processed_services_classify_lead.csv"
        df_leads_out = pd.read_csv("tests/ip/df_leads_out_identify_lead.csv")
        df_leads_out = obj_lead.classify_lead(df_leads_out)
        # df_leads_out.to_csv("tests/ip/classify_leads_ex_op.csv", index=False)
        df_leads_out_ex_op = pd.read_csv("tests/ip/classify_leads_ex_op.csv")
        df_leads_out[["date_code", "age"]] = df_leads_out[["date_code", "age"]].astype(str)
        df_leads_out_ex_op[["date_code", "age"]] = df_leads_out_ex_op[["date_code", "age"]].astype(str)
        df_leads_out = df_leads_out.drop("today", axis=1)
        df_leads_out_ex_op = df_leads_out_ex_op.drop("today", axis=1)

        assert_frame_equal(df_leads_out_ex_op, df_leads_out)

class TestCategoriseDueInCategory:
    @pytest.mark.parametrize(
        "component_due_in_years",
        [None,
         (pd.DataFrame()),
         'dcacac',
         (pd.DataFrame(data={"test_col": ['new', 'new', 'existing']})),
         ])
    def test_categorize_due_in_category_err(self, component_due_in_years):
        with pytest.raises(Exception) as _:
            df_leads_out = obj_lead.categorize_due_in_category(component_due_in_years)

    @pytest.mark.parametrize(
        "component_due_in_years, op_exp",
        [
            (-1, "Past Due" ),
            (1, "Due this year" ),
            (2, "Due in 2-3 years" ),
            (15, "Due after 3 years" ),
            (101, "Unknown"),
        ]
    )
    def test_categorize_due_in_category_valid_scenario(self, component_due_in_years, op_exp):
        op_ac = obj_lead.categorize_due_in_category(
            component_due_in_years)
        assert op_ac == op_exp

class TestUpdateEosl:
    @pytest.mark.parametrize(
        "row",
        [None,
         (pd.DataFrame()),
         'dcacac',
         (pd.DataFrame(data={"test_col": ['new', 'new', 'existing']})),
         ])
    def test_update_eosl_err(self, row):
        with pytest.raises(Exception) as _:
            df_leads_out = obj_lead.update_eosl(row)

    @pytest.mark.parametrize(
        "row, op_exp",
        [
         (pd.Series(data={
             'lead_type': 'EOSL',
             'Component_Due_Date': 3,
             'EOSL': 1
         }), 3),
        (pd.Series(data={
            'lead_type': 'Life',
            'Component_Due_Date': 3,
            'EOSL': 1
        }), 1),
        ]
    )
    def test_update_eosl_valid_ip(self, row, op_exp):
        op_ac = obj_lead.update_eosl(row)
        assert op_exp == op_ac

class TestAddPrices:
    @pytest.mark.parametrize(
        "df_leads, err",
        [
            (-1, "Past Due"),
            (None, "Due this year"),
            ([], "Due in 2-3 years")
        ]
    )
    def test_add_prices_datatype(self, df_leads, err):
        with pytest.raises(Exception) as _:
            df_leads_out = obj_lead.add_prices(df_leads)

class TestOutputileadComponentSummaryReformat:
    def test_output_ilead_component_summary_reformat(self):
        output_ilead = pd.read_csv("tests/ip/output_ilead_component_summary_reformat.csv")
        op = obj_lead.output_ilead_component_summary_reformat(
            output_ilead)
        op_exp = pd.read_csv("tests/ip/output_ilead_component_summary_exp.csv")
        op.reset_index(inplace=True)
        op_exp.reset_index(inplace=True)
        op.drop(columns=['index'], inplace=True)
        op_exp.drop(columns=['index'], inplace=True)
        ls_str = ["Est. life"]
        op[ls_str] = op[ls_str].astype(str)
        op_exp[ls_str] = op_exp[ls_str].astype(str)
        assert_frame_equal(op, op_exp)

class TestRefInstallUnitSummaryReformat:
    def test_ref_install_unit_summary_reformat(self):
        output_ref_install = pd.read_csv(
            "tests/ip/ref_install_unit_summary_reformat.csv")
        op = obj_lead.ref_install_unit_summary_reformat(
            output_ref_install)
        op_exp = pd.read_csv("tests/ip/ref_install_unit_summary_reformat_exp.csv")
        op.reset_index(inplace=True)
        op_exp.reset_index(inplace=True)
        op.drop(columns=['index'], inplace=True)
        op_exp.drop(columns=['index'], inplace=True)
        ls_str = ["value"]
        op[ls_str] = op[ls_str].astype(str)
        op_exp[ls_str] = op_exp[ls_str].astype(str)
        assert_frame_equal(op, op_exp)

class TestRefInstallMapLevelReformat:
    def test_ref_install_map_level_reformat(self):
        output_ref_install = pd.read_csv(
            "tests/ip/ref_install_map_level_reformat.csv")
        op = obj_lead.output_ref_install_map_level_reformat(
            output_ref_install)
        op_exp = pd.read_csv("tests/ip/ref_install_map_level_reformat_exp.csv")
        op.reset_index(inplace=True)
        op_exp.reset_index(inplace=True)
        op.drop(columns=['index'], inplace=True)
        op_exp.drop(columns=['index'], inplace=True)
        assert_frame_equal(op, op_exp)

class TestPostProcessOutputiLead:
    def test_post_process_output_ilead(self):
        output_ilead = pd.read_csv(
            "tests/ip/df_leads_post_process_output_ilead.csv")
        output_ilead['date_code'] = '01/16/2008'
        op = obj_lead.post_process_output_ilead(
            output_ilead)
        op_exp = pd.read_csv("tests/ip/post_process_op_ilead_exp.csv")
        ls_str = [
            "Component_Due_in (years)", "key_chasis", "rpp_chassis_type",
            "Description_chasis", "Color"
        ]
        op[ls_str] = op[ls_str].fillna("").astype(str)
        op_exp[ls_str] = op_exp[ls_str].fillna("").astype(str)
        assert_frame_equal(op, op_exp)
