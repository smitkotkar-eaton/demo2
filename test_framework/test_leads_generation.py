import datetime
import os


import pandas as pd

from libraries.test_status import TestStatus
from utils.dcpd.class_lead_generation import LeadGeneration


class TestLeadGeneration():
    def __init__(self):
        self.ts = TestStatus()
    def test_trigger_lead_generation(self):
        obj = LeadGeneration()
        obj.main_lead_generation()




    def test_time_size_rowcount_output_ilead_lead_generation(self):

        file_path ="C:\\ileads_dcpd\\ileads_lead_generation\\results\\output_iLead.csv"
        test_common_obj = common_testing_functions.TestCommonTestingFunction()

        test_common_obj.test_time(file_path)
        test_common_obj.test_file_size(file_path)
        row_count = test_common_obj.test_row_count(file_path)
        result1 = row_count > 0
        self.ts.mark(result1,"row count  of ilead  data is zero")
        # assert row_count > 0,'row count  of ilead  data is zero '

    def test_column_output_ilead(self):

        expected_columns = ["Serial_Number","Component","Source","BOM_Part_Number","Component_Date_Code","Component_Life","Component_Due_Date","External_Salesperson","Internal_Salesperson","Component_Due_in (years)","Component_Due_in (Category)","flag_raise_lead","is_standard_offering","List_Price_$","Avg_Sales_Price_$","Year_Lead","Component_Description","Component_Age","Lead_Type"]
        df = pd.read_csv("C:\\ileads_dcpd\\ileads_lead_generation\\results\\output_iLead.csv")
        actual_columns = df.columns
        print('actual columns of  output ilead data',actual_columns)
        result2 = len(expected_columns) == len(actual_columns)
        self.ts.mark(result2,"actual column and expected columns length are not matching")


        # assert len(expected_columns) == len(actual_columns), " actual column and expected columns length are not matching "
        exp_sorted = sorted(expected_columns)
        act_sorted = sorted(actual_columns)
        result3 = exp_sorted == act_sorted
        self.ts.mark(result3,"expected and actual column names are not matching")

        # assert exp_sorted == act_sorted, "expected and actual column names are not matching"

    def test_time_size_rowcount_output_ref_install_lead_generation(self):

        file_path = "C:\\ileads_dcpd\\ileads_lead_generation\\results\\output_ref_install.csv"
        common_testing_functions.test_time(file_path)
        common_testing_functions.test_file_size(file_path)
        row_count = common_testing_functions.test_row_count(file_path)
        result4 = row_count > 0
        self.ts.mark(result4,"row count  of lead generation ref install  data is zero")
        # assert row_count > 0,'row count  of lead generation ref install  data is zero '


    def test_column_output_ref_install(self):

        expected_columns = ["Serial_Number","Product_Name","Install_Date","TLN_Part_Number","Product_Age","CSE_Area","Region","Party_Site_Number","Txt_Party_Site_Number","Ship_To_Party_Name","Ship_To_Address1","Ship_To_City","Ship_To_State","Ship_To_Postal_Code","Ship_To_Country","is_under_contract","Contract_Number","Contract_Coverage","Eaton_ContractType","Contract_Coverage_Description","Contract_Coverage_Type","Contract_Line_Status","Customer","flag_valid_installbase","EOSL_Date","Model_Id","Item_Description","Contract_Start_Date","Contract_End_Date","Contract_Conversion","Strategic_Customer_Name","Owner_Party_Name","Owner_Party_Number","Install_Year_Group","Warranty_Start_Date","Warranty_End_Date","Warranty_Coverage","Warranty_Contract_Number","Warranty_Due_Date","First_Contract_Start_Date","Is_Lead_Open","Lead_ID","flag_decommissioned","Segment_Parent","Segment_Child","flag_prior_lead","prior_lead_date","flag_prior_service_lead","prior_service_lead_date","Managed_By","Managed_By_Contact/Email","flag_valid_6month","Assigned_Company","Assigned_Contact","Assigned_Region","Sales_Manager","Sales_Status","Lead_Date","Source_gp","Ship_Date","Billing_Address","Billing_City","Billing_State","Billing_Postal_Code","Billing_Country","Source","Sold_To_Street","Sold_To_City","Sold_To_State","Sold_To_Postal_Code","Sold_To_Country","Startup_Customer","Startup_Address","Startup_City","Startup_State","Startup_PostalCode","Startup_Country","pn_logic_tray","is_valid_logic_tray_lead","pn_door_assembly","is_valid_door_assembly_lead","pn_input_breaker_panel","is_valid_input_breaker_panel_lead","pn_chasis","is_valid_chasis_lead"]
        df = pd.read_csv("C:\\ileads_dcpd\\ileads_lead_generation\\results\\output_ref_install.csv")
        actual_columns = df.columns
        print('actual_columns of output ref install data',actual_columns)
        result5 = len(expected_columns) == len(actual_columns)
        self.ts.mark(result5,"actual column name and expected columns name length are not matching ")
        # assert len(expected_columns) == len(actual_columns), " actual column name and expected columns name length are not matching "
        exp_sorted = sorted(expected_columns)
        act_sorted = sorted(actual_columns)
        result6 = exp_sorted == act_sorted
        self.ts.mark(result6,"expected and actual column names are not matching")
        # assert exp_sorted == act_sorted, "expected and actual column names are not matching"


    def test_generate_lead(self):

        df = pd.read_csv("C:\\ileads_dcpd\\ileads_lead_generation\\results\\output_ref_install.csv")
        df1 = pd.read_csv("C:\\ileads_dcpd\\ileads_lead_generation\\results\\output_iLead.csv")
        output_ref = df.groupby('Serial_Number')
        output_ilead = df1.groupby('Serial_Number')
        serial_no_output_ref = set(output_ref.groups)
        serial_no_output_ilead = set(output_ilead.groups)
        minus = serial_no_output_ilead - serial_no_output_ref
        print(minus)
        result7 = serial_no_output_ref == serial_no_output_ilead
        self.ts.markFinal(result7,"output_ref_install serial Number  is not  present in output ilead")
        # assert serial_no_output_ref == serial_no_output_ilead, 'output_ref_install serial Number  is not  present in output ilead'



tlg = TestLeadGeneration()
tlg.test_trigger_lead_generation()
tlg.test_time_size_rowcount_output_ilead_lead_generation()
tlg.test_column_output_ilead()
tlg.test_time_size_rowcount_output_ref_install_lead_generation()
tlg.test_column_output_ref_install()
tlg.test_generate_lead()
