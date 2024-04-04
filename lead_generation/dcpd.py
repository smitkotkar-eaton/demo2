"""@file DCPD.py



@brief


@details


@copyright 2021 Eaton Corporation. All Rights Reserved.
@note Eaton Corporation claims proprietary rights to the material disclosed
here on. This technical information may not be reproduced or used without
direct written permission from Eaton Corporation.
"""

# %% *** Setup Environment ***

import os
from utils import AppLogger

logger = AppLogger(__name__)
import traceback

path = os.getcwd()
path = os.path.join(path.split('ileads_lead_generation')[0], 'ileads_lead_generation')
os.chdir(path)

from lead_generation.base import LeadGeneration
from utils.dcpd.class_installbase import InstallBase
from utils.dcpd.class_contracts_data import Contract
from utils.dcpd.class_services_data import ProcessServiceIncidents
from utils.dcpd.class_generate_contacts import Contacts
from utils.dcpd.class_lead_generation import LeadGeneration


# %% *** Define Class ***

class DCPD(LeadGeneration):
    """
    Sub-Class that executes pipeline for DCPD product family.
    """

    def __init__(self):
        try:
            self.lead_pipeline()
        except Exception as excp:
            logger.app_fail(f"{self.step_main_install}: {traceback.print_exc()}", excp)
            raise Exception('Lead generation: Failed') from excp

    def lead_pipeline(self):

        try:
            step_ = 'Install Base Iteration 1'
            self.etl_installbase()
            logger.app_success(f"Preprocess {step_} Data")

            step_ = 'Contract'
            self.etl_contracts()
            logger.app_success(f"Preprocess {step_} Data")

            step_ = 'Services'
            self.etl_services()
            logger.app_success(f"Preprocess {step_} Data")

            step_ = 'Contact'
            self.etl_contacts()
            logger.app_success(f"Preprocess {step_} Data")

            step_ = 'Lead Management'
            self.etl_lead_management()
            logger.app_success(f"Preprocess {step_} Data")

            step_ = 'Lead Generation'
            self.lead_generation()
            logger.app_success(f"Preprocess {step_} Data")
        except Exception as e:
            logger.app_fail(
                f"Lead generation pipeline for {__name__}",
                f'{traceback.print_exc()}')
            raise Exception from e


    def etl_installbase(self):
        try:
            obj = InstallBase()
            self.df_data = obj.main_install()
        except Exception as e:
            logger.app_fail(
                f"Process install for {__name__}", f'{traceback.print_exc()}')
            raise Exception from e


    def etl_services(self):
        try:
            obj = ProcessServiceIncidents()
            self.df_data = obj.main_services()
        except Exception as e:
            logger.app_fail(
                f"Process services for {__name__}", f'{traceback.print_exc()}')
            raise Exception from e


    def etl_contracts(self):
        try:
            obj = Contract()
            self.df_data = obj.main_contracts()
        except Exception as e:
            logger.app_fail(
                f"Process contracts for {__name__}", f'{traceback.print_exc()}')
            raise Exception from e


    def etl_contacts(self):
        try:
            obj = Contacts()
            self.df_data = obj.main_contact()
        except Exception as e:
            logger.app_fail(
                f"Process contacts for {__name__}", f'{traceback.print_exc()}')
            raise Exception from e


    def etl_lead_management(self):
        print('NOT Implemented etl_lead_management !')


    def lead_generation(self):
        try:
            obj = LeadGeneration()
            self.df_data = obj.main_lead_generation()
        except Exception as e:
            logger.app_fail(
                f"Lead generation for {__name__}", f'{traceback.print_exc()}')
            raise Exception from e


# %%

if __name__ == "__main__":
    obj_dcpd = DCPD()


# %%
