import datetime
import logging
import json
import os
import azure.functions as func
from utils.dcpd.class_generate_contacts import Contacts



def main(mytimer: func.TimerRequest):
    logging.info(f'{10*"*"} STARTING FUNCTION CONTACTS {10*"*"}')
    config_dir = os.path.join(os.path.dirname(__file__), "../config")
    config_file = os.path.join(config_dir, "config_dcpd.json")

    try:
        with open(config_file, "r") as config_file:
            config = json.load(config_file)
        conf_env = config.get("conf.env", "azure-adls")

        # Create an instance of Contact and call main_contact
        obj = Contacts()
        logging.info("Before calling main_contact")
        out = obj.main_contact()
        logging.info(f'OUTPUT:{out}\n{10*"*"} ENDING FUNCTION CONTACTS {10*"*"}')

    except Exception as e:
        logging.error(e)
        raise e