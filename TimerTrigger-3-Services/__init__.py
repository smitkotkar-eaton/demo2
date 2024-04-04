import datetime
import logging
import json
import os
import azure.functions as func
from utils.dcpd.class_services_data import ProcessServiceIncidents


def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')
    else:
        try:
           
            #obj = ProcessServiceIncidents(conf_env,config)
            obj = ProcessServiceIncidents()
            logging.info('before calling main_services')
            result = obj.main_services()
            logging.info(f"Inside function main defined in __init__.py file with result = {result}")
        except Exception as e:
            logging.info(f"{str(e)}")
            raise e
