import datetime
import logging
import json
import os
import azure.functions as func
from utils.dcpd.class_contracts_data import Contract


def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = (
        datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc).isoformat()
    )

    if mytimer.past_due:
        logging.info("The timer is past due!")
    else:
        logging.info("Python timer trigger function ran at %s", utc_timestamp)
        try:
            
            obj = Contract()
            logging.info("before calling main_contracts")
            obj.main_contracts()
            logging.info("Out of contracts_data.py , came back to timer trigger after success")
        except Exception as e:
            raise e
