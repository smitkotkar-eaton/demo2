import datetime
import logging
import json
import os
import azure.functions as func
from utils.dcpd.class_installbase import InstallBase



def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = (
        datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc).isoformat()
    )

    if mytimer.past_due:
        logging.info("The timer is past due!")
    else:
        logging.info("Python timer trigger function ran at %s", utc_timestamp)
   
    try:

        obj = InstallBase()
        logging.info('before calling main_install')
        result = obj.main_install()
        logging.info('Completed with Install base')

    except Exception as e:
        raise e
