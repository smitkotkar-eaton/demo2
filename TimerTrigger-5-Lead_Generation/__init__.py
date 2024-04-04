import datetime
import logging
import os
from utils.dcpd.class_lead_generation import LeadGeneration
import azure.functions as func
import json


def main(mytimer: func.TimerRequest) -> None:
    logging.info("Lead Generation-start")
    utc_timestamp = (
        datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc).isoformat()
    )

    if mytimer.past_due:
        logging.info("The timer is past due!")

    try:
        # Read the configuration file
        logging.info("Lead Generation")
        # Create an instance of InstallBase and call main_install
        obj = LeadGeneration()
        logging.info("before calling main_lead_generation")
        result = obj.main_lead_generation()
        logging.info("Completed with _lead_generation")

    except Exception as e:
        return e
