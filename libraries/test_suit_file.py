"""@file test_suit_file.py

@brief Standard test suit for files


@details Standard test cases @ file level. Test cases are configurable.
    List of test:
        1. File is not empty
        2. File row count > 0
        3. File generated time

@copyright 2021 Eaton Corporation. All Rights Reserved.
@note Eaton Corporation claims proprietary rights to the material disclosed
here on. This technical information may not be reproduced or used without
direct written permission from Eaton Corporation.
"""

# %%
import datetime
import os
import pandas as pd
import libraries.custom_logger as cl
from libraries.test_status import TestStatus
import logging

current_time = datetime.datetime.now()


class TestFileSuit():

    log = cl.customLogger(logging.DEBUG)

    def __init__(self, file_path):
        self.ts = TestStatus()
        self.file_path = file_path

    def main_test(self):
        ls_output = []
        ls_output.append(self.test_time())
        ls_output.append(self.test_file_size())
        ls_output.append(self.test_row_count())
        return ls_output

    def test_time(self):

        generated_time = os.path.getmtime(self.file_path)
        generated_on = datetime.datetime.fromtimestamp(generated_time)
        self.log.info("Report Generation time is : ")
        self.log.info(generated_on)
        self.log.info("Current time is : ")
        self.log.info(current_time)
        result = generated_on <= current_time
        test_msg = (f"Current time is greater than Generated time  {generated_on}"
                    if result else
                    "Current time is not greater than Generated time")
        self.ts.mark(result,test_msg)
        return result

    def test_file_size(self):
        """ Check that file is not empty """
        file_size = os.path.getsize(self.file_path)
        result = file_size > 0
        self.log.info("Generated Report size is : ")
        self.log.info(file_size)
        test_msg = (f"File size is grater than zero {file_size}" if result else
                    "File size not grater than zero")
        self.ts.mark(result, test_msg)
        return result

    def test_row_count(self):
        df = pd.read_csv(self.file_path)
        count = len(df)
        self.log.info("Generated Report row count is : ")
        self.log.info(count)
        result = count > 0
        test_msg = (
            f"File row count is grater than zero {count}"
            if result else "File row count is zero")
        self.ts.mark(result, test_msg)
        return result

