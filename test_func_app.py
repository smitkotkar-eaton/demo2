import unittest
import json
import azure.functions as func
from function_app import dummy_http_trigger

from sys import path
from os.path import dirname, abspath
SCRIPT_DIR = dirname(abspath(__file__))
path.append(dirname(SCRIPT_DIR))

class TestFunctionApp(unittest.TestCase):
  def __init__(self, methodName: str = "runTest") -> None:
    super().__init__(methodName)
  
  def test_function_app(self):
    request = func.HttpRequest(
      method = "GET",
      url = "/api/dummy_http_trigger",
      params = {"name": "Smit"},
      body = json.dumps({}).encode('utf8')
    )
    func_call = dummy_http_trigger.build().get_user_function()
    response = func_call(request)
    expected_output = "Hello, Smit. This HTTP triggered function executed successfully."
    self.assertEqual(response.status_code, 200, "Expected Status Code: 200")
    self.assertEqual(response.get_body().decode(), expected_output, "Wrong Output")