from fastapi.testclient import TestClient
import os

import sys
sys.path.append("/home/runner/work/fraud_detection/fraud_detection/fastapi_practice/")

from main import app

def func(x):
    return x + 1


def test_answer():
    assert func(3) == 4



client = TestClient(app)
def test_read_main():
    response = client.get("/")
    assert response.status_code == 200
