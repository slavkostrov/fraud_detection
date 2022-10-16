import random
import sys

import requests
from fastapi.testclient import TestClient

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


def test_predict():
    import time
    for i in range(10):
        transaction_id = random.randint(1, 100)
        response = requests.post(
            f'{client.base_url}/predict?transaction_id={transaction_id}',
            data="{" + f'"TX_AMOUNT": {random.random()}, "TX_TIME_SECONDS": {random.random()}, "TERMINAL_ID": {random.random()}, '
                       f'"CUSTOMER_ID": {random.random()}' + "}",
            headers={"Content-Type": "application/json", "accept": "application/json"}
        )

        print(response.json())
        assert response.ok == 200
        assert response.json()['is_fraud'] in [0, 1]

        time.sleep(3)


def test_errors():
    response = requests.post(
        f'{client.base_url}/predict?transaction_id=1',
        data="{" + f'"TX_AMOUNT": x, "TX_TIME_SECONDS": {random.random()}, "TERMINAL_ID": {random.random()}, '
                   f'"CUSTOMER_ID": z' + "}",
        headers={"Content-Type": "application/json", "accept": "application/json"}
    )

    assert response.status_code == 422
