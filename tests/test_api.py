import random
import sys

import pytest
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
        response = client.post(
            f"/predict/",
            headers={"Content-Type": "application/json", "accept": "application/json"},
            json={
                "transaction_id": transaction_id,
                "TX_AMOUNT": random.random() * 1000,
                "TX_TIME_SECONDS": random.random() * 1000,
                "TERMINAL_ID": random.random() * 1000,
                "CUSTOMER_ID": random.random() * 1000
            },
        )
        print(response.json())
        assert response.status_code == 200
        assert response.json()['is_fraud'] in [0, 1]

        time.sleep(3)


def test_errors():
    response = client.post(
        f"/predict?transaction_id=1",
        headers={"Content-Type": "application/json", "accept": "application/json"},
        json={
            "TX_AMOUNT": "x",
            "TX_TIME_SECONDS": random.random() * 1000,
            "TERMINAL_ID": "y",
            "CUSTOMER_ID": random.random() * 1000
        },
    )

    assert response.status_code != 200
