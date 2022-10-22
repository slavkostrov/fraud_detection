import random
import sys

import requests
from fastapi.testclient import TestClient

sys.path.append("/home/runner/work/fraud_detection/fraud_detection/fastapi_practice/")

from main import app, load_model, load_features
from prometheus_client import Summary, Counter
from starlette_exporter import PrometheusMiddleware, handle_metrics

REQUEST_TIME = Summary('request_processing_seconds', 'Time spent processing request')
HTTP_EXCEPTION_COUNTER = Counter('http_exceptions_count', 'Description of counter')
FRAUD_COUNTER = Counter('fraud_count', 'Description of counter')
CUSTOMER_COUNTER = Counter('customer_count', 'Description of counter')
TERMINAL_COUNTER = Counter('terminal_count', 'Description of counter')

def func(x):
    return x + 1


def test_answer():
    assert func(3) == 4

load_features()
load_model()
client = TestClient(app)


def test_read_main():
    response = client.get("/")
    assert response.status_code == 200


def test_predict():
    import time
    for i in range(10):
        transaction_id = random.randint(1, 100)
        response = client.post(f"/predict?transaction_id={transaction_id}",
            data="{" + f'"TX_AMOUNT": {random.random()}, "TX_TIME_SECONDS": {random.random()}, '
                       f'"TERMINAL_ID": {random.random()}, "CUSTOMER_ID": {random.random()}' + "}")

        print(response.json())
        assert response.ok
        assert response.json()['is_fraud'] in [0, 1]

        time.sleep(3)


def test_errors():
    response = client.post("/predict?transaction_id=1",
                           data="{" + f'"TX_AMOUNT": xxx, "TX_TIME_SECONDS": {random.random()}, '
                                      f'"TERMINAL_ID": zzz, "CUSTOMER_ID": yyy' + "}")

    assert not response.ok
    assert response.status_code == 422
