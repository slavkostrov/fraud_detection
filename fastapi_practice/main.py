"""FastAPI fraud detection model inference example"""

import os
from typing import Optional, List, Dict

import boto3
import pandas as pd
from fastapi import FastAPI, HTTPException
from prometheus_client import Summary, Counter
from pydantic import BaseModel
from sklearn.pipeline import Pipeline
from starlette_exporter import PrometheusMiddleware, handle_metrics


def current_environment():
    """get current environment type from env variable."""
    return os.getenv("APP_ENV", default="TEST")


MODEL_NAME = os.getenv("MODEL_NAME", default="fraud_detection_sklearn_model")
MODEL_VERSION = os.getenv("MODEL_VERSION", default="latest")

if (
        os.getenv("AWS_ACCESS_KEY_ID") is None or
        os.getenv("AWS_SECRET_ACCESS_KEY") is None or
        os.getenv("MLFLOW_S3_ENDPOINT_URL") is None or
        os.getenv("AWS_S3_ENDPOINT_URL") is None
):
    raise RuntimeError(
        "Missing env variable (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, MLFLOW_S3_ENDPOINT_URL, AWS_S3_ENDPOINT_URL, MLFLOW_URI).")

if os.getenv("MLFLOW_URI") is None and current_environment() == "PROD":
    raise RuntimeError("Setup `MLFLOW_URI` for production env to get_models.")

app = FastAPI()
app.add_middleware(PrometheusMiddleware)
app.add_route("/metrics", handle_metrics)

REQUEST_TIME = Summary('request_processing_seconds', 'Time spent processing request')
HTTP_EXCEPTION_COUNTER = Counter('http_exceptions_count', 'Description of counter')
FRAUD_COUNTER = Counter('fraud_count', 'Description of counter')
CUSTOMER_COUNTER = Counter('customer_count', 'Description of counter')
TERMINAL_COUNTER = Counter('terminal_count', 'Description of counter')


def _load_model():
    """load model based on current environment specific"""
    env = current_environment()
    if env == "TEST":
        session = boto3.session.Session()
        s3 = session.client(
            service_name='s3',
            endpoint_url=os.getenv("AWS_S3_ENDPOINT_URL"),
        )
        s3.download_file('slava-otus', 'models/testmodel.pkl', 'model.pkl')
        with open("model.pkl", "rb") as file:
            import pickle
            model = pickle.load(file)
        return model, None
    elif env == "PROD":
        import mlflow
        mlflow.set_tracking_uri(os.getenv("MLFLOW_URI"))
        loaded_model = mlflow.pyfunc.load_model(f"models:/{MODEL_NAME}/{MODEL_VERSION}")
        feature_list = loaded_model._model_meta._signature.inputs.input_names()
        pipeline = loaded_model._model_impl
        return pipeline, feature_list
    else:
        raise RuntimeError(f"Setup `APP_ENV` variable (Test, Prod).")


class Model:
    # TODO: any model (spark.ML or sklearn) from MlFlow
    pipeline: Optional[Pipeline] = None
    feature_list: List[str] = ["f_TX_AMOUNT", "f_TX_TIME_SECONDS", "f_mean_customer_TX_AMOUNT",
                               "f_mean_terminal_TX_AMOUNT", "f_unq_terminals_per_customer", "f_customer_amount_ratio",
                               "f_terminal_amount_ratio"]


class CustomerFeatures:
    features: pd.DataFrame


class TerminalFeatures:
    features: pd.DataFrame


class Transaction(BaseModel):
    TX_AMOUNT: float
    TX_TIME_SECONDS: float
    TERMINAL_ID: int
    CUSTOMER_ID: int


@HTTP_EXCEPTION_COUNTER.count_exceptions(HTTPException)
@REQUEST_TIME.time()
def _predict(transaction_id: int, transaction: Transaction) -> Dict:
    df = pd.DataFrame([transaction.dict()])
    if Model.pipeline is None:
        raise HTTPException(status_code=503, detail="No model loaded")

    for column in df.columns:
        if column in Model.feature_list:
            pass
        elif f"f_{column}" in Model.feature_list:
            df[f"f_{column}"] = df[column]

    df = df.merge(CustomerFeatures.features, on="CUSTOMER_ID")
    print("added customer features")

    df = df.merge(TerminalFeatures.features, on="TERMINAL_ID")
    print("added terminal features")

    df["f_customer_amount_ratio"] = df["TX_AMOUNT"] / df["f_mean_customer_TX_AMOUNT"]
    df["f_terminal_amount_ratio"] = df["TX_AMOUNT"] / df["f_mean_terminal_TX_AMOUNT"]

    try:
        print("prediction...")
        pred = int(Model.pipeline.predict(df[Model.feature_list])[0])
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
    return {"transaction_id": transaction_id, "is_fraud": pred}


@app.on_event("startup")
def load_features():
    """download features from s3 to local."""
    s3 = boto3.client('s3', endpoint_url='https://storage.yandexcloud.net')
    for features_file in "customer_features.parquet", "terminal_features.parquet":
        s3.download_file('slava-otus', f'features/{features_file}', features_file)


@app.on_event("startup")
def load_model():
    """load model and features from S3/MlFlow."""
    model, feature_list = _load_model()

    Model.pipeline = model
    if feature_list is not None:
        """for test model feature list is static"""
        Model.feature_list = feature_list

    CustomerFeatures.features = pd.read_parquet("customer_features.parquet")
    TerminalFeatures.features = pd.read_parquet("terminal_features.parquet")


@app.get("/")
def read_healthcheck():
    # TODO: take version from github tag
    return {"status": "Green", "version": "0.2.0"}


@app.post("/predict")
def predict(transaction_id: int, transaction: Transaction) -> Dict:
    """main interface for inference."""
    TERMINAL_COUNTER.labels(customer_id=Transaction.TERMINAL_ID).inc()
    CUSTOMER_COUNTER.labels(customer_id=Transaction.CUSTOMER_ID).inc()

    predictions = _predict(transaction_id, transaction)
    if predictions["is_fraud"]:
        FRAUD_COUNTER.inc()
    return predictions
