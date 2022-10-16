"""FastAPI fraud detection model inference example"""

import os
from typing import Optional, List

import numpy as np
import pandas as pd
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from sklearn.pipeline import Pipeline
import mlflow

app = FastAPI()

MODEL_NAME = os.getenv("MODEL_NAME", default="fraud_detection_sklearn_model")
MODEL_VERSION = os.getenv("MODEL_VERSION", default="latest")

mlflow.set_tracking_uri(os.getenv("MLFLOW_URI"))
if (
    os.getenv("AWS_ACCESS_KEY_ID") is None or 
    os.getenv("AWS_SECRET_ACCESS_KEY") is None or
    os.getenv("MLFLOW_S3_ENDPOINT_URL") is None or
    os.getenv("MLFLOW_URI") is None or
    os.getenv("AWS_S3_ENDPOINT_URL") is None
):
    raise RuntimeError("Missing env variable (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, MLFLOW_S3_ENDPOINT_URL, AWS_S3_ENDPOINT_URL, MLFLOW_URI).")


class Model:
    pipeline: Optional[Pipeline] = None
    feature_list: List[str]


class CustomerFeatures:
    features: pd.DataFrame


class TerminalFeatures:
    features: pd.DataFrame


class Transaction(BaseModel):
    TX_AMOUNT: float
    TX_TIME_SECONDS: float
    TERMINAL_ID: int
    CUSTOMER_ID: int


@app.on_event("startup")
def load_model():
    loaded_model = mlflow.pyfunc.load_model(f"models:/{MODEL_NAME}/{MODEL_VERSION}")
    Model.feature_list = loaded_model._model_meta._signature.inputs.input_names()
    Model.pipeline = loaded_model._model_impl

    CustomerFeatures.features = pd.read_parquet("customer_features.parquet")
    TerminalFeatures.features = pd.read_parquet("terminal_features.parquet")



@app.get("/")
def read_healthcheck():
    return {"status": "Green", "version": "0.2.0"}


@app.post("/predict")
def predict(transaction_id: int, transaction: Transaction):
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
