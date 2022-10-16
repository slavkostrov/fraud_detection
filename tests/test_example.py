def func(x):
    return x + 1

import os
MODEL_NAME = os.getenv("MODEL_NAME", default="fraud_detection_sklearn_model")
MODEL_VERSION = os.getenv("MODEL_VERSION", default="latest")

def test_answer():
    assert func(3) == 4

def test_model():
    # check if model can be loaded 
    from sklearn.pipeline import Pipeline
    import mlflow
    
    mlflow.set_tracking_uri(os.getenv("MLFLOW_URI"))
    loaded_model = mlflow.pyfunc.load_model(f"models:/{MODEL_NAME}/{MODEL_VERSION}")
    feature_list = loaded_model._model_meta._signature.inputs.input_names()
    pipeline = loaded_model._model_impl
    
    assert isinstance(pipeline, Pipeline)
    assert isinstance(feature_list, str)
