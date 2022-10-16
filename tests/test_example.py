def func(x):
    return x + 1


def test_answer():
    assert func(3) == 4

def test_model():
    from sklearn.pipeline import Pipeline
    import mlflow
    import os
    
    mlflow.set_tracking_uri(os.getenv("MLFLOW_URI"))
    loaded_model = mlflow.pyfunc.load_model(f"models:/{MODEL_NAME}/{MODEL_VERSION}")
    feature_list = loaded_model._model_meta._signature.inputs.input_names()
    pipeline = loaded_model._model_impl
    
    assert isinstance(pipeline, Pipeline)
    assert isinstance(feature_list, str)
