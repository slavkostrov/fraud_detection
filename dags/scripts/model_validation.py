import findspark
findspark.init()

import os

import mlflow
from mlflow.client import MlflowClient
from pyspark.ml.pipeline import PipelineModel
from pyspark.ml.classification import LogisticRegression, LogisticRegressionModel
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from scipy.stats import ttest_ind

import numpy as np
import pandas as pd
from sklearn.metrics import f1_score 

def get_spark():
    """return spark."""
    import findspark
    findspark.init()

    import pyspark
    from pyspark import SparkContext, SparkConf

    conf = SparkConf()

    conf.setAppName('practice_6')
    conf.set("spark.dynamicAllocation.enabled", "true")
    conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
    conf.set("spark.driver.maxResultSize", "4G")
    conf.set("spark.driver.memory", "4G")
    conf.set("spark.executor.memory", "4G")
    conf.set("spark.driver.allowMultipleContexts", "true")
    conf.set("spark.jars.packages", "")

    spark = pyspark.sql.SparkSession.builder.config(conf=conf).getOrCreate()
    
    return spark

def get_bs_metrics(model, df, num_iterations=3000):
    np.random.seed(42)
    predictions = model.transform(df)
    
    predictions = predictions.toPandas()
    y = df.select("TX_FRAUD").toPandas()
    
    df_w_predictions = pd.DataFrame(
        {"y": y["TX_FRAUD"], "prediction": predictions["prediction"]}
    )

    bs_metrics = []
    for _ in range(num_iterations):
        sample = df_w_predictions.sample(frac=1.0, replace=True)
        bs_metrics.append(f1_score(sample["y"], sample["prediction"]))

    bs_metrics = pd.DataFrame({"value": bs_metrics})
    return bs_metrics

def val(config):    
    mlflow.set_tracking_uri(f"http://localhost:5000")
    
    spark = get_spark()
    val = spark.read.parquet("practice_1/val.parquet")     

    client = MlflowClient(f"http://localhost:5000")
    latest_model_version = client.get_latest_versions("fraud_detection_model")[-1].version

    current_prod_model = mlflow.spark.load_model("models:/fraud_detection_model/production")
    latest_model = mlflow.spark.load_model(f"models:/fraud_detection_model/{latest_model_version}")

    print(f"Latest model version is {latest_model_version}")
    print(current_prod_model)

    current_prod_model_metrics = get_bs_metrics(current_prod_model, val)
    latest_model_metrics = get_bs_metrics(latest_model, val)
    
    test_result = ttest_ind(
        current_prod_model_metrics["value"], latest_model_metrics["value"], alternative="less"
    )
    print(f"ttest_result = {test_result}")
    pvalue = test_result.pvalue

    NEW_MODEL_BETTER = pvalue < 0.05
    if NEW_MODEL_BETTER:
        print(f"New model is better, pvalue - {pvalue}, sending it to production...")
        client.transition_model_version_stage(
            name="fraud_detection_model",
            version=latest_model_version,
            stage="Production",
            archive_existing_versions=True,
        )
    else:
        print(f"Current production model is better, pvalue - {pvalue}, production will not updated.")
