import os

import mlflow
from pyspark.ml.pipeline import PipelineModel
from pyspark.ml.classification import LogisticRegression, LogisticRegressionModel
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator

def get_spark():
    """return spark."""
    import findspark
    findspark.init()

    import pyspark
    from pyspark import SparkContext, SparkConf

    conf = SparkConf()

    conf.setAppName('practice_5')
    conf.set("spark.dynamicAllocation.enabled", "true")
    conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
    conf.set("spark.driver.maxResultSize", "4G")
    conf.set("spark.driver.memory", "4G")
    conf.set("spark.executor.memory", "4G")
    conf.set("spark.driver.allowMultipleContexts", "true")

    spark = pyspark.sql.SparkSession.builder.config(conf=conf).getOrCreate()
    
    return spark


if __name__ == "__main__":
    # enable pyspark autologs, metrics logging disabled for custom names
    spark = get_spark()
    mlflow.pyspark.ml.autolog(log_post_training_metrics=False)
    
    # set tracking uri (localhost for education)
    mlflow.set_tracking_uri(f"http://localhost:5000")
    
    # set exp name
    mlflow.set_experiment("fraud_transaction_model_evaluation")

    with mlflow.start_run(desctiption="practice_5_model_evaluation") as active_run:
        features = spark.read.parquet("practice_1/features.parquet")
        features = features.drop("date")

        # stratified train test split
        train = features.sampleBy(features.columns[1], fractions={0: 0.7, 1: 0.7})
        test = features.subtract(train)
        
        # fit simple log regression
        model = LogisticRegression(featuresCol=features.columns[0], labelCol=features.columns[1])
        fitted_model = model.fit(train)
        
        # get predictions (raw probs and predicted class)
        train = fitted_model.transform(train)
        test = fitted_model.transform(test)

        os.system("hdfs dfs -rm -r tmp_model")
        fitted_model.save("tmp_model")
        model_ex = LogisticRegressionModel.load("tmp_model")
        
        # temp model saving to local
        mlflow.spark.save_model(model_ex, "spark-model")
        
        # save model to object-storage as artifact
        mlflow.log_artifacts("spark-model")

        # log ROC/PR AUC metrics
        binary_evaluator = BinaryClassificationEvaluator(labelCol=features.columns[1])
        for metric_name in "areaUnderROC", "areaUnderPR":
            value = binary_evaluator.evaluate(test, {binary_evaluator.metricName: metric_name})
            mlflow.log_metric(f"test_{metric_name}", value)

            value = binary_evaluator.evaluate(train, {binary_evaluator.metricName: metric_name})
            mlflow.log_metric(f"train_{metric_name}", value)
        
        # log multiclass metrics for each class
        multiclass_evaluator = MulticlassClassificationEvaluator(labelCol=features.columns[1])
        for metric_label in 0, 1:
            for metric_name in "f1", "weightedPrecision", "weightedRecall", "accuracy":
                params = {multiclass_evaluator.metricName: metric_name, multiclass_evaluator.metricLabel: metric_label}
                value = multiclass_evaluator.evaluate(test, params)
                mlflow.log_metric(f"test_{metric_name}_label_{metric_label}", value)

                value = multiclass_evaluator.evaluate(train, params)
                mlflow.log_metric(f"train_{metric_name}_label_{metric_label}", value)
