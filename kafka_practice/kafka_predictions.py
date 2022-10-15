import findspark
import os
os.environ['SPARK_KAFKA_VERSION'] = '0.10'
findspark.init()

import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import SparkSession

from pyspark.sql.types import StructType, StructField, ArrayType, FloatType
from pyspark.ml.linalg import Vectors, VectorUDT
from pyspark.sql.functions import udf
import mlflow

OUT_TOPIC = "outb"
IN_TOPIC = "inb"
BOOTSTRAP_SERVERS = "localhost:9092"
PACKAGES = [
    f'org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.3',
    "org.apache.kafka:kafka-clients:0.10.2.12",
    "org.apache.commons:commons-pool2:2.8.0"
]

# S3 Creds
os.environ["AWS_ACCESS_KEY_ID"] = ""
os.environ["AWS_SECRET_ACCESS_KEY"] = ""
os.environ["MLFLOW_S3_ENDPOINT_URL"] = "https://storage.yandexcloud.net"
os.environ["AWS_S3_ENDPOINT_URL"] = "https://storage.yandexcloud.net"

if __name__ == "__main__":
    spark = SparkSession.builder\
       .appName("kafka-example")\
       .master("local[*]")\
       .config("spark.jars.packages", ",".join(PACKAGES))\
       .config("spark.driver.extraClassPath", "/home/ubuntu/.ivy2/jars/org.apache.kafka_kafka-clients-3.0.0.jar")\
       .config("spark.executor.extraClassPath", "/home/ubuntu/.ivy2/jars/org.apache.kafka_kafka-clients-3.0.0.jar")\
       .getOrCreate()
    mlflow.set_tracking_uri(f"http://localhost:5000")

    print("Getting current production model from MlFlow.")
    client = mlflow.client.MlflowClient(f"http://localhost:5000")
    current_prod_model = mlflow.spark.load_model("models:/fraud_detection_model/latest")
    
    print(f"Reading requests from '{IN_TOPIC}' topic.")
    requests_df = (
        spark
        .readStream
        .format("kafka") 
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS) 
        .option("failOnDataLoss", "false")
        .option("subscribe", IN_TOPIC) 
        .option("startingOffsets", "earliest") 
        .load()
        .selectExpr("CAST(value AS STRING)")
    )


    schema = StructType([ 
        StructField("scaledFeatures", ArrayType(FloatType()), True)
      ])

    array_from_json = F.udf(lambda v: eval(v), T.ArrayType(T.FloatType()))
    list_to_vector_udf = udf(lambda l: Vectors.dense(l), VectorUDT())

    requests_df = requests_df.select(list_to_vector_udf(array_from_json(F.col("value"))).alias("scaledFeatures"))
    requests_df_with_predictions = (
        current_prod_model
        .transform(requests_df)
        .select(
            F.to_json(F.struct('scaledFeatures', 'rawPrediction', 'probability', 'prediction')).alias("value")
        )
    )

    print(f"Writing answers to '{OUT_TOPIC}' topic.")
    write_predictions_ex = (
        requests_df_with_predictions
        .writeStream
        .format("kafka")
        .option("checkpointLocation", "./checkpoint/test_2")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("topic", OUT_TOPIC)
        .start()
    )
