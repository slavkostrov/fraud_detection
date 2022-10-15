import findspark
import os
os.environ['SPARK_KAFKA_VERSION'] = '0.10'
findspark.init()

import time
import numpy as np
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import SparkSession

packages = [
    f'org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.3',
    "org.apache.kafka:kafka-clients:0.10.2.12",
    "org.apache.commons:commons-pool2:2.8.0"
]

if __name__ == "__main__":
    spark = SparkSession.builder\
       .appName("kafka-example")\
       .master("local[*]")\
       .config("spark.jars.packages", ",".join(packages))\
       .config("spark.driver.extraClassPath", "/home/ubuntu/.ivy2/jars/org.apache.kafka_kafka-clients-3.0.0.jar")\
       .config("spark.executor.extraClassPath", "/home/ubuntu/.ivy2/jars/org.apache.kafka_kafka-clients-3.0.0.jar")\
       .getOrCreate()

    features = spark.read.parquet("./practice_1/features.parquet")
    features.cache()

    features_size = features.count()
    to_array = F.udf(lambda v: v.toArray().tolist(), T.ArrayType(T.FloatType()))

    while True:
        print("*" * 100)
        sample_size = np.random.randint(1, min(5, features_size))


        data_sample = features.sample(fraction=sample_size / features_size)

        data_sample = data_sample.select(F.to_json(to_array("scaledFeatures")).alias("value"))

        print(f"Writing {sample_size} to inb topic.")
        data_sample = (
            data_sample
            .selectExpr("CAST(value AS STRING)")
            .write
            .format("kafka")
            .option("kafka.bootstrap.servers", "localhost:9092")
            .option("topic", "inb")
            .save()
        )
        print("Done.")

