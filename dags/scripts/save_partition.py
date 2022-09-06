import findspark
findspark.init()

import pyspark
from pyspark import SparkContext, SparkConf

conf = SparkConf()
conf.setAppName('practice_2')
conf.set("spark.dynamicAllocation.enabled", "true")
conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
conf.set("spark.driver.maxResultSize", "4G")
conf.set("spark.driver.memory", "4G")
conf.set("spark.executor.memory", "4G")
conf.set("spark.driver.allowMultipleContexts", "true")

spark = pyspark.sql.SparkSession.builder.config(conf=conf).getOrCreate()
table = spark.read.parquet("/user/ubuntu/practice_1/transactions_table_final.parquet")

table.write.mode("OVERWRITE").partitionBy("date").parquet("./all_transactions_table.parquet")

