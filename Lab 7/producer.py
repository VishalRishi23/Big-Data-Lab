from __future__ import absolute_import
from time import sleep
from kafka import KafkaProducer
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import json
brokers = '10.128.0.55'
topic = 'lab7'
data_path = "iris.csv"
producer = KafkaProducer(bootstrap_servers=[brokers], value_serializer=lambda x: x.encode('utf-8') )
spark = SparkSession \
    .builder \
    .appName("IrisPub") \
    .getOrCreate()
schema = StructType() \
  .add("Id", FloatType()) \
  .add("SepalLength", FloatType()) \
  .add("SepalWidth", FloatType()) \
  .add("PetalLength", FloatType()) \
  .add("PetalWidth", FloatType()) \
  .add("Species", StringType())
iris_data = spark.read.csv(data_path,header=True,schema=schema)
cnt = 0
for row in iris_data.toJSON().collect():
    producer.send(topic, value=row)
    sleep(1)