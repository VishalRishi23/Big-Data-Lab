from pyspark import SparkContext, SQLContext
from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
import pyspark.sql.functions as f
from pyspark.mllib.evaluation import MulticlassMetrics
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from itertools import chain
from pyspark.sql.window import Window

import os

# setup arguments
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 pyspark-shell'

def eval_metrics(df, epoch_id):

    if df.count() > 0:
        eval_acc =  MulticlassClassificationEvaluator(labelCol="_c5", predictionCol="prediction", metricName="accuracy")
        eval_f1 =  MulticlassClassificationEvaluator(labelCol="_c5", predictionCol="prediction", metricName="f1")

        print("-"*50)
        print(f"Batch: {epoch_id}")
        print("-"*50)
        df.show(df.count())
        print(f"Accuracy: {eval_acc.evaluate(df):.4f}\nF1 score: {eval_f1.evaluate(df):.4f}")

    pass

spark = SparkSession.builder.appName("YELP").getOrCreate()
spark.sparkContext.setLogLevel("FATAL")

BROKER_IP = "10.162.0.2:9092"
topic = "yelp-topic"
MODEL = 'gs://ch18b013mkvr/model1/'

df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", BROKER_IP).option("subscribe", topic).load()


df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
split_cols = f.split(df.value,'%')

df = df.withColumn('_c5',split_cols.getItem(0))
df = df.withColumn('_c6',split_cols.getItem(1))

df.na.drop('any')
df = df.withColumn('_c5',df['_c5'].cast('double'))

df = df.filter('_c5 <= 5 and _c5 > 0')

model = PipelineModel.load(MODEL)


output_df = model.transform(df)

output_df = output_df.withColumn('correct' , f.when(f.col('prediction')==f.col('_c5'),1).otherwise(0))
df_acc = output_df.select(f.format_number(f.avg('correct')*100,2).alias('accuracy'))

output_df2 = output_df[['prediction','_c5']]
output_df2.createOrReplaceTempView('output')

query = output_df2.writeStream.foreachBatch(eval_metrics).start()
query.awaitTermination()