import sys
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import split,decode,substring
import pyspark.sql.functions as f
from pyspark.sql.types import *
from pyspark.ml.feature import VectorAssembler
from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.feature import IndexToString, StringIndexer, VectorIndexer
from pyspark.ml.feature import PCA
from pyspark.ml.feature import StandardScaler

brokers = '10.128.0.55'
topic = 'lab7'
model_path = "gs://ch18b013/pipeline_model"
spark = SparkSession \
        .builder \
        .appName("IrisSub") \
        .getOrCreate()
spark.sparkContext.setLogLevel("WARN")
df_d = spark \
     .readStream \
     .format("kafka") \
     .option("kafka.bootstrap.servers", brokers) \
     .option("subscribe", topic) \
     .load()
schema = StructType() \
         .add("Id", FloatType()) \
         .add("SepalLengthCm", FloatType()) \
         .add("SepalWidthCm", FloatType()) \
         .add("PetalLengthCm", FloatType()) \
         .add("PetalWidthCm", FloatType()) \
         .add("Species", StringType())
df_d = df_d.select(f.from_json(f.decode(df_d.value, 'utf-8'), schema=schema).alias("input"))
df_d = df_d.select("input.*") # Select only from input
df_d = df_d.withColumn("label", df_d["Species"])
# df_d = df_d.drop('sepal_width')
# cols = df_d.drop('Species').columns

from pyspark.ml import PipelineModel
model = PipelineModel.load(model_path)

# assembler = VectorAssembler(inputCols=cols, outputCol = 'features')
# labelIndexer = StringIndexer(inputCol = "Species", outputCol = "indexedLabel").fit(df_d)
# scaler = StandardScaler(inputCol = "features", outputCol = "scaledFeatures", withStd = False, withMean = True)
# pca_1 = PCA(k = 3, inputCol = 'scaledFeatures', outputCol = 'pcaFeature')
# rf_1 = RandomForestClassifier(labelCol = "indexedLabel", featuresCol = "pcaFeature", numTrees = 10)
# labelConverter = IndexToString(inputCol = "prediction", outputCol = "predictedLabel", labels = labelIndexer.labels)
# pipeline_1 = Pipeline(stages = [labelIndexer, assembler, scaler, pca_1, rf_1, labelConverter])
# model = pipeline_1.fit(df_d)

pred_df = model.transform(df_d)
pred_df = pred_df.select(pred_df.predictedLabel, pred_df.Species, pred_df.prediction, pred_df.indexedLabel)
def batch_func(df, epoch):
  from pyspark.sql import Row
  from pyspark.ml.evaluation import MulticlassClassificationEvaluator
  evaluator = MulticlassClassificationEvaluator(predictionCol='prediction',labelCol='indexedLabel',metricName='accuracy')
  acc = evaluator.evaluate(df)*100 
  acc_row = Row(ba=acc)
  acc_df = spark.createDataFrame([acc_row])
  acc_col = f"Batch {epoch} Accuracy"
  acc_df = acc_df.withColumnRenamed('ba',acc_col) 
  pred_lab_df = df.select(df.predictedLabel, df.Species)
  col1_name = f"Batch {epoch} predicted species" 
  col2_name = f"Batch {epoch} true species" 
  pred_lab_df = pred_lab_df.withColumnRenamed('pred_species',col1_name)
  pred_lab_df = pred_lab_df.withColumnRenamed('species',col2_name)
  pred_lab_df.write.format("console").save()
  acc_df.write.format("console").save()
query = pred_df \
        .writeStream \
        .option("truncate",False) \
        .foreachBatch(batch_func) \
        .start() \

query.awaitTermination()