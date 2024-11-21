from __future__ import print_function 
from pyspark.context import SparkContext
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.sql.session import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import PCA
import numpy as np
import pyspark.sql.functions as f
import pyspark.sql.types

from pyspark.ml.linalg import Vectors
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.util import MLUtils
from pyspark.ml.feature import StandardScaler
from pyspark.sql import Row
from pyspark.sql.types import DoubleType
from pyspark.ml import Pipeline
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.feature import StringIndexer, VectorIndexer
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.feature import IndexToString, StringIndexer, VectorIndexer
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import PCA 

# Clean load the data and clean it

sc = SparkContext()
spark = SparkSession(sc)
spark_df = spark.read.format("bigquery").option("table", "iris.iris").load()
clean_data = spark_df.withColumn("label", spark_df["Species"])
cols = spark_df.drop('Species').columns
assembler = VectorAssembler(inputCols=cols, outputCol = 'features')
labelIndexer = StringIndexer(inputCol = "Species", outputCol = "indexedLabel").fit(spark_df)

# Feature engineering on the data 

scaler = StandardScaler(inputCol = "features", outputCol = "scaledFeatures", withStd = False, withMean = True)
pca_1 = PCA(k = 3, inputCol = 'scaledFeatures', outputCol = 'pcaFeature')
pca_2 = PCA(k = 2, inputCol = 'scaledFeatures', outputCol = 'pcaFeature')
pca_3 = PCA(k = 1, inputCol = 'scaledFeatures', outputCol = 'pcaFeature')

# Train and test sets

(trainingData, testData) = spark_df.randomSplit([0.8, 0.2])

# We apply the best ML model to train the data

rf_1 = RandomForestClassifier(labelCol = "indexedLabel", featuresCol = "pcaFeature", numTrees = 10)
rf_2 = RandomForestClassifier(labelCol = "indexedLabel", featuresCol = "scaledFeatures", numTrees = 10)
rf_3 = RandomForestClassifier(labelCol = "indexedLabel", featuresCol = "pcaFeature", numTrees = 10)
rf_4 = RandomForestClassifier(labelCol = "indexedLabel", featuresCol = "pcaFeature", numTrees = 10)
labelConverter = IndexToString(inputCol = "prediction", outputCol = "predictedLabel", labels = labelIndexer.labels)

# We then use the models to fit the data for maximum accuracy on the test dataset

pipeline_1 = Pipeline(stages = [labelIndexer, assembler, scaler, pca_1, rf_1, labelConverter])
pipeline_2 = Pipeline(stages = [labelIndexer, assembler, scaler, rf_2, labelConverter])
pipeline_3 = Pipeline(stages = [labelIndexer, assembler, scaler, pca_2, rf_3, labelConverter])
pipeline_4 = Pipeline(stages = [labelIndexer, assembler, scaler, pca_3, rf_4, labelConverter])
model_1 = pipeline_1.fit(trainingData)
model_2 = pipeline_2.fit(trainingData)
model_3 = pipeline_3.fit(trainingData)
model_4 = pipeline_4.fit(trainingData)
predictions_1 = model_1.transform(testData)
predictions_2 = model_2.transform(testData)
predictions_3 = model_3.transform(testData)
predictions_4 = model_4.transform(testData)
evaluator = MulticlassClassificationEvaluator(labelCol = "indexedLabel", predictionCol = "prediction", metricName = "accuracy")
accuracy_1 = evaluator.evaluate(predictions_1)
accuracy_2 = evaluator.evaluate(predictions_2)
accuracy_3 = evaluator.evaluate(predictions_3)
accuracy_4 = evaluator.evaluate(predictions_4)

# Accuracy on our model

print("Accuracy on Test Dataset - Pipeline 1 = %g" % (accuracy_1))
print("Accuracy on Test Dataset - Pipeline 2 = %g" % (accuracy_2))
print("Accuracy on Test Dataset - Pipeline 3 = %g" % (accuracy_3))
print("Accuracy on Test Dataset - Pipeline 4 = %g" % (accuracy_4))

model_1.save('gs://ch18b013/pipeline_model')