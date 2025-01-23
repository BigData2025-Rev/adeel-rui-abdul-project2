from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, trim, when, row_number, regexp_replace, from_json, explode_outer, monotonically_increasing_id
from pyspark.sql.types import ArrayType, StringType
from pyspark.sql.window import Window

# import numpy as np
# import pandas as pd

warehouse_location = "hdfs://localhost:9000/user/revature/project2/"

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("DataProcessing") \
    .config("spark.master", "local[*]") \
    .getOrCreate()


df_origional = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(warehouse_location + "cleaned_dataset.csv")

from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml import Pipeline

"""
Machine Learning
- Apply machine learning to dataset.
-   given customer informations (country, city, payment type)
-   predict what category the customer is mostlikely to buy.
- Since this is a classifc question, we should use logistic regression
"""

df = df_origional.select("*")
df.printSchema()

def preprocessData(df):
  # select the columns we need
  selected_columns = ["country", "city", "payment_type", "product_category"]
  df = df.select(*selected_columns)

  # first we design the data preprocess stage, then connect hose stage as a pipeline
  # transform the target column "category" to numerical type for analysis
  label_indexer = StringIndexer(inputCol="product_category", outputCol="label")

  # transform the dependent columns to numerical type and the ntransfer to one-hot code
  # why? because it is easier for machine learning model to anaylsis numerical values
  # one hot encoder can avoid numerical size problem (we only use 1, 2, 3 as index, not number)
  categorical_columns = ["country", "city", "payment_type"]
  indexers = [StringIndexer(inputCol=col, outputCol=col+"_index") for col in categorical_columns]
  encoders = [OneHotEncoder(inputCol=col+"_index", outputCol=col+"_onehot") for col in categorical_columns]

  # assemble all dependent variable columns (one-hot format) to a single vector column 'features'
  assembler = VectorAssembler(inputCols=[col+"_onehot" for col in categorical_columns], outputCol="features")

  # pipeline, connect each data preprocess stage as a whole, then fit it
  # then use pipeline to transform the origional dataframe
  pipeline = Pipeline(stages=indexers + encoders + [assembler, label_indexer])
  df_prepared = pipeline.fit(df).transform(df)
  return df_prepared

df_prepared = preprocessData(df)
df_prepared.printSchema()

# Divide the dataset to training set and test set
# we use training set to train the model and use test set to test it.
train, test = df_prepared.select("features", "label").randomSplit([0.8, 0.2], seed=20250121)

"""
Since we are trying to predict the category, this is a multiclass classification problem
and since our dataset is pretty small,
I decide to use multinomial logistic regression
"""

# create logistic model, train.
from pyspark.ml.classification import LogisticRegression
lr = LogisticRegression(featuresCol="features", labelCol="label", maxIter=10, regParam=0.1, elasticNetParam=0.0)
lr_model = lr.fit(train)

# predict the model based on test dataset
predictions = lr_model.transform(test)
predictions.select("features", "label", "prediction").show()

# Evulate the model
# Accuracy
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
accuracy_evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")
accuracy = accuracy_evaluator.evaluate(predictions)
print(f"Accuracy: {accuracy}")

# Log loss
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
log_loss_evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="logLoss")
log_loss = log_loss_evaluator.evaluate(predictions)
print(f"Log Loss: {log_loss}")

# F1 score
f1_evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="f1")
f1_score = f1_evaluator.evaluate(predictions)
print(f"F1 Score: {f1_score}")

spark.stop()