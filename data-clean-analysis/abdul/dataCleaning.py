from pyspark.sql import SparkSession
from pyspark.sql.functions import col, isnan, when, count
import os

spark = SparkSession.builder.appName("Data-cleaning")\
    .config("spark.master", "local[*]")\
    .getOrCreate()

print(spark.sparkContext.getConf().getAll())

absolute_path = os.path.join(os.getcwd(), "P2generated.csv")

path = f"file://{absolute_path}"

print("Path: ", path)

df = spark.read.format("csv").option("header", "true").load(path)

df.printSchema()

#Show columns with the amount of null values 

df.select([count(when(col(c).isNull() | isnan(c), c)).alias(c) for c in df.columns]).show() 

#Delete the null values from table dateTime

df_cleaned = df.dropna(subset=["dateTime"])

#Display the cleaned dataframe

df_cleaned.select([count(when(col(c).isNull() | isnan(c), c)).alias(c) for c in df.columns]).show()




df_cleaned.coalesce(1).write.csv("file:///home/tareq/testPythonJan8/project2cleaning/output.csv", header=True, mode="overwrite")







