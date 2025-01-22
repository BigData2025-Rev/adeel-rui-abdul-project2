from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, row_number
from pyspark.sql.window import Window


spark = SparkSession.builder \
    .appName("Data Analyzer") \
    .getOrCreate()

def top_selling_product_category(input_path, output_path):
    
    df = spark.read.csv(input_path, header=True, inferSchema=True)

    sales_by_category = (
        df.groupBy("country", "product_category")
        .agg(sum("qty").alias("total_quantity"))
    )

    win = Window.partitionBy("country").orderBy(col("total_quantity").desc())

    ranked = sales_by_category.withColumn("rank", row_number().over(win))

    top_categories = ranked.filter(col("rank") == 1).select(
        col("rank"),
        col("country"),
        col("product_category").alias("category"),
        col("total_quantity").alias("qty")
    )

    top_categories.coalesce(1).write.csv(output_path, header=True, mode="overwrite")

input_path = "hdfs://localhost:9000/data.csv"
output_path = "hdfs://localhost:9000/top_selling.csv"

top_selling_product_category(input_path, output_path)