from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, trim

spark = SparkSession.builder \
    .appName("Data Cleaner") \
    .getOrCreate()

def clean_data(input_path, output_path):
    df = spark.read.option("header", True).csv(input_path)

    initial_count = df.count()
    print(f"Initial row count: {initial_count}")

    df = df.withColumn("dateTime", col("dateTime").cast("timestamp")) \
            .withColumn("price", col("price").cast("float")) \
            .withColumn("qty", col("qty").cast("int")) \
            .withColumn("failure_reason", trim(col("failure_reason")))

    df = df.filter((col("dateTime").isNotNull()) & (year(col("dateTime")).isin(2023, 2024)))

    df = df.filter(df["product_name"] != "UPDATE orders SET product_category=trash;")

    df = df.filter((col("price") > 0) & (col("qty") > 0))

    df = df.dropDuplicates(["order_id"])

    df = df.na.drop()

    cleaned_count = df.count()
    print(f"Cleaned row count: {cleaned_count}")
    print(f"Rows removed: {initial_count - cleaned_count}")

    df.coalesce(1).write.mode("overwrite").option("header", True).csv(output_path)
    spark.stop()

if __name__ == "__main__":
    input_path = "hdfs://localhost:9000/P2generated.csv"  # Input file path in HDFS
    output_path = "hdfs://localhost:9000/cleaned_P2generated"  # Output directory in HDFS

    clean_data(input_path, output_path)