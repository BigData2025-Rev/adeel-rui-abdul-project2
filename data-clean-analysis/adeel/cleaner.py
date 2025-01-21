from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Data Cleaner") \
    .getOrCreate()

def clean_data(input_path, output_path):
    df = spark.read.option("header", True).csv(input_path)

    initial_count = df.count()
    print(f"Initial row count: {initial_count}")

    cleaned_df = df.na.drop()
    cleaned_count = cleaned_df.count()
    print(f"Cleaned row count: {cleaned_count}")
    print(f"Rows removed: {initial_count - cleaned_count}")

    cleaned_df.write.mode("overwrite").option("header", True).csv(output_path)

if __name__ == "__main__":
    input_path = "hdfs://localhost:9000/P2generated.csv"  # Input file path in HDFS
    output_path = "hdfs://localhost:9000/cleaned_P2generated"  # Output directory in HDFS

    clean_data(input_path, output_path)