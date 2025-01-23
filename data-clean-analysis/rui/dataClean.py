from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, when, regexp_replace, from_json, explode_outer, monotonically_increasing_id
from pyspark.sql.types import ArrayType, StringType

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
        .csv(warehouse_location + "P2generated.csv")



def columns_check(columns):
  result = True
  required_columns = set([
    "order_id", "customer_id", "customer_name", "product_id", "product_name",
    "product_category", "payment_type", "qty", "price", "datetime", "country",
    "city", "ecommerce_website_name", "payment_txn_id", "payment_txn_success",
    "failure_reason"])
  missing_columns = required_columns - set(columns)
  reduntant_columns = set(columns) - required_columns
  if len(missing_columns) != 0:
    print("Some columns missed: ", list(missing_columns))
    result = False
  if len(reduntant_columns) != 0:
    print("Some columns reduntant: ", list(reduntant_columns))
    result = False
  return result

def filter_null(df):
  df_filtered = df.filter(
    (col("order_id").isNotNull()) &
    (col("customer_id").isNotNull()) &
    (col("customer_name").isNotNull()) &
    (col("product_id").isNotNull()) &
    (col("product_name").isNotNull()) &
    (col("product_category").isNotNull()) &
    (col("payment_type").isNotNull()) &
    (col("qty").isNotNull()) &
    (col("price").isNotNull()) &
    (col("datetime").isNotNull()) &
    (col("country").isNotNull()) &
    (col("city").isNotNull()) &
    (col("ecommerce_website_name").isNotNull()) &
    (col("payment_txn_id").isNotNull()) &
    (col("payment_txn_success").isNotNull())
  )
  return df_filtered

df = df_origional.select("*")
print("The counts: " + str(df.count()))
df.printSchema()
"""
check columns name
"""
print("\nColumns check: ", columns_check(df.columns))
# columns name incorrect: dateTime->datetime, ecommerce_website->ecommerce_website_name
print("Fix columns...")
df = df.withColumnRenamed("dateTime", "datetime")
df = df.withColumnRenamed("ecommerce_website", "ecommerce_website_name")
print("Columns check: ", columns_check(df.columns))


"""
check if null value exists
"""
print("\nFilter null rows (except failure reason column.)")
print("The counts: " + str(df.count()))
df = filter_null(df)
print("The counts after filter null: " + str(df.count()))


"""
check order_id
- no duplicate
- transform to text datatype
- in format like: 4454673216
"""
print("\nCheck order id, transform to text and remove duplicate rows.")
df = df.dropDuplicates(["order_id"])
df = df.withColumn("order_id", col("order_id").cast("string"))
df = df.filter(col("order_id").rlike(r"^\d{10}$"))
print("The counts after check order id: " + str(df.count()))


"""
check customer id and customer name
"""
print("\nCheck customer id and customer name format, transform to text.")
df = df.withColumn("customer_id", col("customer_id").cast("string"))
df = df.filter(col("customer_id").rlike(r"^\d{10}$"))
df = df.filter(trim(col("customer_name")) != "")
print("The counts after check customer id and customer name: " + str(df.count()))


"""
check product id and product name, category, paymenttype, qty, price
"""
print("\nCheck product id and product name, category, paymenttype, qty, price.")
print("The counts: " + str(df.count()))
df = df.withColumn("product_id", col("product_id").cast("string"))
df = df.filter(col("product_id").rlike(r"^\d{10}$"))
print("The counts after check product id: " + str(df.count()))
df = df.filter(trim(col("product_name")) != "")
df = df.filter(~trim(col("product_name")).startswith("UPDATE")) # there is a strange product named； "UPDATE orders SET product_category=trash;"
print("The counts after check product name: " + str(df.count()))
df = df.filter(trim(col("product_category")) != "")
print("The counts after check product category: " + str(df.count()))
df = df.filter(trim(col("payment_type")) != "")
print("The counts after check payment type: " + str(df.count()))
df = df.filter(col("qty") > 0)
df = df.filter(col("price") > 0.0)
print("The counts after check qty and price: " + str(df.count()))


"""
check datetime
"""
print("\nCheck datetime format: YYYY-MM-DD HH:MM:SS")
print("The counts: " + str(df.count()))
df = df.filter(col("datetime").rlike(r"^(2023|2024)-((0[1-9])|(1[0-2]))-((0[1-9])|([1-2][0-9])|(3[0-1])) \d{2}:\d{2}:\d{2}$"))
# df = df.filter((col("datetime").startswith("2023")) | (col("datetime").startswith("2024")) )
print("The counts after check datetime: " + str(df.count()))


"""
check country, city, ecommerce_website_name
"""
print("\nCheck country, city, ecommerce website name")
print("The counts: " + str(df.count()))
df = df.filter(trim(col("country")) != "")
print("The counts after check country: " + str(df.count()))
df = df.filter(trim(col("city")) != "")
print("The counts after check city: " + str(df.count()))
df = df.filter(col("ecommerce_website_name").rlike(r"^https://"))
print("The counts after check ecommerce website name: " + str(df.count()))


"""
check payment txn id, payment_txn_success, failure_reason
"""
print("\nCheck payment txn id, payment txn success, failure reason")
print("One thing to notice: the empty reason in origional dataser is \" \", isntead of empty or null.")
print("I replaced empty reason to \"\".")
print("The counts: " + str(df.count()))
df = df.withColumn("payment_txn_id", col("payment_txn_id").cast("string"))
df = df.dropDuplicates(["payment_txn_id"])
df = df.filter(col("payment_txn_id").rlike(r"^\d{10}$"))
print("The counts after check payment txn id: " + str(df.count()))
df = df.filter(col("payment_txn_success").rlike(r"^(Y|N)$"))
print("The counts after check payment txn success: " + str(df.count()))
df = df.withColumn("failure_reason", when(col("failure_reason")==" ", "").otherwise(col("failure_reason")))
df = df.filter(
    ((col("payment_txn_success") == "Y") & (col("failure_reason") == "")) |
    ((col("payment_txn_success") == "N") & col("failure_reason").isNotNull() & (trim(col("failure_reason")) != ""))
)
print("The counts after check failure reason: " + str(df.count()))

"""
final check
"""
print("\nFinal check")
print("The counts: " + str(df.count()))
df.printSchema()

cleaned_dataset = df
cleaned_dataset.coalesce(1) .write.csv(warehouse_location + "cleaned_dataset/", header=True, mode="overwrite")

# hdfs dfs -rm /user/revature/project2/cleaned_dataset.csv
# hdfs dfs -mv /user/revature/project2/cleaned_dataset/part-* /user/revature/project2/cleaned_dataset.csv
# hdfs dfs -get /user/revature/project2/cleaned_dataset/part-* ./cleaned_dataset/cleaned_dataset.csv


spark.stop()
