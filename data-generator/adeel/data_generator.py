from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, DoubleType, BooleanType, TimestampType
import datetime
import time
import names
from lists import product_list, price_list, retailers, payment_type, countries, usa_cities, uk_cities, japan_cities, india_cities

NUM_ORDERS = 15000

spark = SparkSession.builder.appName("data-generator")\
.config("spark.master", "local[*]")\
.getOrCreate()

# FIELDS
# ORDER ID (Random for each order)
def generate_order_id():
    pass


# CUSTOMER ID (Hash first, last, country same for same combinations (same customer)) 
def generate_customer_id(full_name, country):
    id = f"{full_name}{country}"
    hashed_id = id.encode('utf-8').hex()
    return hashed_id

# CUSTOMER NAME (Use names library)
def generate_name():
    return names.get_full_name()

# PRODUCT ID (product_names[index] + 1001)


# PRODUCT NAME (Random from list, weights for different products + (increased chance based on) time of day and day of year)
# PRODUCT CATEGORY (Based on location in list, same for every retailer)
# PAYMENT TYPE (Random from card (50%), bank transfer (20%), apple pay (15%), paypal (15%))
# QTY (Realistic based on category)
# PRICE (Use price list + variation based on retailer)
# DATETIME (More people ordering during peak times, days (weights))
# COUNTRY (USA (60%), Germany (10%), UK (10%), Japan (10%), India (10%))
# CITY (Random City based on country (no weights here, top 5 cities (by pop.) in each country))
# ECOMMERCE WEBSITE NAME (Amazon (50%), Target (20%), Walmart(30%))
# PAYMENT TRANSACTION ID (Random for each order)
# PAYMENT TRANSACTION SUCCESS (~90% Success, ~10% Denied)
# FAILURE REASON (Isufficent Funds (40%), Incorrect Information (20%), FRAUD (20%), TECHNICAL ISSUES (20%))

schema = StructType([
    StructField("OrderID", StringType(), False),
    StructField("CustomerID", StringType(), False),
    StructField("CustomerName", StringType(), True),
    StructField("Country", StringType(), True),
    StructField("City", StringType(), True),
    StructField("ProductID", IntegerType(), True),
    StructField("ProductName", StringType(), True),
    StructField("PaymentType", StringType(), True),
    StructField("Quantity", IntegerType(), True),
    StructField("Price", DoubleType(), True),
    StructField("TotalCost", DoubleType(), True),
    StructField("DateTimeOrdered", TimestampType(), True),
    StructField("Retailer", StringType(), True),
    StructField("PaymentSuccess", BooleanType(), True),
    StructField("FailureReason", StringType(), True)
])