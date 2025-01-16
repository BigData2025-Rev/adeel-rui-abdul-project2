from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, DoubleType, BooleanType, TimestampType
import datetime
import uuid
import names
import random
from lists import product_list, price_list, retailers, payment_type, countries, usa_cities, germany_cities, uk_cities, japan_cities, india_cities, payment_failure_reason

NUM_ORDERS = 15000

spark = SparkSession.builder.appName("data-generator")\
.config("spark.master", "local[*]")\
.getOrCreate()

# FIELDS

# ORDER ID (Random for each order)
# PAYMENT TRANSACTION ID (Random for each order)
def generate_unique_id():
    return str(uuid.uuid())[:8].upper()


# CUSTOMER ID (Hash first, last, country same for same combinations (same customer)) 
def generate_customer_id(full_name, country):
    id = f"{full_name}{country}"
    hashed_id = id.encode('utf-8').hex()
    return hashed_id

# CUSTOMER NAME (Use names library)
def generate_name():
    return names.get_full_name()

# PRODUCT ID (product_names[index] + 1001)
# PRODUCT NAME (Random from list, weights for different products based on popularity)
def get_random_product():
    pass


# PRODUCT CATEGORY (Based on location in list, same for every retailer)
# PAYMENT TYPE (Random from card (50%), bank transfer (20%), apple pay (15%), paypal (15%))
def get_payment_type():
    return random.choices(payment_type, weights=[50, 20, 15, 15])[0]

# QTY (Realistic based on category)
def generate_quantity(category):
    pass

# PRICE (Use price list + variation based on retailer)


# DATETIME (More people ordering during peak times, days (weights))
def getDateTime(city):
    pass


# COUNTRY (USA (60%), Germany (10%), UK (10%), Japan (10%), India (10%))
# CITY (Random City based on country (no weights here, top 5 cities (by pop.) in each country))
def get_country_city():
    country = random.choices(countries, weights=[60, 10, 10, 10, 10])[0]
    city = ""
    if country == "USA":
        city = random.choices(usa_cities, weights=[20, 20, 20, 20, 20])[0]
    elif country == "Germany":
        city = random.choices(germany_cities, weights=[20, 20, 20, 20, 20])[0]
    elif country == "UK":
        city = random.choices(uk_cities, weights=[20, 20, 20, 20, 20])[0]
    elif country == "Japan":
        city = random.choices(japan_cities, weights=[20, 20, 20, 20, 20])[0]
    elif country == "India":
        city = random.choices(india_cities, weights=[20, 20, 20, 20, 20])[0]
    return country, city

# ECOMMERCE WEBSITE NAME (Amazon (50%), Target (20%), Walmart(30%))
def get_retailer():
    return random.choices(retailers, weights=[50, 20, 30])[0]

# PAYMENT TRANSACTION SUCCESS (~90% Success, ~10% Denied)
# FAILURE REASON (Isufficent Funds (40%), Incorrect Information (20%), FRAUD (20%), TECHNICAL ISSUES (20%))
def generate_payment_status():
    success = random.choices([True, False], weights=[90, 10])[0]
    reason = random.choices(payment_failure_reason, weights=[40, 20, 20, 20])[0] if not success else None
    return success, reason

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

# Process