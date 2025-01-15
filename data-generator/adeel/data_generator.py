from pyspark import SparkContext, SparkConf
import datetime
import time
import names

NUM_ORDERS = 15000
sc = SparkContext("local", "DataGenerator")

# FIELDS
# ORDER ID (Random for each order)
# CUSTOMER ID (Hash first, last, country same for same combinations (same customer)) 
# CUSTOMER NAME (Use names library)
def get_name():
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




# ORDER ID (Unique ID for each order)
def generate_order_id():
    pass

def get_random_product():
    pass