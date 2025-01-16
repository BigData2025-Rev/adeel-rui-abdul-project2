from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, DoubleType, BooleanType, TimestampType
from datetime import timedelta, datetime
import uuid
import names
import random
from lists import product_dict, quantity_ranges, peak_dates_by_country, country_specific_weights, season_weights, holiday_weights, price_dict, product_categories, retailers, payment_type, countries, usa_cities, germany_cities, uk_cities, japan_cities, india_cities, payment_failure_reason

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
def get_product_id(product_name):
    product_list = list(product_dict.keys())
    return product_list.index(product_name) + 1001 if product_name in product_list else None

def get_season(month):
    """Determine the season based on the month."""
    if month in [12, 1, 2]:
        return "Winter"
    elif month in [3, 4, 5]:
        return "Spring"
    elif month in [6, 7, 8]:
        return "Summer"
    elif month in [9, 10, 11]:
        return "Fall"

# Make certain catagories more popular in certain countries
# Seasonal products more popular in various seasons
# Weekend boost
# Time of day boost
def adjust_product_weights(country, date):
    dynamic_weights = product_dict.copy()

    # Season Adjustment
    season = get_season(date.month)
    if season in season_weights:
        for category, boost in season_weights[season].items():
            for product in dynamic_weights.keys():
                if product in category:
                    dynamic_weights[product] += boost

    # Holiday Adjustment
    month_day = date.strftime("%m-%d")
    if month_day in holiday_weights:
        for category, boost in holiday_weights[month_day].items():
            for product in dynamic_weights.keys():
                if product in category:
                    dynamic_weights[product] += boost
    
    # Country Specific Adjustment
    if country in country_specific_weights:
        for product, boost in country_specific_weights[country].items():
            if product in dynamic_weights:
                dynamic_weights[product] += boost

    return dynamic_weights



# PRODUCT NAME (Random from list, weights for different products based on popularity)
def get_random_product(country, date):
    products = list(product_dict.keys())
    weights = list(adjust_product_weights(country, date).values())
    
    return random.choices(products, weights=weights)[0]

def get_product_category(product_id):
    for category, ids in product_categories.items():
        if product_id in ids:
            return category

# PRICE (Use price list + variation based on retailer)
def get_product_price(product, retailer):
    price = price_dict[product]
    if retailer == "Amazon":
        return price * 0.97
    elif retailer == "Target":
        return price * 1.03
    return price

# PRODUCT CATEGORY (Based on location in list, same for every retailer)
# PAYMENT TYPE (Random from card (50%), bank transfer (20%), apple pay (15%), paypal (15%))
def get_payment_type():
    return random.choices(payment_type, weights=[50, 20, 15, 15])[0]

# QTY (Realistic based on category)
# ELECTRONICS 1 - 3
# CLOTHING 1 - 8
# HOME APPLIANCES 1 - 2
# TOYS 1 - 3
# BOOKS 1 - 3
# TOOLS 1 - 2
# SPORTING GOODS 1 - 4
def generate_quantity(category):
    quantity_range = quantity_ranges.get(category)
    return random.choice(quantity_range)

# DATETIME (More people ordering during peak times, days (weights))
# 2024 (full year)
# more people order on: Black Friday, Cyber Monday, Christmas, Weekends
# Time based on time zone of city, weights based on time:
# 12 AM - 6 AM 10%
# 6 AM - 9 AM 5%
# 9 AM - 12 PM 15%
# 12 PM - 3 PM 25%
# 3 PM - 7 PM 15%
# 7 PM - 10 PM 25%
# 10 PM - 12 AM 5%
def getDateTime(city, country):
    start_date = datetime(2024, 1, 1)
    end_date = datetime(2024, 12, 31)
    all_dates = [start_date + timedelta(days=i) for i in range((end_date - start_date).days + 1)]
    
    peak_dates = peak_dates_by_country(country, [])

    date_weights = []
    for date in all_dates:
        weight = 10 
        if date.weekday() >= 5:
            weight += 10
        if date.strftime("%m-%d") in peak_dates:
            weight += 20
        date_weights.append(weight)
    
    actual_date = random.choice(all_dates, weights=date_weights)[0]
        


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
    return "Y" if success else "N", reason

schema = StructType([
    StructField("order_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("customer_name", StringType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("product_name", StringType(), True),
    StructField("product_category", StringType(), True),
    StructField("payment_type", StringType(), True),
    StructField("qty", IntegerType(), True),
    StructField("price", DoubleType(), True),
    StructField("datetime", TimestampType(), True),
    StructField("country", StringType(), True),
    StructField("city", StringType(), True),
    StructField("ecommerce_website_name", StringType(), True),
    StructField("payment_txn_id", StringType(), False),
    StructField("payment_txn_success", StringType(), True),
    StructField("failure_reason", StringType(), True)
])

# Process

def generate_order():
    country, city = get_country_city()
    customer_name = generate_name()
    customer_id = generate_customer_id(customer_name, country)
    product_name = get_random_product(country, datetime.datetime.now())
    product_id = get_product_id(product_name)
    product_category = get_product_category(product_id)
    retailer = get_retailer()
    quantity = generate_quantity(product_category)
    price = get_product_price(product_name, retailer)
    payment_type = get_payment_type()
    datetime_ordered = getDateTime(city, country)
    payment_success, failure_reason = generate_payment_status()

    return {
        "order_id": generate_unique_id(),
        "customer_id": customer_id,
        "customer_name": customer_name,
        "product_id": product_id,
        "product_name": product_name,
        "product_category": product_category,
        "payment_type": payment_type,
        "qty": quantity,
        "price": price,
        "datetime": datetime_ordered,
        "country": country,
        "city": city,
        "ecommerce_website_name": retailer,
        "payment_txn_id": generate_unique_id(),
        "payment_txn_success": payment_success,
        "failure_reason": failure_reason
    }