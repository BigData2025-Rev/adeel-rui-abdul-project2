import random
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, IntegerType
from config import listOfNames, products, categories, prices, failure_reasons, rogue_values, countries

def get_name(index):
    return listOfNames[index]

name_udf = udf(get_name, StringType())

def get_product_name(index):
    index = int(index)
    return products[index]

product_udf = udf(get_product_name, StringType())

def find_category(value):
    for key, val_array in categories.items():
        if value in val_array:
            return key

category_udf = udf(find_category, StringType())

def get_price(index):
    return prices[index][1]


get_price_udf = udf(get_price, IntegerType())


def pick_failure(rand):
    return failure_reasons[rand] 

pick_failure_udf = udf(pick_failure, StringType())


def random_datetime_by_month(monthList, country):
    month = random.choice(monthList)
    day = random.randint(1, 28 if month == 2 else 30 if month in [4, 6, 9, 11] else 31)
    year = random.randint(2020, 2024)  # Random year
    chance = random.random()

    # Generate a random time
    if country == "USA" and  chance < 0.8:  # 80% chance to be between 4 PM and 7 PM
        hour = random.randint(16, 19)  # 4 PM to 7 PM
    elif country == "UK" and chance < 0.8:  # 80% chance to be between 12AM and 3AM
        hour = random.randint(0, 3)  # 12AM to 3AM
    elif country == "Germany" and  chance < 0.8:  # 80% chance to be between 3AM and 6AM
        hour = random.randint(3, 6)  # 3 AM to 6 AM
    elif country == "India" and chance < 0.8:  # 80% chance to be between 9 AM and 12 PM
        hour = random.randint(9, 12)  # 9 AM to 12 PM
    else:  # 20% chance for other times
        hour = random.randint(0, 23)
    minute = random.randint(0, 59)
    second = random.randint(0, 59)

    return f"{year}-{month:02d}-{day:02d} {hour:02d}:{minute:02d}:{second:02d}"


random_datetime_udf = udf(random_datetime_by_month, StringType())

def pick_country(index):
    index = int(index)
    return countries[index]

pick_country_udf = udf(pick_country, StringType())

def rogue_value(index):
    index = int(index)
    return rogue_values[index]

rogue_value_udf = udf(rogue_value, StringType())