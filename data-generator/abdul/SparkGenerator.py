from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, rand, when, expr, udf, floor, to_timestamp
from pyspark.sql.types import StringType, IntegerType
import random
import os

spark = SparkSession.builder.appName("DataGenerator").config("spark.master", "local[*]").getOrCreate()


listOfNames = [
    "Alice", "Bob", "Charlie", "Daisy", "Edward", "Fiona", "Grace", "Henry", "Irene", "Jack",
    "Karen", "Liam", "Mia", "Noah", "Olivia", "Paul", "Quinn", "Rachel", "Steve", "Tina",
    "Uma", "Victor", "Wendy", "Xavier", "Yara", "Zach", "Amanda", "Brian", "Chloe", "Daniel",
    "Emily", "Frank", "Gina", "Harry", "Isabelle", "Jacob", "Kate", "Lucas", "Molly", "Nathan",
    "Ophelia", "Peter", "Quinn", "Ruby", "Samuel", "Tessa", "Ulrich", "Vanessa", "Walter", "Xena",
    "Yvonne", "Zoe", "Adam", "Bella", "Caleb", "Diana", "Ethan", "Felicity", "Gavin", "Hazel",
    "Ivan", "Jenna", "Kyle", "Laura", "Marcus", "Nina", "Owen", "Paige", "Riley", "Sarah",
    "Thomas", "Ursula", "Veronica", "William", "Xavier", "Yasmine", "Zara", "Abigail", "Brandon",
    "Clara", "Derek", "Eliza", "Fabian", "Gemma", "Hugo", "Isla", "Jason", "Kylie", "Liam",
    "Melissa", "Noah", "Olivia", "Patrick", "Quentin", "Rose", "Simon", "Taylor", "Ulrika", "Victor",
    "Whitney"
]

products = [
    # Electronics
    "Smartphone", "Laptop", "Tablet", "Desktop PC", "Smartwatch", "Bluetooth Headphones",
    "Wireless Earbuds", "Gaming Console", "Digital Camera", "Action Camera", "Power Bank",
    "Smart Speaker", "Router", "Monitor", "External Hard Drive", "Keyboard", "Mouse",
    "Graphics Card", "Printer", "Drone",

    # Clothing
    "T-Shirt", "Jeans", "Hoodie", "Jacket", "Formal Shirt", "Skirt", "Dress", "Blouse",
    "Shorts", "Sweater", "Tracksuit", "Socks", "Hat", "Gloves", "Scarf", "Suit", "Tie",
    "Belt", "Sportswear", "Pajamas",

    # Home Appliances
    "Refrigerator", "Washing Machine", "Microwave Oven", "Air Conditioner", "Vacuum Cleaner",
    "Toaster", "Blender", "Electric Kettle", "Coffee Maker", "Dishwasher", "Rice Cooker",
    "Ceiling Fan", "Water Heater", "Induction Cooktop", "Mixer Grinder", "Air Purifier",
    "Juicer", "Iron", "Slow Cooker", "Food Processor",

    # Books
    "Fiction Novel", "Science Fiction", "Mystery Book", "Thriller Book", "Biography",
    "Self-Help Book", "Textbook", "Children's Book", "Fantasy Book", "History Book",
    "Cookbook", "Travel Guide", "Graphic Novel", "Poetry Book", "Philosophy Book",
    "Psychology Book", "Business Book", "Health and Fitness Book", "DIY Guide", "Encyclopedia",

    # Sports
    "Soccer Ball", "Basketball", "Tennis Racket", "Badminton Racket", "Cricket Bat",
    "Baseball Glove", "Hockey Stick", "Table Tennis Paddle", "Golf Club", "Yoga Mat",
    "Dumbbells"]

categories = {
    
        "electronics":
    ["Smartphone", "Laptop", "Tablet", "Desktop PC", "Smartwatch", "Bluetooth Headphones", 
    "Wireless Earbuds", "Gaming Console", "Digital Camera", "Action Camera", "Power Bank", 
    "Smart Speaker", "Router", "Monitor", "External Hard Drive", "Keyboard", "Mouse", 
    "Graphics Card", "Printer", "Drone"],

    "clothing":
    ["T-Shirt", "Jeans", "Hoodie", "Jacket", "Formal Shirt", "Skirt", "Dress", "Blouse", 
    "Shorts", "Sweater", "Tracksuit", "Socks", "Hat", "Gloves", "Scarf", "Suit", "Tie", 
    "Belt", "Sportswear", "Pajamas"],

    
    "home appliances":
    ["Refrigerator", "Washing Machine", "Microwave Oven", "Air Conditioner", "Vacuum Cleaner", 
    "Toaster", "Blender", "Electric Kettle", "Coffee Maker", "Dishwasher", "Rice Cooker", 
    "Ceiling Fan", "Water Heater", "Induction Cooktop", "Mixer Grinder", "Air Purifier", 
    "Juicer", "Iron", "Slow Cooker", "Food Processor"],

"books":
    ["Fiction Novel", "Science Fiction", "Mystery Book", "Thriller Book", "Biography", 
    "Self-Help Book", "Textbook", "Children's Book", "Fantasy Book", "History Book", 
    "Cookbook", "Travel Guide", "Graphic Novel", "Poetry Book", "Philosophy Book", 
    "Psychology Book", "Business Book", "Health and Fitness Book", "DIY Guide", "Encyclopedia"],

    "sports": ["Soccer Ball", "Basketball", "Tennis Racket", "Badminton Racket", "Cricket Bat", 
    "Baseball Glove", "Hockey Stick", "Table Tennis Paddle", "Golf Club", "Yoga Mat", 
    "Dumbbells", "Treadmill", "Exercise Bike", "Skipping Rope", "Swimming Goggles", 
    "Sports Shoes", "Running Shorts", "Gym Bag", "Resistance Bands", "Boxing Gloves"]
    }

prices = [
    ('Smartphone', 500),
    ('Laptop', 700),
    ('Tablet', 550),
    ('Desktop PC', 800),
    ('Smartwatch', 200),
    ('Bluetooth Headphones', 214),
    ('Wireless Earbuds', 300),
    ('Gaming Console', 200),
    ('Digital Camera', 300),
    ('Action Camera', 200),
    ('Power Bank', 100),
    ('Smart Speaker', 300),
    ('Router', 100),
    ('Monitor', 250),
    ('External Hard Drive', 80),
    ('Keyboard', 90),
    ('Mouse', 60),
    ('Graphics Card', 280),
    ('Printer', 300),
    ('Drone', 316),
    ('T-Shirt', 28),
    ('Jeans', 70),
    ('Hoodie', 23),
    ('Jacket', 104),
    ('Formal Shirt', 129),
    ('Skirt', 20),
    ('Dress', 45),
    ('Blouse', 25),
    ('Shorts', 46),
    ('Sweater', 37),
    ('Tracksuit', 80),
    ('Socks', 6),
    ('Hat', 20),
    ('Gloves', 10),
    ('Scarf', 30),
    ('Suit', 218),
    ('Tie', 25),
    ('Belt', 55),
    ('Sportswear', 12),
    ('Pajamas', 18),
    ('Refrigerator', 500),
    ('Washing Machine', 400),
    ('Microwave Oven', 52),
    ('Air Conditioner', 76),
    ('Vacuum Cleaner', 48),
    ('Toaster', 248),
    ('Blender', 161),
    ('Electric Kettle', 67),
    ('Coffee Maker', 45),
    ('Dishwasher', 300),
    ('Rice Cooker', 70),
    ('Ceiling Fan', 220),
    ('Water Heater', 309),
    ('Induction Cooktop', 192),
    ('Mixer Grinder', 100),
    ('Air Purifier', 153),
    ('Juicer', 157),
    ('Iron', 130),
    ('Slow Cooker', 225),
    ('Food Processor', 210),
    ('Fiction Novel', 16),
    ('Science Fiction', 19),
    ('Mystery Book', 33),
    ('Thriller Book', 21),
    ('Biography', 12),
    ('Self-Help Book', 48),
    ('Textbook', 21),
    ("Children's Book", 97),
    ('Fantasy Book', 36),
    ('History Book', 19),
    ('Cookbook', 17),
    ('Travel Guide', 87),
    ('Graphic Novel', 37),
    ('Poetry Book', 39),
    ('Philosophy Book', 37),
    ('Psychology Book', 35),
    ('Business Book', 95),
    ('Health and Fitness Book', 40),
    ('DIY Guide', 18),
    ('Encyclopedia', 18),
    ('Soccer Ball', 34),
    ('Basketball', 15),
    ('Tennis Racket', 39),
    ('Badminton Racket', 41),
    ('Cricket Bat', 35),
    ('Baseball Glove', 42),
    ('Hockey Stick', 16),
    ('Table Tennis Paddle', 68),
    ('Golf Club', 14),
    ('Yoga Mat', 21),
    ('Dumbbells', 42)
]

product_categories = ["Electronics", "Clothing", "Home Appliances", "Books", "Sports"]
payment_types = ["Debit Card", "Credit Card", "Cryptocurrency", "PayPal"]
failure_reasons = ["Insufficient Funds", "Network Issue", "Card Expired", "Other"]
countries = ["USA", "India", "UK", "Germany"]
rogue_values = ["Null", "None", " ", "-1", "!@$@", "_-", "..."]

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

rows = 15000
first_customer_id = 1000

df = spark.range(1, rows + 1).withColumnRenamed("id", "order_id")

df = df.withColumn("rogue_random", rand())

df = (df.withColumn("customer_id", (rand() * 100).cast("int")))

df = (df.withColumn("customer_name",when(df["rogue_random"] <= 0.05, rogue_value_udf(lit(rand() * 7)) ).otherwise(name_udf(df["customer_id"]))))
      
df = (df.withColumn("product_id", (rand() * 90).cast("int")))

df = (df.withColumn("product_name",when(df["rogue_random"] <= 0.05, rogue_value_udf(lit(rand() * 7)) ).otherwise(product_udf(df["product_id"]))))

df = (df.withColumn("product_category",when(df["rogue_random"] <= 0.05, rogue_value_udf(lit(rand() * 7))).otherwise(category_udf(df["product_name"]))))

df = (df.withColumn("random_value", rand()))


# The following will make sure which country buys from which category

df = df.withColumn(
    "country",
    when((df["random_value"] < 0.15) & (df["product_category"] == "clothing"), "USA")
    .when((df["random_value"] >= 0.15) & (df["random_value"] < 0.3) & (df["product_category"] == "Home Appliances"), "India")
    .when((df["random_value"] >= 0.3) & (df["random_value"] < 0.45) & (df["product_category"] == "books"), "UK")
    .when((df["random_value"] >= 0.45) & (df["random_value"] < 0.6) & (df["product_category"] == "electronics"), "Germany")
    .otherwise(pick_country_udf(rand() * 3)  )
)

df = df.withColumn("random_payment_val", rand())


# USA mostly pay with credit cards, India with PayPal, UK with cryptocurrency, and Germany with debit cards

df = df.withColumn("payment_type", when((df["country"] == "USA") & (df["random_payment_val"] < 0.6), "Credit Card").when((df["country"] == "Germany") & (df["random_payment_val"] < 0.6), "Debit Card").when((df["country"] == "India") & (df["random_payment_val"] < 0.6), "PayPal").when((df["country"] == "UK") & (df["random_payment_val"] <  0.6), "Cryptocurrency").otherwise("Debit Card"))



df = df.withColumn("city",  when(df["rogue_random"] <= 0.05, rogue_value_udf(lit(rand() * 7))).   
                            when((df["country"] == "USA") & (df["random_payment_val"] < 0.5), "LA").
                                when((df["country"] == "USA") & (df["random_payment_val"] >= 0.5), "New York").
                                when((df["country"] == "India") & (df["random_payment_val"] < 0.5), "New Delhi").
                                when((df["country"] == "India") & (df["random_payment_val"] >= 0.5), "Mumbai").
                                when((df["country"] == "UK") & (df["random_payment_val"] < 0.5), "London").
                                when((df["country"] == "UK") & (df["random_payment_val"] >= 0.5), "Manchester").
                                when((df["country"] == "Germany") & (df["random_payment_val"] < 0.5), "Berlin").
                                when((df["country"] == "Germany") & (df["random_payment_val"] >= 0.5), "Hamburg"))

# The following shows the quantity of sales per country which will determine who has the highest sales per location

df = df.withColumn("qty", when((df["city"] == "LA") | (df["city"] == "New York"),floor(rand() * (15-1) + 1)).
                                when((df["city"] == "Mumbai") | (df["city"] == "New Delhi"),floor(rand() * (5-1) + 1)).
                                when((df["city"] == "Manchester") | (df["city"] == "London"),floor(rand() * (7-2) + 1)).
                                when((df["city"] == "Berlin") | (df["city"] == "Hamburg"),floor(rand() * (10-2) + 2)))

# Prices for India are 4 times cheaper than other countries

df = df.withColumn("price", when(df["country"] == "India", (get_price_udf(df["product_id"]) * df["qty"]) / 4)
                   .otherwise(get_price_udf(df["product_id"]) * df["qty"])
)

# People from the US mostly order from Amazon, people from India mostly order from Ali Express
# People from Germany mostly order from Ebay, people from the UK mostly order from Argos
# While all the counries order from Ebay or Ali Express

df = df.withColumn("ecommerce_website_name", when((df["country"] == "USA") & (df["random_payment_val"] < 0.8), "Amazon").
                                             when((df["country"] == "India") & (df["random_payment_val"] < 0.8), "Ali Express").
                                             when((df["country"] == "Germany") & (df["random_payment_val"] < 0.8), "Ebay").
                                             when((df["country"] == "UK") & (df["random_payment_val"] < 0.8), "Argos").
                                             otherwise(when(rand() < 0.5, "Ali Express").otherwise("Ebay")))

df = df.withColumn("payment_txn_id", df["order_id"] + 10000)

df = df.withColumn("random_success_val", rand())

# USA and India have higher chance of payment failures compared to Germany and UK which only have 10% of the failures

df = df.withColumn("payment_txn_success", when( ((df["country"] == "Germany") | (df["country"] == "UK")) &
                                               (df["random_success_val"] <= 0.1), "N").
                                          when( ((df["country"] == "India") | (df["country"] == "USA")) &
                                                (df["random_success_val"] <= 0.3), "N").
                                          otherwise("Y"))

df = df.withColumn("random_failure_val", rand())

# Most people in the USA will have incufficient reason for payment failures while India will have network issues

df = df.withColumn("failure_reason", when( df["payment_txn_success"] == "Y", "N/A" ).
                                     when( (df["country"] == "USA") & ( df["random_failure_val"] <= 0.5 ),
                                          "Incufficient Funds").
                                     when( (df["country"] == "India") & ( df["random_failure_val"] <= 0.5 ),
                                          "Network error").
                                     otherwise( pick_failure_udf( floor( (rand() * 3) ) ) ) )

# The following will show the type of products typically bought by each country, season of the year and 
# what time the items are ususally bought the most

df = df.withColumn(
    "datetime",
    when(
        ((df["product_name"] == "Socks") |
         (df["product_name"] == "Hat") |
         (df["product_name"] == "Jacket") |
         (df["product_name"] == "Gloves") |
         (df["product_name"] == "Pajamas")) &
        (rand() <= 0.8) & (df["country"] == "USA"),
        random_datetime_udf(lit([12,1]), df["country"])  # December
    ).when(
        ((df["product_name"] == "Fiction Novel") |
         (df["product_name"] == "Mystery Book") |
         (df["product_name"] == "Fantasy Book") |
         (df["product_name"] == "History Book") |
         (df["product_name"] == "Cookbook") ) &
        (rand() <= 0.8) & (df["country"] == "UK"),
        random_datetime_udf(lit([3,9]), df["country"])  # March
    ).when(
        ((df["product_name"] == "Coffee Maker") |
         (df["product_name"] == "Toaster") |
         (df["product_name"] == "Air Conditioner") |
         (df["product_name"] == "Ceiling Fan") |
         (df["product_name"] == "Iron")) &
        (rand() <= 0.8) & (df["country"] == "India"),
        random_datetime_udf(lit([6,7,8]), df["country"])  # June
    ).when(
        ((df["product_name"] == "Laptop") |
         (df["product_name"] == "Smartphone") |
         (df["product_name"] == "Tablet") |
         (df["product_name"] == "Desktop PC") |
         (df["product_name"] == "Smartwatch") |
         (df["product_name"] == "Gaming Console") |
         (df["product_name"] == "Bluetooth Headphones")) &
        (rand() <= 0.8) & (df["country"] == "Germany"),
        random_datetime_udf(lit([10,11,12]), df["country"])  # November
    ).otherwise(random_datetime_udf(lit([1,2,3,4,5,6,7,8,9,10,11,12]), df["country"]))
)

df = df.drop("random_failure_val","rogue_random", "random_success_val", "random_value", "random_payment_val")

df.printSchema()

print(df.count())

df = df.coalesce(1)  # Combine all partitions into a single partition


output_directory = "output_directory"


df.write.csv(output_directory, mode="overwrite", header=True)

print(f"Data saved to: csv_data.csv")
for filename in os.listdir(output_directory):
    if filename.startswith("part-") and filename.endswith(".csv"):
        os.rename(
            os.path.join(output_directory, filename),
            os.path.join(output_directory, "combined_output.csv")
        )
        break

print(f"Data saved to: {os.path.join(output_directory, 'combined_output.csv')}")










