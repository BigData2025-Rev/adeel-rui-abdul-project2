from pyspark.sql import SparkSession
from pyspark.sql.functions import rand, col, floor, lit, when
from utils import name_udf, product_udf, category_udf, pick_country_udf, get_price_udf, random_datetime_udf, rogue_value_udf, pick_failure_udf
import random
import os

spark = SparkSession.builder.appName("DataGenerator").config("spark.master", "local[*]").getOrCreate()

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
    .when((df["random_value"] >= 0.6) & (df["random_value"] < 0.80), "USA")
    .when((df["random_value"] >= 0.8) & (df["random_value"] < 0.90), "UK")
    .when((df["random_value"] >= 0.90) & (df["random_value"] < 0.95), "Germany")

    .otherwise(pick_country_udf(rand() * 4)  )
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
        ((df["product_name"] == "Laptop") |
         (df["product_name"] == "Smartphone") |
          (df["product_name"] == "Tablet") |
           (df["product_name"] == "Wireless Earbuds")  ) &
        (rand() <= 0.8) ,
        random_datetime_udf(lit([11,12]), df["country"])  
    ).
     when(
        ((df["product_name"] == "T-Shirt") |
         (df["product_name"] == "Shorts") |
          (df["product_name"] == "Air Conditioner") |
           (df["product_name"] == "Ceiling Fan") ) &
        (rand() <= 0.8) ,
        random_datetime_udf(lit([6,7,8]), df["country"])  
    ).
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
        (
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










