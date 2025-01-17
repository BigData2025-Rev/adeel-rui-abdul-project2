import numpy as np
import pandas as pd
import os
import random
from datetime import datetime, timedelta
from collections import Counter
import time

def data_generator(num=15000):

  script_dir = os.path.dirname(os.path.abspath(__file__))

  order_id_counter = 0
  customers = pd.read_csv(os.path.join(script_dir, "data", "customers.csv"))

  # read the products json file and store in pandas dataframe
  products_path = os.path.join(script_dir, "data", "products.csv")
  products = pd.read_csv(products_path)
  
  """
  categories:
    'FurnitureAndAppliances', 'Computers', 'Food', 'Condiments',
    'WarmClothing', 'CoolingClothing', 'Beverages', 'Toys',
    'BabyProducts', 'Books', 'Music', 'Movies', 'Games',
    'OfficeEquipment', 'HealthAndBeauty', 'Automotive',
    'SportsAndOutdoors'
  """
  Category_Weights = {category:1 for category in products['category'].unique()}
  Category_Weights['Computers'] = 4
  Category_Weights['FurnitureAndAppliances'] = 2
  Category_Weights['WarmClothing'] = 1
  Category_Weights['CoolingClothing'] = 1
  Category_Weights['Automotive'] = 1.5
  Category_Weights['Food'] = 1.5
  Category_Weights['Beverages'] = 1.5
  Category_Weights['Toys'] = 1.2


  Special_Dates = {
    "SpringFestival": ["2024-02-07", "2024-02-08", "2024-02-09", "2024-02-10", "2024-02-11", "2024-02-12", "2024-02-13"],
    "Christmas": ["2024-12-22", "2024-12-23", "2024-12-24", "2024-12-25", "2024-12-26", "2024-12-27"],
    "BlackFriday": ["2024-11-29", "2024-11-30", "2024-12-01", "2024-12-02", "2024-12-03", "2024-12-04", "2024-12-05"],
    "NewYear": ["2024-01-01", "2024-12-31"]
  }
  Special_Dates_Weights = {
    "SpringFestival": 0.3,
    "Christmas": 0.4,
    "BlackFriday": 0.2,
    "NewYear": 0.1
  }

  cities_path = os.path.join(script_dir, "data", "Cities_Countries_Continents.csv")
  Cities = pd.read_csv(cities_path)
  Country_Weights = {country: 1 for country in Cities["Country"].unique()}
  Country_Weights['China'] = 2
  Country_Weights['Japan'] = 1.2
  Country_Weights['South Korea'] = 1.2
  Country_Weights['United States'] = 2

  """
  define trendline
  先是日期，圣诞节，春节，黑色星期五，几率更高
  然后是地区，美国，中国最高，然后日本，韩国，欧洲国家等
  - 根据传入的日期，如果是春节，中国，日本高。黑色星期五，圣诞节等，则欧美高
  然后是城市，随机吧，也可以弄些权重。
  然后是标签，传入参数：日期，地区。
  - 产品本身就有权重，PC，调味料等产品的权重高
  - 然后，根据传入的地区参数
    - 如果是中国，调味料权重拉高
    - 如果是美国，PC权重拉高
    - etc
  - 然后，根据传入的时间参数
    - 如果是冬季，棉衣，取暖物品拉高
    - 如果是夏季，T恤，冰淇淋，空调，几率拉高
    - 如果是黑色星期五，家电，电脑，几率拉高
    - 如果是圣诞节，春节，食品几率拉高
  """
  

  def getDate():
    isSpecialDate = 0.25 # rate of special date purchases
    random_float = random.random()
    if random_float < isSpecialDate:
      # if festival date
      keys = list(Special_Dates.keys())
      special_date = random.choices(keys, weights=[Special_Dates_Weights[key] for key in keys])[0]
      date = random.choice(Special_Dates[special_date])
      return date
    else:
      # if not, return a random date
      random_days = random.randint(0, (datetime(2024,12,31)-datetime(2024,1,1)).days)
      random_date = datetime(2024,1,1) + timedelta(days=random_days)
      date = random_date.strftime('%Y-%m-%d')
      return date
  
  def getCountry(date):
    weights = Country_Weights.copy()
    
    # Add weights based on festival.
    # Asia Country with more weight in Spring Festival
    # While EU and NA with more weights in chrismas and black friday
    if date in Special_Dates["SpringFestival"]:
      for country in Cities[Cities['Continent']=="Asia"]['Country']:
        weights[country] += 1
    elif date in Special_Dates["Christmas"] or date in Special_Dates["BlackFriday"]:
      for country in Cities[Cities['Continent']=="North America"]['Country']:
        weights[country] += 1
      for country in Cities[Cities['Continent']=="Europe"]['Country']:
        weights[country] += 1
    country = random.choices(list(weights.keys()), weights=list(weights.values()), k=1)[0]
    return country
  
  def getCity(Country):
    cities_in_country = Cities[Cities['Country'] == Country]['City']
    if cities_in_country.empty:
      return None
    return random.choice(cities_in_country.tolist())
  
  def getCategory(date, country):
    # 首先初始化所有的权重都是1
    # 然后：是圣诞节，xx+权重。是冬季，xx+权重……
    """
    categories:
      'FurnitureAndAppliances', 'Computers', 'Food', 'Condiments',
      'WarmClothing', 'CoolingClothing', 'Beverages', 'Toys',
      'BabyProducts', 'Books', 'Music', 'Movies', 'Games',
      'OfficeEquipment', 'HealthAndBeauty', 'Automotive',
      'SportsAndOutdoors'
    """
    weights = Category_Weights.copy()
    # Spring Festival
    if date in Special_Dates["SpringFestival"]:
      weights['Computers'] += 1
      weights['Food'] += 1
      weights['Condiments'] += 1
      weights['Books'] += 0.7
      weights['Music'] += 0.7
      weights['Games'] += 1
      weights['Movies'] += 0.7
      # Spring festival, people in Asia buy more foods and Condiments
      continent_series = Cities[Cities["Country"] == country]["Continent"]
      if not continent_series.empty and continent_series.iloc[0] == "Asia":
        weights['Computers'] += 1
        weights['HealthAndBeauty'] += 4
        weights['Food'] += 3
        weights['Condiments'] += 2
    # Chrisms
    if date in Special_Dates["Christmas"]:
      weights['Food'] += 3
      weights['Toys'] += 3
      weights['Condiments'] += 2
      weights['Books'] += 0.7
      weights['Music'] += 0.7
      weights['Games'] += 1
      weights['Movies'] += 0.7
    # Black Friday, people buy more electronics and furniture
    if date in Special_Dates["BlackFriday"]:
      weights['FurnitureAndAppliances'] += 3
      weights['Computers'] += 3
      weights['Games'] += 3
      weights['Automotive'] += 3
    
    month = datetime.strptime(date, "%Y-%m-%d").month
    if month in [12, 1, 2]:
      weights['WarmClothing'] += 3
    elif month in [6, 7, 8]:
      weights['CoolingClothing'] += 3
    
    result = random.choices(list(weights.keys()), weights=list(weights.values()), k=1)[0]
    return result

  def getProduct(date, country, category):
    weights = {name:1 for name in products[products['category']==category]['name'].unique()}

    # some detail weight modification
    if category == 'Food':
      # for food
      continent_series = Cities[Cities["Country"] == country]["Continent"]
      if not continent_series.empty and continent_series.iloc[0] == "Asia":
        # if asian country, buy rice more
        weights['Rice'] += 4
        if date in Special_Dates["SpringFestival"]:
          # if asian country and spring festival
          weights['Spring Festival Dumplings'] += 4
          weights['Spring Festival Rice Cakes'] += 4
      if date in Special_Dates["Christmas"]:
        # if not asian country but in christmas
        weights['Christmas Gingerbread Cookies'] += 4
        weights['Christmas Roasted Turkey'] += 8
    if category == "Toys":
      if date in Special_Dates["SpringFestival"] or date in Special_Dates["Christmas"]:
        weights['Gift Toy'] += 1
    
    result = random.choices(list(weights.keys()), weights=list(weights.values()), k=1)[0]
    return result

  def getOrderId():
    return int(time.time() * 1000)
  
  def getCustomerId(country):
    return random.choice(customers[customers['country'] == country]['id'].tolist())
  
  def getCustomerName(id):
    return customers[customers['id'] == id]['name'].iloc[0]
  
  def getProductId(product):
    return products[products['name'] == product]['id'].iloc[0]
  
  def getPaymentType(country):
    if country == "China":
      return random.choice(['Weixin', 'Taobao', 'Paypal'])
    return random.choice(['Visa', 'Apple Pay', 'Paypal'])
  
  def getQty(category):
    """
    categories:
      'FurnitureAndAppliances', 'Computers', 'Food', 'Condiments',
      'WarmClothing', 'CoolingClothing', 'Beverages', 'Toys',
      'BabyProducts', 'Books', 'Music', 'Movies', 'Games',
      'OfficeEquipment', 'HealthAndBeauty', 'Automotive',
      'SportsAndOutdoors'
    """
    if category in ['FurnitureAndAppliances', 'Computers', 'Automotive']:
      return random.choice([1,1,1,1,1,1,1,1,1,1,2])
    elif category in ['Food', 'Condiments', 'Beverages', 'Toys']:
      return random.choice([1,1,1,2,2,3,4])
    elif category in ['OfficeEquipment']:
      return random.choice([1,1,1,2,2,2,2,3,3,3,3,4,4,4,4,5,5,5,5,6,6,6,6,10,12,13,14])
    
    return random.choice([1,1,1,1,1,1,1,2,2,2,2,3,3,4])
  
  def getPrice(product):
    return products[products['name'] == product]['price'].iloc[0]
  
  def getWebsite(country):
    websites = ['www.amazon.com', 'www.ftatacliq.com', 'www.ebay.com']
    if country == 'China':
      websites = []
      websites.append('www.taobao.com')
      websites.append('www.tianmao.com')
      return random.choice(websites)
    return random.choice(websites)
  
  def getTxnId():
    return int(time.time() * 1000)
  
  def getSuccess():
    seed = random.random()
    if seed > 0.99:
      return 'N'
    return 'Y'
  
  def getFailureReason(success, country):
    if success == 'Y':
      return ""
    reasons = ['Invalid CVV', 'You failed because I want you fail, go report me!', 'Insufficient deposit', 'Account not recognized.']
    if country == 'China':
      reasons.append('Network Error.')
      reasons.append('Network Error.')
      reasons.append('Network Error.')
      reasons.append('Network Error.')
    return random.choice(reasons)  

  def generateOneRecord():
    date = getDate()
    country = getCountry(date)
    city = getCity(country)
    category = getCategory(date, country)
    product = getProduct(date, country, category)

    orderId = getOrderId()
    customerId = getCustomerId(country)
    customerName = getCustomerName(customerId)
    payment = getPaymentType(country)
    qty = getQty(category)
    price = getPrice(product)
    website = getWebsite(country)
    txnid = getTxnId()
    success = getSuccess()
    reason = getFailureReason(success, country)

    newRecord = {
      "order_id": orderId,
      "customer_id": customerId,
      "customer_name": customerName,
      "product_id": getProductId(product),
      "product_name": product,
      "product_category": category,
      "payment_type": payment,
      "qty": qty,
      "price": price,
      "datetime": date,
      "country": country,
      "city": city,
      "ecommerce_website_name": website,
      "payment_txn_id": txnid,
      "payment_txn_success": success,
      "failure_reason": reason,
    }
    return newRecord

  def generateRecords(num):
    return [generateOneRecord() for _ in range(num)]

  return pd.DataFrame(generateRecords(num))


def rogue_records(df, percent=0.05):
    
    df = df.copy()

    for index, row in df.iterrows():
        # seed to determine if rogue the record
        seed = random.random()
        if seed < percent:
            if seed < 0.01:
                df.at[index, 'datetime'] = "2077-10-31" if seed < 0.005 else "1900-01-01"
            elif seed < 0.02:
                df.at[index, 'qty'] = -random.randint(1, 100)
            elif seed < 0.03:
                df.at[index, 'order_id'] = None 
            elif seed < 0.04:
                df.at[index, 'payment_txn_success'] = 'WOW!WOW!WOW!WOW!' 
            else:
                df.at[index, 'customer_name'] = None 
    return df

whole_df = data_generator()
final_df = rogue_records(whole_df, 0.05)

print(whole_df['qty'].unique())
print(final_df['qty'].unique())

script_dir = os.path.dirname(os.path.abspath(__file__))
whole_df.to_csv(os.path.join(script_dir, "data", "whole_df.csv"), index=False)
final_df.to_csv(os.path.join(script_dir, "data", "final_df.csv"), index=False)