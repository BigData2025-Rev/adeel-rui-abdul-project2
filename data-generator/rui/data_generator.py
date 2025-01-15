import numpy as np
import pandas as pd
import os
import random
from datetime import datetime, timedelta
from collections import Counter

def data_generator():
  
  # read the products json file and store in pandas dataframe
  script_dir = os.path.dirname(os.path.abspath(__file__))
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
  Category_Weights = {category:0.5 for category in products['category'].unique()}
  Category_Weights['Computers'] = 6
  Category_Weights['FurnitureAndAppliances'] = 3.5
  Category_Weights['WarmClothing'] = 2.75
  Category_Weights['CoolingClothing'] = 2.75
  Category_Weights['Automotive'] = 2.5
  Category_Weights['Food'] = 2.5
  Category_Weights['Toys'] = 2.5


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
    isSpecialDate = 0.5 # rate of special date purchases
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
    pass
  
  def getOrderId():
    pass
  def getCustomerId():
    pass
  def getCustomerName():
    pass
  def getProductId(product):
    pass
  def getPaymentType(country):
    pass
  def getQty():
    pass
  def getPrice(product):
    pass
  def getWebsite():
    pass
  def getTxnId():
    pass
  def getSuccess():
    pass
  def getFailureReason():
    pass
  
  def test_getDate(testNum=10000):
    results = [getDate() for _ in range(testNum)]
    counter = Counter(results)

    special_count = sum(counter[date] for dates in Special_Dates.values() for date in dates)
    non_special_count = testNum - special_count

    special_breakdown = {key: sum(counter[date] for date in Special_Dates[key]) for key in Special_Dates.keys()}
    
    result = {
      "special_count": special_count,
      "non_special_count": non_special_count,
      "special_breakdown": special_breakdown,
      "total": testNum}
    print(result)
  
  def test_getCountry_City_Category(testNum=1000):
    # 存储测试结果
    country_results = []
    city_results = []
    category_results = []

    for _ in range(testNum):
        # 生成随机日期
        random_days = random.randint(0, (datetime(2024, 12, 31) - datetime(2024, 1, 1)).days)
        test_date = (datetime(2024, 1, 1) + timedelta(days=random_days)).strftime('%Y-%m-%d')

        # 获取国家和城市
        test_country = getCountry(test_date)
        test_city = getCity(test_country)

        # 获取分类
        test_category = getCategory(test_date, test_country)

        # 保存结果
        country_results.append(test_country)
        city_results.append(test_city)
        category_results.append(test_category)

    # 统计结果分布
    country_counter = Counter(country_results)
    city_counter = Counter(city_results)
    category_counter = Counter(category_results)

    # 打印统计结果
    print("Country Distribution:")
    for country, count in country_counter.items():
        print(f"{country}: {count} ({count / testNum * 100:.2f}%)")

    print("\nCity Distribution:")
    for city, count in city_counter.items():
        print(f"{city}: {count} ({count / testNum * 100:.2f}%)")

    print("\nCategory Distribution:")
    for category, count in category_counter.items():
        print(f"{category}: {count} ({count / testNum * 100:.2f}%)")

  test_getDate()
  test_getCountry_City_Category()
  

data_generator()