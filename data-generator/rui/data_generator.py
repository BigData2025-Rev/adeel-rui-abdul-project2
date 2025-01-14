import numpy as np
import pandas as pd
import os

# read the products json file and store in pandas dataframe
with open('./products_data/products.json', 'r') as file:
    data = pd.read_json(file)
categories = data['categories']
rows = []
for category, products in categories.items():
    for product in products:
        rows.append({"name": product["product"], "price": product["price"], "category": category})
products = pd.DataFrame(rows)
products['id'] = pd.util.hash_pandas_object(products['name'])
categories = products['category'].unique()

def data_generator():
  
  """
  define trendline
  先是日期，圣诞节，春节，黑色星期五，几率更高
  然后是地区，美国，中国最高，然后日本，韩国，欧洲国家等
  然后是城市，随机吧，也可以弄些权重。
  然后是产品，传入参数：日期，地区。
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
  
print(products.head())