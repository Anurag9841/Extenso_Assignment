from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *
import json

spark = SparkSession.builder.appName("spark_dataframe_py").config("spark.jars", "C:\spark-3.5.1-bin-hadoop3\jars\mysql-connector-j-8.4.0.jar").getOrCreate()
url = "jdbc:mysql://localhost:3306/extenso_config"
properties = {
    "user": "root",
    "password": "root",
    "driver": "com.mysql.jdbc.Driver"
}

def table(table_name, start_date, end_date):
  df = spark.read.jdbc(url=url, table=table_name, properties=properties)
  if table_name == "rw_transaction_data":
    df = df.filter((df.last_modified_date <= to_date(lit(end_date))) & (df.last_modified_date >= to_date(lit(start_date))))
  return df
product_category_map = table("product_category_map",'2023-01-01','2024-01-01')
rw_transaction_data = table("rw_transaction_data",'2023-01-01','2024-01-01')

def most_used_product(joined):
    most_used_product = joined.groupBy("product_name").count().fillna(0)
    most_used_product = most_used_product.orderBy("count",ascending=[0])
    top_10 =most_used_product.select("product_name").take(10)
    top_product = [row['product_name'] for row in top_10]
    return top_product

def mapping(product_category_map = product_category_map,rw_transaction_data = rw_transaction_data):
    joined = rw_transaction_data.join(product_category_map, ['product_id', 'product_type_id', 'module_id'])
    joined = joined.withColumn("months", month(col("last_modified_date")))
    top_product = most_used_product(joined)
    filtered_df = joined.filter(col("product_name").isin(top_product))
    product_used_count = filtered_df.groupBy("payer_account_id", "product_name").pivot("months").count().fillna(0)
    cols_to_change = ['1', '2', '3', '4', '5']
    for column in cols_to_change:
        product_used_count = product_used_count.withColumn(column, when(col(column) > 0, 1).otherwise(col(column)))
    product_used_count = product_used_count.withColumn("used_map",
                                                       concat(col("1"), col("2"), col("3"), col("4"), col("5")))
    return product_used_count.show(n=37)

print(mapping())
