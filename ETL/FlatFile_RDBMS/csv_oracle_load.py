import os

# import findspark
# findspark.init()
# import pandas as pd
# print(findspark)

from pyspark.sql import SparkSession

spark = SparkSession.builder\
    .master('local[*]')\
    .appName('Load_CSV_Oracle')\
    .config('spark.jars','file:///home//hduser//Documents//Dependencies//ojdbc8-19.3.0.0.jar')\
    .getOrCreate()

df = spark.read.format('csv')\
    .option("inferSchema", "true")\
    .option("header", "true")\
    .load('file:///home//hduser//Documents//Dataset//chats_2021-08.csv')

df.show()

print("Starting loading data into oracle table...")

df.write.format('jdbc').options(
      url='jdbc:oracle:thin:@192.168.211.130:1521/orcl',
      driver='oracle.jdbc.driver.OracleDriver',
      dbtable='chat_data',
      user='yogov',
      password='yogov',
      ).mode('append').save()


