# Databricks notebook source
# DBTITLE 1,Widgets
dbutils.widgets.text("date_begin", "2024-01-01")
date_begin = dbutils.widgets.get("date_begin")

dbutils.widgets.text("date_end", "2024-01-02")
date_end = dbutils.widgets.get("date_end")

dbutils.widgets.text("table_name", "test.data_sensor_community")
table_name = dbutils.widgets.get("table_name")

dbutils.widgets.text("unique_sensor_id_list", "")
unique_sensor_id = dbutils.widgets.get("unique_sensor_id_list")

# Country - Poland
dbutils.widgets.text("country_id", "PL")
param_country_id = dbutils.widgets.get("country_id")

# GPS - Krakow
dbutils.widgets.text("city", "Krakow")
param_city = dbutils.widgets.get("city")

# COMMAND ----------

# DBTITLE 1,Parameters
import datetime
import ast
import requests
import pandas as pd
from pandas.io.json import json_normalize

start_date = datetime.datetime.strptime(date_begin, "%Y-%m-%d").date()
print("start_date:", start_date)

end_date = datetime.datetime.strptime(date_end, "%Y-%m-%d").date()
print("end_date:", end_date)

table_name_raw = table_name + '_raw'
print("table_name_raw:", table_name_raw)

unique_sensor_id_list = ast.literal_eval(unique_sensor_id)
print("unique_sensor_id_list:", unique_sensor_id_list)

# COMMAND ----------

# DBTITLE 1,Download history
df_csv = pd.DataFrame()
current_date = start_date

while current_date <= end_date:
  print("START LOADING:", current_date, "..........")
    
  for id in unique_sensor_id_list:
    url_csv = f'https://archive.sensor.community/{current_date}/{current_date}_{id}.csv'
    print(url_csv)

    try:
      response = requests.get(url_csv)
      response.raise_for_status()
      df_temp = pd.read_csv(url_csv, sep=';')
      df_csv = pd.concat([df_csv, df_temp], ignore_index=True)
    except requests.exceptions.HTTPError as err:
      print(f"FAIL: {url_csv}. ERROR: {err}")
  
  current_date += datetime.timedelta(days=1)

df_csv['timestamp'] = pd.to_datetime(df_csv['timestamp'])

df_csv['country_id'] = param_country_id
df_csv['city'] = param_city

display(df_csv)

# COMMAND ----------

# DBTITLE 1,Create the RAW table if not exist
spark.sql(f"""
  CREATE TABLE IF NOT EXISTS {table_name_raw} (
    country_id STRING,
    city STRING,
    sensor_id INT,
    sensor_type STRING,
    location INT,
    lat FLOAT,
    lon FLOAT,
    timestamp TIMESTAMP,
    p1 FLOAT,
    p2 FLOAT
  )
""")

# COMMAND ----------

# DBTITLE 1,Update the RAW table
existing_data = spark.sql(f"SELECT * FROM {table_name_raw} WHERE country_id = '{param_country_id}' AND city = '{param_city}' AND (timestamp BETWEEN '{start_date}' AND '{end_date}') ")
existing_data_cnt = existing_data.count()
print("existing_data_cnt in the TABLE", table_name_raw, "=", existing_data_cnt)

df_csv_spark = spark.createDataFrame(df_csv)
df_csv_spark_cnt = df_csv_spark.count()
print("cnt in the df_csv =", df_csv_spark_cnt)

if existing_data_cnt > 0:
    spark.sql(f"DELETE FROM {table_name_raw} WHERE country_id = '{param_country_id}' AND city = '{param_city}' AND (timestamp BETWEEN '{start_date}' AND '{end_date}') ")
    print(f"- Deleted {existing_data_cnt} rows in the {table_name_raw} where country_id = {param_country_id} AND city = {param_city} AND (timestamp BETWEEN {start_date} AND {end_date})")

df_csv_spark_transposed = df_csv_spark.select('country_id','city','sensor_id','sensor_type','location','lat','lon','timestamp','p1','p2')

df_csv_spark_transposed.write.mode('append').insertInto(table_name_raw)
print(f"--- Added {df_csv_spark_cnt} rows in the {table_name_raw} where country_id = {param_country_id} AND city = {param_city} AND (timestamp BETWEEN {start_date} AND {end_date})")