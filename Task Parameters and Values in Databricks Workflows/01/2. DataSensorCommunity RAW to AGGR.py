# Databricks notebook source
# DBTITLE 1,Widgets
dbutils.widgets.text("date_begin", "2024-01-01")
date_begin = dbutils.widgets.get("date_begin")

dbutils.widgets.text("date_end", "2024-01-02")
date_end = dbutils.widgets.get("date_end")

dbutils.widgets.text("table_name", "test.data_sensor_community")
table_name = dbutils.widgets.get("table_name")

# Country - Poland
dbutils.widgets.text("country_id", "PL")
param_country_id = dbutils.widgets.get("country_id")

# GPS - Krakow
dbutils.widgets.text("city", "Krakow")
param_city = dbutils.widgets.get("city")

# COMMAND ----------

# DBTITLE 1,Parameters
import datetime
import pandas as pd

start_date = datetime.datetime.strptime(date_begin, "%Y-%m-%d").date()
print("start_date:", start_date)

end_date = datetime.datetime.strptime(date_end, "%Y-%m-%d").date()
print("end_date:", end_date)

table_name_raw = table_name + '_raw'
print(table_name_raw)

table_name_aggr = table_name + '_aggr'
print(table_name_aggr)

# COMMAND ----------

# DBTITLE 1,Load data from the RAW
df_spark = spark.sql(f"""
                     SELECT *
                     FROM {table_name_raw}
                     WHERE
                      country_id = '{param_country_id}' AND
                      city = '{param_city}' AND
                      (timestamp BETWEEN '{start_date}' AND '{end_date}')
                     """)

df_spark.display()

# COMMAND ----------

# DBTITLE 1,AVG group DAY
df_csv = df_spark.toPandas().copy()
#df_csv['timestamp'] = pd.to_datetime(df_csv['timestamp'])

df_csv['time_date'] = df_csv['timestamp'].dt.date
df_csv['time_hour'] = df_csv['timestamp'].dt.hour

df_csv['time_date'] = df_csv['time_date'].astype(str)
df_csv['time_hour'] = df_csv['time_hour'].astype(str).str.zfill(2)
df_csv['datetime'] = df_csv['time_date'] + ' ' + df_csv['time_hour'] + ':00:00'
df_csv['datetime'] = pd.to_datetime(df_csv['datetime'])

df_csv_avg_day_hour = df_csv.groupby(['country_id','city','sensor_id', 'datetime'], as_index=False).mean()
display(df_csv_avg_day_hour)

#df_csv_avg = df_csv.groupby(['sensor_id'], as_index=False)[['P1','P2']].mean()

#display(df_csv_avg)

# COMMAND ----------

# DBTITLE 1,Create the AGGR table if not exist
spark.sql(f"""
  CREATE TABLE IF NOT EXISTS {table_name_aggr} (
    country_id STRING,
    city STRING,
    datetime TIMESTAMP,
    sensor_id INT,
    location INT,
    lat FLOAT,
    lon FLOAT,    
    p1 FLOAT,
    p2 FLOAT
  )
""")

# COMMAND ----------

# DBTITLE 1,Update the AGGR table
existing_data = spark.sql(f"SELECT * FROM {table_name_aggr} WHERE country_id = '{param_country_id}' AND city = '{param_city}' AND (datetime BETWEEN '{start_date}' AND '{end_date}') ")
existing_data_cnt = existing_data.count()
print("existing_data_cnt in the TABLE", table_name_aggr, "=", existing_data_cnt)

df_csv_avg_day_hour_spark = spark.createDataFrame(df_csv_avg_day_hour)
df_csv_avg_day_hour_spark_cnt = df_csv_avg_day_hour_spark.count()
print("cnt in the df_csv_avg_day_hour =", df_csv_avg_day_hour_spark_cnt)

if existing_data_cnt > 0:
    spark.sql(f"DELETE FROM {table_name_aggr} WHERE country_id = '{param_country_id}' AND city = '{param_city}' AND (datetime BETWEEN '{start_date}' AND '{end_date}') ")
    print(f"- Deleted {existing_data_cnt} rows in the {table_name_aggr} where country_id = {param_country_id} AND city = {param_city} AND (datetime BETWEEN {start_date} AND {end_date})")

df_csv_avg_day_hour_spark_transposed = df_csv_avg_day_hour_spark.select('country_id','city','datetime','sensor_id','location','lat','lon','p1','p2')

df_csv_avg_day_hour_spark_transposed.write.mode('append').insertInto(table_name_aggr)
print(f"--- Added {df_csv_avg_day_hour_spark_cnt} rows in the {table_name_aggr} where country_id = {param_country_id} AND city = {param_city} AND (datetime BETWEEN {start_date} AND {end_date})")