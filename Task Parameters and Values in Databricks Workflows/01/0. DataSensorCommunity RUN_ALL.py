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

dbutils.widgets.text("gps_latitude_begin", "50")
param_gps_latitude_begin = float(dbutils.widgets.get("gps_latitude_begin"))

dbutils.widgets.text("gps_latitude_end", "50.12")
param_gps_latitude_end = float(dbutils.widgets.get("gps_latitude_end"))

dbutils.widgets.text("gps_longitude_begin", "19.8")
param_gps_longitude_begin = float(dbutils.widgets.get("gps_longitude_begin"))

dbutils.widgets.text("gps_longitude_end", "20.1")
param_gps_longitude_end = float(dbutils.widgets.get("gps_longitude_end"))

# COMMAND ----------

# DBTITLE 1,Parameters
import requests
import pandas as pd
from pandas.io.json import json_normalize

job_folder = '/Users/maksim.pachkovskiy@t1a.com/Databricks parameters/01'

path_notebook_raw = f'/{job_folder}/1. DataSensorCommunity Download to RAW'
path_notebook_aggr = f'/{job_folder}/2. DataSensorCommunity RAW to AGGR'

# COMMAND ----------

# DBTITLE 1,Loading list sensors
url = 'https://data.sensor.community/static/v2/data.24h.json'

response = requests.get(url)
data = response.json()

df = pd.json_normalize(
  data, 'sensordatavalues', 
    ['id', 'sampling_rate','timestamp', 
      ['location', 'id'],
      ['location', 'latitude'],
      ['location', 'longitude'],
      ['location', 'altitude'],
      ['location', 'country'],
      ['location', 'exact_location'],
      ['location', 'indoor'],
                                
      ['sensor', 'id'],
      ['sensor', 'pin'],
      ['sensor', 'sensor_type', 'name'],
      ['sensor', 'sensor_type', 'manufacturer']
    ],
  record_prefix='sensor_'
)

df['location.latitude'] = df['location.latitude'].astype(float)
df['location.longitude'] = df['location.longitude'].astype(float)
df['sensor_value'] = df['sensor_value'].astype(float)
#display(df)

df_filtered = df[
                  (df['location.country'] == param_country_id) &
                  (df['location.latitude'] >= param_gps_latitude_begin) & (df['location.latitude'] <= param_gps_latitude_end) &
                  (df['location.longitude'] >= param_gps_longitude_begin) & (df['location.longitude'] <= param_gps_longitude_end) & 
                  (df['sensor_value_type'] == 'P1') &
                  (df['location.exact_location'] == 0) &
                  (df['location.indoor'] == 0)
                ]

df_filt = df_filtered[df_filtered['id'] > 0].copy()
df_filt.loc[:, 'sensor_type_id'] = (df_filt['sensor.sensor_type.name'].astype(str).str.lower() +
                                        '_sensor_' +
                                        df_filt['sensor.id'].astype(str))
#display(df_filt)

unique_sensor_id = df_filt['sensor_type_id'].unique()
unique_sensor_id_list = unique_sensor_id.tolist()
print("unique_sensor_id_list:", unique_sensor_id_list)

# COMMAND ----------

# DBTITLE 1,Run Notebooks
# run the Notebook_1
parameters = {
    "date_begin": str(date_begin),
    "date_end": str(date_end),
    "table_name": table_name,
    "unique_sensor_id_list": str(unique_sensor_id_list),
    "param_country_id": param_country_id,
    "param_city": param_city
}
dbutils.notebook.run(path_notebook_raw, 1200, arguments=parameters)

# run the Notebook_2
parameters = {
    "date_begin": str(date_begin),
    "date_end": str(date_end),
    "table_name": table_name,
    "param_country_id": param_country_id,
    "param_city": param_city
}
dbutils.notebook.run(path_notebook_aggr, 1200, arguments=parameters)