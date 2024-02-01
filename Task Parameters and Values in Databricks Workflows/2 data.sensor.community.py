# Databricks notebook source
# DBTITLE 1,Loading list sensors
import requests
import pandas as pd
from pandas.io.json import json_normalize

url = 'https://data.sensor.community/static/v2/data.24h.json'
#url = 'https://data.sensor.community/airrohr/v1/filter/country=PL'
#url = 'https://data.sensor.community/airrohr/v1/filter/box=50,19.8,50.12,20.1'
#url = 'https://data.sensor.community/airrohr/v1/filter/area=50.062649,19.945527,10'

response = requests.get(url)
data = response.json()

df = pd.json_normalize(data, 'sensordatavalues', 
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
                    record_prefix='sensor_')

df['location.latitude'] = df['location.latitude'].astype(float)
df['location.longitude'] = df['location.longitude'].astype(float)
df['sensor_value'] = df['sensor_value'].astype(float)
#display(df)

# GPS Krakow
filtered_df = df[
                  (df['location.country'] == 'PL') &
                  (df['location.latitude'] >= 50) & (df['location.latitude'] <= 50.12) &
                  (df['location.longitude'] >= 19.8) & (df['location.longitude'] <= 20.1) & 
                  (df['sensor_value_type'] == 'P2') &
                  (df['location.exact_location'] == 0) &
                  (df['location.indoor'] == 0)
                ]

filtered_df['sensor_type_id'] = filtered_df['sensor.sensor_type.name'].astype(str).str.lower() + '_sensor_' + filtered_df['sensor.id'].astype(str)

display(filtered_df)

unique_sensor_id = filtered_df['sensor_type_id'].unique()
unique_sensor_id_list = unique_sensor_id.tolist()
print(unique_sensor_id_list)

# COMMAND ----------

# DBTITLE 1,Download history
import pandas as pd

df_csv = pd.DataFrame()

for id in unique_sensor_id_list:
  url_csv = f'https://archive.sensor.community/2024-01-10/2024-01-10_{id}.csv'
  print(url_csv)

  try:
    response = requests.get(url_csv)
    response.raise_for_status()
    df_temp = pd.read_csv(url_csv, sep=';')
    df_csv = pd.concat([df_csv, df_temp], ignore_index=True)
  except requests.exceptions.HTTPError as err:
    print(f"FAIL: {url_csv}. ERROR: {err}")

display(df_csv)

# COMMAND ----------

df_csv['timestamp'] = pd.to_datetime(df_csv['timestamp'])
display(df_csv)

# COMMAND ----------

# DBTITLE 1,AVG group DAY
df_csv['timestamp'] = pd.to_datetime(df_csv['timestamp'])

df_csv['time_date'] = df_csv['timestamp'].dt.date
df_csv['time_hour'] = df_csv['timestamp'].dt.hour

df_csv['time_date'] = df_csv['time_date'].astype(str)
df_csv['time_hour'] = df_csv['time_hour'].astype(str).str.zfill(2)
df_csv['datetime'] = df_csv['time_date'] + ' ' + df_csv['time_hour'] + ':00:00'
df_csv['datetime'] = pd.to_datetime(df_csv['datetime'])

df_csv_avg_day_hour = df_csv.groupby(['sensor_id', 'datetime'], as_index=False).mean()
display(df_csv_avg_day_hour)

#df_csv_avg = df_csv.groupby(['sensor_id'], as_index=False)[['P1','P2']].mean()

#display(df_csv_avg)

# COMMAND ----------

# DBTITLE 1,AVG group total DAY
df_csv_avg_total  = df_csv_avg_day_hour.groupby(['sensor_id'], as_index=False)[['P1','P2']].mean()
display(df_csv_avg_total)

df_csv_avg_totals = df_csv_avg_total[['P1','P2']].mean()
display(df_csv_avg_totals)