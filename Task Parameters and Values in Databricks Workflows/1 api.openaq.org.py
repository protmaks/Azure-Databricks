# Databricks notebook source
# DBTITLE 1,Parameters
import requests
import json
import pandas as pd

country_code = "PL" #you can change country code

# COMMAND ----------

# DBTITLE 1,Show Countries
url = f"https://api.openaq.org/v2/cities?country={country_code}&limit=1000"

headers = {"Accept": "application/json"}
response = requests.get(url, headers=headers)

response_content = response.content

decoded_content = response_content.decode('utf-8')
json_content = json.loads(decoded_content)

results = json_content['results']
df = pd.DataFrame(results).sort_values(by='count', ascending=False)

#display(df)

city_list = df['city'].tolist()
print(city_list)

# COMMAND ----------

# DBTITLE 1,Widgets
dbutils.widgets.dropdown("city_1", "Kraków", city_list)
city_1 = dbutils.widgets.get("city_1")

dbutils.widgets.dropdown("parameter_1", "pm25", ["pm10", "pm25"])
parameter_1 = dbutils.widgets.get("parameter_1")

dbutils.widgets.text("date_begin", "2024-01-15T23:59:59Z")
date_begin = dbutils.widgets.get("date_begin")

dbutils.widgets.text("date_end", "2024-01-16T23:59:59Z")
date_end = dbutils.widgets.get("date_end")

# COMMAND ----------

# DBTITLE 1,Get measurements
url = f"https://api.openaq.org/v2/measurements?country={country_code}&city={city_1}&date_from={date_begin}&date_to={date_end}&limit=100000"

headers = {"Accept": "application/json"}
response = requests.get(url, headers=headers)

response_content = response.content
#print(response_content)

decoded_content = response_content.decode('utf-8')
json_content = json.loads(decoded_content)

results = json_content['results']
df_measurements = pd.DataFrame(results)

#display(df_measurements)

df_measurements['datetime'] = df_measurements['date'].apply(lambda x: x['local'])

df_measurements['gps_latitude'] = df_measurements['coordinates'].apply(lambda x: x['latitude'])
df_measurements['gps_longitude'] = df_measurements['coordinates'].apply(lambda x: x['longitude'])
df_measurements = df_measurements.drop('coordinates', axis=1)

df_measurements['city'] = df_measurements['location'].str.split(',').str[0]

df_measurements['datetime'] = pd.to_datetime(df_measurements['datetime'])
df_measurements['date'] = df_measurements['datetime'].dt.strftime('%Y-%m-%d')
df_measurements['date_year'] = df_measurements['datetime'].dt.strftime('%Y')
df_measurements['date_month'] = df_measurements['datetime'].dt.strftime('%m')
df_measurements['date_day'] = df_measurements['datetime'].dt.strftime('%d')
df_measurements['date_year_month'] = df_measurements['datetime'].dt.strftime('%Y-%m')
display(df_measurements)

# COMMAND ----------

# DBTITLE 1,AVG Get measurements by MONTH
df_measurements_avg_month = df_measurements.groupby(['parameter','date_year_month','city'], as_index=False)['value'].mean()

display(df_measurements_avg_month)

# COMMAND ----------

# DBTITLE 1,Pivot AVG Get measurements by MONTH
pivot_df_measurements_avg_month = df_measurements_avg_month.pivot_table(index=['date_year_month','city'], columns='parameter', values='value')
pivot_df_measurements_avg_month.reset_index(inplace=True)

display(pivot_df_measurements_avg_month)

# COMMAND ----------

# DBTITLE 1,AVG Get measurements by DAY
df_measurements_avg_day = df_measurements.groupby(['parameter','date','city'], as_index=False)['value'].mean()

display(df_measurements_avg_day)

# COMMAND ----------

# DBTITLE 1,Pivot AVG Get measurements by DAY
pivot_df_day = df_measurements_avg_day.pivot_table(index=['date','city'], columns='parameter', values='value')
pivot_df_day.reset_index(inplace=True)

display(pivot_df_day)

# COMMAND ----------

# DBTITLE 1,Dashboard
import matplotlib.pyplot as plt
import pandas as pd

df_city = pivot_df_day[pivot_df_day['city'] == city_1]
#df_city_krk = pivot_df[pivot_df['city'] == 'Kraków']

plt.figure(figsize=(10, 6))
#plt.plot(df_city_wrsw['date'], df_city_wrsw['pm25'], label='pm25 - Warszawa')
plt.plot(df_city['date'], df_city[parameter_1], label=f'{parameter_1} - {city_1}')
plt.plot(df_city['date'], df_city['pm10'], label=f'pm10- {city_1}')

plt.xlabel('Date')
plt.ylabel('Value')
plt.title(f'Time Series ')
plt.legend()
plt.show()


# COMMAND ----------

#filtered_df = df_measurements_avg_day[(df_measurements_avg_day['city'] == city_1) & (df_measurements_avg_day['date'] == '2024-01-10') & (df_measurements_avg_day['parameter'] == 'pm25')]

#display(filtered_df)