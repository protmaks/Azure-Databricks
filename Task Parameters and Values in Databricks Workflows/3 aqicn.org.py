# Databricks notebook source
import requests
import json
import pandas as pd

#url = 'https://api.waqi.info/feed/here/?token=4d7a4ee8bf65587722ddb298dbe987698de45c12'

# /feed/:city/?token=:token

#$ curl -i "http://api.waqi.info/feed/shanghai/?token=demo"
url = 'https://api.waqi.info/feed/Krakow/?token=4d7a4ee8bf65587722ddb298dbe987698de45c12'

headers = {"Accept": "application/json"}
response = requests.get(url, headers=headers)

response_content = response.content
print(response_content)

decoded_content = response_content.decode('utf-8')
json_content = json.loads(decoded_content)

#results = json_content['results']
#df = pd.DataFrame(results).sort_values(by='count', ascending=False)

#display(df)