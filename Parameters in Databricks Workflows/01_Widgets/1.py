# Databricks notebook source
# DBTITLE 1,Widgets
dbutils.widgets.text("text_widget", "default")
input_value = dbutils.widgets.get("text_widget")

dbutils.widgets.text("global_text_widget", "default")
global_input_value = dbutils.widgets.get("text_widget")

# COMMAND ----------

# DBTITLE 1,Prints
print("input_value:", input_value)
print("global_input_value:", global_input_value)