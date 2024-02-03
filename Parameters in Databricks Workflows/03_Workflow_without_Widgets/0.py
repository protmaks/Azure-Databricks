# Databricks notebook source
# DBTITLE 1,Parameters
test_text = "NEW Workflow without WIDGETS"

# COMMAND ----------

# DBTITLE 1,Set Parameters
dbutils.jobs.taskValues.set(key = "test_text_wg", value = test_text)