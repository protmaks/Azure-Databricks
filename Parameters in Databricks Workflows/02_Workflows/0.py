# Databricks notebook source
# DBTITLE 1,Parameters
test_text = "NEW Workflow test"

# COMMAND ----------

# DBTITLE 1,Set Parameters
dbutils.jobs.taskValues.set(key = "test_text_new", value = test_text)