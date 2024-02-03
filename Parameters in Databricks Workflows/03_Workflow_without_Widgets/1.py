# Databricks notebook source
# DBTITLE 1,Widgets
input_value = dbutils.jobs.taskValues.get(taskKey = "job_0", key = "test_text_wg", default = '', debugValue = 0)

print("input_value:", input_value)