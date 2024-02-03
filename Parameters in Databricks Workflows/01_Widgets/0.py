# Databricks notebook source
# DBTITLE 1,Parameters
test_text = "test test test"

job_folder = '/Users/maksim.pachkovskiy@t1a.com'
path_notebook_1 = f'/{job_folder}/1'

# COMMAND ----------

# DBTITLE 1,Run Notebooks
# run the Notebook_1
parameters = {
    "text_widget": test_text
}
dbutils.notebook.run(path_notebook_1, 1200, arguments=parameters)