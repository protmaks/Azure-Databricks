# Databricks notebook source
# DBTITLE 1,catalog
# MAGIC %sql
# MAGIC use catalog edl_producrion  /* set your catalog name in Unity */

# COMMAND ----------

# DBTITLE 1,imports
from pyspark.sql.functions import concat, lit, col, isnull

# COMMAND ----------

# DBTITLE 1,params
# specify the list of schemes in which the owner needs to be changed
schema_names = [
    "schema_name_1",
    "schema_name_2",
]


check_owners = ["azure_group_name1", "azure_group_name2"]   # list groups or owners that should not be changed

new_owner = "azure_group_name1"     # set the name of the group that will become the owner of the tables

# COMMAND ----------

# DBTITLE 1,loading tables
df_all = None

for schema_name in schema_names:
    df_tables_all = spark.sql(f"SELECT * FROM information_schema.tables WHERE table_schema IN ('{schema_name}')")

    if df_all is None:
        df_all = df_tables_all
    else:
        df_all = df_all.union(df_tables_all)

df_all.display()

# COMMAND ----------

# DBTITLE 1,filter by owners
df_owners = df_all.filter(
    (~df_all.table_owner.isin(check_owners)) |
    (df_all.table_owner == '') |
    isnull(df_all.table_owner)
)

df_owners.display()

# COMMAND ----------

# DBTITLE 1,set new owners
tables_own = df_owners.collect()

for row in tables_own:
    schema = row['table_schema']
    table = row['table_name']

    print(f"** owner is empty for the table - {schema}.{table}")
    spark.sql(f"ALTER TABLE {schema}.{table} SET OWNER TO `{new_owner}`")
    print(f"* owner for the {table} changed to the {new_owner}\n")
