# Databricks notebook source
# DBTITLE 1,catalog
# MAGIC %sql
# MAGIC use catalog hive_metastore

# COMMAND ----------

# DBTITLE 1,imports
from pyspark.sql.functions import concat, lit

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

# DBTITLE 1,foading tables from schemas
df_all = None

for schema_name in schema_names:
    df_tables_all = spark.sql(f"SHOW TABLES IN {schema_name}")

    if df_all is None:
        df_all = df_tables_all
    else:
        df_all = df_all.union(df_tables_all)

df_all = df_all.withColumn('schema_table', concat(lit("`"), df_all['database'], lit("`.`"), df_all['tableName'], lit("`")))
#df_all.display()

tables = [row['schema_table'] for row in df_all.collect()]
print(tables)

# COMMAND ----------

# DBTITLE 1,foading owners from tables
df_full = None

for table in tables:
    df_schemas_tables = spark.sql(f"SHOW GRANTS ON {table}")
    
    if df_full is None:
        df_full = df_schemas_tables
    else:
        df_full = df_full.union(df_schemas_tables)

df_owners = df_full.filter((df_full.ActionType == 'OWN') & (df_full.ObjectType == 'TABLE')  & (df_full.Principal.isin(check_owners)))
df_owners = df_owners.dropDuplicates(['Principal','ActionType','ObjectType','ObjectKey'])
#df_owners.display()

# COMMAND ----------

# DBTITLE 1,find tables without owner
df_without_owner = df_all.join(df_owners, df_all.schema_table == df_owners.ObjectKey, "left_anti")
#df_without_owner.display()

# COMMAND ----------

# DBTITLE 1,set owners
tables_own = df_without_owner.collect()

for row in tables_own:
    schema = row['database']
    table = row['tableName']

    print(f"** owner is empty for the table - {schema}.{table}")
    spark.sql(f"ALTER TABLE {schema}.{table} SET OWNER TO `{new_owner}`")
    print(f"* owner for the {table} changed to the {new_owner}")
