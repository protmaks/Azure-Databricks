# Databricks notebook source
# DBTITLE 1,Widgets
dbutils.widgets.removeAll()

dbutils.widgets.text("group_source", "") 
group_source = dbutils.widgets.get("group_source")

dbutils.widgets.text("group_target", "") 
group_target = dbutils.widgets.get("group_target")

dbutils.widgets.text("group_new_owner", "") 
group_new_owner = dbutils.widgets.get("group_new_owner")

# COMMAND ----------

# DBTITLE 1,Loading Schemas(Databases)
df_schemas = spark.sql("SHOW DATABASES")
schemas = [row['databaseName'] for row in df_schemas.collect()]
print(schemas)

# COMMAND ----------

# DBTITLE 1,Loading Tables from Schemas
from pyspark.sql.functions import concat, lit

df_all = None

for schema in schemas:
    df_tables_all = spark.sql(f"SHOW TABLES IN {schema}")

    if df_all is None:
        df_all = df_tables_all
    else:
        df_all = df_all.union(df_tables_all)

df_all = df_all.withColumn('schema_table', concat(df_all['database'], lit("."), df_all['tableName']))
df_all.display()

tables = [row['schema_table'] for row in df_all.collect()]
#print(tables)

# COMMAND ----------

# DBTITLE 1,Filtering databses and tables by 'group_source'
df_full = None

for table in tables:
    df_schemas_tables = spark.sql(f"SHOW GRANTS ON hive_metastore.{table}")
    
    if df_full is None:
        df_full = df_schemas_tables
    else:
        df_full = df_full.union(df_schemas_tables)

#temp display
df_full.display()

#filtering tables by 'group_source'
df = df_full.filter(df_full.Principal == group_source)

#create df with owner tables
df_owners = df.filter((df.ActionType == 'OWN') & (df.ObjectType == 'TABLE'))
df_owners = df_owners.dropDuplicates(['Principal','ActionType','ObjectType','ObjectKey'])

#create df with schemas
df_schemas = df.filter((df.ObjectType == 'DATABASE') & (df.ActionType != 'DENIED_SELECT'))
df_schemas = df_schemas.dropDuplicates(['Principal','ActionType','ObjectType','ObjectKey'])

#create df with tables
df_tables = df.filter((df.ActionType != 'OWN') & (df.ObjectType != 'DATABASE'))
df_tables = df_tables.dropDuplicates(['Principal','ActionType','ObjectType','ObjectKey'])

# COMMAND ----------

# DBTITLE 1,Show Schemas
df_schemas.display()

# COMMAND ----------

# DBTITLE 1,Grant permission from Schemas 'group_target' like 'group_source'
if group_target:
    schemas_list = df_schemas.collect()

    for row in schemas_list:
        action = row['ActionType']
        object = row['ObjectType']
        okey = row['ObjectKey']

        query = f'GRANT {action} ON {object} {okey} TO `{group_target}`'
        print(query)
        spark.sql(query)
else:
    print('Value -  group_target  is Empty')

# COMMAND ----------

# DBTITLE 1,Show Tables
df_tables.display()

# COMMAND ----------

# DBTITLE 1,Grant permission from Tables 'group_target' like 'group_source'
if group_target:
    tables_list = df_tables.collect()

    for row in tables_list:
        action = row['ActionType']
        object = row['ObjectType']
        okey = row['ObjectKey']

        query = f'GRANT {action} ON {object} {okey} TO `{group_target}`'
        print(query)
        spark.sql(query)
else:
    print('Value -  group_target  is Empty')

# COMMAND ----------

# DBTITLE 1,Show Owners
df_owners.display()

# COMMAND ----------

# DBTITLE 1,Change table OWNER from 'group_source' to 'group_new_owner'
if group_new_owner:
    owners_list = df_owners.collect()

    for row in owners_list:
        action = row['ActionType']
        object = row['ObjectType']
        okey = row['ObjectKey']

        query = f'ALTER TABLE {okey} SET OWNER TO `{group_new_owner}`'
        print(query)
        spark.sql(query)
else:
    print('Value -  group_new_owner  is Empty')
