# Databricks notebook source
# DBTITLE 1,Cell 1
catalog = "sales_cta"
schema = dbName = db = "db_salesdp_cdc"

volume_name = "raw_data"
volume_folder = f"/Volumes/{catalog}/{db}/{volume_name}"

# COMMAND ----------

print(f"{volume_folder}")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### See the all files in the raw_data

# COMMAND ----------

# DBTITLE 1,Cell 4
dbutils.fs.ls(volume_folder)

# COMMAND ----------

table_name =['customer','transactions']
print(type(table_name))

# COMMAND ----------

print(table_name[0])

# COMMAND ----------

print(table_name[1])

# COMMAND ----------

