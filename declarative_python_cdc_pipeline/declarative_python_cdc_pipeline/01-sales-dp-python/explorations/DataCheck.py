# Databricks notebook source
# MAGIC %md
# MAGIC ### Bronze layer table

# COMMAND ----------

spark.sql("SELECT *FROM sales_cta.bronze.customers_py_cdc").count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Silver layer table

# COMMAND ----------

spark.sql("SELECT *FROM sales_cta.silver.customers_py_cdc_clean").count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gold layer tables

# COMMAND ----------

#Materialize the final table
spark.sql("SELECT *FROM sales_cta.gold.customers_py").count()

# COMMAND ----------

#Slowly Changing Dimension of type 2 (SCD2)
spark.sql("SELECT *FROM sales_cta.gold.scd2_customers_py").count()

# COMMAND ----------

