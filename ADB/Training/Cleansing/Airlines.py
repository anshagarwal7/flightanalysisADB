# Databricks notebook source
# MAGIC %run /Training/Utilities

# COMMAND ----------

from pyspark.sql.functions import explode

df=spark.read.json("/mnt/raw_datalake/airlines/")
df1=df.select(explode("response"),"Date_Part")
df_final=df1.select("col.*","Date_Part")

# COMMAND ----------

df_final.write.format("delta").mode("overwrite").save("/mnt/cleansed_datalake/airline")

# COMMAND ----------

f_delta_cleansed_load('airline','/mnt/cleansed_datalake/airline','cleansed_training')
