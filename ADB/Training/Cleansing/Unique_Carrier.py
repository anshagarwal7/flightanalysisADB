# Databricks notebook source
# MAGIC %run /Training/Utilities

# COMMAND ----------

df = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaLocation", "/dbfs/FileStore/tables/schema/UNIQUE_CARRIERS")
    .load("/mnt/raw_datalake/UNIQUE_CARRIERS/")
)

# COMMAND ----------

df_base = df.selectExpr(
    "replace(Code,'\"','') as code",
    "replace(Description,'\"','') as description",
    "to_date(Date_Part,'yyyy-MM-dd') as Date_Part"
)
df_base.writeStream.trigger(once=True).format("delta").option(
    "checkpointLocation", "/dbfs/FileStore/tables/checkpointLocation/UNIQUE_CARRIERS"
).start("/mnt/cleansed_datalake/unique_carriers")

# COMMAND ----------

f_delta_cleansed_load('unique_carriers','/mnt/cleansed_datalake/unique_carriers','cleansed_training')
