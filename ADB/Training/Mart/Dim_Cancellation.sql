-- Databricks notebook source
use mart_training;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS Dim_Cancellation (
  code STRING,
  description STRING
) USING DELTA LOCATION '/mnt/mart_datalake/Dim_Cancellation'

-- COMMAND ----------

INSERT OVERWRITE Dim_Cancellation
SELECT 
code 
,description 
FROM  cleansed_training.Cancellation
