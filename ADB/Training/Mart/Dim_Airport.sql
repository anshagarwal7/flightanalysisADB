-- Databricks notebook source
use mart_training;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS Dim_Airport (
  code STRING,
  city STRING,
  country STRING,
  airport STRING
) USING DELTA LOCATION '/mnt/mart_datalake/Dim_Airport'

-- COMMAND ----------

INSERT OVERWRITE Dim_Airport
SELECT 
code 
,city 
,country 
,airport 
FROM  cleansed_training.Airport 
