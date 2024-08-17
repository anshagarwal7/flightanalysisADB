-- Databricks notebook source
use mart_training;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS Reporting_Flight (
  date date,
  ArrDelay int,
  DepDelay int,
  Origin string,
  Cancelled string,
  CancellationCode string,
  UniqueCarrier string,
  FlightNum int,
  TailNum string,
  deptime string
)
USING DELTA 
PARTITIONED BY (date_year int) 
LOCATION '/mnt/mart_datalake/Reporting_Flight'

-- COMMAND ----------

-- MAGIC %py
-- MAGIC max_year=spark.sql("select year(max(date)) from cleansed_geekcoders.flight").collect()[0][0]

-- COMMAND ----------

-- MAGIC %py
-- MAGIC spark.sql(f"""
-- MAGIC INSERT
-- MAGIC   OVERWRITE Reporting_Flight PARTITION (date_year = {max_year}) 
-- MAGIC SELECT
-- MAGIC   date,
-- MAGIC   ArrDelay,
-- MAGIC   DepDelay,
-- MAGIC   Origin,
-- MAGIC   Cancelled,
-- MAGIC   CancellationCode,
-- MAGIC   UniqueCarrier,
-- MAGIC   FlightNum,
-- MAGIC   TailNum,
-- MAGIC   deptime
-- MAGIC FROM
-- MAGIC   cleansed_training.flight where year(date)={max_year} """)
