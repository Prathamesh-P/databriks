-- Databricks notebook source
create table airportnew(name STRING,country STRING,area_code INT,code STRING);

-- COMMAND ----------

load data inpath"/FileStore/tables/airport1.csv" overwrite into table default.airportnew

-- COMMAND ----------

select * from airport limit 10;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC airport=spark.read.csv("/FileStore/tables/airport.csv",header="true",inferSchema="true")
-- MAGIC airport.show()
-- MAGIC display(airport)