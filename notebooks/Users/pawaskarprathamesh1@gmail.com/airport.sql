-- Databricks notebook source
create table airportnew(name STRING,country STRING,area_code INT,code STRING);

-- COMMAND ----------

load data inpath"/FileStore/tables/airport1.csv" overwrite into table default.airportnew

-- COMMAND ----------

select * from airport limit 10;