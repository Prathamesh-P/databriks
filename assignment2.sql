-- Databricks notebook source
-- MAGIC %python
-- MAGIC from pyspark.sql import *

-- COMMAND ----------

-- MAGIC %python
-- MAGIC department1=Row(id='123456', name='Computer science')
-- MAGIC department2=Row(id='789012', name='Mechanical Engineering')
-- MAGIC department3=Row(id='345678', name='Theater and Drama')
-- MAGIC department4=Row(id='921234', name='Indoor Recreation')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC Employee = Row("firstName","lastName","email","salary")
-- MAGIC employee1=Employee('michael','armbrust','no-reply@berkeley.edu',100000)
-- MAGIC employee2=Employee('xiangrui','meng','no-reply@standford.edu',120000)
-- MAGIC employee3=Employee('matei',None,'no-reply@waterloo.edu',140000)
-- MAGIC employee4=Employee(None,'wedell','no-reply@berkeley.edu',160000)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC departmentWithEmployees1 = Row(department=department1, employees=[employee1,employee2])
-- MAGIC departmentWithEmployees2 = Row(department=department2, employees=[employee3,employee4])
-- MAGIC departmentWithEmployees3 = Row(department=department3, employees=[employee1,employee4])
-- MAGIC departmentWithEmployees4 = Row(department=department4, employees=[employee2,employee3])
-- MAGIC 
-- MAGIC print (department1)
-- MAGIC print (Employee)
-- MAGIC print (departmentWithEmployees1)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC departmentsWithEmployeesSeq1=[departmentWithEmployees1, departmentWithEmployees2]
-- MAGIC df1 = spark.createDataFrame(departmentsWithEmployeesSeq1)
-- MAGIC display(df1)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC departmentsWithEmployeesSeq2=[departmentWithEmployees3, departmentWithEmployees4]
-- MAGIC df2 = spark.createDataFrame(departmentsWithEmployeesSeq2)
-- MAGIC display(df2)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df3 = df1.union(df2)
-- MAGIC display(df3)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df3.count()
-- MAGIC df3.first()
-- MAGIC df3.take(2)
-- MAGIC df3.collect()
-- MAGIC df3.show()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df3.write.parquet("/Filestore/df3-result")

-- COMMAND ----------

-- MAGIC %fs ls "Filestore/df3-result"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df4=spark.read.parquet("/Filestore/df3-result/part-00000-tid-2952814846653564246-77d80fea-9282-4161-aeb2-5162e185f726-294-c000.snappy.parquet")
-- MAGIC display(df4)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import explode
-- MAGIC df5=df3.select(explode("employees").alias("e"))
-- MAGIC explodeDF=df5.selectExpr("e.firstName","e.lastName","e.email","e.salary")
-- MAGIC explodeDF.show()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import col, asc
-- MAGIC filterdf=explodeDF.filter((col("firstName")=="michael")|(col("lastName")=="armburst"))
-- MAGIC display(filterdf)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC nonNulldf=explodeDF.fillna("--")
-- MAGIC display(nonNulldf)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC salarySumDF = explodeDF.filter((col("firstName")=="xiangrui")).agg({"salary" : "sum"})
-- MAGIC display(salarySumDF)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC explodeDF.show()
-- MAGIC display(explodeDF)
