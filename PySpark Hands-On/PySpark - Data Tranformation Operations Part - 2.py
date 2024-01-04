# Databricks notebook source
# MAGIC %md
# MAGIC ### Aggregate Function | sum,min,max,avg

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

emp_data = [
    (1,'manish',26,20000,'india','IT'),
    (2,'rahul',None,40000,'germany','engineering'),
    (3,'pawan',12,60000,'india','sales'),
    (4,'roshini',44,None,'uk','engineering'),
    (5,'raushan',35,70000,'india','sales'),
    (6,None,29,200000,'uk','IT'),
    (7,'adam',37,65000,'us','IT'),
    (8,'chris',16,40000,'us','sales'),
    (None,None,None,None,None,None),
    (7,'adam',37,65000,'us','IT')
]
columns = ["id", "name", "age", "salary", "country", "dept"]

emp_df = spark.createDataFrame(data=emp_data, schema=columns)
emp_df.show()
emp_df.printSchema()

# COMMAND ----------

# operations
# 1. Count
print(emp_df.count())
emp_df.select(count(col("id"))).show()

# 2. sum, min, max
emp_df.select(sum("salary").alias("sum_of_salary"), 
              max("salary").alias("max_of_salary"), 
              min("salary").alias("min_of_salary")).show()

# 3. avg
emp_df.select(avg("salary").cast("int").alias("avg_of_salary")).show()
