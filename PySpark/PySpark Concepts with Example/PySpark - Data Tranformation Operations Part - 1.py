# Databricks notebook source
# MAGIC %md
# MAGIC #### Filtering data -> When otherwise (dataFrame)   ===   In SparkSQL case when then else end as

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# we creating one dataframe using data and columns

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
columns=["id", "name", "age", "salary", "country", "dept"]

emp_df=spark.createDataFrame(data=emp_data, schema=columns)
emp_df.show()
emp_df.printSchema()

# COMMAND ----------

                                                    # Using dataframe methods

# apply adult filter method
emp_df.withColumn("adult", when(col("age")<18, "no")
                            .when(col("age")>18, "yes")
                            .otherwise("null_value")).show()

# here we fix null value in col age and then apply adult condition
emp_df.withColumn("age", when(col("age").isNull(), lit(19))
                        .otherwise(col("age")))\
        .withColumn("adult", when(col("age")>18, "yes")
                            .otherwise("no")).show()

# COMMAND ----------

emp_df.withColumn("age_wise", when((col("age")>0) & (col("age")<18), "minor")
                            .when((col("age")>18) & (col("age")>30), "mid")
                            .otherwise("major")).show()

# COMMAND ----------

                                                # Using SparkSQL in PySpark

emp_df.createOrReplaceTempView("tbl_emp_df")
spark.sql("""
          
          select *,
          case when age<18 then "minor"
          when age>18 then "mid"
          else "major"
          end as adult
          from tbl_emp_df

          """).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Unique & Sorted Records

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# we creating one dataframe using data and columns

data=[
    (10 ,'Anil',50000, 18),
    (11 ,'Vikas',75000,  16),
    (12 ,'Nisha',40000,  18),
    (13 ,'Nidhi',60000,  17),
    (14 ,'Priya',80000,  18),
    (15 ,'Mohit',45000,  18),
    (16 ,'Rajesh',90000, 10),
    (17 ,'Raman',55000, 16),
    (18 ,'Sam',65000,   17),
    (15 ,'Mohit',45000,  18),
    (13 ,'Nidhi',60000,  17),      
    (14 ,'Priya',90000,  18),  
    (18 ,'Sam',65000,   17)
]
columns=["id", "name", "sal", "mngr_id"]

emp_df=spark.createDataFrame(data=data, schema=columns)
emp_df.show(3)
emp_df.printSchema()

# COMMAND ----------

# select only unique data in dataset

emp_df.distinct().show()
emp_df.select("id", "name").distinct().show()

# drop duplicate records from dataset
emp_df.drop_duplicates(["id", "name", "sal", "mngr_id"]).show()


# COMMAND ----------

emp_df.sort(col("sal").desc()).show()
emp_df.sort(col("sal").desc(), col("name").desc()).show()

# COMMAND ----------

# Exersize - 1
from pyspark.sql.functions import *
from pyspark.sql.types import *

leet_code_data = [
    (1, 'Will', None),
    (2, 'Jane', None),
    (3, 'Alex', 2),
    (4, 'Bill', None),
    (5, 'Zack', 1),
    (6, 'Mark', 2)
]
columns = ["id", "name", "referce_id"]

exersize_df = spark.createDataFrame(data=leet_code_data, schema=columns)
exersize_df.show()

# COMMAND ----------

# Query - name the customer that are not referred by the customer with id = 2
# Using Spark SQL
exersize_df.createOrReplaceTempView("tbl_exersize")
spark.sql("""

    select name from tbl_exersize
    where referce_id != 2 or referce_id is null

""").show()

# using dataframe
exersize_df.select(col("name")).filter((col("referce_id")!=2) | (col("referce_id").isNull())).show()

# using withColumn (when and otherwise)
exersize_df.withColumn("referce_id",when(col("referce_id").isNull(),"0")\
                                .otherwise(col("referce_id")))\
                                .select("name").filter(col("referce_id")!=2).show()
