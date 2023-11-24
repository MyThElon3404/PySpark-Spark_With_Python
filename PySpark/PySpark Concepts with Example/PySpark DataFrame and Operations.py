# Databricks notebook source
# MAGIC #### =================================================================
# MAGIC # PySpark DataFrame
# MAGIC #### =================================================================
# MAGIC #### DataFrame is a distributed collection of data organized into named columns. It is conceptually equivalent to a table in a relational database or a data frame in R/Python, but with richer optimizations under the hood.

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

data_frame = SparkSession.builder.appName("PySpark DataFrame").getOrCreate()
print(data_frame)

# COMMAND ----------

# here we creating a dataframe using data and columns
#### =================================================================
data = [('James','','Smith','1991-04-01','M',3000),
  ('Michael','Rose','','2000-05-19','M',4000),
  ('Robert','','Williams','1978-09-05','M',4000),
  ('Maria','Anne','Jones','1967-12-01','F',4000),
  ('Jen','Mary','Brown','1980-02-17','F',-1)
]
columns = ["firstname","middlename","lastname","dob","gender","salary"]
df = data_frame.createDataFrame(data=data, schema = columns)
df.show()

# we are creating a dataframe with new schema
#### =================================================================
data2 = [("James","","Smith","36636","M",3000),
    ("Michael","Rose","","40288","M",4000),
    ("Robert","","Williams","42114","M",4000),
    ("Maria","Anne","Jones","39192","F",4000),
    ("Jen","Mary","Brown","","F",-1)
]
columns2 = StructType ([
    StructField("firstname", StringType(), True),
    StructField("middlename", StringType(), True),
    StructField("lastname", StringType(), True),
    StructField("id", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("salary", IntegerType(), True)    
])
df2 = data_frame.createDataFrame(data=data2, schema=columns2)
df2.printSchema()
df2.show(truncate=False)

# Retrieve DataType & Column Names of DataFrame
#### =================================================================
df2.printSchema()
print(df2.columns)

# now we are creating dataframe by Data sources (CSV, Text, JSON, XML e.t.c.)
#### =================================================================
df3 = data_frame.read.csv("/FileStore/tables/sales_data.csv", header=True, inferSchema=True)
df3.printSchema()
df3.show(5, truncate=False)

# Quick Examples of Getting Number of Rows & Columns
#### =================================================================
rows = df3.count()
print("DataFrame Rows Count - ", rows)

columns = len(df3.columns)
print("DataFrame Rows Count - ", columns)

# Now we write to CSV file - we put data from one file and write another file using PySpark
#### =======================================================================================
df3.write.option("header", True)\
    .mode('overwrite')\
    .csv("/FileStore/tables/csv_write/")

# we can check our result as well (Note - csv file address always change when we re-run our query)
#### =============================================================================================
df4 = data_frame.read.csv("/FileStore/tables/csv_write/part-00000-tid-8144195812023277525-f32e93b4-1d25-424d-b790-db4574233c55-11-1-c000.csv", header=True, inferSchema=True)
df4.show(truncate=False)


# COMMAND ----------

# here we can check our dataset in databricks

%fs
ls /FileStore/tables/csv_write/


# COMMAND ----------

# PySpark withColumnRenamed to Rename Column on DataFrame - Use PySpark withColumnRenamed() to rename a DataFrame column, we often need to rename one column or multiple (or all) columns on PySpark DataFrame, you can do this in several ways. When columns are nested it becomes complicated.
#### ====================================================================================================================
dataDF = [
    (('James','','Smith'),'1991-04-01','M',3000),
    (('Michael','Rose',''),'2000-05-19','M',4000),
    (('Robert','','Williams'),'1978-09-05','M',4000),
    (('Maria','Anne','Jones'),'1967-12-01','F',4000),
    (('Jen','Mary','Brown'),'1980-02-17','F',-1)
]
schemaDF = StructType([
        StructField('name', 
                    StructType([
                        StructField('firstname', StringType(), True),
                        StructField('middlename', StringType(), True),
                        StructField('lastname', StringType(), True)
                    ])),
        StructField('dob', StringType(), True),
        StructField('gender', StringType(), True),
        StructField('salary', IntegerType(), True)
        ])

with_column_df = data_frame.createDataFrame(data=dataDF, schema=schemaDF)
with_column_df.printSchema()
with_column_df.show()

# COMMAND ----------

# 1. PySpark withColumnRenamed – To rename DataFrame column name
#### =================================================================
with_column_df.withColumnRenamed("dob", "DateOfBirth").printSchema()

# 2. PySpark withColumnRenamed – To rename multiple columns
#### =================================================================
with_column_df.withColumnRenamed("name", "FullName").withColumnRenamed("gender", "Sex").printSchema()

# 3. Using PySpark StructType – To rename a nested column in Dataframe
#### =================================================================
schema2 = StructType([
    StructField("fname",StringType()),
    StructField("middlename",StringType()),
    StructField("lname",StringType())
])
with_column_df.select(col("name").cast(schema2)).printSchema()

# 4. Using Select – To rename nested elements.
#### =================================================================
with_column_df.select(col("name.firstname").alias("fname"),\
    col("name.middlename").alias("mname"),\
    col("name.lastname").alias("lname")).printSchema()

# 5. Using PySpark DataFrame withColumn – To rename nested columns
#### =================================================================
with_column_df.withColumn("fname", col("name.firstname"))\
    .withColumn("mname", col("name.middlename"))\
    .withColumn("lname", col("name.lastname"))\
    .drop("name").printSchema()

# COMMAND ----------

# DBTITLE 1,# PySpark GroupBy
# PySpark Groupby - Similar to SQL GROUP BY clause, PySpark groupBy() function is used to collect the identical data into groups on DataFrame and perform count, sum, avg, min, max functions on the grouped data.
#### =====================================================================================================================
simpleData = [("James","Sales","NY",90000,34,10000),
    ("Michael","Sales","NY",86000,56,20000),
    ("Robert","Sales","CA",81000,30,23000),
    ("Maria","Finance","CA",90000,24,23000),
    ("Raman","Finance","CA",99000,40,24000),
    ("Scott","Finance","NY",83000,36,19000),
    ("Jen","Finance","NY",79000,53,15000),
    ("Jeff","Marketing","CA",80000,25,18000),
    ("Kumar","Marketing","NY",91000,50,21000)
  ]
schema = ["employee_name","department","state","salary","age","bonus"]
group_by_df = spark.createDataFrame(data=simpleData, schema=schema)
group_by_df.printSchema()
group_by_df.show(truncate=False)

# PySpark groupBy on DataFrame Columns
#### =======================================================================
group_by_df.groupBy("department").sum("salary").show(truncate=False)
group_by_df.groupBy("department").count().show(truncate=False)
group_by_df.groupBy("department").min("salary").show(truncate=False)
group_by_df.groupBy("department").max("salary").show(truncate=False)
group_by_df.groupBy("department").avg("salary").show(truncate=False)
group_by_df.groupBy("department").mean("salary").show(truncate=False)

# Using Multiple columns
#### =======================================================================
group_by_df.groupBy("department", "state").sum("salary", "bonus").show()

# Running more aggregates at a time
#### =======================================================================
group_by_df.groupBy("department")\
  .agg(sum("salary").alias("sum_salary"),
       avg("salary").alias("sum_salary"),
       sum("bonus").alias("sum_bouns"),
       max("bonus").alias("max_bonus")
      ).show()
  
# Using filter on aggregate data
#### =======================================================================
group_by_df.groupBy("department").agg(sum("salary").alias("sum_salary"),
                                      avg("salary").alias("avg_salary"),
                                      sum("bonus").alias("sum_bouns"),
                                      max("bonus").alias("max_bonus")
                                      ).where(col("sum_salary") >= 50000).show(truncate=False)




# COMMAND ----------

# DBTITLE 1,# PySpark Join Types | Join Two DataFrames
# PySpark Join Types | Join Two DataFrames - PySpark Join is used to combine two DataFrames and by chaining these you can join multiple DataFrames; it supports all basic join type operations available in traditional SQL like INNER, LEFT OUTER, RIGHT OUTER, LEFT ANTI, LEFT SEMI, CROSS, SELF JOIN.
#### ====================================================================================================================
# first DataFrame
#### =======================================================================

emp = [
    (1,"Smith",-1,"2018","10","M",3000),
    (2,"Rose",1,"2010","20","M",4000),
    (3,"Williams",1,"2010","10","M",1000),
    (4,"Jones",2,"2005","10","F",2000),
    (5,"Brown",2,"2010","40","",-1),
    (6,"Brown",2,"2010","50","",-1)
]
empColumns = ["emp_id","name","superior_emp_id","year_joined","emp_dept_id","gender","salary"]
empDF = data_frame.createDataFrame(data=emp, schema=empColumns)
empDF.printSchema()
empDF.show(truncate=False)

# Second DataFrame
#### =======================================================================
dept = [
    ("Finance",10),
    ("Marketing",20),
    ("Sales",30),
    ("IT",40)
]
deptColumns = ["dept_name","dept_id"]
deptDF = data_frame.createDataFrame(data=dept, schema=deptColumns)
deptDF.printSchema()
deptDF.show(truncate=False)

# COMMAND ----------

# DBTITLE 1,# Performing Join operations
# Common Key: In order to join two or more datasets we need a common key or a column on which you want to join. This key is used to join the matching rows from the datasets.
#### ==================================================================================================================

# Join Execution: PySpark performs the join by comparing the values in the common key column between the Datasets.
#### ==================================================================================================================

# 1. Inner Join: Returns only the rows with matching keys in both DataFrames.
#### =============================================================================
empDF.join(deptDF, empDF.emp_dept_id == deptDF.dept_id, "inner").show(truncate=False)

# 2. Full Outer Join: Returns all rows from both DataFrames, including matching and non-matching rows.
#### =============================================================================
empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"outer").show(truncate=False)
empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"full").show(truncate=False)
empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"fullouter").show(truncate=False)

# 3. Left Join: Returns all rows from the left DataFrame and matching rows from the right DataFrame.
#### ============================================================================= 
empDF.join(deptDF, empDF.emp_dept_id == deptDF.dept_id, "left").show()
empDF.join(deptDF, empDF.emp_dept_id == deptDF.dept_id, "leftouter").show()

# 4. Right Join: Returns all rows from the right DataFrame and matching rows from the left DataFrame.
#### ============================================================================= 
empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"right").show(truncate=False)
empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"rightouter").show(truncate=False)

# 4. Left Semi Join: Returns all rows from the left DataFrame where there is a match in the right DataFrame.
#### ============================================================================= 
empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"leftsemi").show(truncate=False)

# 5. Left Anti Join: Returns all rows from the left DataFrame where there is no match in the right DataFrame.
#### ============================================================================= 
empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"leftanti").show(truncate=False)

# 6. PySpark Self Join: Joins are not complete without a self join, Though there is no self-join type available, we can use any of the above-explained join types to join DataFrame to itself. below example use inner self join.
#### ============================================================================= 
