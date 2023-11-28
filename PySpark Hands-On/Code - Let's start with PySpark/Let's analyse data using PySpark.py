# Databricks notebook source
# MAGIC %md
# MAGIC ### 1. Loading Dataset Into PySpark

# COMMAND ----------

import pyspark
from pyspark.sql import SparkSession
import pandas as pd

# Create a spark session
ss = SparkSession.builder.appName("Introduction to PySpark").getOrCreate()
print(ss)

# Loading dataset to PySpark
df = ss.read.csv("/FileStore/tables/Sample_Dataset-2.txt", header=True, inferSchema=True)
df.show()

# Print Data Types
# Like pandas dtypes, PySpark has an inbuilt method printSchema(), which can be used to print the data types.
df.printSchema()

# to get columns from dataset
# df.columns # ['id', 'total_bill', 'tip', 'sex', 'smoker', 'day', 'time', 'size']    

# we can use pandas head() method, but it will print the rows as a list
# df.head(3)


# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Exploring DataFrame

# COMMAND ----------

df.columns # ['id', 'total_bill', 'tip', 'sex', 'smoker', 'day', 'time', 'size']

# # # Tips - there are many diff way's to select one or columns from dataset

from pyspark.sql.functions import *
from pyspark.sql.types import *

# Selecting one columns from dataset
df.select("sex").show(5)
df.select(col("total_bill")).show(5)
df.select(df[5]).show(5)

# Selecting multiple columns from dataset
df.select(["id", "size"]).show(5)
df.select(col("total_bill"), col("tip"), col("day")).show(5)
df.select(df[4], df[5], df[6]).show(5)

# Describe Data Frame - Similar to pandas, PySpark also supports describe( ) method which provides count, mean, standard deviation, min, and max.
df.describe().show()

# Generating a New Column - To generate a new column, we need to use the withcolumn( ) method, which takes the new column name as first argument and then the computation

df1 = df.withColumn("tip_bill_ratio", (df["tip"]/df["total_bill"])*100)
df1.show(5)

# Dropping Columns - To drop a column, we need to use the .drop( ) method and pass the single column name or column names as a list 

df1 = df1.drop("tip_bill_ratio")
df1.show()

# Rename Columns - To rename a column, we need to use the withColumnRenamed( ) method and pass the old column as first argument and new column name as second argument.

df1 = df1.withColumnRenamed("sex", "gender")
df1.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Let's Handling Missing Values

# COMMAND ----------

# Load the Data Frame with Missing Values - Before we begin handling missing values, we require loading a separate tips dataset with missing values. The missing values are denoted using null.
miss_df = ss.read.csv("/FileStore/tables/missing_value_dataset-1.csv", header=True, inferSchema=True)
miss_df.show()
miss_df.printSchema()

# Removing observations with null values - Let’s start with the easiest one, where say we want to drop any observation (row) where any of the cell contains a missing value. We could do that by using the .na.drop() method. Remember that to view it we need to add the .show() method.
miss_df.na.drop().show()

# The drop() method contains a how argument and drop rows accordingly
# 1. By default, how was set to “any” (how = “any”), which means it will drop any observation that contains a null value.
    # how == any
miss_df.na.drop(how = "any").show()

# 2. If we set how = “all”, it will delete an observation if all the values are null.
    # how == all
miss_df.na.drop(how="all").show()

# Setting Threshold - The .drop( ) method also contains a threshold argument. This argument indicates that how many non-null values should be there in an observation (i.e., in a row) to keep it.
miss_df.na.drop(how = "any", thresh = 0).show()

# Subset - The subset argument inside the .drop( ) method helps in dropping entire observations [i.e., rows] based on null values in columns. For a particular column where null value is present, it will delete the entire observation/row.
miss_df.na.drop(how = "any", subset = ["time"]).show()

# Filling null Values - Dealing with missing values is easy in PySpark. Let’s say we want to fill the null values with string “Missing”. We can do that by using .na.fill(“Missing”) notation. This going to fill only the null values of columns with string type.
miss_df.na.fill("MISSING").show()

# Similarly, if we supply a numeric value, then it will replace the values in the numeric columns.
miss_df.na.fill(3).show()

# We can also specify the column names explicitly to fill missing values by supplying the column names inside a square brackets. Like here, we supplied [“sex”, “day”] inside the fill( ) method.
miss_df.na.fill("MISSING", ["sex", "time"]).show()

# Imputing Missing Values with Column Mean/Average - We often need to impute missing values with column statistics like mean, median and standard deviation. To achieve that the best approach will be to use an imputer.
# Step1: import the Imputer class from pyspark.ml.feature
# Step2: Create an Imputer object by specifying the input columns, output columns, and setting a strategy (here: mean).
        # Note: The outputCols contains a list comprehension.
# Step3: fit and transform the data frame using the imputer object.
# This will create two different columns with imputed mean values.

from pyspark.ml.feature import Imputer
### Create an imputer object
imputer = Imputer(
          inputCols= ["total_bill", "tip"],
          outputCols = ["{}_imputed".format(c) for c in ["total_bill", "tip"]]
    ).setStrategy("mean")
### Fit imputer on Data Frame and Transform it
imputer.fit(miss_df).transform(miss_df).show()


# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Filtering Operations

# COMMAND ----------

df.show()
# df.columns # ['id', 'total_bill', 'tip', 'sex', 'smoker', 'day', 'time', 'size']

from pyspark.sql.functions import *
from pyspark.sql.types import *

# We often use filtering to filter out a chunk of data that we need for a specific task. PySpark’s .filter( ) method makes is very easy to filter data frames.
df.filter("tip <= 2").show()
df.filter("tip >= 5").select(["total_bill", "tip"]).show()

# We can perform multiple conditional filtering, separating by an OR (|) or AND (&) operators. While using multiple filtering, it is recommended to enclose them using parenthesis ( ).
df.filter((df["tip"] < 1) | (df["tip"] > 5)).select(["id", "total_bill", "tip", "time"]).show(5)
df.filter(~(df["tip"] <= 2)).select(["total_bill", "tip"]).show(5)



# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Group by and Aggregate

# COMMAND ----------

# We can do group by applying groupby(“sex” ) method and subsequently the sum() method.
df.groupBy("sex").sum().show()

# We can apply .groupBy() and .select() methods together. Here is an example where we calculated the day wise maximum tip.
df.groupBy("day").max().select(["day", "max(tip)", "max(size)"]).show()

# We can also apply .count( ) method to count number of observations for each day label/categories.
df.groupBy("day").count().show()

# COMMAND ----------

# Finally, close the spark session once you're done with it using the .stop() method.
ss.stop()
