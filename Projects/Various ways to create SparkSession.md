# <- Various ways to create a SparkSession object in PySpark ->
## In PySpark, a SparkSession is the entry point for reading data and executing SQL queries.

### Method 1 - Using SparkSession.builder Method
- The most common and recommended way is to use the SparkSession.builder method to configure and create a SparkSession.
```sql
from pyspark.sql import SparkSession

spark = SparkSession.builder \
.appName(“MySparkApp”) \
.config(“key”, “value”) \
.getOrCreate()
```
- In this approach, you can set various configuration options using the config method, such as the application name, master URL, and other Spark properties.
## ----------------------------------------------------------------------------------
### Method 2 - Using SparkConf and SparkSession.builder.config Method
- You can also use a combination of SparkConf and SparkSession.builder.config to set configuration options.
```sql
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf

conf = SparkConf().setAppName(“MySparkApp”).setMaster(“local[2]”)
spark = SparkSession.builder.config(conf=conf).getOrCreate()
```
- This allows you to configure Spark through a SparkConf object before creating the SparkSession.
## ----------------------------------------------------------------------------------
### Method 3 - Implicitly via Spark Shell or Jupyter Notebook or Databricks Notebook
- When you are using the Spark Shell (interactive mode) or Jupyter Notebook with PySpark, a SparkSession is automatically created for you, and you can access it using the spark variable.
## ----------------------------------------------------------------------------------
### Method 4 -  Using pyspark.sql.SparkSession.newSession Method
- You can create a new session from an existing SparkSession using the newSession method. This new session shares the same SparkContext but has its own SQL configurations and temporary views.
```sql
new_spark = spark.newSession()
```
## ----------------------------------------------------------------------------------
## Example -
```sql
from pyspark.sql import SparkSession

-- Creating a SparkSession using builder method
spark = SparkSession.builder \
.appName(“MySparkApp”) \
.config(“key”, “value”) \
.getOrCreate()

-- Using the SparkSession
df = spark.read.csv(“path/to/data.csv”)
df.show()
```
- In general, the first method using SparkSession.builder is the most commonly used and provides the flexibility to configure various Spark parameters. The choice of method depends on your specific use case and preferences.
