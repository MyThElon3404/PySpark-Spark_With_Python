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
-In this approach, you can set various configuration options using the config method, such as the application name, master URL, and other Spark properties.
## --------------------------------------------------------------------------------
### Method 2 - Using SparkConf and SparkSession.builder.config Method
```sql
-- You can also use a combination of SparkConf and SparkSession.builder.config to set configuration options.
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf

conf = SparkConf().setAppName(“MySparkApp”).setMaster(“local[2]”)
spark = SparkSession.builder.config(conf=conf).getOrCreate()

-- This allows you to configure Spark through a SparkConf object before creating the SparkSession.
```
