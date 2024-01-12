# <- Various ways to create a SparkSession object in PySpark ->
## In PySpark, a SparkSession is the entry point for reading data and executing SQL queries.

### Method 1 - Using SparkSession.builder Method
```sql
-- The most common and recommended way is to use the SparkSession.builder method to configure and create a SparkSession.

from pyspark.sql import SparkSession

spark = SparkSession.builder \
.appName(“MySparkApp”) \
.config(“key”, “value”) \
.getOrCreate()

-- In this approach, you can set various configuration options using the config method, such as the application name, master URL, and other Spark properties.
```
