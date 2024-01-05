# This case is related to Danny's Dinner Restaurant
#### Did you know that over 115 million kilograms of pizza is consumed daily worldwide??? (Well according to Wikipedia anyway…)
#### Danny was scrolling through his Instagram feed when something really caught his eye - “80s Retro Styling and Pizza Is The Future!”
#### Danny was sold on the idea, but he knew that pizza alone was not going to help him get seed funding to expand his new Pizza Empire - so he had one more genius idea to combine with it - he was going to Uberize it - and so [ Pizza Runner - the choice of taste ] was launched!

# Below we are creating a spark session and dataframe so we can perform actions accordingly

```` sql
# creating a spark session to get started with our spark jobs
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

pizza_ss= SparkSession.builder.appName("Restaurant_Dannys_Dinner").getOrCreate()
print(pizza_ss)
````
### - Answer :
- spark session created
# -------------------------------------------------
````sql
# we are creating dataframes
from pyspark.sql.functions import col, to_date
runner_data = [
    (1, '2021-01-01'),
	(2, '2021-01-03'),
	(3, '2021-01-08'),
	(4, '2021-01-15')]
runner_col = ["runner_id", "registration_date"]
runners_df = pizza_ss.createDataFrame(data=runner_data, schema=runner_col)
runners_df = runners_df.withColumn("registration_date", to_date(col("registration_date"), "yyyy-MM-dd"))
runners_df.show()
runners_df.printSchema()
````
### - Answer :
```
+---------+-----------------+
|runner_id|registration_date|
+---------+-----------------+
|        1|       2021-01-01|
|        2|       2021-01-03|
|        3|       2021-01-08|
|        4|       2021-01-15|
+---------+-----------------+
root
 |-- runner_id: long (nullable = true)
 |-- registration_date: date (nullable = true)
```
# -------------------------------------------------
````sql
# we are creating dataframes
customer_order_data = [
    ('1', '101', '1', '', '', '2020-01-01 18:05:02'),
	('2', '101', '1', '', '', '2020-01-01 19:00:52'),
	('3', '102', '1', '', '', '2020-01-02 23:51:23'),
	('3', '102', '2', '', '', '2020-01-02 23:51:23'),
	('4', '103', '1', '4', '', '2020-01-04 13:23:46'),
	('4', '103', '1', '4', '', '2020-01-04 13:23:46'),
	('4', '103', '2', '4', '', '2020-01-04 13:23:46'),
	('5', '104', '1', 'null', '1', '2020-01-08 21:00:29'),
	('6', '101', '2', 'null', 'null', '2020-01-08 21:03:13'),
	('7', '105', '2', 'null', '1', '2020-01-08 21:20:29'),
	('8', '102', '1', 'null', 'null', '2020-01-09 23:54:33'),
	('9', '103', '1', '4', '1, 5', '2020-01-10 11:22:59'),
	('10', '104', '1', 'null', 'null', '2020-01-11 18:34:49'),
	('10', '104', '1', '2, 6', '1, 4', '2020-01-11 18:34:49')
]
customer_order_col = ["order_id", "customer_id", "pizza_id", "exclusions", "extras", "order_time"]
customer_orders_df = pizza_ss.createDataFrame(data=customer_order_data, schema=customer_order_col)
customer_orders_df = customer_orders_df.withColumn("order_id", col("order_id").cast(IntegerType()))\
                                    .withColumn("customer_id", col("customer_id").cast(IntegerType()))\
                                    .withColumn("pizza_id", col("pizza_id").cast(IntegerType()))
customer_orders_df.show()
customer_orders_df.printSchema()
````
### - Answer :
```
+--------+-----------+--------+----------+------+-------------------+
|order_id|customer_id|pizza_id|exclusions|extras|         order_time|
+--------+-----------+--------+----------+------+-------------------+
|       1|        101|       1|          |      |2020-01-01 18:05:02|
|       2|        101|       1|          |      |2020-01-01 19:00:52|
|       3|        102|       1|          |      |2020-01-02 23:51:23|
|       3|        102|       2|          |      |2020-01-02 23:51:23|
|       4|        103|       1|         4|      |2020-01-04 13:23:46|
|       4|        103|       1|         4|      |2020-01-04 13:23:46|
|       4|        103|       2|         4|      |2020-01-04 13:23:46|
|       5|        104|       1|      null|     1|2020-01-08 21:00:29|
|       6|        101|       2|      null|  null|2020-01-08 21:03:13|
|       7|        105|       2|      null|     1|2020-01-08 21:20:29|
|       8|        102|       1|      null|  null|2020-01-09 23:54:33|
|       9|        103|       1|         4|  1, 5|2020-01-10 11:22:59|
|      10|        104|       1|      null|  null|2020-01-11 18:34:49|
|      10|        104|       1|      2, 6|  1, 4|2020-01-11 18:34:49|
+--------+-----------+--------+----------+------+-------------------+
root
 |-- order_id: integer (nullable = true)
 |-- customer_id: integer (nullable = true)
 |-- pizza_id: integer (nullable = true)
 |-- exclusions: string (nullable = true)
 |-- extras: string (nullable = true)
 |-- order_time: string (nullable = true)
```
# -------------------------------------------------
````sql
# we are creating dataframes
runner_orders_data = [
    ('1', '1', '2020-01-01 18:15:34', '20km', '32 minutes', ''),
	('2', '1', '2020-01-01 19:10:54', '20km', '27 minutes', ''),
	('3', '1', '2020-01-03 00:12:37', '13.4km', '20 mins', ''),
	('4', '2', '2020-01-04 13:53:03', '23.4', '40', ''),
	('5', '3', '2020-01-08 21:10:57', '10', '15', ''),
	('6', '3', 'null', 'null', 'null', 'Restaurant Cancellation'),
	('7', '2', '2020-01-08 21:30:45', '25km', '25mins', 'null'),
	('8', '2', '2020-01-10 00:15:02', '23.4 km', '15 minute', 'null'),
	('9', '2', 'null', 'null', 'null', 'Customer Cancellation'),
	('10', '1', '2020-01-11 18:50:20', '10km', '10minutes', 'null')
]
runner_orders_col = ["order_id", "runner_id", "pickup_time", "distance", "duration", "cancellation"]
runner_orders_df = pizza_ss.createDataFrame(runner_orders_data, runner_orders_col)
runner_orders_df = runner_orders_df.withColumn("order_id", col("order_id").cast(IntegerType()))\
                                .withColumn("runner_id", col("runner_id").cast(IntegerType()))
runner_orders_df.show(truncate=False)
runner_orders_df.printSchema()
````
### - Answer :
```
+--------+---------+-------------------+--------+----------+-----------------------+
|order_id|runner_id|pickup_time        |distance|duration  |cancellation           |
+--------+---------+-------------------+--------+----------+-----------------------+
|1       |1        |2020-01-01 18:15:34|20km    |32 minutes|                       |
|2       |1        |2020-01-01 19:10:54|20km    |27 minutes|                       |
|3       |1        |2020-01-03 00:12:37|13.4km  |20 mins   |                       |
|4       |2        |2020-01-04 13:53:03|23.4    |40        |                       |
|5       |3        |2020-01-08 21:10:57|10      |15        |                       |
|6       |3        |null               |null    |null      |Restaurant Cancellation|
|7       |2        |2020-01-08 21:30:45|25km    |25mins    |null                   |
|8       |2        |2020-01-10 00:15:02|23.4 km |15 minute |null                   |
|9       |2        |null               |null    |null      |Customer Cancellation  |
|10      |1        |2020-01-11 18:50:20|10km    |10minutes |null                   |
+--------+---------+-------------------+--------+----------+-----------------------+
root
 |-- order_id: integer (nullable = true)
 |-- runner_id: integer (nullable = true)
 |-- pickup_time: string (nullable = true)
 |-- distance: string (nullable = true)
 |-- duration: string (nullable = true)
 |-- cancellation: string (nullable = true)
```
# -------------------------------------------------
````sql
# we are creating dataframes
pizza_names_data = [
    (1, 'Meatlovers'),
	(2, 'Vegetarian')
]
pizza_names_col = StructType([
    StructField("pizza_id", IntegerType(), True),
    StructField("pizza_name", StringType(), True)
])
pizza_names_df = pizza_ss.createDataFrame(pizza_names_data, pizza_names_col)
pizza_names_df.show()
pizza_names_df.printSchema()
````
### - Answer :
```
+--------+----------+
|pizza_id|pizza_name|
+--------+----------+
|       1|Meatlovers|
|       2|Vegetarian|
+--------+----------+
root
 |-- pizza_id: integer (nullable = true)
 |-- pizza_name: string (nullable = true)
```
# -------------------------------------------------
````sql
# we are creating dataframes
pizza_recipes_data = [
    (1, '1, 2, 3, 4, 5, 6, 8, 10'),
	(2, '4, 6, 7, 9, 11, 12')
]
pizza_recipes_col = StructType([
    StructField("pizza_id", IntegerType(), True), 
    StructField("toppings", StringType(), True)
])
pizza_recipes_df = pizza_ss.createDataFrame(pizza_recipes_data, pizza_recipes_col)
pizza_recipes_df.show(truncate=False)
pizza_recipes_df.printSchema()
````
### - Answer :
```
+--------+-----------------------+
|pizza_id|toppings               |
+--------+-----------------------+
|1       |1, 2, 3, 4, 5, 6, 8, 10|
|2       |4, 6, 7, 9, 11, 12     |
+--------+-----------------------+

root
 |-- pizza_id: integer (nullable = true)
 |-- toppings: string (nullable = true)
```
# -------------------------------------------------
````sql
# we are creating dataframes
pizza_toppings_data = [
    (1, 'Bacon'),
	(2, 'BBQ Sauce'),
	(3, 'Beef'),
	(4, 'Cheese'),
	(5, 'Chicken'),
	(6, 'Mushrooms'),
	(7, 'Onions'),
	(8, 'Pepperoni'),
	(9, 'Peppers'),
	(10, 'Salami'),
	(11, 'Tomatoes'),
	(12, 'Tomato Sauce')
]
pizza_toppings_col = StructType([
    StructField("topping_id", IntegerType(), True), 
    StructField("topping_name", StringType(), True)
])
pizza_toppings_df = pizza_ss.createDataFrame(pizza_toppings_data, pizza_toppings_col)
pizza_toppings_df.show()
pizza_toppings_df.printSchema()
````
### - Answer :
```
+----------+------------+
|topping_id|topping_name|
+----------+------------+
|         1|       Bacon|
|         2|   BBQ Sauce|
|         3|        Beef|
|         4|      Cheese|
|         5|     Chicken|
|         6|   Mushrooms|
|         7|      Onions|
|         8|   Pepperoni|
|         9|     Peppers|
|        10|      Salami|
|        11|    Tomatoes|
|        12|Tomato Sauce|
+----------+------------+
root
 |-- topping_id: integer (nullable = true)
 |-- topping_name: string (nullable = true)
```
# -------------------------------------------------

### - to enable sparkSQL functionality in dataframe we need to create a temp view on top of dataframe

````
runners_df.createOrReplaceTempView("runners_tb")
customer_orders_df.createOrReplaceTempView("customer_orders_tb")
runner_orders_df.createOrReplaceTempView("runner_orders_tb")
pizza_names_df.createOrReplaceTempView("pizza_names_tb")
pizza_recipes_df.createOrReplaceTempView("pizza_recipes_df")
pizza_toppings_df.createOrReplaceTempView("pizza_toppings_tb")
````
# -------------------------------------------------

## - Let's clean our datasets so we can perform oprations on that
