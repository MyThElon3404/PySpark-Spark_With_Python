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
## - Answer :
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
## - Answer :
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
