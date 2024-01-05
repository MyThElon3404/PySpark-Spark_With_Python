# This case is related to Danny's Dinner Restaurant
#### Did you know that over 115 million kilograms of pizza is consumed daily worldwide??? (Well according to Wikipedia anyway…)
#### Danny was scrolling through his Instagram feed when something really caught his eye - “80s Retro Styling and Pizza Is The Future!”
#### Danny was sold on the idea, but he knew that pizza alone was not going to help him get seed funding to expand his new Pizza Empire - so he had one more genius idea to combine with it - he was going to Uberize it - and so [ Pizza Runner - the choice of taste ] was launched!

```
# creating a spark session to get started with our spark jobs

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

pizza_ss= SparkSession.builder.appName("Restaurant_Dannys_Dinner").getOrCreate()
print(pizza_ss)
```
