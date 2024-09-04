# 🛍️ Retail Store Sales Analysis 🥗

-----------------------------------------------------------------------------------------------------------------------------------------

## QUESTIONS & ANSWERS :

```sql
from pyspark.sql import SparkSession
from pyspark.sql.functions import (count, when, isnull, col, max, date_sub, to_date,lit,sum, round,
                                   desc, month, year, avg, rank, lag, dayofweek, mean, collect_set, explode)
from pyspark.sql.window import Window


# create spark session
ss = SparkSession.builder.appName("Retail_Sales").master("local[*]").getOrCreate()
print("Spark Session created: ", ss)


# read data file and show schema, Data
file_path = "E:\My Workspace\Data Files for Project\pyspark_retail_sales.csv" # keep you file path here
df = ss.read.csv(file_path, header=True, inferSchema=True)
df.printSchema()
df.show(n=10, truncate=False)


# Data Cleaning - Handle Missing Values
# step 1 - check for missing data
df.select([count(when(isnull(c), c)).alias(c) for c in df.columns]).show()
# step 2 - if there are any null value, drop it
df_clean = df.dropna()
df_clean.show(9)
print(df.count())


# Filter Transactions Based on Date
df = df.withColumn("Date", to_date(df["Date"], "yyyy-MM-dd"))
max_date = df.select(max(col("Date"))).collect()[0][0]
six_month_ago = date_sub(lit(max_date), 180)
recent_tranc = df.filter(df["Date"] >= six_month_ago)
recent_tranc.show(9)


# Aggregate Total Sales by Product Category and round final result by 2
sales_by_category = df.groupBy("ProductCategory").agg(sum("TotalSales").alias("CateWiseTotalSales"))
result = sales_by_category.withColumn("CateWiseTotalSales", round(col("CateWiseTotalSales"), 2))
result.show()


# Find the Top 5 Selling Products
top_selling_product = df.groupBy("ProductID").agg(sum("QuantitySold").alias("TotalQuantitySold"))
top_5_selling_products = top_selling_product.orderBy(desc(col("TotalQuantitySold"))).limit(5)
top_5_selling_products.show()


# Analyze Sales Trends Over Time
df = df.withColumn("Date", to_date(col("Date"), "yyyy-MM-dd"))
max_date = df.select(max(col("Date"))).collect()[0][0]
last_year = date_sub(lit(max_date), 365)
year_sales = df.filter(col("Date") >= last_year)
montly_sales = (year_sales.groupBy(year(col("Date")).alias("Year"), month(col("Date")).alias("Month"))
                .agg(sum(col("TotalSales")).alias("TotalSales")).orderBy(col("Year"), col("Month")))
montly_sales.withColumn("TotalSales", round(col("TotalSales"), 2)).show(9)


# Customer Segmentation Based on Total Spending
customer_spending = df.groupBy("CustomerID").agg(sum(col("TotalSales")).alias("TotalSales"))
segmented_customer = customer_spending.select("CustomerID", round(col("TotalSales"), 2).alias("TotalSales"),
                                        when(col("TotalSales") < 500, "Low")
                                        .when(col("TotalSales").between(500, 2000), "Medium")
                                        .otherwise("High").alias("CustomerSegmented")).orderBy(col("CustomerID"))
segmented_customer.show()


# Identify the Most Popular Payment Method
popular_payment_method = df.groupBy("PaymentMethod").count().orderBy(desc("count"))
popular_payment_method.show(1)


# Calculate Average Sales per Transaction
avg_transaction_per_sale = df.agg(avg(col("TotalSales")).alias("AverageSalesPerTransaction"))
avg_transaction_per_sale.show()
# OR Suppose transaction id's are repetitive
sales_per_tranc = df.groupBy("TransactionID").agg(sum(col("TotalSales")).alias("TotalSalesPerTransaction"))
avg_tranc = sales_per_tranc.agg(avg(col("TotalSalesPerTransaction")).alias("AvgSalesPerTranc"))
avg_tranc.show(9)


# Detect and Remove Duplicate Transactions
duplicate = df.groupBy("TransactionID").count().filter(col("count") > 1)
duplicate.show() # show us duplicate is there or not
df_remove_duplicate = df.dropDuplicates(["TransactionID"])
df_remove_duplicate.show(9)


# Calculate Sales Contribution by Each Store
store_sales = df.groupBy("StoreID").agg(sum("TotalSales").alias("StoreSales"))
total_sales = df.agg(sum("TotalSales").alias("TotalSales")).collect()[0]["TotalSales"]
store_sales_contribution = store_sales.withColumn("StoreSales", round((col("StoreSales") / total_sales)*100, 2))
store_sales_contribution.show()


# Analyze Sales Performance by Product Category
category_sales = df.groupBy("ProductCategory").agg(round(sum("TotalSales"), 2).alias("TotalCategorySales"))
window_spec = Window.orderBy("TotalCategorySales")
ranked_category = category_sales.withColumn("Rank", rank().over(window_spec))
ranked_category.show(9)
# OR (WARN WindowExec: No Partition Defined for Window operation! Moving all data to a single partition,
# this can cause serious performance degradation.)
window_spec_new = Window.partitionBy("StoreID").orderBy("TotalCategorySales")
ranked_category_new = category_sales.withColumn("Rank", rank().over(window_spec))
ranked_category_new.show(9)


# Identify Customers with the Highest Transaction Frequency
tranc_freq = df.groupBy("CustomerID").count().alias("TranCount")
top_customer = tranc_freq.orderBy(desc("count")).limit(5)
top_customer.show()


# Calculate the Average Number of Products Sold per Transaction
avg_prod_per_tranc = df.agg(avg("QuantitySold").alias("AvgProdPerTranc"))
avg_prod_per_tranc.show(9)


# Perform a Year-Over-Year Sales Growth Analysis
df_with_year = df.withColumn("Year", year("date").alias("Year"))
yearly_sales = df_with_year.groupBy("Year").agg(round(sum("TotalSales"), 2).alias("TotalSales"))
window_spec = Window.orderBy("Year")
yoy_sales_growth = yearly_sales.withColumn("YoyGrowth",
                                           (col("TotalSales") - lag("TotalSales", 1).over(window_spec)) /
                                           (lag("TotalSales", 1).over(window_spec)) * 100)
yoy_sales_growth = yoy_sales_growth.withColumn("YoyGrowth", round("YoyGrowth", 2))
yoy_sales_growth.show()


# Identify the Most Profitable Day of the Week
df_with_days = df.withColumn("DayOfWeek", dayofweek("Date"))
df_with_days.show()
sales_by_day = df_with_days.groupBy("DayOfWeek").agg(sum("TotalSales").alias("TotalSales")).orderBy(desc("TotalSales"))
sales_by_day.show(1)


# Perform a Sales Forecasting
monthly_sales = (df.groupBy(year("Date").alias("Year"), month("Date").alias("Month"))
                 .agg(sum("TotalSales").alias("TotalSales")).orderBy("Year", "Month"))
window_spec = Window.orderBy("Year", "Month").rowsBetween(-2, 0)
monthly_sales_with_forecast = monthly_sales.withColumn("MovingAverage", mean("TotalSales").over(window_spec))
monthly_sales_with_forecast.show()


# Analyze Payment Method Preferences by Customer Segment
customer_spending = df.groupBy("CustomerID").agg(sum("TotalSales").alias("TotalSpending"))
segmented_customers = customer_spending.withColumn("SpenderCategory",
                                                   when(col("TotalSpending") < 500, "Low")
                                                   .when(col("TotalSpending").between(500, 2000), "Medium")
                                                   .otherwise("High"))
df_with_segments = df.join(segmented_customers, "CustomerID")
payment_preferences = (df_with_segments.groupBy("SpenderCategory", "PaymentMethod").count()
                       .orderBy("SpenderCategory", "PaymentMethod"))
payment_preferences.show()


# purchased products together (purchased products together)
product_baskets = df.groupBy("TransactionID").agg(collect_set("ProductID").alias("ProductBasket"))
product_pairs = product_baskets.withColumn("ProductID", explode("ProductBasket")).select("TransactionID", "ProductID")
co_occurrence = product_pairs.alias("a").join(product_pairs.alias("b"), "TransactionID") \
                .filter(col("a.ProductID") != col("b.ProductID")) \
                .groupBy("a.ProductID", "b.ProductID").count().orderBy(desc("count"))
co_occurrence.show()

```
