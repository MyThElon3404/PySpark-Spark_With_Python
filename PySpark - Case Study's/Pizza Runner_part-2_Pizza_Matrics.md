# Analysing Pizza data -> Pizza Matrics Solution
### tables (created on part-1) -> runners_tb, customer_orders_tb_new, runner_orders_tb_new, pizza_names_tb, pizza_recipes_df, pizza_toppings_tb 
## ------------------------------------------------------------------------------

### Q1. How many pizzas were ordered?
```sql
pizza_ss.sql("select count(*) as pizza_ordered_count from customer_orders_tb_new;").show()
```
### output :
+-------------------+
|pizza_ordered_count|
+-------------------+
|                 14|
+-------------------+
## ------------------------------------------------------------------------------
### Q2. How many unique customer orders were made?
```sql
pizza_ss.sql("select count(distinct order_id) as unique_orders from customer_orders_tb_new").show()
```
### output :
+-------------+
|unique_orders|
+-------------+
|           10|
+-------------+

## ------------------------------------------------------------------------------
### Q3. How many successful orders were delivered by each runner?
```sql
pizza_ss.sql("""
                select runner_id, count(distinct order_id) as delivered_successfully
                from runner_orders_tb_new
                where distance <> 0
                group by runner_id
                order by delivered_successfully desc;
""").show()
```
### output :
+---------+----------------------+
|runner_id|delivered_successfully|
+---------+----------------------+
|        1|                     4|
|        2|                     3|
|        3|                     1|
+---------+----------------------+
## ------------------------------------------------------------------------------

## ------------------------------------------------------------------------------

## ------------------------------------------------------------------------------

## ------------------------------------------------------------------------------

## ------------------------------------------------------------------------------

## ------------------------------------------------------------------------------

## ------------------------------------------------------------------------------

## ------------------------------------------------------------------------------

## ------------------------------------------------------------------------------
