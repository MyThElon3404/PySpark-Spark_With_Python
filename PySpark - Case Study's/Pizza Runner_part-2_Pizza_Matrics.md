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
