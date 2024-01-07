# Analysing Pizza data -> Pizza Matrics Solution
### tables (created on part-1) -> runners_tb, customer_orders_tb_new, runner_orders_tb_new, pizza_names_tb, pizza_recipes_df, pizza_toppings_tb 
## ------------------------------------------------------------------------------

### Q1. How many pizzas were ordered?
```sql
pizza_ss.sql("select count(*) as pizza_ordered_count from customer_orders_tb_new;").show()
```
### output :
```
+-------------------+
|pizza_ordered_count|
+-------------------+
|                 14|
+-------------------+
```
## ------------------------------------------------------------------------------
### Q2. How many unique customer orders were made?
```sql
pizza_ss.sql("select count(distinct order_id) as unique_orders from customer_orders_tb_new").show()
```
### output :
```
+-------------+
|unique_orders|
+-------------+
|           10|
+-------------+
```
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
```
+---------+----------------------+
|runner_id|delivered_successfully|
+---------+----------------------+
|        1|                     4|
|        2|                     3|
|        3|                     1|
+---------+----------------------+
```
## ------------------------------------------------------------------------------
### Q4. How many of each type of pizza was delivered?
```sql
pizza_ss.sql("""
                select p.pizza_name, count(c.pizza_id) as total_delivered 
                from runner_orders_tb r join customer_orders_tb_new c using (order_id)
                join pizza_names_tb p using (pizza_id)
                where r.distance != 0
                group by p.pizza_name;
""").show()
```
### output :
```
+----------+---------------+
|pizza_name|total_delivered|
+----------+---------------+
|Meatlovers|              3|
|Vegetarian|              1|
+----------+---------------+
```
## ------------------------------------------------------------------------------
### Q5. How many Vegetarian and Meatlovers were ordered by each customer?
```sql
pizza_ss.sql("""
                select c.customer_id, p.pizza_name, count(c.pizza_id) as total_ordered
                from customer_orders_tb_new c 
                join pizza_names_tb p using (pizza_id)
                group by c.customer_id, p.pizza_name
                order by c.customer_id;
""").show()
```
### output :
```
+-----------+----------+-------------+
|customer_id|pizza_name|total_ordered|
+-----------+----------+-------------+
|        101|Meatlovers|            2|
|        101|Vegetarian|            1|
|        102|Meatlovers|            2|
|        102|Vegetarian|            1|
|        103|Meatlovers|            3|
|        103|Vegetarian|            1|
|        104|Meatlovers|            3|
|        105|Vegetarian|            1|
+-----------+----------+-------------+
```
## ------------------------------------------------------------------------------
### Q6. What was the maximum number of pizzas delivered in a single order?
```sql
pizza_ss.sql("""
                    -- here they are asking about delivered pizza
                    select c.order_id, count(c.pizza_id) as pizza_per_order
                    from customer_orders_tb_new c 
                    join runner_orders_tb r using (order_id)
                    where r.distance != 0
                    group by c.order_id
                    order by pizza_per_order desc limit 1;    
""").show()
```
### output :
```
+--------+---------------+
|order_id|pizza_per_order|
+--------+---------------+
|       4|              3|
+--------+---------------+
```
## ------------------------------------------------------------------------------
### Q7. For each customer, how many delivered pizzas had at least 1 change and how many had no changes?
```sql
pizza_ss.sql("""
                select c.customer_id,
                    sum(case
                            when c.exclusions <> ' ' or extras <> ' ' then 1
                            else 0
                        end) as with_change,
                    sum(case
                            when c.exclusions = ' ' or extras = ' ' then 1
                            else 0
                        end) as no_change
                from customer_orders_tb_new c 
                join runner_orders_tb_new r using (order_id)
                where r.distance <> 0
                group by c.customer_id
                order by c.customer_id;
""").show()
```
### output :
```
+-----------+-----------+---------+
|customer_id|with_change|no_change|
+-----------+-----------+---------+
|        101|          2|        0|
|        102|          2|        1|
|        103|          3|        0|
|        104|          2|        2|
|        105|          1|        1|
+-----------+-----------+---------+
```
## ------------------------------------------------------------------------------
### Q8. How many pizzas were delivered that had both exclusions and extras?
```sql
pizza_ss.sql("""
                -- select count(c. pizza_id) as total_pizza_have_both_exclusions_extras
                -- from customer_orders_tb_new c
                -- join runner_orders_tb_new r using (order_id)
                -- where r.distance != 0 AND exclusions != ' ' AND extras != ' ';
                
                select sum(case 
                                when exclusions is not null and extras is not null then 1
                                else 0
                            end) as total_pizza_have_both_exclusions_extras
                from customer_orders_tb_new c
                join runner_orders_tb_new r using (order_id)
                where r.distance != 0 AND exclusions != ' ' AND extras != ' ';
""").show()
# runners_tb, customer_orders_tb_new, runner_orders_tb_new, pizza_names_tb, pizza_recipes_df, pizza_toppings_tb
```
### output :
```
+---------------------------------------+
|total_pizza_have_both_exclusions_extras|
+---------------------------------------+
|                                      8|
+---------------------------------------+
```
## ------------------------------------------------------------------------------
