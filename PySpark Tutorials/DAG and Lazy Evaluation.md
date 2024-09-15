# DAG - Directed Acyclic Graph ->
### DAG (Directed Acyclic Graph) in Spark/PySpark is a fundamental concept that plays a crucial role in the Spark execution model. The DAG is “directed” because the operations are executed in a specific order, and “acyclic” because there are no loops or cycles in the execution plan. This means that each stage depends on the completion of the previous stage, and each task within a stage can run independently of the other.

![image](https://sparkbyexamples.com/ezoimgfmt/i0.wp.com/sparkbyexamples.com/wp-content/uploads/2023/02/image-10.png?w=928&ssl=1&ezimgfmt=ng:webp/ngcb1)

# Dive Deep into DAGs
### Let’s explore more about DAGs by writing some code and understanding diff terms like  the jobs, stages, and tasks

#### for this refer - https://medium.com/plumbersofdatascience/understanding-spark-dags-b82020503444

# ----------------------------------------------------------------------------------------------------------------

# Lazy Evaluation ->
### Lazy evaluation refers to the way Spark handles the execution of data transformations and actions in a deferred manner, rather than immediately executing them when they are defined. This approach offers several advantages in terms of optimization and performance.

#### To know more - https://medium.com/@think-data/mastering-lazy-evaluation-a-must-know-for-pyspark-pros-ac855202495e
