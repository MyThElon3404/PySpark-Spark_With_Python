## 1. Spark Core - 
#### All the functionalities being provided by Apache Spark are built on top of Spark Core. It delivers speed by providing in-memory computation capability. Thus Spark Core is the foundation of parallel and distributed processing of huge datasets.

## The key features of Apache Spark Core are:-

#### 1. It is in charge of essential I/O functionalities.
#### 2. Significant in programming and observing the role of the Spark cluster.
#### 3. Task dispatching.
#### 4. Fault recovery.
#### 5. It overcomes the snag of MapReduce by using in-memory computation.

#### ---------------------------------------------------------------------------------------------------------------

## 2. Spark SQL - 
#### Spark SQL is a new module in Spark that integrates relational processing with Spark’s functional programming API. It supports querying data either via SQL or via the Hive Query Language. For those of you familiar with RDBMS, Spark SQL will be an easy transition from your earlier tools where you can extend the boundaries of traditional relational data processing.

#### ---------------------------------------------------------------------------------------------------------------

## 3. Spark Streaming - 
#### Spark Streaming is the component of Spark that is used to process real-time streaming data. Thus, it is a useful addition to the core Spark API. It enables high-throughput and fault-tolerant stream processing of live data streams.

#### ---------------------------------------------------------------------------------------------------------------

## 4. Spark MLlib (Machine Learning Library) - 
#### MLlib in Spark is a scalable Machine learning library that discusses both high-quality algorithms and high speed. The motive behind MLlib creation is to make machine learning scalable and easy. It contains machine learning libraries that have an implementation of various machine learning algorithms. For example, clustering, regression, classification, and collaborative filtering.

#### ---------------------------------------------------------------------------------------------------------------

## 5. Spark GraphX - 
#### GraphX in Spark is an API for graphs and graph parallel execution. It is a network graph analytics engine and data store. Clustering, classification, traversal, searching, and pathfinding are also possible in graphs.

#### ---------------------------------------------------------------------------------------------------------------

## 6. SparkR - 
#### R also provides software facilities for data manipulation, calculation, and graphical display. Hence, the main idea behind SparkR was to explore different techniques to integrate the usability of R with the scalability of Spark. It is an R package that gives a lightweight frontend to use Apache Spark from R.

![name-of-you-image](https://miro.medium.com/v2/resize:fit:1100/format:webp/1*UsRTz2Xlz6hnhj5cWnCtcQ.png)
![name-of-you-image](https://miro.medium.com/v2/resize:fit:1400/format:webp/0*kGd77SnPU7i8gjtl.png)

#### ---------------------------------------------------------------------------------------------------------------

# Resilient Distributed Dataset(RDD) ->
### RDDs are the building blocks of any Spark application. RDDs Stand for:

#### 1. Resilient:- Fault-tolerant and capable of rebuilding data on failure
#### 2. Distributed:- Distributed data among the multiple nodes in a cluster
#### 3. Dataset:- Collection of partitioned data with values

![name-of-you-image](https://d1jnx9ba8s6j9r.cloudfront.net/blog/wp-content/uploads/2018/07/Partitions.png)
