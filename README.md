![name-of-you-image](https://cdn-images-1.medium.com/v2/resize:fit:851/1*nPcdyVwgcuEZiEZiRqApug.jpeg)
# Introduction about Apache Spark and PySpark - 
- ###  Spark - The Apache Spark is a fast and powerful framework that provides an API to perform massive distributed processing over resilient sets of data. It also ensures data processing with lightning speed and supports various languages like Scala, Python, Java, and R.

##### ------------------------------------------------------------------------------------------------------------------------------------------------------------

- ### PySpark (Python API for Spark) - The PySpark is the Python API for using Apache Spark, which is a parallel and distributed engine used to perform big data analytics. In the era of big data, PySpark is extensively used by Python users for performing data analytics on massive datasets and building applications using distributed clusters.

##### ------------------------------------------------------------------------------------------------------------------------------------------------------------

# Spark architecture
![name-of-you-image](https://miro.medium.com/v2/resize:fit:1100/format:webp/1*oP13RtCYqYJS74NoqonTpA.png)

#### 1. The Spark driver - The driver is the program or process responsible for coordinating the execution of the Spark application. It runs the main function and creates the SparkContext, which connects to the cluster manager.

#### 2. The Spark executors - Executors are worker processes responsible for executing tasks in Spark applications. They are launched on worker nodes and communicate with the driver program and cluster manager. Executors run tasks concurrently and store data in memory or disk for caching and intermediate storage.

#### 3. The cluster manager - The cluster manager is responsible for allocating resources and managing the cluster on which the Spark application runs. Spark supports various cluster managers like Apache Mesos, Hadoop YARN, and standalone cluster manager.

#### 4. sparkContext/SparkSession - SparkContext is the entry point for any Spark functionality. It represents the connection to a Spark cluster and can be used to create RDDs (Resilient Distributed Datasets), accumulators, and broadcast variables. SparkContext also coordinates the execution of tasks.

#### 5. Task - A task is the smallest unit of work in Spark, representing a unit of computation that can be performed on a single partition of data. The driver program divides the Spark job into tasks and assigns them to the executor nodes for execution.

##### ------------------------------------------------------------------------------------------------------------------------------------------------------------

- ## Working Of Spark Architecture
#### When the Driver Program in the Apache Spark architecture executes, it calls the real program of an application and creates a SparkContext. SparkContext contains all of the basic functions. The Spark Driver includes several other components, including a DAG Scheduler, Task Scheduler, Backend Scheduler, and Block Manager, all of which are responsible for translating user-written code into jobs that are actually executed on the cluster. Spark Driver and SparkContext collectively watch over the job execution within the cluster

#### The Cluster Manager manages the execution of various jobs in the cluster. Spark Driver works in conjunction with the Cluster Manager to control the execution of various other jobs. The cluster Manager does the task of allocating resources for the job. Once the job has been broken down into smaller jobs, which are then distributed to worker nodes, SparkDriver will control the execution. Many worker nodes can be used to process an RDD(Resilient Distributed Dataset) created in SparkContext, and the results can also be cached.

#### The SparkContext receives task information from the Cluster Manager and enqueues it on worker nodes. The executor is in charge of carrying out these duties. The lifespan of executors is the same as that of the Spark Application. We can increase the number of workers if we want to improve the performance of the system. In this way, we can divide jobs into more coherent parts.

- ## Two Main Abstractions of Apache Spark
#### Apache Spark has a well-defined layer architecture that is designed on two main abstractions:

#### 1. Resilient Distributed Dataset (RDD): RDD is an immutable (read-only), fundamental collection of elements or items that can be operated on many devices at the same time (spark parallel processing). Each dataset in an RDD can be divided into logical portions, which are then executed on different nodes of a cluster.

#### 2. Directed Acyclic Graph (DAG): DAG is the scheduling layer of the Apache Spark architecture that implements stage-oriented scheduling. Compared to MapReduce which creates a graph in two stages, Map and Reduce, Apache Spark can create DAGs that contain many stages.

- ## Cluster Manager Types
#### The system currently supports several cluster managers:

#### 1. Standalone — a simple cluster manager included with Spark that makes it easy to set up a cluster.
#### 2. Apache Mesos — a general cluster manager that can also run Hadoop MapReduce and service applications.
#### 3. Hadoop YARN — the resource manager in Hadoop 2.
#### 4. Kubernetes — an open-source system for automating deployment, scaling, and management of containerized applications.

- ## Execution Modes

#### 1. Cluster mode - Cluster mode is probably the most common way of running Spark Applications. In cluster mode, a user submits a pre-compiled JAR, Python script, or R script to a cluster manager. The cluster manager then launches the driver process on a worker node inside the cluster, in addition to the executor processes. This means that the cluster manager is responsible for maintaining all Spark Application–related processes.

#### 2. Client mode - Client mode is nearly the same as cluster mode except that the Spark driver remains on the client machine that submitted the application. This means that the client machine is responsible for maintaining the Spark driver process, and the cluster manager maintains the executor processes.

#### 3. Local mode - Local mode is a significant departure from the previous two modes: it runs the entire Spark Application on a single machine. It achieves parallelism through threads on that single machine. This is a common way to learn Spark, test your applications, or experiment iteratively with local development.

##### ------------------------------------------------------------------------------------------------------------------------------------------------------------

# Spark Components
![name-of-you-image](https://miro.medium.com/v2/resize:fit:1100/format:webp/1*VUp64pzbEwIJgS6eev0MwQ.png)

#### 1. Core - Contains the basic functionality of Spark. Also home to the API that defines RDDs, which is Spark’s main programming abstraction.
#### 2. SQL - Package for working with structured data. It allows querying data via SQL as well as Apache Hive. It supports various sources of data, like Hive tables, Parquet, JSON, CSV, etc.
#### 3. Streaming - Enables processing of live streams of data. Spark Streaming provides an API for manipulating data streams that are similar to Spark Core’s RDD API.
#### 4. MLlib - Provides multiple types of machine learning algorithms, like classification, regression, clustering, etc.
#### 5. GraphX - Library for manipulating graphs and performing graph-parallel computations. This library is where you can find PageRank and triangle counting algorithms.

##### ------------------------------------------------------------------------------------------------------------------------------------------------------------

# Spark features
![name-of-you-image](https://miro.medium.com/v2/resize:fit:828/format:webp/1*R1iWlKNh9cMxayTJgCoOeg.png)
