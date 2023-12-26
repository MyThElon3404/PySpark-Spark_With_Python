![name-of-you-image](https://tse3.mm.bing.net/th/id/OIP.HQOz-XE1ErWcjeXWCcYijQHaEL?rs=1&pid=ImgDetMain)
# Introduction about Apache Spark and PySpark - 
### <!> Spark - The Apache Spark is a fast and powerful framework that provides an API to perform massive distributed processing over resilient sets of data. It also ensures data processing with lightning speed and supports various languages like Scala, Python, Java, and R.

### <!> PySpark (Python API for Spark) - The PySpark is the Python API for using Apache Spark, which is a parallel and distributed engine used to perform big data analytics. In the era of big data, PySpark is extensively used by Python users for performing data analytics on massive datasets and building applications using distributed clusters.

# How a Spark Application Runs on a Cluster
![name-of-you-image](https://miro.medium.com/v2/resize:fit:1100/format:webp/1*LK_ZDRzQKHrFp8RqOfijeA.png)

#### 1. A Spark application runs as independent processes, coordinated by the SparkSession object in the driver program.
#### 2. The resource or cluster manager assigns tasks to workers, one task per partition.
#### 3. A task applies its unit of work to the dataset in its partition and outputs a new partition dataset. Because iterative algorithms apply operations repeatedly to data, they benefit from caching datasets across iterations.
#### 4. Results are sent back to the driver application or can be saved to disk.

# Spark Components
![name-of-you-image](https://miro.medium.com/v2/resize:fit:1100/format:webp/1*VUp64pzbEwIJgS6eev0MwQ.png)

#### 1. Core - Contains the basic functionality of Spark. Also home to the API that defines RDDs, which is Spark’s main programming abstraction.
#### 2. SQL - Package for working with structured data. It allows querying data via SQL as well as Apache Hive. It supports various sources of data, like Hive tables, Parquet, JSON, CSV, etc.
#### 3. Streaming - Enables processing of live streams of data. Spark Streaming provides an API for manipulating data streams that are similar to Spark Core’s RDD API.
#### 4. MLlib - Provides multiple types of machine learning algorithms, like classification, regression, clustering, etc.
#### 5. GraphX - Library for manipulating graphs and performing graph-parallel computations. This library is where you can find PageRank and triangle counting algorithms.

# Spark features
![name-of-you-image](https://miro.medium.com/v2/resize:fit:828/format:webp/1*R1iWlKNh9cMxayTJgCoOeg.png)
