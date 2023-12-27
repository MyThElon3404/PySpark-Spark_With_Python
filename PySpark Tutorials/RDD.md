# RDD - Resilient Distributed Datasets
#### Resilient Distributed Datasets (RDDs) are the building blocks of PySpark. RDD is the fundamental data structure of Apache Spark. It is an immutable collection of objects that can be split across the cluster facilitating in-memory data storage thus making it more efficient. RDD being a schema-less data structure, can handle structured as well as unstructured data. While RDDs can be logically partitioned across various nodes in a cluster, operations on RDDs can also be split and executed in parallel, facilitating faster and more scalable parallel processing. RDD is a distributed memory abstraction, and one of its salient features is that it is highly resilient. This is because the data chunks are replicated across multiple nodes in the cluster and thus, they can recover quickly from any issue as the data will still be processed by other nodes even if one executor node fails, making it highly fault tolerant. Also, RDDs follow lazy evaluation, that is, the execution is not started right away, but rather they are triggered only when needed. Two basic and important operations can be carried out on RDDs — Transformations and Actions.

## Transformations - 
#### Transformations are operations on the RDDs that create a new RDD by making changes to the original RDD. Briefly, they are functions that take an existing RDD as input to give new RDDs as output, without making changes to the original RDD (Note that RDDs are immutable!) This process of transforming an RDD to a new one is done through operations such as filter, map, reduceByKey, sortBy etc.

###  The transformations on RDD can be categorized into two: Narrow and Wide.

#### 1. Narrow transformations - the result of the transformation is such that in the output RDD each of the partitions has records that are from the same partition in the parent RDD. Operations like Map, FlatMap, Filter, and Sample come under narrow transformations.

#### 2. Wide transformations - the data in each of the partitions of the resulting RDD comes from multiple different partitions in the parent RDD. Transformation functions like groupByKey(), and reduceByKey() fall under the category of wide transformation.

![name-of-you-image](https://miro.medium.com/v2/resize:fit:1400/format:webp/1*6dYzs4GK3Q9nSPkMp-3w2w.png)

## Actions - 
#### Actions are the operations on the RDD to carry out computations and return the final result back to the driver. It triggers the execution to carry out intermediate transformations after loading the data into the RDD and finally passing the result back. These actions are operations that result in non-RDD values. As we already saw, the transformations are executed only when an action requires a result to be returned. Collect, reduce, countByKey, count are some of the actions.

#### For hands-on approach - https://medium.com/analytics-vidhya/understanding-spark-rdds-part-3-3b1b9331652a
