#### When it comes to creating Spark applications, there are two primary approaches: SparkContext and SparkSession.

# SparkContext -
### SparkContext is the traditional entry point to any Spark functionality. It represents the connection to a Spark cluster and is the place where the user can configure the common properties for the entire application and acts as a gateway to creating Resilient Distributed Datasets (RDDs). RDDs are the fundamental data structure in Spark, providing fault-tolerant and parallelized data processing. SparkContext is designed for low-level programming and fine-grained control over Spark operations. However, it requires explicit management and can only be used once in a Spark application, and it must be created before creating any RDDs or SQLContext.

# SparkSession - 
### SparkSession was introduced in Spark 2.0, and is a unified interface that combines Spark’s various functionalities into a single entry point. SparkSession integrates SparkContext and provides a higher-level API for working with structured data through Spark SQL, streaming data with Spark Streaming, and performing machine learning tasks with MLlib. It simplifies application development by automatically creating a SparkContext and providing a seamless experience across different Spark modules. With SparkSession, developers can leverage Spark’s capabilities without explicitly managing multiple contexts.

![SparkSession](https://miro.medium.com/v2/resize:fit:1400/format:webp/0*ivIifEQVPN9s_8wi.png)

To know about Use cases and scenarios - https://medium.com/@akhilasaineni7/exploring-sparkcontext-and-sparksession-8369e60f658e

Also for Hands-on approach - https://medium.com/@akhilasaineni7/exploring-sparkcontext-and-sparksession-8369e60f658e
