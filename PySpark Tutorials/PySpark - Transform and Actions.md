![name-of-you-image](https://miro.medium.com/v2/resize:fit:1100/format:webp/0*kGd77SnPU7i8gjtl.png)

# Transformations and Actions - 
### Common Spark jobs are created using operations in DataFrame API. These operations are either transformations or actions.

## Transformation: In Apache Spark, transformations are operations that are applied to an RDD (Resilient Distributed Dataset) to create a new RDD. Transformations are lazy, which means that they are not executed until an action is called. This allows Spark to optimize the execution plan for a series of transformations by delaying the execution until the final result is needed.

## Action: Actions are operations that trigger the execution of a Spark job and return a result. i.e. A spark operation that either returns a result or writes to the disc.

### To know more - https://medium.com/@Rohit_Varma/unleashing-the-power-of-transformation-and-action-in-apache-spark-5d778f3c0ae5#:~:text=While%20transformations%20are%20lazy%2C%20actions%20are%20operations%20that,compute%20the%20result%20and%20schedules%20them%20for%20execution.
