# Tasks - 
### A spark task is an operation over a single partition of data. A partition is a logical chunk of data. Data is split into Partitions so that each Executor core can operate on a single partition and run in parallel to each core (parallel data processing power comes from here).

# Stages - 
### A stage consists of multiple tasks which execute in parallel. Usually, a wide transformation (an operation that is applied on multiple partitions,

# Jobs - 
### A job consists of multiple stages. Usually, a job is created once an action is executed. Any transformation creates RDDs using multiple other RDDs. But when working with actual datasets, action must be performed on such RDDs. Actions are RDD operations that give non-RDD values such as take(), collect(), count(), read(), and write().

![Tasks-jobs-stage](https://miro.medium.com/v2/resize:fit:1400/format:webp/1*JptbIrrLqKriVfPHXKX36Q.png)
