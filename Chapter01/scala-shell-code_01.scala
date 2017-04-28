/*
	This code is intended to be run in the Scala shell. 
	Launch the Scala Spark shell by running ./bin/spark-shell from the Spark directory.
	You can enter each line in the shell and see the result immediately.
	The expected output in the Spark console is presented as commented lines following the
	relevant code

	The Scala shell creates a SparkContex variable available to us as 'sc'
*/
/* Create a Scala collection of Strings */
val collection = List("a", "b", "c", "d", "e")
// collection: List[String] = List(a, b, c, d, e)

/* Create a Spark RDD[String] from the Scala collection */
val rddFromCollection = sc.parallelize(collection)
// rddFromCollection: org.apache.spark.rdd.RDD[String] = ParallelCollectionRDD[0] at parallelize at <console>:14

/* create a Spark RDD[String] from a text file on the local filesystem */
val rddFromTextFile = sc.textFile("LICENSE")
// 14/01/29 22:24:15 INFO MemoryStore: ensureFreeSpace(32960) called with curMem=0, maxMem=311387750
// 14/01/29 22:24:15 INFO MemoryStore: Block broadcast_0 stored as values to memory (estimated size 32.2 KB, free 296.9 MB)
// rddFromTextFile: org.apache.spark.rdd.RDD[String] = MappedRDD[2] at textFile at <console>:12

/* Transform an RDD[String] => RDD[Int] */
val intsFromStringsRDD = rddFromTextFile.map(line => line.size)
// intsFromStringsRDD: org.apache.spark.rdd.RDD[Int] = MappedRDD[5] at map at <console>:14

/* The count method returns the number of elements in an RDD */
intsFromStringsRDD.count
// 14/01/29 23:28:28 INFO SparkContext: Starting job: count at <console>:17
// ...
// 14/01/29 23:28:28 INFO DAGScheduler: Completed ResultTask(3, 0)
// 14/01/29 23:28:28 INFO DAGScheduler: Stage 3 (count at <console>:17) finished in 0.014 s
// 14/01/29 23:28:28 INFO SparkContext: Job finished: count at <console>:17, took 0.019227 s
// res4: Long = 398

/* Find the average length of records in the RDD */
val sumOfRecords = intsFromStringsRDD.sum
// 14/01/29 23:35:22 INFO SparkContext: Starting job: sum at <console>:16
// ...
// 14/01/29 23:35:22 INFO SparkContext: Job finished: sum at <console>:16, took 0.015528 s
// sumOfRecords: Double = 20720.0
val numRecords = intsFromStringsRDD.count
// 14/01/29 23:35:10 INFO SparkContext: Starting job: count at <console>:16
// ...
// 14/01/29 23:35:10 INFO SparkContext: Job finished: count at <console>:16, took 0.01694 s
// numRecords: Long = 398
val aveLengthOfRecord = sumOfRecords / numRecords
// aveLengthOfRecord: Double = 52.06030150753769

/* Find the same average as above, but chaining together our method calls */
val aveLengthOfRecordChained = rddFromTextFile.map(line => line.size).sum / rddFromTextFile.count
// 14/01/29 23:42:37 INFO SparkContext: Starting job: sum at <console>:14
// ...
// 14/01/29 23:42:37 INFO SparkContext: Job finished: sum at <console>:14, took 0.020479 s
// 14/01/29 23:42:37 INFO SparkContext: Starting job: count at <console>:14
// ...
// 14/01/29 23:42:37 INFO SparkContext: Job finished: count at <console>:14, took 0.01141 s
// aveLengthOfRecordChained: Double = 52.06030150753769

/* Chaining transformations does not trigger computation immediately, but returned a new RDD */
val transformedRDD = rddFromTextFile.map(line => line.size).filter(size => size > 10).map(size => size * 2)
// transformedRDD: org.apache.spark.rdd.RDD[Int] = MappedRDD[8] at map at <console>:14
/* Computation is only triggered once we call an action on the RDD */
val computation = transformedRDD.sum
// ...
// 14/11/27 21:48:21 INFO SparkContext: Job finished: sum at <console>:16, took 0.193513 s
// computation: Double = 60468.0

/* An example of caching a dataset in Spark */
rddFromTextFile.cache
// res7: org.apache.spark.rdd.RDD[String] = MappedRDD[2] at textFile at <console>:12
val aveLengthOfRecordChained = rddFromTextFile.map(line => line.size).sum / rddFromTextFile.count
// ...
// 14/01/30 06:59:27 INFO MemoryStore: ensureFreeSpace(63454) called with curMem=32960, maxMem=311387750
// 14/01/30 06:59:27 INFO MemoryStore: Block rdd_2_0 stored as values to memory (estimated size 62.0 KB, free 296.9 MB)
// 14/01/30 06:59:27 INFO BlockManagerMasterActor$BlockManagerInfo: Added rdd_2_0 in memory on 10.0.0.3:55089 (size: 62.0 KB, free: 296.9 MB)
// ...
val aveLengthOfRecordChainedFromCached = rddFromTextFile.map(line => line.size).sum / rddFromTextFile.count
// ...
// 14/01/30 06:59:34 INFO BlockManager: Found block rdd_2_0 locally
// ...

/* An example of using broadcast variables */
val broadcastAList = sc.broadcast(List("a", "b", "c", "d", "e"))
// 14/01/30 07:13:32 INFO MemoryStore: ensureFreeSpace(488) called with curMem=96414, maxMem=311387750
// 14/01/30 07:13:32 INFO MemoryStore: Block broadcast_1 stored as values to memory (estimated size 488.0 B, free 296.9 MB)
// broadCastAList: org.apache.spark.broadcast.Broadcast[List[String]] = Broadcast(1)
sc.parallelize(List("1", "2", "3")).map(x => broadcastAList.value ++ x).collect
// 14/01/31 10:15:39 INFO SparkContext: Job finished: collect at <console>:15, took 0.025806 s
// res6: Array[List[Any]] = Array(List(a, b, c, d, e, 1), List(a, b, c, d, e, 2), List(a, b, c, d, e, 3))