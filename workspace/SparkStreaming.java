package com.poc.rcm.SparkStreaming;
import java.util.Arrays;

import org.apache.spark.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;

import scala.Tuple2;
public class SparkStreaming {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		// spark-submit --class com.spark.streaming.SparkStreaming /home/cloudera/dev/runnableJars/Streaming.jar
		// spark-submit --class com.poc.rcm.SparkStreaming.SparkStreaming /home/cloudera/dev/SparkStreaming.jar

		// nc -lk 9999
		

		// Create a local StreamingContext with two working thread and batch interval of 1 second
		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount");
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));
		
		JavaReceiverInputDStream<String> lines = jssc.socketTextStream("localhost", 9999);
		
	/*	JavaDStream<String> words = lines.filter(new Function<String, Boolean>() {
	        @Override
	        public Boolean call(String v1) throws Exception {
	            return v1.contains("aparnesh");
	        }});*/
		
		/*JavaDStream<String> words = lines.map(new Function<String, String>() {
	        @Override
	        public String call(String v1) throws Exception {
	            return v1.concat(" hello ");
	        }});*/
		
//		JavaDStream<String> words = lines;	
		JavaDStream<String> words = lines.map(record -> record.concat("hello"));	
		
		// Count each word in each batch
		JavaPairDStream<String, Integer> pairs = words.mapToPair(record -> new Tuple2(record, 1));
		
		
		System.out.println("#######################");
		System.out.println("printing the created key value pairs , map phase output : "+pairs);
		System.out.println("#######################");
//		pairs.foreach(record -> System.out.println(record));
		pairs.print();
		
		JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey((a, b) -> a + b);
		
		System.out.println("#######################");
		System.out.println("printing the counts per key , reduce phase output : "+wordCounts);
		System.out.println("#######################");
//		wordCounts.foreach(record -> System.out.println(record));
		wordCounts.print();

		
		
		jssc.start();              // Start the computation
		jssc.awaitTermination();   // Wait for the computation to terminate
		
		
	}

}

/* output 
 * 
 * 
 * ----------------------------------------------------
 * ----------------------------------------------------
 * ----------------------------------------------------
 * ----------------------------------------------------
 * ----------------------------------------------------
 * 
 * output of the first console first of all 

nc console 

[cloudera@quickstart eclipse]$ nc -lk 9999
rani
aparnesh
neha
^C

spark streaming console 

-------------------------------------------
Time: 1595848035000 ms
-------------------------------------------
(aparnesh welcome to home,1)
(rani welcome to home,1)

-------------------------------------------
Time: 1595848040000 ms
-------------------------------------------
(neha welcome to home,1)


----------------------------------------------------
----------------------------------------------------
----------------------------------------------------
----------------------------------------------------
---------------------------------------------------------------
second output for second run 

 parents
20/09/06 19:58:50 INFO storage.MemoryStore: Block broadcast_68 stored as values in memory (estimated size 2.7 KB, free 529.9 MB)
20/09/06 19:58:50 INFO storage.MemoryStore: Block broadcast_68_piece0 stored as bytes in memory (estimated size 1712.0 B, free 529.9 MB)
20/09/06 19:58:50 INFO storage.BlockManagerInfo: Added broadcast_68_piece0 in memory on localhost:35115 (size: 1712.0 B, free: 530.0 MB)
20/09/06 19:58:50 INFO spark.SparkContext: Created broadcast 68 from broadcast at DAGScheduler.scala:1004
20/09/06 19:58:50 INFO scheduler.DAGScheduler: Submitting 1 missing tasks from ResultStage 118 (MapPartitionsRDD[83] at mapToPair at SparkStreaming.java:42) (first 15 tasks are for partitions Vector(0))
20/09/06 19:58:50 INFO scheduler.TaskSchedulerImpl: Adding task set 118.0 with 1 tasks
20/09/06 19:58:50 INFO scheduler.TaskSetManager: Starting task 0.0 in stage 118.0 (TID 89, localhost, executor driver, partition 0, NODE_LOCAL, 2080 bytes)
20/09/06 19:58:50 INFO executor.Executor: Running task 0.0 in stage 118.0 (TID 89)
20/09/06 19:58:50 INFO storage.BlockManager: Found block input-0-1599447525000 locally
20/09/06 19:58:50 INFO executor.Executor: Finished task 0.0 in stage 118.0 (TID 89). 1091 bytes result sent to driver
20/09/06 19:58:50 INFO scheduler.TaskSetManager: Finished task 0.0 in stage 118.0 (TID 89) in 4 ms on localhost (executor driver) (1/1)
20/09/06 19:58:50 INFO scheduler.TaskSchedulerImpl: Removed TaskSet 118.0, whose tasks have all completed, from pool 
20/09/06 19:58:50 INFO scheduler.DAGScheduler: ResultStage 118 (print at SparkStreaming.java:49) finished in 0.003 s
20/09/06 19:58:50 INFO scheduler.DAGScheduler: Job 64 finished: print at SparkStreaming.java:49, took 0.015269 s
20/09/06 19:58:50 INFO spark.SparkContext: Starting job: print at SparkStreaming.java:49
20/09/06 19:58:50 INFO scheduler.DAGScheduler: Got job 65 (print at SparkStreaming.java:49) with 1 output partitions
20/09/06 19:58:50 INFO scheduler.DAGScheduler: Final stage: ResultStage 119 (print at SparkStreaming.java:49)
20/09/06 19:58:50 INFO scheduler.DAGScheduler: Parents of final stage: List()
20/09/06 19:58:50 INFO scheduler.DAGScheduler: Missing parents: List()
20/09/06 19:58:50 INFO scheduler.DAGScheduler: Submitting ResultStage 119 (MapPartitionsRDD[83] at mapToPair at SparkStreaming.java:42), which has no missing parents
20/09/06 19:58:50 INFO storage.MemoryStore: Block broadcast_69 stored as values in memory (estimated size 2.7 KB, free 529.9 MB)
20/09/06 19:58:50 INFO storage.MemoryStore: Block broadcast_69_piece0 stored as bytes in memory (estimated size 1712.0 B, free 529.9 MB)
20/09/06 19:58:50 INFO storage.BlockManagerInfo: Added broadcast_69_piece0 in memory on localhost:35115 (size: 1712.0 B, free: 530.0 MB)
20/09/06 19:58:50 INFO spark.SparkContext: Created broadcast 69 from broadcast at DAGScheduler.scala:1004
20/09/06 19:58:50 INFO scheduler.DAGScheduler: Submitting 1 missing tasks from ResultStage 119 (MapPartitionsRDD[83] at mapToPair at SparkStreaming.java:42) (first 15 tasks are for partitions Vector(1))
20/09/06 19:58:50 INFO scheduler.TaskSchedulerImpl: Adding task set 119.0 with 1 tasks
20/09/06 19:58:50 INFO scheduler.TaskSetManager: Starting task 0.0 in stage 119.0 (TID 90, localhost, executor driver, partition 1, NODE_LOCAL, 2080 bytes)
20/09/06 19:58:50 INFO executor.Executor: Running task 0.0 in stage 119.0 (TID 90)
20/09/06 19:58:50 INFO storage.BlockManager: Found block input-0-1599447526000 locally
20/09/06 19:58:50 INFO executor.Executor: Finished task 0.0 in stage 119.0 (TID 90). 1061 bytes result sent to driver
20/09/06 19:58:50 INFO scheduler.TaskSetManager: Finished task 0.0 in stage 119.0 (TID 90) in 3 ms on localhost (executor driver) (1/1)
20/09/06 19:58:50 INFO scheduler.TaskSchedulerImpl: Removed TaskSet 119.0, whose tasks have all completed, from pool 
20/09/06 19:58:50 INFO scheduler.DAGScheduler: ResultStage 119 (print at SparkStreaming.java:49) finished in 0.004 s
20/09/06 19:58:50 INFO scheduler.DAGScheduler: Job 65 finished: print at SparkStreaming.java:49, took 0.010001 s
-------------------------------------------
Time: 1599447530000 ms
-------------------------------------------
(b,1)
(b,1)
(b,1)
(b,1)
---------------------------------------------------------------
/09/06 20:00:15 INFO storage.BlockManagerInfo: Removed broadcast_77_piece0 on localhost:35115 in memory (size: 2.0 KB, free: 530.0 MB)
20/09/06 20:00:15 INFO spark.ContextCleaner: Cleaned accumulator 78
20/09/06 20:00:15 INFO spark.ContextCleaner: Cleaned shuffle 30
20/09/06 20:00:15 INFO executor.Executor: Finished task 0.0 in stage 194.0 (TID 146). 1328 bytes result sent to driver
20/09/06 20:00:15 INFO scheduler.TaskSetManager: Finished task 0.0 in stage 194.0 (TID 146) in 9 ms on localhost (executor driver) (1/1)
20/09/06 20:00:15 INFO scheduler.TaskSchedulerImpl: Removed TaskSet 194.0, whose tasks have all completed, from pool 
20/09/06 20:00:15 INFO scheduler.DAGScheduler: ResultStage 194 (print at SparkStreaming.java:57) finished in 0.009 s
20/09/06 20:00:15 INFO scheduler.DAGScheduler: Job 104 finished: print at SparkStreaming.java:57, took 0.017741 s
-------------------------------------------
Time: 1599447615000 ms
-------------------------------------------
(b,4)
(a,4)
(ab,1)


----------------------------------------------------
----------------------------------------------------
----------------------------------------------------
----------------------------------------------------
----------------------------------------------------------------------
input being given from nc-lk 9999 console 

a
a
a
a
ab
b
b
b
b
a
a
a
a
ab
b
b
b
b
b


----------------------------------------------------
----------------------------------------------------
----------------------------------------------------
----------------------------------------------------
----------------------------------------------------
third round of output

(ahello,1)
(ahello,1)
(ahello,1)
(ahello,1)
(ahello,1)
(bhello,1)
(hello,1)
(bhello,1)
(bhello,1)
(bhello,1)
...

20/09/06 20:02:45 INFO scheduler.JobScheduler: Finished job streaming job 1599447765000 ms.0 from job set of time 1599447765000 ms
20/09/06 20:02:45 INFO scheduler.JobScheduler: Starting job streaming job 1599447765000 ms.1 from job set of time 1599447765000 ms
20/09/06 20:02:45 INFO spark.SparkContext: Starting job: print at SparkStreaming.java:57
20/09/06 20:02:45 INFO scheduler.DAGScheduler: Registering RDD 15 (mapToPair at SparkStreaming.java:42)
20/09/06 20:02:45 INFO scheduler.DAGScheduler: Got job 12 (print at SparkStreaming.java:57) with 1 output partitions
20/09/06 20:02:45 INFO scheduler.DAGScheduler: Final stage: ResultStage 19 (print at SparkStreaming.java:57)
20/09/06 20:02:45 INFO scheduler.DAGScheduler: Parents of final stage: List(ShuffleMapStage 18)
20/09/06 20:02:45 INFO scheduler.DAGScheduler: Missing parents: List(ShuffleMapStage 18)
20/09/06 20:02:45 INFO scheduler.DAGScheduler: Submitting ShuffleMapStage 18 (MapPartitionsRDD[15] at mapToPair at SparkStreaming.java:42), which has no missing parents
20/09/06 20:02:45 INFO storage.MemoryStore: Block broadcast_13 stored as values in memory (estimated size 4.1 KB, free 529.9 MB)
20/09/06 20:02:45 INFO storage.MemoryStore: Block broadcast_13_piece0 stored as bytes in memory (estimated size 2.2 KB, free 529.9 MB)
20/09/06 20:02:45 INFO storage.BlockManagerInfo: Added broadcast_13_piece0 in memory on localhost:59452 (size: 2.2 KB, free: 530.0 MB)
20/09/06 20:02:45 INFO spark.SparkContext: Created broadcast 13 from broadcast at DAGScheduler.scala:1004
20/09/06 20:02:45 INFO scheduler.DAGScheduler: Submitting 13 missing tasks from ShuffleMapStage 18 (MapPartitionsRDD[15] at mapToPair at SparkStreaming.java:42) (first 15 tasks are for partitions Vector(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12))
20/09/06 20:02:45 INFO scheduler.TaskSchedulerImpl: Adding task set 18.0 with 13 tasks
20/09/06 20:02:45 INFO scheduler.TaskSetManager: Starting task 0.0 in stage 18.0 (TID 26, localhost, executor driver, partition 0, NODE_LOCAL, 2069 bytes)
20/09/06 20:02:45 INFO executor.Executor: Running task 0.0 in stage 18.0 (TID 26)
20/09/06 20:02:45 INFO storage.BlockManager: Found block input-0-1599447759800 locally
20/09/06 20:02:45 INFO executor.Executor: Finished task 0.0 in stage 18.0 (TID 26). 1159 bytes result sent to driver
20/09/06 20:02:45 INFO scheduler.TaskSetManager: Starting task 1.0 in stage 18.0 (TID 27, localhost, executor driver, partition 1, NODE_LOCAL, 2069 bytes)
20/09/06 20:02:45 INFO executor.Executor: Running task 1.0 in stage 18.0 (TID 27)
20/09/06 20:02:45 INFO scheduler.TaskSetManager: Finished task 0.0 in stage 18.0 (TID 26) in 8 ms on localhost (executor driver) (1/13)
20/09/06 20:02:45 INFO storage.BlockManager: Found block input-0-1599447760000 locally
20/09/06 20:02:45 INFO executor.Executor: Finished task 1.0 in stage 18.0 (TID 27). 1159 bytes result sent to driver
20/09/06 20:02:45 INFO scheduler.TaskSetManager: Starting task 2.0 in stage 18.0 (TID 28, localhost, executor driver, partition 2, NODE_LOCAL, 2069 bytes)
20/09/06 20:02:45 INFO executor.Executor: Running task 2.0 in stage 18.0 (TID 28)
20/09/06 20:02:45 INFO scheduler.TaskSetManager: Finished task 1.0 in stage 18.0 (TID 27) in 8 ms on localhost (executor driver) (2/13)
20/09/06 20:02:45 INFO storage.BlockManager: Found block input-0-1599447760200 locally
20/09/06 20:02:45 INFO executor.Executor: Finished task 2.0 in stage 18.0 (TID 28). 1159 bytes result sent to driver
20/09/06 20:02:45 INFO scheduler.TaskSetManager: Starting task 3.0 in stage 18.0 (TID 29, localhost, executor driver, partition 3, NODE_LOCAL, 2069 bytes)
20/09/06 20:02:45 INFO executor.Executor: Running task 3.0 in stage 18.0 (TID 29)
20/09/06 20:02:45 INFO scheduler.TaskSetManager: Finished task 2.0 in stage 18.0 (TID 28) in 10 ms on localhost (executor driver) (3/13)
20/09/06 20:02:45 INFO storage.BlockManager: Found block input-0-1599447760400 locally
20/09/06 20:02:45 INFO executor.Executor: Finished task 3.0 in stage 18.0 (TID 29). 1159 bytes result sent to driver
20/09/06 20:02:45 INFO scheduler.TaskSetManager: Starting task 4.0 in stage 18.0 (TID 30, localhost, executor driver, partition 4, NODE_LOCAL, 2069 bytes)
20/09/06 20:02:45 INFO scheduler.TaskSetManager: Finished task 3.0 in stage 18.0 (TID 29) in 7 ms on localhost (executor driver) (4/13)
20/09/06 20:02:45 INFO executor.Executor: Running task 4.0 in stage 18.0 (TID 30)
20/09/06 20:02:45 INFO storage.BlockManager: Found block input-0-1599447760800 locally
20/09/06 20:02:45 INFO executor.Executor: Finished task 4.0 in stage 18.0 (TID 30). 1159 bytes result sent to driver
20/09/06 20:02:45 INFO scheduler.TaskSetManager: Starting task 5.0 in stage 18.0 (TID 31, localhost, executor driver, partition 5, NODE_LOCAL, 2069 bytes)
20/09/06 20:02:45 INFO executor.Executor: Running task 5.0 in stage 18.0 (TID 31)
20/09/06 20:02:45 INFO scheduler.TaskSetManager: Finished task 4.0 in stage 18.0 (TID 30) in 7 ms on localhost (executor driver) (5/13)
20/09/06 20:02:45 INFO storage.BlockManager: Found block input-0-1599447761000 locally
20/09/06 20:02:45 INFO executor.Executor: Finished task 5.0 in stage 18.0 (TID 31). 1159 bytes result sent to driver
20/09/06 20:02:45 INFO scheduler.TaskSetManager: Starting task 6.0 in stage 18.0 (TID 32, localhost, executor driver, partition 6, NODE_LOCAL, 2069 bytes)
20/09/06 20:02:45 INFO scheduler.TaskSetManager: Finished task 5.0 in stage 18.0 (TID 31) in 7 ms on localhost (executor driver) (6/13)
20/09/06 20:02:45 INFO executor.Executor: Running task 6.0 in stage 18.0 (TID 32)
20/09/06 20:02:45 INFO storage.BlockManager: Found block input-0-1599447761200 locally
20/09/06 20:02:45 INFO executor.Executor: Finished task 6.0 in stage 18.0 (TID 32). 1159 bytes result sent to driver
20/09/06 20:02:45 INFO scheduler.TaskSetManager: Starting task 7.0 in stage 18.0 (TID 33, localhost, executor driver, partition 7, NODE_LOCAL, 2069 bytes)
20/09/06 20:02:45 INFO executor.Executor: Running task 7.0 in stage 18.0 (TID 33)
20/09/06 20:02:45 INFO scheduler.TaskSetManager: Finished task 6.0 in stage 18.0 (TID 32) in 8 ms on localhost (executor driver) (7/13)
20/09/06 20:02:45 INFO storage.BlockManager: Found block input-0-1599447761400 locally
20/09/06 20:02:45 INFO executor.Executor: Finished task 7.0 in stage 18.0 (TID 33). 1159 bytes result sent to driver
20/09/06 20:02:45 INFO scheduler.TaskSetManager: Starting task 8.0 in stage 18.0 (TID 34, localhost, executor driver, partition 8, NODE_LOCAL, 2069 bytes)
20/09/06 20:02:45 INFO executor.Executor: Running task 8.0 in stage 18.0 (TID 34)
20/09/06 20:02:45 INFO scheduler.TaskSetManager: Finished task 7.0 in stage 18.0 (TID 33) in 8 ms on localhost (executor driver) (8/13)
20/09/06 20:02:45 INFO storage.BlockManager: Found block input-0-1599447761600 locally
20/09/06 20:02:45 INFO executor.Executor: Finished task 8.0 in stage 18.0 (TID 34). 1159 bytes result sent to driver
20/09/06 20:02:45 INFO scheduler.TaskSetManager: Starting task 9.0 in stage 18.0 (TID 35, localhost, executor driver, partition 9, NODE_LOCAL, 2069 bytes)
20/09/06 20:02:45 INFO executor.Executor: Running task 9.0 in stage 18.0 (TID 35)
20/09/06 20:02:45 INFO scheduler.TaskSetManager: Finished task 8.0 in stage 18.0 (TID 34) in 7 ms on localhost (executor driver) (9/13)
20/09/06 20:02:45 INFO storage.BlockManager: Found block input-0-1599447761800 locally
20/09/06 20:02:45 INFO executor.Executor: Finished task 9.0 in stage 18.0 (TID 35). 1159 bytes result sent to driver
20/09/06 20:02:45 INFO scheduler.TaskSetManager: Starting task 10.0 in stage 18.0 (TID 36, localhost, executor driver, partition 10, NODE_LOCAL, 2069 bytes)
20/09/06 20:02:45 INFO executor.Executor: Running task 10.0 in stage 18.0 (TID 36)
20/09/06 20:02:45 INFO scheduler.TaskSetManager: Finished task 9.0 in stage 18.0 (TID 35) in 8 ms on localhost (executor driver) (10/13)
20/09/06 20:02:45 INFO storage.BlockManager: Found block input-0-1599447762000 locally
20/09/06 20:02:45 INFO executor.Executor: Finished task 10.0 in stage 18.0 (TID 36). 1159 bytes result sent to driver
20/09/06 20:02:45 INFO scheduler.TaskSetManager: Starting task 11.0 in stage 18.0 (TID 37, localhost, executor driver, partition 11, NODE_LOCAL, 2069 bytes)
20/09/06 20:02:45 INFO executor.Executor: Running task 11.0 in stage 18.0 (TID 37)
20/09/06 20:02:45 INFO scheduler.TaskSetManager: Finished task 10.0 in stage 18.0 (TID 36) in 9 ms on localhost (executor driver) (11/13)
20/09/06 20:02:45 INFO storage.BlockManager: Found block input-0-1599447762200 locally
20/09/06 20:02:45 INFO executor.Executor: Finished task 11.0 in stage 18.0 (TID 37). 1159 bytes result sent to driver
20/09/06 20:02:45 INFO scheduler.TaskSetManager: Starting task 12.0 in stage 18.0 (TID 38, localhost, executor driver, partition 12, NODE_LOCAL, 2069 bytes)
20/09/06 20:02:45 INFO executor.Executor: Running task 12.0 in stage 18.0 (TID 38)
20/09/06 20:02:45 INFO scheduler.TaskSetManager: Finished task 11.0 in stage 18.0 (TID 37) in 10 ms on localhost (executor driver) (12/13)
20/09/06 20:02:45 INFO storage.BlockManager: Found block input-0-1599447762400 locally
20/09/06 20:02:45 INFO executor.Executor: Finished task 12.0 in stage 18.0 (TID 38). 1159 bytes result sent to driver
20/09/06 20:02:45 INFO scheduler.TaskSetManager: Finished task 12.0 in stage 18.0 (TID 38) in 7 ms on localhost (executor driver) (13/13)
20/09/06 20:02:45 INFO scheduler.TaskSchedulerImpl: Removed TaskSet 18.0, whose tasks have all completed, from pool 
20/09/06 20:02:45 INFO scheduler.DAGScheduler: ShuffleMapStage 18 (mapToPair at SparkStreaming.java:42) finished in 0.098 s
20/09/06 20:02:45 INFO scheduler.DAGScheduler: looking for newly runnable stages
20/09/06 20:02:45 INFO scheduler.DAGScheduler: running: Set(ResultStage 0)
20/09/06 20:02:45 INFO scheduler.DAGScheduler: waiting: Set(ResultStage 19)
20/09/06 20:02:45 INFO scheduler.DAGScheduler: failed: Set()
20/09/06 20:02:45 INFO scheduler.DAGScheduler: Submitting ResultStage 19 (ShuffledRDD[16] at reduceByKey at SparkStreaming.java:51), which has no missing parents
20/09/06 20:02:45 INFO storage.MemoryStore: Block broadcast_14 stored as values in memory (estimated size 3.5 KB, free 529.9 MB)
20/09/06 20:02:45 INFO storage.MemoryStore: Block broadcast_14_piece0 stored as bytes in memory (estimated size 2.0 KB, free 529.9 MB)
20/09/06 20:02:45 INFO storage.BlockManagerInfo: Added broadcast_14_piece0 in memory on localhost:59452 (size: 2.0 KB, free: 530.0 MB)
20/09/06 20:02:45 INFO spark.SparkContext: Created broadcast 14 from broadcast at DAGScheduler.scala:1004
20/09/06 20:02:45 INFO scheduler.DAGScheduler: Submitting 1 missing tasks from ResultStage 19 (ShuffledRDD[16] at reduceByKey at SparkStreaming.java:51) (first 15 tasks are for partitions Vector(0))
20/09/06 20:02:45 INFO scheduler.TaskSchedulerImpl: Adding task set 19.0 with 1 tasks
20/09/06 20:02:45 INFO scheduler.TaskSetManager: Starting task 0.0 in stage 19.0 (TID 39, localhost, executor driver, partition 0, NODE_LOCAL, 1957 bytes)
20/09/06 20:02:45 INFO executor.Executor: Running task 0.0 in stage 19.0 (TID 39)
20/09/06 20:02:45 INFO storage.ShuffleBlockFetcherIterator: Getting 9 non-empty blocks out of 13 blocks
20/09/06 20:02:45 INFO storage.ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
20/09/06 20:02:45 INFO executor.Executor: Finished task 0.0 in stage 19.0 (TID 39). 1336 bytes result sent to driver
20/09/06 20:02:45 INFO scheduler.TaskSetManager: Finished task 0.0 in stage 19.0 (TID 39) in 15 ms on localhost (executor driver) (1/1)
20/09/06 20:02:45 INFO scheduler.TaskSchedulerImpl: Removed TaskSet 19.0, whose tasks have all completed, from pool 
20/09/06 20:02:45 INFO scheduler.DAGScheduler: ResultStage 19 (print at SparkStreaming.java:57) finished in 0.015 s
20/09/06 20:02:45 INFO scheduler.DAGScheduler: Job 12 finished: print at SparkStreaming.java:57, took 0.135714 s
20/09/06 20:02:45 INFO spark.SparkContext: Starting job: print at SparkStreaming.java:57
20/09/06 20:02:45 INFO spark.MapOutputTrackerMaster: Size of output statuses for shuffle 3 is 173 bytes
20/09/06 20:02:45 INFO scheduler.DAGScheduler: Got job 13 (print at SparkStreaming.java:57) with 1 output partitions
20/09/06 20:02:45 INFO scheduler.DAGScheduler: Final stage: ResultStage 21 (print at SparkStreaming.java:57)
20/09/06 20:02:45 INFO scheduler.DAGScheduler: Parents of final stage: List(ShuffleMapStage 20)
20/09/06 20:02:45 INFO scheduler.DAGScheduler: Missing parents: List()
20/09/06 20:02:45 INFO scheduler.DAGScheduler: Submitting ResultStage 21 (ShuffledRDD[16] at reduceByKey at SparkStreaming.java:51), which has no missing parents
20/09/06 20:02:45 INFO storage.MemoryStore: Block broadcast_15 stored as values in memory (estimated size 3.5 KB, free 529.9 MB)
20/09/06 20:02:45 INFO storage.MemoryStore: Block broadcast_15_piece0 stored as bytes in memory (estimated size 2.0 KB, free 529.9 MB)
20/09/06 20:02:45 INFO storage.BlockManagerInfo: Added broadcast_15_piece0 in memory on localhost:59452 (size: 2.0 KB, free: 530.0 MB)
20/09/06 20:02:45 INFO spark.SparkContext: Created broadcast 15 from broadcast at DAGScheduler.scala:1004
20/09/06 20:02:45 INFO scheduler.DAGScheduler: Submitting 1 missing tasks from ResultStage 21 (ShuffledRDD[16] at reduceByKey at SparkStreaming.java:51) (first 15 tasks are for partitions Vector(1))
20/09/06 20:02:45 INFO scheduler.TaskSchedulerImpl: Adding task set 21.0 with 1 tasks
20/09/06 20:02:45 INFO scheduler.TaskSetManager: Starting task 0.0 in stage 21.0 (TID 40, localhost, executor driver, partition 1, NODE_LOCAL, 1957 bytes)
20/09/06 20:02:45 INFO executor.Executor: Running task 0.0 in stage 21.0 (TID 40)
20/09/06 20:02:45 INFO storage.ShuffleBlockFetcherIterator: Getting 4 non-empty blocks out of 13 blocks
20/09/06 20:02:45 INFO storage.ShuffleBlockFetcherIterator: Started 0 remote fetches in 0 ms
20/09/06 20:02:45 INFO executor.Executor: Finished task 0.0 in stage 21.0 (TID 40). 1312 bytes result sent to driver
20/09/06 20:02:45 INFO scheduler.TaskSetManager: Finished task 0.0 in stage 21.0 (TID 40) in 8 ms on localhost (executor driver) (1/1)
20/09/06 20:02:45 INFO scheduler.TaskSchedulerImpl: Removed TaskSet 21.0, whose tasks have all completed, from pool 
20/09/06 20:02:45 INFO scheduler.DAGScheduler: ResultStage 21 (print at SparkStreaming.java:57) finished in 0.009 s
20/09/06 20:02:45 INFO scheduler.DAGScheduler: Job 13 finished: print at SparkStreaming.java:57, took 0.021730 s
-------------------------------------------
Time: 1599447765000 ms
-------------------------------------------
(bhello,7)
(hello,3)
(ahello,5)

----------------------------------------------------------------

intput from nc -lk 9999 for the third round of input , output just above 
ab
b
b
b
b
b
a
a
a
a
a
a
a
b

b
b
b

b
b
b






*/


