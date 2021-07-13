package com.poc.rcm.sparkcore;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.SparkConf;

import scala.Tuple2;
public class CheckCodeTavant {
// spark-submit --class com.poc.rcm.sparkcore.SparkCoreTemperatureCountForTotalTempAcrossYearsForMay /home/cloudera/dev/practice/Temperature.jar
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		
		List<String> data = Arrays.asList("aparnesh","rani","neha","aparnesh","neha","rani","rani");
		JavaRDD<String> distData = sc.parallelize(data);
		
		JavaRDD<Integer> lineLengths = distData.map(s -> s.length());
		int totalLength = lineLengths.reduce((a, b) -> a + b);
		
		System.out.println(" the total length is : "+totalLength);
		
				JavaPairRDD<String, Integer> pairs = distData.mapToPair(record -> new Tuple2(record, 1));
				System.out.println("#######################");
				System.out.println("printing the created key value pairs , map phase output : "+pairs);
				System.out.println("#######################");
				pairs.foreach(record -> System.out.println(record));
				
				JavaPairRDD<String, Integer> counts = pairs.reduceByKey((a, b) -> a + b);
				
				System.out.println("#######################");
				System.out.println("printing the counts per key , reduce phase output : "+counts);
				System.out.println("#######################");
				counts.foreach(record -> System.out.println(record));

	}
	
	
	
	/**
	 * 
	 * 
	 * spark core word count output for the input mentioned in the program itself 
	 * 
	 * 
	 * (aparnesh,1)
(neha,1)
(rani,1)
(rani,1)
(aparnesh,1)
(rani,1)
(neha,1)
20/09/06 19:37:14 INFO executor.Executor: Finished task 0.0 in stage 1.0 (TID 2). 915 bytes result sent to driver
20/09/06 19:37:14 INFO executor.Executor: Finished task 1.0 in stage 1.0 (TID 3). 915 bytes result sent to driver
20/09/06 19:37:14 INFO scheduler.TaskSetManager: Finished task 0.0 in stage 1.0 (TID 2) in 22 ms on localhost (executor driver) (1/2)
20/09/06 19:37:14 INFO scheduler.TaskSetManager: Finished task 1.0 in stage 1.0 (TID 3) in 22 ms on localhost (executor driver) (2/2)
20/09/06 19:37:14 INFO scheduler.TaskSchedulerImpl: Removed TaskSet 1.0, whose tasks have all completed, from pool 
20/09/06 19:37:14 INFO scheduler.DAGScheduler: ResultStage 1 (foreach at SparkCoreWordCount.java:41) finished in 0.023 s
20/09/06 19:37:14 INFO scheduler.DAGScheduler: Job 1 finished: foreach at SparkCoreWordCount.java:41, took 0.039840 s
#######################
printing the counts per key , reduce phase output : org.apache.spark.api.java.JavaPairRDD@6fa0450e
#######################
20/09/06 19:37:14 INFO spark.SparkContext: Starting job: foreach at SparkCoreWordCount.java:48
20/09/06 19:37:14 INFO scheduler.DAGScheduler: Registering RDD 2 (mapToPair at SparkCoreWordCount.java:37)
20/09/06 19:37:14 INFO scheduler.DAGScheduler: Got job 2 (foreach at SparkCoreWordCount.java:48) with 2 output partitions
20/09/06 19:37:14 INFO scheduler.DAGScheduler: Final stage: ResultStage 3 (foreach at SparkCoreWordCount.java:48)
20/09/06 19:37:14 INFO scheduler.DAGScheduler: Parents of final stage: List(ShuffleMapStage 2)
20/09/06 19:37:14 INFO scheduler.DAGScheduler: Missing parents: List(ShuffleMapStage 2)
20/09/06 19:37:14 INFO scheduler.DAGScheduler: Submitting ShuffleMapStage 2 (MapPartitionsRDD[2] at mapToPair at SparkCoreWordCount.java:37), which has no missing parents
20/09/06 19:37:14 INFO storage.MemoryStore: Block broadcast_2 stored as values in memory (estimated size 3.9 KB, free 530.0 MB)
20/09/06 19:37:14 INFO storage.MemoryStore: Block broadcast_2_piece0 stored as bytes in memory (estimated size 2.2 KB, free 530.0 MB)
20/09/06 19:37:14 INFO storage.BlockManagerInfo: Added broadcast_2_piece0 in memory on localhost:35105 (size: 2.2 KB, free: 530.0 MB)
20/09/06 19:37:14 INFO spark.SparkContext: Created broadcast 2 from broadcast at DAGScheduler.scala:1004
20/09/06 19:37:14 INFO scheduler.DAGScheduler: Submitting 2 missing tasks from ShuffleMapStage 2 (MapPartitionsRDD[2] at mapToPair at SparkCoreWordCount.java:37) (first 15 tasks are for partitions Vector(0, 1))
20/09/06 19:37:14 INFO scheduler.TaskSchedulerImpl: Adding task set 2.0 with 2 tasks
20/09/06 19:37:14 INFO scheduler.TaskSetManager: Starting task 0.0 in stage 2.0 (TID 4, localhost, executor driver, partition 0, PROCESS_LOCAL, 2152 bytes)
20/09/06 19:37:14 INFO scheduler.TaskSetManager: Starting task 1.0 in stage 2.0 (TID 5, localhost, executor driver, partition 1, PROCESS_LOCAL, 2157 bytes)
20/09/06 19:37:14 INFO executor.Executor: Running task 0.0 in stage 2.0 (TID 4)
20/09/06 19:37:14 INFO executor.Executor: Running task 1.0 in stage 2.0 (TID 5)
20/09/06 19:37:14 INFO executor.Executor: Finished task 1.0 in stage 2.0 (TID 5). 1159 bytes result sent to driver
20/09/06 19:37:14 INFO executor.Executor: Finished task 0.0 in stage 2.0 (TID 4). 1159 bytes result sent to driver
20/09/06 19:37:14 INFO scheduler.TaskSetManager: Finished task 0.0 in stage 2.0 (TID 4) in 77 ms on localhost (executor driver) (1/2)
20/09/06 19:37:14 INFO scheduler.TaskSetManager: Finished task 1.0 in stage 2.0 (TID 5) in 75 ms on localhost (executor driver) (2/2)
20/09/06 19:37:14 INFO scheduler.TaskSchedulerImpl: Removed TaskSet 2.0, whose tasks have all completed, from pool 
20/09/06 19:37:14 INFO scheduler.DAGScheduler: ShuffleMapStage 2 (mapToPair at SparkCoreWordCount.java:37) finished in 0.081 s
20/09/06 19:37:14 INFO scheduler.DAGScheduler: looking for newly runnable stages
20/09/06 19:37:14 INFO scheduler.DAGScheduler: running: Set()
20/09/06 19:37:14 INFO scheduler.DAGScheduler: waiting: Set(ResultStage 3)
20/09/06 19:37:14 INFO scheduler.DAGScheduler: failed: Set()
20/09/06 19:37:14 INFO scheduler.DAGScheduler: Submitting ResultStage 3 (ShuffledRDD[3] at reduceByKey at SparkCoreWordCount.java:43), which has no missing parents
20/09/06 19:37:14 INFO storage.MemoryStore: Block broadcast_3 stored as values in memory (estimated size 3.6 KB, free 530.0 MB)
20/09/06 19:37:14 INFO storage.MemoryStore: Block broadcast_3_piece0 stored as bytes in memory (estimated size 2.1 KB, free 530.0 MB)
20/09/06 19:37:14 INFO storage.BlockManagerInfo: Added broadcast_3_piece0 in memory on localhost:35105 (size: 2.1 KB, free: 530.0 MB)
20/09/06 19:37:14 INFO spark.SparkContext: Created broadcast 3 from broadcast at DAGScheduler.scala:1004
20/09/06 19:37:14 INFO scheduler.DAGScheduler: Submitting 2 missing tasks from ResultStage 3 (ShuffledRDD[3] at reduceByKey at SparkCoreWordCount.java:43) (first 15 tasks are for partitions Vector(0, 1))
20/09/06 19:37:14 INFO scheduler.TaskSchedulerImpl: Adding task set 3.0 with 2 tasks
20/09/06 19:37:14 INFO scheduler.TaskSetManager: Starting task 0.0 in stage 3.0 (TID 6, localhost, executor driver, partition 0, NODE_LOCAL, 1952 bytes)
20/09/06 19:37:14 INFO scheduler.TaskSetManager: Starting task 1.0 in stage 3.0 (TID 7, localhost, executor driver, partition 1, PROCESS_LOCAL, 1952 bytes)
20/09/06 19:37:14 INFO executor.Executor: Running task 0.0 in stage 3.0 (TID 6)
20/09/06 19:37:14 INFO executor.Executor: Running task 1.0 in stage 3.0 (TID 7)
20/09/06 19:37:14 INFO storage.ShuffleBlockFetcherIterator: Getting 2 non-empty blocks out of 2 blocks
20/09/06 19:37:14 INFO storage.ShuffleBlockFetcherIterator: Getting 0 non-empty blocks out of 2 blocks
20/09/06 19:37:14 INFO storage.ShuffleBlockFetcherIterator: Started 0 remote fetches in 6 ms
20/09/06 19:37:14 INFO storage.ShuffleBlockFetcherIterator: Started 0 remote fetches in 6 ms
20/09/06 19:37:14 INFO executor.Executor: Finished task 1.0 in stage 3.0 (TID 7). 1165 bytes result sent to driver
20/09/06 19:37:14 INFO scheduler.TaskSetManager: Finished task 1.0 in stage 3.0 (TID 7) in 45 ms on localhost (executor driver) (1/2)
(rani,3)
(neha,2)
(aparnesh,2)
20/09/06 19:37:14 INFO executor.Executor: Finished task 0.0 in stage 3.0 (TID 6). 1165 bytes result sent to driver
20/09/06 19:37:14 INFO scheduler.TaskSetManager: Finished task 0.0 in stage 3.0 (TID 6) in 60 ms on localhost (executor driver) (2/2)
20/09/06 19:37:14 INFO scheduler.TaskSchedulerImpl: Removed TaskSet 3.0, whose tasks have all completed, from pool 
20/09/06 19:37:14 INFO scheduler.DAGScheduler: ResultStage 3 (foreach at SparkCoreWordCount.java:48) finished in 0.060 s
20/09/06 19:37:14 INFO scheduler.DAGScheduler: Job 2 finished: foreach at SparkCoreWordCount.java:48, took 0.205806 s
20/09/06 19:37:14 INFO spark.SparkContext: Invoking stop() from shutdown hook
20/09/06 19:37:14 INFO handler.ContextHandler: stopped o.s.j.s.ServletContextHandler{/metrics/json,null}
20/09/06 19:37:14 INFO handler.ContextHandler: stopped o.s.j.s.ServletContextHandler{/stages/stage/kill,null}
20/09/06 19:37:14 INFO handler.ContextHandler: stopped o.s.j.s.ServletContextHandler{/api,null}
20/09/06 19:37:14 INFO handler.ContextHandler: stopped o.s.j.s.ServletContextHandler{/,null}
20/09/06 19:37:14 INFO handler.ContextHandler: stopped o.s.j.s.ServletContextHandler{/static,null}
20/09/06 19:37:14 INFO handler.ContextHandler: stopped o.s.j.s.ServletContextHandler{/executors/threadDump/json,null}
20/09/06 19:37:14 INFO handler.ContextHandler: stopped o.s.j.s.ServletContextHandler{/executors/threadDump,null}
20/09/06 19:37:14 INFO handler.ContextHandler: stopped o.s.j.s.ServletContextHandler{/executors/json,null}
20/09/06 19:37:14 INFO handler.ContextHandler: stopped o.s.j.s.ServletContextHandler{/executors,null}

	 * 
	 * 
	 * 
	 * 
	 */
	
	
			

}
