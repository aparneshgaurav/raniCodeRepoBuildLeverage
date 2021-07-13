package com.poc.rcm.sparkcore;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;

import scala.Tuple2;
public class SparkCoreTemperatureCountForMaxTempYearAndMonthComboAndMay {
// spark-submit --class com.poc.rcm.sparkcore.SparkCoreWordCount /home/cloudera/dev/SparkCore.jar
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		/*
		 * to capture the readings of temperature in month of May across the years . 
		 *  
		 */
		
		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("TemperatureSparkWordCount");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		
		List<String> data = Arrays.asList("1947 May 33","1948 June 35","1947 July 58","1950 August 41","1948 May 45","1947 June 56","1948 May 46");
		JavaRDD<String> distData = sc.parallelize(data);
		
		JavaRDD<Integer> lineLengths = distData.map(s -> s.length());
		int totalLength = lineLengths.reduce((a, b) -> a + b);
		
		System.out.println(" the total length is : "+totalLength);
		// map phase 
		System.out.println("######### MAP PHASE ##############");
		System.out.println("#######################");
				JavaPairRDD<String, Integer> pairs = distData
						.filter(record
								->
						{
							Boolean bool = false;
							bool = record.toString().toLowerCase().contains("may");
							return bool;
						}
								)
						.mapToPair(
						record
						-> 
						{
//							if(record.toString().toLowerCase().contains("may")){
						String[] dataArray = record.split(" ");
						return new Tuple2(dataArray[0].toString()+"-"+dataArray[1].toString(), Integer.parseInt(dataArray[2]));
//						}
	                     }						
						);
				System.out.println("#######################");
				System.out.println("printing the created key value pairs , map phase output , printing the pairedRDD : "+pairs);
				System.out.println("#######################");
				pairs.foreach(record -> System.out.println(record));
				
				
				System.out.println("#######################");
				System.out.println(" Transforming the pairedRDD considering the choice of keys and values , considering the query or the question you want to answer .  ");
				System.out.println(" key is going to be the month , value is going to be the temperature readings for that year");
				System.out.println("#######################");
				
				
				System.out.println("######## REDUCE PHASE ###############");
				System.out.println(" Now going to run reduceByKey , which will aggregate the values corresponding to the keys from across the machines in the cluster , and then will run the aggregation logic as per the lambda function on the transformed pairedRDD ");
				System.out.println("#######################");
				// reduce phase 
				JavaPairRDD<String, Integer> counts = pairs.reduceByKey(
						(a, b) 
						->
						{
				if(a>b) return a;
				else return b;
						}
				);
				
				System.out.println("#######################");
				System.out.println("printing the years and their corresponding maximum temperature of that year "+counts);
				System.out.println("#######################");
				counts.foreach(record -> System.out.println(record));

	}
	
	
	
	/*
	 * 
[cloudera@quickstart practice]$ spark-submit --class com.poc.rcm.sparkcore.SparkCoreTemperatureCountForMaxTempYearAndMonthCombo /home/cloudera/dev/practice/Temperature.jar 
SLF4J: Class path contains multiple SLF4J bindings.
SLF4J: Found binding in [jar:file:/usr/lib/zookeeper/lib/slf4j-log4j12-1.7.5.jar!/org/slf4j/impl/StaticLoggerBinder.class]
20/12/22 02:57:11 INFO scheduler.TaskSchedulerImpl: Removed TaskSet 0.0, whose tasks have all completed, from pool 
20/12/22 02:57:11 INFO scheduler.DAGScheduler: ResultStage 0 (reduce at SparkCoreTemperatureCountForMaxTempYearAndMonthCombo.java:25) finished in 1.278 s
20/12/22 02:57:11 INFO scheduler.DAGScheduler: Job 0 finished: reduce at SparkCoreTemperatureCountForMaxTempYearAndMonthCombo.java:25, took 5.461918 s
 the total length is : 83
######### MAP PHASE ##############
#######################
#######################
printing the created key value pairs , map phase output , printing the pairedRDD : org.apache.spark.api.java.JavaPairRDD@53311681
#######################
20/12/22 02:57:11 INFO spark.SparkContext: Starting job: foreach at SparkCoreTemperatureCountForMaxTempYearAndMonthCombo.java:42
20/12/22 02:57:11 INFO scheduler.DAGScheduler: Got job 1 (foreach at SparkCoreTemperatureCountForMaxTempYearAndMonthCombo.java:42) with 2 output partitions
20/12/22 02:57:11 INFO scheduler.DAGScheduler: Final stage: ResultStage 1 (foreach at SparkCoreTemperatureCountForMaxTempYearAndMonthCombo.java:42)
20/12/22 02:57:11 INFO scheduler.DAGScheduler: Parents of final stage: List()
20/12/22 02:57:11 INFO scheduler.DAGScheduler: Missing parents: List()
20/12/22 02:57:11 INFO scheduler.DAGScheduler: Submitting ResultStage 1 (MapPartitionsRDD[2] at mapToPair at SparkCoreTemperatureCountForMaxTempYearAndMonthCombo.java:31), which has no missing parents
20/12/22 02:57:11 INFO storage.MemoryStore: Block broadcast_1 stored as values in memory (estimated size 3.1 KB, free 530.0 MB)
20/12/22 02:57:12 INFO storage.MemoryStore: Block broadcast_1_piece0 stored as bytes in memory (estimated size 1864.0 B, free 530.0 MB)
20/12/22 02:57:12 INFO storage.BlockManagerInfo: Added broadcast_1_piece0 in memory on localhost:37990 (size: 1864.0 B, free: 530.0 MB)
20/12/22 02:57:12 INFO spark.SparkContext: Created broadcast 1 from broadcast at DAGScheduler.scala:1004
20/12/22 02:57:12 INFO scheduler.DAGScheduler: Submitting 2 missing tasks from ResultStage 1 (MapPartitionsRDD[2] at mapToPair at SparkCoreTemperatureCountForMaxTempYearAndMonthCombo.java:31) (first 15 tasks are for partitions Vector(0, 1))
20/12/22 02:57:12 INFO scheduler.TaskSchedulerImpl: Adding task set 1.0 with 2 tasks
20/12/22 02:57:12 INFO scheduler.TaskSetManager: Starting task 0.0 in stage 1.0 (TID 2, localhost, executor driver, partition 0, PROCESS_LOCAL, 2184 bytes)
20/12/22 02:57:12 INFO scheduler.TaskSetManager: Starting task 1.0 in stage 1.0 (TID 3, localhost, executor driver, partition 1, PROCESS_LOCAL, 2200 bytes)
20/12/22 02:57:12 INFO executor.Executor: Running task 0.0 in stage 1.0 (TID 2)
20/12/22 02:57:12 INFO executor.Executor: Running task 1.0 in stage 1.0 (TID 3)
(1947-May,33)
(1948-June,35)
(1947-July,58)
(1950-August,41)
(1948-May,45)
(1947-June,56)
(1948-May,46)
20/12/22 02:57:12 INFO executor.Executor: Finished task 0.0 in stage 1.0 (TID 2). 915 bytes result sent to driver
20/12/22 02:57:12 INFO executor.Executor: Finished task 1.0 in stage 1.0 (TID 3). 915 bytes result sent to driver
20/12/22 02:57:12 INFO scheduler.TaskSetManager: Finished task 0.0 in stage 1.0 (TID 2) in 18 ms on localhost (executor driver) (1/2)
20/12/22 02:57:12 INFO scheduler.TaskSetManager: Finished task 1.0 in stage 1.0 (TID 3) in 17 ms on localhost (executor driver) (2/2)
20/12/22 02:57:12 INFO scheduler.TaskSchedulerImpl: Removed TaskSet 1.0, whose tasks have all completed, from pool 
20/12/22 02:57:12 INFO scheduler.DAGScheduler: ResultStage 1 (foreach at SparkCoreTemperatureCountForMaxTempYearAndMonthCombo.java:42) finished in 0.020 s
20/12/22 02:57:12 INFO scheduler.DAGScheduler: Job 1 finished: foreach at SparkCoreTemperatureCountForMaxTempYearAndMonthCombo.java:42, took 0.597435 s
#######################
 Transforming the pairedRDD considering the choice of keys and values , considering the query or the question you want to answer .  
 key is going to be the month , value is going to be the temperature readings for that year
#######################
######## REDUCE PHASE ###############
 Now going to run reduceByKey , which will aggregate the values corresponding to the keys from across the machines in the cluster , and then will run the aggregation logic as per the lambda function on the transformed pairedRDD 
#######################
#######################
printing the years and their corresponding maximum temperature of that year org.apache.spark.api.java.JavaPairRDD@11900483
#######################
20/12/22 02:57:12 INFO spark.SparkContext: Starting job: foreach at SparkCoreTemperatureCountForMaxTempYearAndMonthCombo.java:67
20/12/22 02:57:12 INFO scheduler.DAGScheduler: Registering RDD 2 (mapToPair at SparkCoreTemperatureCountForMaxTempYearAndMonthCombo.java:31)
20/12/22 02:57:12 INFO scheduler.DAGScheduler: Got job 2 (foreach at SparkCoreTemperatureCountForMaxTempYearAndMonthCombo.java:67) with 2 output partitions
20/12/22 02:57:12 INFO scheduler.DAGScheduler: Final stage: ResultStage 3 (foreach at SparkCoreTemperatureCountForMaxTempYearAndMonthCombo.java:67)
20/12/22 02:57:12 INFO scheduler.DAGScheduler: Parents of final stage: List(ShuffleMapStage 2)
20/12/22 02:57:12 INFO scheduler.DAGScheduler: Missing parents: List(ShuffleMapStage 2)
20/12/22 02:57:12 INFO scheduler.DAGScheduler: Submitting ShuffleMapStage 2 (MapPartitionsRDD[2] at mapToPair at SparkCoreTemperatureCountForMaxTempYearAndMonthCombo.java:31), which has no missing parents
20/12/22 02:57:12 INFO storage.MemoryStore: Block broadcast_2 stored as values in memory (estimated size 3.9 KB, free 530.0 MB)
20/12/22 02:57:12 INFO storage.MemoryStore: Block broadcast_2_piece0 stored as bytes in memory (estimated size 2.2 KB, free 530.0 MB)
20/12/22 02:57:12 INFO storage.BlockManagerInfo: Added broadcast_2_piece0 in memory on localhost:37990 (size: 2.2 KB, free: 530.0 MB)
20/12/22 02:57:12 INFO spark.SparkContext: Created broadcast 2 from broadcast at DAGScheduler.scala:1004
20/12/22 02:57:12 INFO scheduler.DAGScheduler: Submitting 2 missing tasks from ShuffleMapStage 2 (MapPartitionsRDD[2] at mapToPair at SparkCoreTemperatureCountForMaxTempYearAndMonthCombo.java:31) (first 15 tasks are for partitions Vector(0, 1))
20/12/22 02:57:12 INFO scheduler.TaskSchedulerImpl: Adding task set 2.0 with 2 tasks
20/12/22 02:57:12 INFO scheduler.TaskSetManager: Starting task 0.0 in stage 2.0 (TID 4, localhost, executor driver, partition 0, PROCESS_LOCAL, 2173 bytes)
20/12/22 02:57:12 INFO scheduler.TaskSetManager: Starting task 1.0 in stage 2.0 (TID 5, localhost, executor driver, partition 1, PROCESS_LOCAL, 2189 bytes)
20/12/22 02:57:12 INFO executor.Executor: Running task 0.0 in stage 2.0 (TID 4)
20/12/22 02:57:12 INFO executor.Executor: Running task 1.0 in stage 2.0 (TID 5)
20/12/22 02:57:13 INFO executor.Executor: Finished task 0.0 in stage 2.0 (TID 4). 1159 bytes result sent to driver
20/12/22 02:57:13 INFO executor.Executor: Finished task 1.0 in stage 2.0 (TID 5). 1159 bytes result sent to driver
20/12/22 02:57:13 INFO scheduler.TaskSetManager: Finished task 0.0 in stage 2.0 (TID 4) in 378 ms on localhost (executor driver) (1/2)
20/12/22 02:57:13 INFO scheduler.TaskSetManager: Finished task 1.0 in stage 2.0 (TID 5) in 373 ms on localhost (executor driver) (2/2)
20/12/22 02:57:13 INFO scheduler.TaskSchedulerImpl: Removed TaskSet 2.0, whose tasks have all completed, from pool 
20/12/22 02:57:13 INFO scheduler.DAGScheduler: ShuffleMapStage 2 (mapToPair at SparkCoreTemperatureCountForMaxTempYearAndMonthCombo.java:31) finished in 0.381 s
20/12/22 02:57:13 INFO scheduler.DAGScheduler: looking for newly runnable stages
20/12/22 02:57:13 INFO scheduler.DAGScheduler: running: Set()
20/12/22 02:57:13 INFO scheduler.DAGScheduler: waiting: Set(ResultStage 3)
20/12/22 02:57:13 INFO scheduler.DAGScheduler: failed: Set()
20/12/22 02:57:13 INFO scheduler.DAGScheduler: Submitting ResultStage 3 (ShuffledRDD[3] at reduceByKey at SparkCoreTemperatureCountForMaxTempYearAndMonthCombo.java:55), which has no missing parents
20/12/22 02:57:13 INFO storage.MemoryStore: Block broadcast_3 stored as values in memory (estimated size 3.7 KB, free 530.0 MB)
20/12/22 02:57:13 INFO storage.MemoryStore: Block broadcast_3_piece0 stored as bytes in memory (estimated size 2.1 KB, free 530.0 MB)
20/12/22 02:57:13 INFO storage.BlockManagerInfo: Added broadcast_3_piece0 in memory on localhost:37990 (size: 2.1 KB, free: 530.0 MB)
20/12/22 02:57:13 INFO spark.SparkContext: Created broadcast 3 from broadcast at DAGScheduler.scala:1004
20/12/22 02:57:13 INFO scheduler.DAGScheduler: Submitting 2 missing tasks from ResultStage 3 (ShuffledRDD[3] at reduceByKey at SparkCoreTemperatureCountForMaxTempYearAndMonthCombo.java:55) (first 15 tasks are for partitions Vector(0, 1))
20/12/22 02:57:13 INFO scheduler.TaskSchedulerImpl: Adding task set 3.0 with 2 tasks
20/12/22 02:57:13 INFO scheduler.TaskSetManager: Starting task 0.0 in stage 3.0 (TID 6, localhost, executor driver, partition 0, NODE_LOCAL, 1954 bytes)
20/12/22 02:57:13 INFO scheduler.TaskSetManager: Starting task 1.0 in stage 3.0 (TID 7, localhost, executor driver, partition 1, NODE_LOCAL, 1954 bytes)
20/12/22 02:57:13 INFO executor.Executor: Running task 1.0 in stage 3.0 (TID 7)
20/12/22 02:57:13 INFO executor.Executor: Running task 0.0 in stage 3.0 (TID 6)
20/12/22 02:57:13 INFO storage.ShuffleBlockFetcherIterator: Getting 2 non-empty blocks out of 2 blocks
20/12/22 02:57:13 INFO storage.ShuffleBlockFetcherIterator: Getting 2 non-empty blocks out of 2 blocks
20/12/22 02:57:13 INFO storage.ShuffleBlockFetcherIterator: Started 0 remote fetches in 6 ms
20/12/22 02:57:13 INFO storage.ShuffleBlockFetcherIterator: Started 0 remote fetches in 6 ms
(1948-May,46)
(1947-July,58)
(1947-May,33)
(1948-June,35)
(1950-August,41)
(1947-June,56)
20/12/22 02:57:13 INFO executor.Executor: Finished task 0.0 in stage 3.0 (TID 6). 1165 bytes result sent to driver
20/12/22 02:57:13 INFO executor.Executor: Finished task 1.0 in stage 3.0 (TID 7). 1165 bytes result sent to driver

	*/
	
			

}
