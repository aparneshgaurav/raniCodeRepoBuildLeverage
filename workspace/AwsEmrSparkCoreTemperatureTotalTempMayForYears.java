package com.poc.rcm.sparkcore;

import java.util.Arrays;
import java.util.List;

//import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;


import scala.Tuple2;
public class AwsEmrSparkCoreTemperatureTotalTempMayForYears {
// spark-submit --class com.poc.rcm.sparkcore.AwsEmrSparkCoreTemperatureTotalTempMayForYears /home/cloudera/dev/SparkCore.jar
	
	/*step type spark application
	name spark application 
	deploy mode : client
	spark submit options : 
	--class com.poc.sparkcore.SparkMay
	application location : 
	s3://emr/Temperature.jar
	arguments : 
	blank 
	Action on failure : 
	continue*/
	
/*	logs you may also see from the cluster where the s3 logging is configured . 
	Which is like the edge node of the cluster configured in a way . */
	
//	 static Logger logger = Logger.getLogger(AwsEmrSparkCoreTemperatureTotalTempMayForYears.class);
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		/*
		 * to capture the readings of temperature in month of May across the years . 
		 *  
		 */
	
		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("TemperatureSparkWordCount");
//		SparkConf conf = new SparkConf().setAppName("TemperatureSparkWordCount");
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
						return new Tuple2("may", Integer.parseInt(dataArray[2]));
//						}
	                     }						
						);
				System.out.println("#######################");
				System.out.println("printing the created key value pairs , map phase output , printing the pairedRDD : "+pairs);
//				logger.info("printing the created key value pairs , map phase output , printing the pairedRDD : "+pairs);
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
				return a+b;
						}
				);
				
				System.out.println("#######################");
				System.out.println("printing the years and their corresponding maximum temperature of that year "+counts);
				System.out.println("#######################");
				counts.foreach(record -> System.out.println(record));

	}
	
	
	
	/*
	 * 
20/12/28 02:45:37 INFO executor.Executor: Running task 1.0 in stage 1.0 (TID 3)
(may,45)
(may,46)
(may,33)
20/12/28 02:45:37 INFO executor.Executor: Finished task 1.0 in stage 1.0 (TID 3). 915 bytes result sent to driver
20/12/28 02:45:37 INFO executor.Executor: Finished task 0.0 in stage 1.0 (TID 2). 915 bytes result sent to driver
20/12/28 02:45:37 INFO scheduler.TaskSetManager: Finished task 1.0 in stage 1.0 (TID 3) in 18 ms on localhost (executor driver) (1/2)
20/12/28 02:45:37 INFO scheduler.TaskSetManager: Finished task 0.0 in stage 1.0 (TID 2) in 23 ms on localhost (executor driver) (2/2)
20/12/28 02:45:37 INFO scheduler.DAGScheduler: ResultStage 1 (foreach at SparkCoreTemperatureCountForTotalTempAcrossYearsForMay.java:58) finished in 0.023 s
20/12/28 02:45:37 INFO scheduler.TaskSchedulerImpl: Removed TaskSet 1.0, whose tasks have all completed, from pool 
20/12/28 02:45:37 INFO scheduler.DAGScheduler: Job 1 finished: foreach at SparkCoreTemperatureCountForTotalTempAcrossYearsForMay.java:58, took 0.043381 s
#######################
 Transforming the pairedRDD considering the choice of keys and values , considering the query or the question you want to answer .  
 key is going to be the month , value is going to be the temperature readings for that year
#######################
######## REDUCE PHASE ###############
 Now going to run reduceByKey , which will aggregate the values corresponding to the keys from across the machines in the cluster , and then will run the aggregation logic as per the lambda function on the transformed pairedRDD 
#######################
#######################
printing the years and their corresponding maximum temperature of that year org.apache.spark.api.java.JavaPairRDD@37468787
#######################
20/12/28 02:45:37 INFO spark.SparkContext: Starting job: foreach at SparkCoreTemperatureCountForTotalTempAcrossYearsForMay.java:82
20/12/28 02:45:37 INFO scheduler.DAGScheduler: Registering RDD 3 (mapToPair at SparkCoreTemperatureCountForTotalTempAcrossYearsForMay.java:45)
20/12/28 02:45:37 INFO scheduler.DAGScheduler: Got job 2 (foreach at SparkCoreTemperatureCountForTotalTempAcrossYearsForMay.java:82) with 2 output partitions
20/12/28 02:45:37 INFO scheduler.DAGScheduler: Final stage: ResultStage 3 (foreach at SparkCoreTemperatureCountForTotalTempAcrossYearsForMay.java:82)
20/12/28 02:45:37 INFO scheduler.DAGScheduler: Parents of final stage: List(ShuffleMapStage 2)
20/12/28 02:45:37 INFO scheduler.DAGScheduler: Missing parents: List(ShuffleMapStage 2)
20/12/28 02:45:37 INFO scheduler.DAGScheduler: Submitting ShuffleMapStage 2 (MapPartitionsRDD[3] at mapToPair at SparkCoreTemperatureCountForTotalTempAcrossYearsForMay.java:45), which has no missing parents
20/12/28 02:45:37 INFO storage.MemoryStore: Block broadcast_2 stored as values in memory (estimated size 4.4 KB, free 530.0 MB)
20/12/28 02:45:37 INFO storage.MemoryStore: Block broadcast_2_piece0 stored as bytes in memory (estimated size 2.4 KB, free 530.0 MB)
20/12/28 02:45:37 INFO storage.BlockManagerInfo: Added broadcast_2_piece0 in memory on localhost:40185 (size: 2.4 KB, free: 530.0 MB)
20/12/28 02:45:37 INFO spark.SparkContext: Created broadcast 2 from broadcast at DAGScheduler.scala:1004
20/12/28 02:45:37 INFO scheduler.DAGScheduler: Submitting 2 missing tasks from ShuffleMapStage 2 (MapPartitionsRDD[3] at mapToPair at SparkCoreTemperatureCountForTotalTempAcrossYearsForMay.java:45) (first 15 tasks are for partitions Vector(0, 1))
20/12/28 02:45:37 INFO scheduler.TaskSchedulerImpl: Adding task set 2.0 with 2 tasks
20/12/28 02:45:37 INFO scheduler.TaskSetManager: Starting task 0.0 in stage 2.0 (TID 4, localhost, executor driver, partition 0, PROCESS_LOCAL, 2173 bytes)
20/12/28 02:45:37 INFO scheduler.TaskSetManager: Starting task 1.0 in stage 2.0 (TID 5, localhost, executor driver, partition 1, PROCESS_LOCAL, 2189 bytes)
20/12/28 02:45:37 INFO executor.Executor: Running task 0.0 in stage 2.0 (TID 4)
20/12/28 02:45:37 INFO executor.Executor: Running task 1.0 in stage 2.0 (TID 5)
20/12/28 02:45:37 INFO executor.Executor: Finished task 0.0 in stage 2.0 (TID 4). 1159 bytes result sent to driver
20/12/28 02:45:37 INFO executor.Executor: Finished task 1.0 in stage 2.0 (TID 5). 1159 bytes result sent to driver
20/12/28 02:45:37 INFO scheduler.TaskSetManager: Finished task 1.0 in stage 2.0 (TID 5) in 78 ms on localhost (executor driver) (1/2)
20/12/28 02:45:37 INFO scheduler.TaskSetManager: Finished task 0.0 in stage 2.0 (TID 4) in 81 ms on localhost (executor driver) (2/2)
20/12/28 02:45:37 INFO scheduler.TaskSchedulerImpl: Removed TaskSet 2.0, whose tasks have all completed, from pool 
20/12/28 02:45:37 INFO scheduler.DAGScheduler: ShuffleMapStage 2 (mapToPair at SparkCoreTemperatureCountForTotalTempAcrossYearsForMay.java:45) finished in 0.085 s
20/12/28 02:45:37 INFO scheduler.DAGScheduler: looking for newly runnable stages
20/12/28 02:45:37 INFO scheduler.DAGScheduler: running: Set()
20/12/28 02:45:37 INFO scheduler.DAGScheduler: waiting: Set(ResultStage 3)
20/12/28 02:45:37 INFO scheduler.DAGScheduler: failed: Set()
20/12/28 02:45:37 INFO scheduler.DAGScheduler: Submitting ResultStage 3 (ShuffledRDD[4] at reduceByKey at SparkCoreTemperatureCountForTotalTempAcrossYearsForMay.java:71), which has no missing parents
20/12/28 02:45:37 INFO storage.MemoryStore: Block broadcast_3 stored as values in memory (estimated size 3.7 KB, free 530.0 MB)
20/12/28 02:45:37 INFO storage.MemoryStore: Block broadcast_3_piece0 stored as bytes in memory (estimated size 2.1 KB, free 530.0 MB)
20/12/28 02:45:37 INFO storage.BlockManagerInfo: Added broadcast_3_piece0 in memory on localhost:40185 (size: 2.1 KB, free: 530.0 MB)
20/12/28 02:45:37 INFO spark.SparkContext: Created broadcast 3 from broadcast at DAGScheduler.scala:1004
20/12/28 02:45:37 INFO scheduler.DAGScheduler: Submitting 2 missing tasks from ResultStage 3 (ShuffledRDD[4] at reduceByKey at SparkCoreTemperatureCountForTotalTempAcrossYearsForMay.java:71) (first 15 tasks are for partitions Vector(0, 1))
20/12/28 02:45:37 INFO scheduler.TaskSchedulerImpl: Adding task set 3.0 with 2 tasks
20/12/28 02:45:37 INFO scheduler.TaskSetManager: Starting task 1.0 in stage 3.0 (TID 6, localhost, executor driver, partition 1, NODE_LOCAL, 1954 bytes)
20/12/28 02:45:37 INFO scheduler.TaskSetManager: Starting task 0.0 in stage 3.0 (TID 7, localhost, executor driver, partition 0, PROCESS_LOCAL, 1954 bytes)
20/12/28 02:45:37 INFO executor.Executor: Running task 1.0 in stage 3.0 (TID 6)
20/12/28 02:45:37 INFO executor.Executor: Running task 0.0 in stage 3.0 (TID 7)
20/12/28 02:45:37 INFO storage.ShuffleBlockFetcherIterator: Getting 2 non-empty blocks out of 2 blocks
20/12/28 02:45:37 INFO storage.ShuffleBlockFetcherIterator: Getting 0 non-empty blocks out of 2 blocks
20/12/28 02:45:37 INFO storage.ShuffleBlockFetcherIterator: Started 0 remote fetches in 6 ms
20/12/28 02:45:37 INFO storage.ShuffleBlockFetcherIterator: Started 0 remote fetches in 6 ms
20/12/28 02:45:37 INFO executor.Executor: Finished task 0.0 in stage 3.0 (TID 7). 1165 bytes result sent to driver
20/12/28 02:45:37 INFO scheduler.TaskSetManager: Finished task 0.0 in stage 3.0 (TID 7) in 47 ms on localhost (executor driver) (1/2)
(may,124)

	*/
	
			

}
