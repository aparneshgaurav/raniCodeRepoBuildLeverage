package com.poc.rcm.sparkcore;

import java.io.FileNotFoundException;
import java.util.Arrays;
import java.util.List;

import javax.xml.xquery.XQException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.SparkConf;

import com.xml.parser.xQueryParser;

import scala.Tuple2;
public class SparkCoreTemperatureCountWithFunctionMapAndReduceFromFile {
	// spark-submit --class com.poc.rcm.sparkcore.SparkCoreWordCount /home/cloudera/dev/SparkCore.jar
	//	spark-submit --class com.poc.rcm.sparkcore.SparkCoreTemperatureCount /home/cloudera/dev/practice/TemperatureSparkCount.jar 
	// spark-submit --class com.poc.rcm.sparkcore.SparkCoreTemperatureCountWithFunctionMap /home/cloudera/dev/practice/TemperatureSparkCountWithFunction.jar
	
	// command to run with data from file and also command to make a third party call 
	// via map or reduce method ( map means on each row ) and reduce means aggregation 
	// across records 
	/*spark-submit --class com.poc.rcm.sparkcore.SparkCoreTemperatureCountWithFunctionMapAndReduceFromFile --jars /home/cloudera/dev/practice/SaxonHE10-3J/jline-2.9.jar,/home/cloudera/dev/practice/SaxonHE10-3J/saxon-he-10.3.jar,/home/cloudera/dev/practice/SaxonHE10-3J/saxon-he-test-10.3.jar,/home/cloudera/dev/practice/SaxonHE10-3J/saxon-xqj-10.3.jar /home/cloudera/dev/practice/TemperatureSparkCountWithFunction.jar
*/
	
	
	// latest comamnd at bottom row 
	
	// for reduce side methods to work , in the list of values / stream for key at reduce size , there should be distinct 
	// values , hence had to take two values of 1947 , two values of 1948 and so on ... 
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("TemperatureSparkWordCount");
		JavaSparkContext sc = new JavaSparkContext(conf);


		/*List<String> data = Arrays.asList("1947 33","1948 35","1950 41","1947 45","1948 56","1950 46");
		JavaRDD<String> distData = sc.parallelize(data);*/
		JavaRDD<String> distData = sc.textFile("/user/cloudera/data/");

		JavaRDD<Integer> lineLengths = distData.map(s -> s.length());
		int totalLength = lineLengths.reduce((a, b) -> a + b);

		System.out.println(" the total length is : "+totalLength);
		// map phase 
		System.out.println("######### MAP PHASE ##############");
		System.out.println("#######################");
		JavaPairRDD<String, Integer> pairs = distData.mapToPair(
				record
				-> 
				{
					String[] dataArray = record.split(" ");
					return new Tuple2(dataArray[0], computeIntegerMapAmplify(Integer.parseInt(dataArray[1])));
				}
				);
		System.out.println("#######################");
		System.out.println("printing the created key value pairs , map phase output , printing the pairedRDD : "+pairs);
		System.out.println("#######################");
		pairs.foreach(record -> System.out.println(record));


		System.out.println("#######################");
		System.out.println(" Transforming the pairedRDD considering the choice of keys and values , considering the query or the question you want to answer .  ");
		System.out.println(" key is going to be the year , value is going to be the temperature readings for that year");
		System.out.println("#######################");


		System.out.println("######## REDUCE PHASE ###############");
		System.out.println(" Now going to run reduceByKey , which will aggregate the values corresponding to the keys from across the machines in the cluster , and then will run the aggregation logic as per the lambda function on the transformed pairedRDD ");
		System.out.println("#######################");
		// reduce phase 
		JavaPairRDD<String, Integer> counts = pairs.reduceByKey(
				(a, b) 
				->
				{
					if(a>b){ 
						Integer temp = null;
						temp = computeIntegerReduceMultiply(a);
						return temp; 
					}
					else {
						Integer tempOther = null;
						tempOther = computeIntegerReduceMultiply(b);
						return tempOther;
					}
				}
				);

		System.out.println("#######################");
		System.out.println("printing the years and their corresponding maximum temperature of that year "+counts);
		System.out.println("#######################");
		counts.foreach(record -> System.out.println("%%%%%%%%%%%%%%%%%%%%%%"+record));

	}

	public static Integer computeIntegerMapAmplify(Integer value){
		value = value + 40;
		System.out.println("*************************************"+value);
		return value ; 
	}
	
	public static Integer computeIntegerReduceMultiply(Integer value) throws FileNotFoundException, XQException{
		value = value * 1000;
		System.out.println("*************************************"+value);
		xQueryParser.execute();
		return value ; 
	}



	/*
	 * 
######### MAP PHASE ##############
#######################
#######################
printing the created key value pairs , map phase output , printing the pairedRDD : org.apache.spark.api.java.JavaPairRDD@7c8c70d6
#######################
21/02/27 15:26:39 INFO spark.SparkContext: Starting job: foreach at SparkCoreTemperatureCountWithFunctionMapAndReduce.java:51
21/02/27 15:26:39 INFO scheduler.DAGScheduler: Got job 1 (foreach at SparkCoreTemperatureCountWithFunctionMapAndReduce.java:51) with 2 output partitions
21/02/27 15:26:39 INFO scheduler.DAGScheduler: Final stage: ResultStage 1 (foreach at SparkCoreTemperatureCountWithFunctionMapAndReduce.java:51)
21/02/27 15:26:39 INFO scheduler.DAGScheduler: Parents of final stage: List()
21/02/27 15:26:39 INFO scheduler.DAGScheduler: Missing parents: List()
21/02/27 15:26:39 INFO scheduler.DAGScheduler: Submitting ResultStage 1 (MapPartitionsRDD[2] at mapToPair at SparkCoreTemperatureCountWithFunctionMapAndReduce.java:40), which has no missing parents
21/02/27 15:26:39 INFO storage.MemoryStore: Block broadcast_1 stored as values in memory (estimated size 3.1 KB, free 530.0 MB)
21/02/27 15:26:39 INFO storage.MemoryStore: Block broadcast_1_piece0 stored as bytes in memory (estimated size 1857.0 B, free 530.0 MB)
21/02/27 15:26:39 INFO storage.BlockManagerInfo: Added broadcast_1_piece0 in memory on localhost:44422 (size: 1857.0 B, free: 530.0 MB)
21/02/27 15:26:39 INFO spark.SparkContext: Created broadcast 1 from broadcast at DAGScheduler.scala:1004
21/02/27 15:26:39 INFO scheduler.DAGScheduler: Submitting 2 missing tasks from ResultStage 1 (MapPartitionsRDD[2] at mapToPair at SparkCoreTemperatureCountWithFunctionMapAndReduce.java:40) (first 15 tasks are for partitions Vector(0, 1))
21/02/27 15:26:39 INFO scheduler.TaskSchedulerImpl: Adding task set 1.0 with 2 tasks
21/02/27 15:26:39 INFO scheduler.TaskSetManager: Starting task 0.0 in stage 1.0 (TID 2, localhost, executor driver, partition 0, PROCESS_LOCAL, 2192 bytes)
21/02/27 15:26:39 INFO scheduler.TaskSetManager: Starting task 1.0 in stage 1.0 (TID 3, localhost, executor driver, partition 1, PROCESS_LOCAL, 2192 bytes)
21/02/27 15:26:39 INFO executor.Executor: Running task 0.0 in stage 1.0 (TID 2)
21/02/27 15:26:39 INFO executor.Executor: Running task 1.0 in stage 1.0 (TID 3)
*************************************73
(1947,73)
*************************************75
(1948,75)
*************************************81
(1950,81)
*************************************85
(1947,85)
*************************************96
(1948,96)
*************************************86
(1950,86)
21/02/27 15:26:39 INFO executor.Executor: Finished task 0.0 in stage 1.0 (TID 2). 915 bytes result sent to driver
21/02/27 15:26:39 INFO executor.Executor: Finished task 1.0 in stage 1.0 (TID 3). 915 bytes result sent to driver
21/02/27 15:26:39 INFO scheduler.TaskSetManager: Finished task 0.0 in stage 1.0 (TID 2) in 31 ms on localhost (executor driver) (1/2)
21/02/27 15:26:39 INFO scheduler.TaskSetManager: Finished task 1.0 in stage 1.0 (TID 3) in 29 ms on localhost (executor driver) (2/2)
21/02/27 15:26:39 INFO scheduler.TaskSchedulerImpl: Removed TaskSet 1.0, whose tasks have all completed, from pool 
21/02/27 15:26:39 INFO scheduler.DAGScheduler: ResultStage 1 (foreach at SparkCoreTemperatureCountWithFunctionMapAndReduce.java:51) finished in 0.030 s
21/02/27 15:26:39 INFO scheduler.DAGScheduler: Job 1 finished: foreach at SparkCoreTemperatureCountWithFunctionMapAndReduce.java:51, took 0.063858 s
#######################
 Transforming the pairedRDD considering the choice of keys and values , considering the query or the question you want to answer .  
 key is going to be the year , value is going to be the temperature readings for that year
#######################
######## REDUCE PHASE ###############
 Now going to run reduceByKey , which will aggregate the values corresponding to the keys from across the machines in the cluster , and then will run the aggregation logic as per the lambda function on the transformed pairedRDD 
#######################
#######################
printing the years and their corresponding maximum temperature of that year org.apache.spark.api.java.JavaPairRDD@43d9f1a2
#######################
21/02/27 15:26:39 INFO spark.SparkContext: Starting job: foreach at SparkCoreTemperatureCountWithFunctionMapAndReduce.java:84
21/02/27 15:26:39 INFO scheduler.DAGScheduler: Registering RDD 2 (mapToPair at SparkCoreTemperatureCountWithFunctionMapAndReduce.java:40)
21/02/27 15:26:39 INFO scheduler.DAGScheduler: Got job 2 (foreach at SparkCoreTemperatureCountWithFunctionMapAndReduce.java:84) with 2 output partitions
21/02/27 15:26:39 INFO scheduler.DAGScheduler: Final stage: ResultStage 3 (foreach at SparkCoreTemperatureCountWithFunctionMapAndReduce.java:84)
21/02/27 15:26:39 INFO scheduler.DAGScheduler: Parents of final stage: List(ShuffleMapStage 2)
21/02/27 15:26:39 INFO scheduler.DAGScheduler: Missing parents: List(ShuffleMapStage 2)
21/02/27 15:26:39 INFO scheduler.DAGScheduler: Submitting ShuffleMapStage 2 (MapPartitionsRDD[2] at mapToPair at SparkCoreTemperatureCountWithFunctionMapAndReduce.java:40), which has no missing parents
21/02/27 15:26:39 INFO storage.MemoryStore: Block broadcast_2 stored as values in memory (estimated size 3.9 KB, free 530.0 MB)
21/02/27 15:26:39 INFO storage.MemoryStore: Block broadcast_2_piece0 stored as bytes in memory (estimated size 2.2 KB, free 530.0 MB)
21/02/27 15:26:39 INFO storage.BlockManagerInfo: Added broadcast_2_piece0 in memory on localhost:44422 (size: 2.2 KB, free: 530.0 MB)
21/02/27 15:26:39 INFO spark.SparkContext: Created broadcast 2 from broadcast at DAGScheduler.scala:1004
21/02/27 15:26:39 INFO scheduler.DAGScheduler: Submitting 2 missing tasks from ShuffleMapStage 2 (MapPartitionsRDD[2] at mapToPair at SparkCoreTemperatureCountWithFunctionMapAndReduce.java:40) (first 15 tasks are for partitions Vector(0, 1))
21/02/27 15:26:39 INFO scheduler.TaskSchedulerImpl: Adding task set 2.0 with 2 tasks
21/02/27 15:26:39 INFO scheduler.TaskSetManager: Starting task 0.0 in stage 2.0 (TID 4, localhost, executor driver, partition 0, PROCESS_LOCAL, 2181 bytes)
21/02/27 15:26:39 INFO scheduler.TaskSetManager: Starting task 1.0 in stage 2.0 (TID 5, localhost, executor driver, partition 1, PROCESS_LOCAL, 2181 bytes)
21/02/27 15:26:39 INFO executor.Executor: Running task 0.0 in stage 2.0 (TID 4)
21/02/27 15:26:39 INFO executor.Executor: Running task 1.0 in stage 2.0 (TID 5)
*************************************73
*************************************85
*************************************75
*************************************96
*************************************81
*************************************86
21/02/27 15:26:39 INFO executor.Executor: Finished task 1.0 in stage 2.0 (TID 5). 1159 bytes result sent to driver
21/02/27 15:26:39 INFO executor.Executor: Finished task 0.0 in stage 2.0 (TID 4). 1159 bytes result sent to driver
21/02/27 15:26:39 INFO scheduler.TaskSetManager: Finished task 0.0 in stage 2.0 (TID 4) in 120 ms on localhost (executor driver) (1/2)
21/02/27 15:26:40 INFO scheduler.TaskSetManager: Finished task 1.0 in stage 2.0 (TID 5) in 124 ms on localhost (executor driver) (2/2)
21/02/27 15:26:40 INFO scheduler.TaskSchedulerImpl: Removed TaskSet 2.0, whose tasks have all completed, from pool 
21/02/27 15:26:40 INFO scheduler.DAGScheduler: ShuffleMapStage 2 (mapToPair at SparkCoreTemperatureCountWithFunctionMapAndReduce.java:40) finished in 0.129 s
21/02/27 15:26:40 INFO scheduler.DAGScheduler: looking for newly runnable stages
21/02/27 15:26:40 INFO scheduler.DAGScheduler: running: Set()
21/02/27 15:26:40 INFO scheduler.DAGScheduler: waiting: Set(ResultStage 3)
21/02/27 15:26:40 INFO scheduler.DAGScheduler: failed: Set()
21/02/27 15:26:40 INFO scheduler.DAGScheduler: Submitting ResultStage 3 (ShuffledRDD[3] at reduceByKey at SparkCoreTemperatureCountWithFunctionMapAndReduce.java:64), which has no missing parents
21/02/27 15:26:40 INFO storage.MemoryStore: Block broadcast_3 stored as values in memory (estimated size 3.7 KB, free 530.0 MB)
21/02/27 15:26:40 INFO storage.MemoryStore: Block broadcast_3_piece0 stored as bytes in memory (estimated size 2.1 KB, free 530.0 MB)
21/02/27 15:26:40 INFO storage.BlockManagerInfo: Added broadcast_3_piece0 in memory on localhost:44422 (size: 2.1 KB, free: 530.0 MB)
21/02/27 15:26:40 INFO spark.SparkContext: Created broadcast 3 from broadcast at DAGScheduler.scala:1004
21/02/27 15:26:40 INFO scheduler.DAGScheduler: Submitting 2 missing tasks from ResultStage 3 (ShuffledRDD[3] at reduceByKey at SparkCoreTemperatureCountWithFunctionMapAndReduce.java:64) (first 15 tasks are for partitions Vector(0, 1))
21/02/27 15:26:40 INFO scheduler.TaskSchedulerImpl: Adding task set 3.0 with 2 tasks
21/02/27 15:26:40 INFO scheduler.TaskSetManager: Starting task 0.0 in stage 3.0 (TID 6, localhost, executor driver, partition 0, NODE_LOCAL, 1976 bytes)
21/02/27 15:26:40 INFO scheduler.TaskSetManager: Starting task 1.0 in stage 3.0 (TID 7, localhost, executor driver, partition 1, NODE_LOCAL, 1976 bytes)
21/02/27 15:26:40 INFO executor.Executor: Running task 0.0 in stage 3.0 (TID 6)
21/02/27 15:26:40 INFO executor.Executor: Running task 1.0 in stage 3.0 (TID 7)
21/02/27 15:26:40 INFO storage.ShuffleBlockFetcherIterator: Getting 2 non-empty blocks out of 2 blocks
21/02/27 15:26:40 INFO storage.ShuffleBlockFetcherIterator: Getting 2 non-empty blocks out of 2 blocks
21/02/27 15:26:40 INFO storage.ShuffleBlockFetcherIterator: Started 0 remote fetches in 9 ms
21/02/27 15:26:40 INFO storage.ShuffleBlockFetcherIterator: Started 0 remote fetches in 7 ms
*************************************96000
*************************************85000
*************************************86000
%%%%%%%%%%%%%%%%%%%%%%(1948,96000)
%%%%%%%%%%%%%%%%%%%%%%(1950,86000)
%%%%%%%%%%%%%%%%%%%%%%(1947,85000)


	 */



}
