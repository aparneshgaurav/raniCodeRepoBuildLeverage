package com.poc.rcm.sparkcore;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;

import javax.xml.xquery.XQException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.rdd.SequenceFileRDDFunctions;
import org.apache.spark.SparkConf;

import com.xml.parser.xQueryParser;

import scala.Tuple2;
public class SparkCoreTemperatureCountWithFunctionMapAndReduceFromSequenceFileAndBroadCastUsage {
	// spark-submit --class com.poc.rcm.sparkcore.SparkCoreWordCount /home/cloudera/dev/SparkCore.jar
	//	spark-submit --class com.poc.rcm.sparkcore.SparkCoreTemperatureCount /home/cloudera/dev/practice/TemperatureSparkCount.jar 
	// spark-submit --class com.poc.rcm.sparkcore.SparkCoreTemperatureCountWithFunctionMap /home/cloudera/dev/practice/TemperatureSparkCountWithFunction.jar

	// command to run with data from file and also command to make a third party call 
	// via map or reduce method ( map means on each row ) and reduce means aggregation 
	// across records 
	/*spark-submit --class com.poc.rcm.sparkcore.SparkCoreTemperatureCountWithFunctionMapAndReduceFromSequenceFileAndBroadCastUsage --jars /home/cloudera/dev/practice/SaxonHE10-3J/jline-2.9.jar,/home/cloudera/dev/practice/SaxonHE10-3J/saxon-he-10.3.jar,/home/cloudera/dev/practice/SaxonHE10-3J/saxon-he-test-10.3.jar,/home/cloudera/dev/practice/SaxonHE10-3J/saxon-xqj-10.3.jar /home/cloudera/dev/practice/TemperatureSparkCountWithFunction.jar
	 */


	// latest comamnd at bottom row 

	// for reduce side methods to work , in the list of values / stream for key at reduce size , there should be distinct 
	// values , hence had to take two values of 1947 , two values of 1948 and so on ... 
	
//	codes may print / collect the results and actions can be invoked only by action / collect methods 
	
	/*pointers
	https://spark.apache.org/docs/2.3.0/api/java/org/apache/spark/api/java/JavaSparkContext.html
	started with sequence file concept where key is file name and value is content of the file 
	while working after creating sequence file in pocRcmHadoop project , found that hadoop sequence file 
	had text to string compaitibility issues with sparks' string . 
	so used wholeFile on sc as a new api as a substitute in spark */
	
	
	
	
	
	
		
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("TemperatureSparkWordCount");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		Broadcast<String> broadCastVariable = sc.broadcast("hello");


		/*List<String> data = Arrays.asList("1947 33","1948 35","1950 41","1947 45","1948 56","1950 46");
		JavaRDD<String> distData = sc.parallelize(data);*/
		
//		JavaPairRDD<Long,String> distData =  sc.sequenceFile("/user/cloudera/data1/",Long.class,String.class);
		
		JavaPairRDD<String,String> distData = sc.wholeTextFiles("/user/cloudera/data");
		
	/*	System.out.println("@@@@@@@@@@@@@@   @@@@@@@  @@@@@@@@@@@@@@");
		List<Tuple2<Long, String>> listMap =  sc.sequenceFile("/user/cloudera/data1/",Long.class,String.class)
				.map(
						(record) 
						->
						{
							System.out.println("@@@@@@@@@@@@@@   @@@@@@@  @@@@@@@@@@@@@@"+record);
							System.out.println("@@@@@@@@@@@@@@   @@@@@@@  @@@@@@@@@@@@@@"+record._1);
							System.out.println("@@@@@@@@@@@@@@   @@@@@@@  @@@@@@@@@@@@@@"+record._2);
							return record;
							
						}).collect();
						;
						
						System.out.println("@@@@@@@@@@@@@@   @@@@@@@  @@@@@@@@@@@@@@");
						System.out.println("@@@@@@@@@@@@@@   @@@@@@@  @@@@@@@@@@@@@@");
						System.out.println("@@@@@@@@@@@@@@   @@@@@@@  @@@@@@@@@@@@@@"+"listMap"+listMap.listIterator().next()._2);
						System.out.println("@@@@@@@@@@@@@@   @@@@@@@  @@@@@@@@@@@@@@"+"listMap"+listMap.listIterator().next()._1);
		*/
						
						/*JavaRDD<Integer> lineLengths = distData.map(s -> s.length());*/
		/*int totalLength = lineLengths.reduce((a, b) -> a + b);*/

		//		System.out.println(" the total length is : "+totalLength);
		// map phase 
		System.out.println("######### MAP PHASE ##############");
		System.out.println("#######################");

		// mapToPair function is run on a single rdd to make a paired rdd which isn't going to be used 
		// in this code base 
		/*JavaPairRDD<String, Integer> pairs = distData.mapToPair(
				record
				-> 
				{
					String[] dataArray = record.split(" ");
					return new Tuple2(dataArray[0], computeIntegerMapAmplify(Integer.parseInt(dataArray[1])));
				}
				);*/
		System.out.println("#######################");
		//		System.out.println("printing the created key value pairs , map phase output , printing the pairedRDD : "+pairs);
		System.out.println("#######################");
		
//		commented
		distData.foreach(record -> System.out.println("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^"+record));


		System.out.println("#######################");
		System.out.println(" Transforming the pairedRDD considering the choice of keys and values , considering the query or the question you want to answer .  ");
		System.out.println(" key is going to be the year , value is going to be the temperature readings for that year");
		System.out.println("#######################");


		System.out.println("######## REDUCE PHASE ###############");
		System.out.println(" Now going to run reduceByKey , which will aggregate the values corresponding to the keys from across the machines in the cluster , and then will run the aggregation logic as per the lambda function on the transformed pairedRDD ");
		System.out.println("#######################");
		// reduce phase 
//		commented
		distData.map(
				(record) 
				->
				{
					System.out.println("@@@@@@@@@@@@@@ key metadata of file    @@@@@@@  @@@@@@@@@@@@@@"+record._1);
					System.out.println("@@@@@@@@@@@@@@ content of the file   @@@@@@@  @@@@@@@@@@@@@@"+record._2);
					processOnMapViaThirdPartyLibraryCall(record._2,broadCastVariable);
					return ("@@@@@@@@@@@@@@   @@@@@@@  @@@@@@@@@@@@@@"+record._1+"@@@@@@@@@@@@@@@@@@"
							+record);
				}
				).collect();

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
	
	public static void processOnMapViaThirdPartyLibraryCall(String value,Broadcast<String>  variableValue) throws FileNotFoundException, XQException{
		/*value = value * 1000;
		System.out.println("*************************************"+value);
		xQueryParser.execute();
		System.
		return value ; */
		/*persist it in present working directory   . 
		 * System.getProperty("user.dir"));
		load the xquery in executor file / jar list 
		load it and give path of directory where xml got stored 
		run xquery on xml */
		
		String path = System.getProperty("user.dir");
		path = path + Math.random()+variableValue.getValue().toString()+".txt";
		try{
			  // Create file 
			  FileWriter fstream = new FileWriter(path);
			  BufferedWriter out = new BufferedWriter(fstream);
			  out.write(value);
			  //Close the output stream
			  out.close();
			  System.out.println("@@@@@@@@@@@@@@@ file written to executor working direcotry at path : @@@@@@@"+path);
			  }catch (Exception e){//Catch exception if any
			  System.err.println("Error: file couldn't be written  " + e.getMessage());
			  }
		
	}



	/*
	 * 
######### MAP PHASE ##############
#######################
#######################
#######################
21/02/28 15:53:00 INFO input.FileInputFormat: Total input paths to process : 2
21/02/28 15:53:00 INFO input.FileInputFormat: Total input paths to process : 2
21/02/28 15:53:00 INFO input.CombineFileInputFormat: DEBUG: Terminated node allocation with : CompletedNodes: 1, size left: 0
21/02/28 15:53:00 INFO spark.SparkContext: Starting job: foreach at SparkCoreTemperatureCountWithFunctionMapAndReduceFromSequenceFile.java:96
21/02/28 15:53:00 INFO scheduler.DAGScheduler: Got job 0 (foreach at SparkCoreTemperatureCountWithFunctionMapAndReduceFromSequenceFile.java:96) with 2 output partitions
21/02/28 15:53:00 INFO scheduler.DAGScheduler: Final stage: ResultStage 0 (foreach at SparkCoreTemperatureCountWithFunctionMapAndReduceFromSequenceFile.java:96)
21/02/28 15:53:00 INFO scheduler.DAGScheduler: Parents of final stage: List()
21/02/28 15:53:00 INFO scheduler.DAGScheduler: Missing parents: List()
21/02/28 15:53:00 INFO scheduler.DAGScheduler: Submitting ResultStage 0 (/user/cloudera/data MapPartitionsRDD[1] at wholeTextFiles at SparkCoreTemperatureCountWithFunctionMapAndReduceFromSequenceFile.java:51), which has no missing parents
21/02/28 15:53:00 INFO storage.MemoryStore: Block broadcast_1 stored as values in memory (estimated size 3.4 KB, free 529.7 MB)
21/02/28 15:53:00 INFO storage.MemoryStore: Block broadcast_1_piece0 stored as bytes in memory (estimated size 2009.0 B, free 529.7 MB)
21/02/28 15:53:00 INFO storage.BlockManagerInfo: Added broadcast_1_piece0 in memory on localhost:59614 (size: 2009.0 B, free: 530.0 MB)
21/02/28 15:53:00 INFO spark.SparkContext: Created broadcast 1 from broadcast at DAGScheduler.scala:1004
21/02/28 15:53:00 INFO scheduler.DAGScheduler: Submitting 2 missing tasks from ResultStage 0 (/user/cloudera/data MapPartitionsRDD[1] at wholeTextFiles at SparkCoreTemperatureCountWithFunctionMapAndReduceFromSequenceFile.java:51) (first 15 tasks are for partitions Vector(0, 1))
21/02/28 15:53:00 INFO scheduler.TaskSchedulerImpl: Adding task set 0.0 with 2 tasks
21/02/28 15:53:00 INFO scheduler.TaskSetManager: Starting task 0.0 in stage 0.0 (TID 0, localhost, executor driver, partition 0, ANY, 2560 bytes)
21/02/28 15:53:00 INFO scheduler.TaskSetManager: Starting task 1.0 in stage 0.0 (TID 1, localhost, executor driver, partition 1, ANY, 2560 bytes)
21/02/28 15:53:00 INFO executor.Executor: Running task 1.0 in stage 0.0 (TID 1)
21/02/28 15:53:00 INFO executor.Executor: Running task 0.0 in stage 0.0 (TID 0)
21/02/28 15:53:00 INFO executor.Executor: Fetching spark://192.168.234.128:47161/jars/jline-2.9.jar with timestamp 1614556376009
21/02/28 15:53:00 INFO util.Utils: Fetching spark://192.168.234.128:47161/jars/jline-2.9.jar to /tmp/spark-e9d72801-5025-4357-ac16-8a3f5f266997/userFiles-e3d92660-e717-4500-a9ff-4bdbaf865504/fetchFileTemp5242230127715150286.tmp
21/02/28 15:53:01 INFO executor.Executor: Adding file:/tmp/spark-e9d72801-5025-4357-ac16-8a3f5f266997/userFiles-e3d92660-e717-4500-a9ff-4bdbaf865504/jline-2.9.jar to class loader
21/02/28 15:53:01 INFO executor.Executor: Fetching spark://192.168.234.128:47161/jars/saxon-he-10.3.jar with timestamp 1614556376011
21/02/28 15:53:01 INFO util.Utils: Fetching spark://192.168.234.128:47161/jars/saxon-he-10.3.jar to /tmp/spark-e9d72801-5025-4357-ac16-8a3f5f266997/userFiles-e3d92660-e717-4500-a9ff-4bdbaf865504/fetchFileTemp6365655538259503676.tmp
21/02/28 15:53:01 INFO executor.Executor: Adding file:/tmp/spark-e9d72801-5025-4357-ac16-8a3f5f266997/userFiles-e3d92660-e717-4500-a9ff-4bdbaf865504/saxon-he-10.3.jar to class loader
21/02/28 15:53:01 INFO executor.Executor: Fetching spark://192.168.234.128:47161/jars/saxon-xqj-10.3.jar with timestamp 1614556376012
21/02/28 15:53:01 INFO util.Utils: Fetching spark://192.168.234.128:47161/jars/saxon-xqj-10.3.jar to /tmp/spark-e9d72801-5025-4357-ac16-8a3f5f266997/userFiles-e3d92660-e717-4500-a9ff-4bdbaf865504/fetchFileTemp4701398499264839725.tmp
21/02/28 15:53:01 INFO executor.Executor: Adding file:/tmp/spark-e9d72801-5025-4357-ac16-8a3f5f266997/userFiles-e3d92660-e717-4500-a9ff-4bdbaf865504/saxon-xqj-10.3.jar to class loader
21/02/28 15:53:01 INFO executor.Executor: Fetching spark://192.168.234.128:47161/jars/TemperatureSparkCountWithFunction.jar with timestamp 1614556376013
21/02/28 15:53:01 INFO util.Utils: Fetching spark://192.168.234.128:47161/jars/TemperatureSparkCountWithFunction.jar to /tmp/spark-e9d72801-5025-4357-ac16-8a3f5f266997/userFiles-e3d92660-e717-4500-a9ff-4bdbaf865504/fetchFileTemp4851234941445414709.tmp
21/02/28 15:53:01 INFO executor.Executor: Adding file:/tmp/spark-e9d72801-5025-4357-ac16-8a3f5f266997/userFiles-e3d92660-e717-4500-a9ff-4bdbaf865504/TemperatureSparkCountWithFunction.jar to class loader
21/02/28 15:53:01 INFO executor.Executor: Fetching spark://192.168.234.128:47161/jars/saxon-he-test-10.3.jar with timestamp 1614556376011
21/02/28 15:53:01 INFO util.Utils: Fetching spark://192.168.234.128:47161/jars/saxon-he-test-10.3.jar to /tmp/spark-e9d72801-5025-4357-ac16-8a3f5f266997/userFiles-e3d92660-e717-4500-a9ff-4bdbaf865504/fetchFileTemp7105965898662889679.tmp
21/02/28 15:53:01 INFO executor.Executor: Adding file:/tmp/spark-e9d72801-5025-4357-ac16-8a3f5f266997/userFiles-e3d92660-e717-4500-a9ff-4bdbaf865504/saxon-he-test-10.3.jar to class loader
21/02/28 15:53:01 INFO rdd.WholeTextFileRDD: Input split: Paths:/user/cloudera/data/data1.txt:0+48
21/02/28 15:53:01 INFO rdd.WholeTextFileRDD: Input split: Paths:/user/cloudera/data/data2.txt:0+48
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^(hdfs://quickstart.cloudera:8020/user/cloudera/data/data2.txt,1967 33
1968 35
1960 41
1967 45
1971 56
1975 46
)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^(hdfs://quickstart.cloudera:8020/user/cloudera/data/data1.txt,1947 33
1948 35
1950 41
1947 45
1948 56
1950 46
)
21/02/28 15:53:02 INFO executor.Executor: Finished task 1.0 in stage 0.0 (TID 1). 2044 bytes result sent to driver
21/02/28 15:53:02 INFO executor.Executor: Finished task 0.0 in stage 0.0 (TID 0). 2044 bytes result sent to driver
21/02/28 15:53:02 INFO scheduler.TaskSetManager: Finished task 1.0 in stage 0.0 (TID 1) in 1322 ms on localhost (executor driver) (1/2)
21/02/28 15:53:02 INFO scheduler.TaskSetManager: Finished task 0.0 in stage 0.0 (TID 0) in 1391 ms on localhost (executor driver) (2/2)
21/02/28 15:53:02 INFO scheduler.DAGScheduler: ResultStage 0 (foreach at SparkCoreTemperatureCountWithFunctionMapAndReduceFromSequenceFile.java:96) finished in 1.419 s
21/02/28 15:53:02 INFO scheduler.TaskSchedulerImpl: Removed TaskSet 0.0, whose tasks have all completed, from pool 
21/02/28 15:53:02 INFO scheduler.DAGScheduler: Job 0 finished: foreach at SparkCoreTemperatureCountWithFunctionMapAndReduceFromSequenceFile.java:96, took 1.676730 s
#######################
 Transforming the pairedRDD considering the choice of keys and values , considering the query or the question you want to answer .  
 key is going to be the year , value is going to be the temperature readings for that year
#######################
######## REDUCE PHASE ###############
 Now going to run reduceByKey , which will aggregate the values corresponding to the keys from across the machines in the cluster , and then will run the aggregation logic as per the lambda function on the transformed pairedRDD 
#######################
21/02/28 15:53:02 INFO spark.SparkContext: Starting job: collect at SparkCoreTemperatureCountWithFunctionMapAndReduceFromSequenceFile.java:119
21/02/28 15:53:02 INFO scheduler.DAGScheduler: Got job 1 (collect at SparkCoreTemperatureCountWithFunctionMapAndReduceFromSequenceFile.java:119) with 2 output partitions
21/02/28 15:53:02 INFO scheduler.DAGScheduler: Final stage: ResultStage 1 (collect at SparkCoreTemperatureCountWithFunctionMapAndReduceFromSequenceFile.java:119)
21/02/28 15:53:02 INFO scheduler.DAGScheduler: Parents of final stage: List()
21/02/28 15:53:02 INFO scheduler.DAGScheduler: Missing parents: List()
21/02/28 15:53:02 INFO scheduler.DAGScheduler: Submitting ResultStage 1 (MapPartitionsRDD[2] at map at SparkCoreTemperatureCountWithFunctionMapAndReduceFromSequenceFile.java:110), which has no missing parents
21/02/28 15:53:02 INFO storage.MemoryStore: Block broadcast_2 stored as values in memory (estimated size 3.9 KB, free 529.7 MB)
21/02/28 15:53:02 INFO storage.MemoryStore: Block broadcast_2_piece0 stored as bytes in memory (estimated size 2.2 KB, free 529.7 MB)
21/02/28 15:53:02 INFO storage.BlockManagerInfo: Added broadcast_2_piece0 in memory on localhost:59614 (size: 2.2 KB, free: 530.0 MB)
21/02/28 15:53:02 INFO spark.SparkContext: Created broadcast 2 from broadcast at DAGScheduler.scala:1004
21/02/28 15:53:02 INFO scheduler.DAGScheduler: Submitting 2 missing tasks from ResultStage 1 (MapPartitionsRDD[2] at map at SparkCoreTemperatureCountWithFunctionMapAndReduceFromSequenceFile.java:110) (first 15 tasks are for partitions Vector(0, 1))
21/02/28 15:53:02 INFO scheduler.TaskSchedulerImpl: Adding task set 1.0 with 2 tasks
21/02/28 15:53:02 INFO scheduler.TaskSetManager: Starting task 0.0 in stage 1.0 (TID 2, localhost, executor driver, partition 0, ANY, 2560 bytes)
21/02/28 15:53:02 INFO scheduler.TaskSetManager: Starting task 1.0 in stage 1.0 (TID 3, localhost, executor driver, partition 1, ANY, 2560 bytes)
21/02/28 15:53:02 INFO executor.Executor: Running task 0.0 in stage 1.0 (TID 2)
21/02/28 15:53:02 INFO spark.ContextCleaner: Cleaned accumulator 1
21/02/28 15:53:02 INFO executor.Executor: Running task 1.0 in stage 1.0 (TID 3)
21/02/28 15:53:02 INFO rdd.WholeTextFileRDD: Input split: Paths:/user/cloudera/data/data2.txt:0+48
21/02/28 15:53:02 INFO rdd.WholeTextFileRDD: Input split: Paths:/user/cloudera/data/data1.txt:0+48
21/02/28 15:53:02 INFO storage.BlockManagerInfo: Removed broadcast_1_piece0 on localhost:59614 in memory (size: 2009.0 B, free: 530.0 MB)
@@@@@@@@@@@@@@ key metadata of file    @@@@@@@  @@@@@@@@@@@@@@hdfs://quickstart.cloudera:8020/user/cloudera/data/data1.txt
@@@@@@@@@@@@@@ content of the file   @@@@@@@  @@@@@@@@@@@@@@1947 33
1948 35
1950 41
1947 45
1948 56
1950 46

21/02/28 15:53:02 INFO executor.Executor: Finished task 0.0 in stage 1.0 (TID 2). 2279 bytes result sent to driver
21/02/28 15:53:02 INFO scheduler.TaskSetManager: Finished task 0.0 in stage 1.0 (TID 2) in 183 ms on localhost (executor driver) (1/2)
@@@@@@@@@@@@@@ key metadata of file    @@@@@@@  @@@@@@@@@@@@@@hdfs://quickstart.cloudera:8020/user/cloudera/data/data2.txt
@@@@@@@@@@@@@@ content of the file   @@@@@@@  @@@@@@@@@@@@@@1967 33
1968 35
1960 41
1967 45
1971 56
1975 46



	 */



}
