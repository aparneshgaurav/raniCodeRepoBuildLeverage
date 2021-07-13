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
public class SparkCoreTemperatureCountThirdPartySequenceBroadCastExecutorMemOps {
	// spark-submit --class com.poc.rcm.sparkcore.SparkCoreWordCount /home/cloudera/dev/SparkCore.jar
	//	spark-submit --class com.poc.rcm.sparkcore.SparkCoreTemperatureCount /home/cloudera/dev/practice/TemperatureSparkCount.jar 
	// spark-submit --class com.poc.rcm.sparkcore.SparkCoreTemperatureCountWithFunctionMap /home/cloudera/dev/practice/TemperatureSparkCountWithFunction.jar

	// command to run with data from file and also command to make a third party call 
	// via map or reduce method ( map means on each row ) and reduce means aggregation 
	// across records 
	/*spark-submit --class com.poc.rcm.sparkcore.SparkCoreTemperatureCountThirdPartySequenceBroadCastExecutorMemOps --jars /home/cloudera/dev/practice/SaxonHE10-3J/jline-2.9.jar,/home/cloudera/dev/practice/SaxonHE10-3J/saxon-he-10.3.jar,/home/cloudera/dev/practice/SaxonHE10-3J/saxon-he-test-10.3.jar,/home/cloudera/dev/practice/SaxonHE10-3J/saxon-xqj-10.3.jar /home/cloudera/dev/practice/TemperatureSparkCountWithFunction.jar
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
		
//		Broadcast<String> broadCastVariable = sc.broadcast("for $x in doc(\"/home/cloudera/dev/practice/books.xml\")/books/book where $x/price>30 return $x/title");
		Broadcast<String> broadCastVariable = sc.broadcast("for $x in doc(xQueryFilePath)/books/book where $x/price>30 return $x/title");


		/*List<String> data = Arrays.asList("1947 33","1948 35","1950 41","1947 45","1948 56","1950 46");
		JavaRDD<String> distData = sc.parallelize(data);*/
		
//		JavaPairRDD<Long,String> distData =  sc.sequenceFile("/user/cloudera/data1/",Long.class,String.class);
		// no use of hadoop sequence file generation step 
		// have all the xmls in hadoop / distributed file systems . 
		JavaPairRDD<String,String> distData = sc.wholeTextFiles("/user/cloudera/xmlNonSeq");
		
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
		
		String directoryPath = System.getProperty("user.dir");
		String xmlPath = directoryPath + "xmlFile"+Math.random()+".xml";
		String contentOfXQueryFile = variableValue.getValue().toString();
		contentOfXQueryFile = contentOfXQueryFile.replace("xQueryFilePath", '"'+xmlPath+'"');
		String xQueryFilePath = directoryPath+"xQueryFile"+".txt";
		
		// persisting xQuery file , yes this file would get created every time 
		try{
			  // Create file 
			  FileWriter fstream = new FileWriter(xQueryFilePath);
			  BufferedWriter out = new BufferedWriter(fstream);
			  out.write(contentOfXQueryFile);
			  //Close the output stream
			  out.close();
			  System.out.println("@@@@@@@@@@@@@@@ file written to executor working direcotry at path : @@@@@@@"+xQueryFilePath);
			  }catch (Exception e){//Catch exception if any
			  System.err.println("Error: xquery file couldn't be written  " + e.getMessage());
			  }
		
		// persisting record which is an xml file itself 
		try{
			  // Create file 
			  FileWriter fstream1 = new FileWriter(xmlPath);
			  BufferedWriter out1 = new BufferedWriter(fstream1);
			  out1.write(value);
			  //Close the output stream
			  out1.close();
			  System.out.println("@@@@@@@@@@@@@@@ file written to executor working direcotry at path : @@@@@@@"+xmlPath);
			  }catch (Exception e){//Catch exception if any
			  System.err.println("Error: file couldn't be written  " + e.getMessage());
			  }
		
		// calling the execute method with files as parameter 
		xQueryParser.executeWithXQueryFilePath(xQueryFilePath);
		
	}



	/*
[cloudera@quickstart practice]$ spark-submit --class com.poc.rcm.sparkcore.SparkCoreTemperatureCountThirdPartySequenceBroadCastExecutorMemOps --jars /home/cloudera/dev/practice/SaxonHE10-3J/jline-2.9.jar,/home/cloudera/dev/practice/SaxonHE10-3J/saxon-he-10.3.jar,/home/cloudera/dev/practice/SaxonHE10-3J/saxon-he-test-10.3.jar,/home/cloudera/dev/practice/SaxonHE10-3J/saxon-xqj-10.3.jar /home/cloudera/dev/practice/TemperatureSparkCountWithFunction.jar
SLF4J: Class path contains multiple SLF4J bindings.
SLF4J: Found binding in [jar:file:/usr/lib/zookeeper/lib/slf4j-log4j12-1.7.5.jar!/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: Found binding in [jar:file:/usr/lib/flume-ng/lib/slf4j-log4j12-1.7.5.jar!/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: Found binding in [jar:file:/usr/lib/parquet/lib/slf4j-log4j12-1.7.5.jar!/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: Found binding in [jar:file:/usr/lib/avro/avro-tools-1.7.6-cdh5.13.0.jar!/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: See http://www.slf4j.org/codes.html#multiple_bindings for an explanation.
SLF4J: Actual binding is of type [org.slf4j.impl.Log4jLoggerFactory]
21/03/01 15:20:25 INFO spark.SparkContext: Running Spark version 1.6.0
21/03/01 15:20:26 WARN util.NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
21/03/01 15:20:27 INFO spark.SecurityManager: Changing view acls to: cloudera
21/03/01 15:20:27 INFO spark.SecurityManager: Changing modify acls to: cloudera
21/03/01 15:20:27 INFO spark.SecurityManager: SecurityManager: authentication disabled; ui acls disabled; users with view permissions: Set(cloudera); users with modify permissions: Set(cloudera)
21/03/01 15:20:27 INFO util.Utils: Successfully started service 'sparkDriver' on port 48091.
21/03/01 15:20:28 INFO slf4j.Slf4jLogger: Slf4jLogger started
21/03/01 15:20:28 INFO Remoting: Starting remoting
21/03/01 15:20:29 INFO Remoting: Remoting started; listening on addresses :[akka.tcp://sparkDriverActorSystem@192.168.234.128:55471]
21/03/01 15:20:29 INFO Remoting: Remoting now listens on addresses: [akka.tcp://sparkDriverActorSystem@192.168.234.128:55471]
21/03/01 15:20:29 INFO util.Utils: Successfully started service 'sparkDriverActorSystem' on port 55471.
21/03/01 15:20:29 INFO spark.SparkEnv: Registering MapOutputTracker
21/03/01 15:20:29 INFO spark.SparkEnv: Registering BlockManagerMaster
21/03/01 15:20:29 INFO storage.DiskBlockManager: Created local directory at /tmp/blockmgr-07df0a53-32bc-45a4-9846-b14a00a33954
21/03/01 15:20:29 INFO storage.MemoryStore: MemoryStore started with capacity 530.0 MB
21/03/01 15:20:29 INFO spark.SparkEnv: Registering OutputCommitCoordinator
21/03/01 15:20:30 INFO server.Server: jetty-8.y.z-SNAPSHOT
21/03/01 15:20:30 INFO server.AbstractConnector: Started SelectChannelConnector@0.0.0.0:4040
21/03/01 15:20:30 INFO util.Utils: Successfully started service 'SparkUI' on port 4040.
21/03/01 15:20:30 INFO ui.SparkUI: Started SparkUI at http://192.168.234.128:4040
21/03/01 15:20:30 INFO spark.SparkContext: Added JAR file:/home/cloudera/dev/practice/SaxonHE10-3J/jline-2.9.jar at spark://192.168.234.128:48091/jars/jline-2.9.jar with timestamp 1614640830342
21/03/01 15:20:30 INFO spark.SparkContext: Added JAR file:/home/cloudera/dev/practice/SaxonHE10-3J/saxon-he-10.3.jar at spark://192.168.234.128:48091/jars/saxon-he-10.3.jar with timestamp 1614640830345
21/03/01 15:20:30 INFO spark.SparkContext: Added JAR file:/home/cloudera/dev/practice/SaxonHE10-3J/saxon-he-test-10.3.jar at spark://192.168.234.128:48091/jars/saxon-he-test-10.3.jar with timestamp 1614640830346
21/03/01 15:20:30 INFO spark.SparkContext: Added JAR file:/home/cloudera/dev/practice/SaxonHE10-3J/saxon-xqj-10.3.jar at spark://192.168.234.128:48091/jars/saxon-xqj-10.3.jar with timestamp 1614640830348
21/03/01 15:20:30 INFO spark.SparkContext: Added JAR file:/home/cloudera/dev/practice/TemperatureSparkCountWithFunction.jar at spark://192.168.234.128:48091/jars/TemperatureSparkCountWithFunction.jar with timestamp 1614640830349
21/03/01 15:20:30 INFO executor.Executor: Starting executor ID driver on host localhost
21/03/01 15:20:30 INFO util.Utils: Successfully started service 'org.apache.spark.network.netty.NettyBlockTransferService' on port 44489.
21/03/01 15:20:30 INFO netty.NettyBlockTransferService: Server created on 44489
21/03/01 15:20:30 INFO storage.BlockManagerMaster: Trying to register BlockManager
21/03/01 15:20:30 INFO storage.BlockManagerMasterEndpoint: Registering block manager localhost:44489 with 530.0 MB RAM, BlockManagerId(driver, localhost, 44489)
21/03/01 15:20:30 INFO storage.BlockManagerMaster: Registered BlockManager
21/03/01 15:20:31 INFO storage.MemoryStore: Block broadcast_0 stored as values in memory (estimated size 216.0 B, free 530.0 MB)
21/03/01 15:20:31 INFO storage.MemoryStore: Block broadcast_0_piece0 stored as bytes in memory (estimated size 102.0 B, free 530.0 MB)
21/03/01 15:20:31 INFO storage.BlockManagerInfo: Added broadcast_0_piece0 in memory on localhost:44489 (size: 102.0 B, free: 530.0 MB)
21/03/01 15:20:31 INFO spark.SparkContext: Created broadcast 0 from broadcast at SparkCoreTemperatureCountThirdPartySequenceBroadCastExecutorMemOps.java:64
21/03/01 15:20:33 WARN shortcircuit.DomainSocketFactory: The short-circuit local reads feature cannot be used because libhadoop cannot be loaded.
21/03/01 15:20:33 INFO storage.MemoryStore: Block broadcast_1 stored as values in memory (estimated size 297.1 KB, free 529.7 MB)
21/03/01 15:20:34 INFO storage.MemoryStore: Block broadcast_1_piece0 stored as bytes in memory (estimated size 24.6 KB, free 529.7 MB)
21/03/01 15:20:34 INFO storage.BlockManagerInfo: Added broadcast_1_piece0 in memory on localhost:44489 (size: 24.6 KB, free: 530.0 MB)
21/03/01 15:20:34 INFO spark.SparkContext: Created broadcast 1 from wholeTextFiles at SparkCoreTemperatureCountThirdPartySequenceBroadCastExecutorMemOps.java:72
######### MAP PHASE ##############
#######################
#######################
#######################
21/03/01 15:20:34 INFO input.FileInputFormat: Total input paths to process : 3
21/03/01 15:20:34 INFO input.FileInputFormat: Total input paths to process : 3
21/03/01 15:20:34 INFO input.CombineFileInputFormat: DEBUG: Terminated node allocation with : CompletedNodes: 1, size left: 738
21/03/01 15:20:34 INFO spark.SparkContext: Starting job: foreach at SparkCoreTemperatureCountThirdPartySequenceBroadCastExecutorMemOps.java:117
21/03/01 15:20:34 INFO scheduler.DAGScheduler: Got job 0 (foreach at SparkCoreTemperatureCountThirdPartySequenceBroadCastExecutorMemOps.java:117) with 2 output partitions
21/03/01 15:20:34 INFO scheduler.DAGScheduler: Final stage: ResultStage 0 (foreach at SparkCoreTemperatureCountThirdPartySequenceBroadCastExecutorMemOps.java:117)
21/03/01 15:20:34 INFO scheduler.DAGScheduler: Parents of final stage: List()
21/03/01 15:20:34 INFO scheduler.DAGScheduler: Missing parents: List()
21/03/01 15:20:34 INFO scheduler.DAGScheduler: Submitting ResultStage 0 (/user/cloudera/xmlNonSeq MapPartitionsRDD[1] at wholeTextFiles at SparkCoreTemperatureCountThirdPartySequenceBroadCastExecutorMemOps.java:72), which has no missing parents
21/03/01 15:20:34 INFO storage.MemoryStore: Block broadcast_2 stored as values in memory (estimated size 3.4 KB, free 529.7 MB)
21/03/01 15:20:34 INFO storage.MemoryStore: Block broadcast_2_piece0 stored as bytes in memory (estimated size 2021.0 B, free 529.7 MB)
21/03/01 15:20:34 INFO storage.BlockManagerInfo: Added broadcast_2_piece0 in memory on localhost:44489 (size: 2021.0 B, free: 530.0 MB)
21/03/01 15:20:34 INFO spark.SparkContext: Created broadcast 2 from broadcast at DAGScheduler.scala:1004
21/03/01 15:20:34 INFO scheduler.DAGScheduler: Submitting 2 missing tasks from ResultStage 0 (/user/cloudera/xmlNonSeq MapPartitionsRDD[1] at wholeTextFiles at SparkCoreTemperatureCountThirdPartySequenceBroadCastExecutorMemOps.java:72) (first 15 tasks are for partitions Vector(0, 1))
21/03/01 15:20:34 INFO scheduler.TaskSchedulerImpl: Adding task set 0.0 with 2 tasks
21/03/01 15:20:35 INFO scheduler.TaskSetManager: Starting task 1.0 in stage 0.0 (TID 0, localhost, executor driver, partition 1, PROCESS_LOCAL, 2566 bytes)
21/03/01 15:20:35 INFO scheduler.TaskSetManager: Starting task 0.0 in stage 0.0 (TID 1, localhost, executor driver, partition 0, ANY, 2651 bytes)
21/03/01 15:20:35 INFO executor.Executor: Running task 1.0 in stage 0.0 (TID 0)
21/03/01 15:20:35 INFO executor.Executor: Running task 0.0 in stage 0.0 (TID 1)
21/03/01 15:20:35 INFO executor.Executor: Fetching spark://192.168.234.128:48091/jars/jline-2.9.jar with timestamp 1614640830342
21/03/01 15:20:35 INFO util.Utils: Fetching spark://192.168.234.128:48091/jars/jline-2.9.jar to /tmp/spark-8939af7c-4858-482d-9a7d-d41e1e1bc577/userFiles-17b3741a-f682-42ae-b72e-951cd5428224/fetchFileTemp3209670940136046100.tmp
21/03/01 15:20:36 INFO executor.Executor: Adding file:/tmp/spark-8939af7c-4858-482d-9a7d-d41e1e1bc577/userFiles-17b3741a-f682-42ae-b72e-951cd5428224/jline-2.9.jar to class loader
21/03/01 15:20:36 INFO executor.Executor: Fetching spark://192.168.234.128:48091/jars/saxon-he-10.3.jar with timestamp 1614640830345
21/03/01 15:20:36 INFO util.Utils: Fetching spark://192.168.234.128:48091/jars/saxon-he-10.3.jar to /tmp/spark-8939af7c-4858-482d-9a7d-d41e1e1bc577/userFiles-17b3741a-f682-42ae-b72e-951cd5428224/fetchFileTemp2556961464187020298.tmp
21/03/01 15:20:36 INFO executor.Executor: Adding file:/tmp/spark-8939af7c-4858-482d-9a7d-d41e1e1bc577/userFiles-17b3741a-f682-42ae-b72e-951cd5428224/saxon-he-10.3.jar to class loader
21/03/01 15:20:36 INFO executor.Executor: Fetching spark://192.168.234.128:48091/jars/saxon-he-test-10.3.jar with timestamp 1614640830346
21/03/01 15:20:36 INFO util.Utils: Fetching spark://192.168.234.128:48091/jars/saxon-he-test-10.3.jar to /tmp/spark-8939af7c-4858-482d-9a7d-d41e1e1bc577/userFiles-17b3741a-f682-42ae-b72e-951cd5428224/fetchFileTemp2726796252773849319.tmp
21/03/01 15:20:36 INFO executor.Executor: Adding file:/tmp/spark-8939af7c-4858-482d-9a7d-d41e1e1bc577/userFiles-17b3741a-f682-42ae-b72e-951cd5428224/saxon-he-test-10.3.jar to class loader
21/03/01 15:20:36 INFO executor.Executor: Fetching spark://192.168.234.128:48091/jars/TemperatureSparkCountWithFunction.jar with timestamp 1614640830349
21/03/01 15:20:36 INFO util.Utils: Fetching spark://192.168.234.128:48091/jars/TemperatureSparkCountWithFunction.jar to /tmp/spark-8939af7c-4858-482d-9a7d-d41e1e1bc577/userFiles-17b3741a-f682-42ae-b72e-951cd5428224/fetchFileTemp187866215084755617.tmp
21/03/01 15:20:36 INFO executor.Executor: Adding file:/tmp/spark-8939af7c-4858-482d-9a7d-d41e1e1bc577/userFiles-17b3741a-f682-42ae-b72e-951cd5428224/TemperatureSparkCountWithFunction.jar to class loader
21/03/01 15:20:36 INFO executor.Executor: Fetching spark://192.168.234.128:48091/jars/saxon-xqj-10.3.jar with timestamp 1614640830348
21/03/01 15:20:36 INFO util.Utils: Fetching spark://192.168.234.128:48091/jars/saxon-xqj-10.3.jar to /tmp/spark-8939af7c-4858-482d-9a7d-d41e1e1bc577/userFiles-17b3741a-f682-42ae-b72e-951cd5428224/fetchFileTemp7635813911297058529.tmp
21/03/01 15:20:36 INFO executor.Executor: Adding file:/tmp/spark-8939af7c-4858-482d-9a7d-d41e1e1bc577/userFiles-17b3741a-f682-42ae-b72e-951cd5428224/saxon-xqj-10.3.jar to class loader
21/03/01 15:20:36 INFO rdd.WholeTextFileRDD: Input split: Paths:/user/cloudera/xmlNonSeq/books.xml:0+797,/user/cloudera/xmlNonSeq/books1.xml:0+720
21/03/01 15:20:36 INFO rdd.WholeTextFileRDD: Input split: Paths:/user/cloudera/xmlNonSeq/books2.xml:0+738
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^(hdfs://quickstart.cloudera:8020/user/cloudera/xmlNonSeq/books2.xml,<?xml version="1.0" encoding="UTF-8"?>
<books>   <book category="JAVA"><title lang="en">Learn Java in 24 Hours</title><author>Robert</author><year>2005</year><price>30.00</price></book><book category="DOTNET"><title lang="en">Learn .Net in 24 hours</title><author>Peter</author><year>2011</year><price>40.50</price></book>
   
   <book category="XML">
      <title lang="en">Modified Learn XQuery in 24 hours</title>
      <author>Robert</author>
      <author>Peter</author> 
      <year>2013</year>
      <price>50.00</price>
   </book>
   
   <book category="XML">
      <title lang="en">Modified Learn XPath in 24 hours</title>
      <author>Jay Ban</author>
      <year>2010</year>
      <price>16.50</price>
   </book>
   
</books>
)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^(hdfs://quickstart.cloudera:8020/user/cloudera/xmlNonSeq/books.xml,<?xml version="1.0" encoding="UTF-8"?>
<books>
   
   <book category="JAVA">
      <title lang="en">Learn Java in 24 Hours</title>
      <author>Robert</author>
      <year>2005</year>
      <price>30.00</price>
   </book>
   
   <book category="DOTNET">
      <title lang="en">Learn .Net in 24 hours</title>
      <author>Peter</author>
      <year>2011</year>
      <price>40.50</price>
   </book>
   
   <book category="XML">
      <title lang="en">Learn XQuery in 24 hours</title>
      <author>Robert</author>
      <author>Peter</author> 
      <year>2013</year>
      <price>50.00</price>
   </book>
   
   <book category="XML">
      <title lang="en">Learn XPath in 24 hours</title>
      <author>Jay Ban</author>
      <year>2010</year>
      <price>16.50</price>
   </book>
   
</books>
)
21/03/01 15:20:37 INFO executor.Executor: Finished task 1.0 in stage 0.0 (TID 0). 2044 bytes result sent to driver
21/03/01 15:20:37 INFO scheduler.TaskSetManager: Finished task 1.0 in stage 0.0 (TID 0) in 2121 ms on localhost (executor driver) (1/2)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^(hdfs://quickstart.cloudera:8020/user/cloudera/xmlNonSeq/books1.xml,<?xml version="1.0" encoding="UTF-8"?>
<books>   <book category="JAVA"><title lang="en">Learn Java in 24 Hours</title><author>Robert</author><year>2005</year><price>30.00</price></book><book category="DOTNET"><title lang="en">Learn .Net in 24 hours</title><author>Peter</author><year>2011</year><price>40.50</price></book>
   
   <book category="XML">
      <title lang="en">Learn XQuery in 24 hours</title>
      <author>Robert</author>
      <author>Peter</author> 
      <year>2013</year>
      <price>50.00</price>
   </book>
   
   <book category="XML">
      <title lang="en">Learn XPath in 24 hours</title>
      <author>Jay Ban</author>
      <year>2010</year>
      <price>16.50</price>
   </book>
   
</books>
)
21/03/01 15:20:37 INFO executor.Executor: Finished task 0.0 in stage 0.0 (TID 1). 2044 bytes result sent to driver
21/03/01 15:20:37 INFO scheduler.DAGScheduler: ResultStage 0 (foreach at SparkCoreTemperatureCountThirdPartySequenceBroadCastExecutorMemOps.java:117) finished in 2.242 s
21/03/01 15:20:37 INFO scheduler.TaskSetManager: Finished task 0.0 in stage 0.0 (TID 1) in 2154 ms on localhost (executor driver) (2/2)
21/03/01 15:20:37 INFO scheduler.TaskSchedulerImpl: Removed TaskSet 0.0, whose tasks have all completed, from pool 
21/03/01 15:20:37 INFO scheduler.DAGScheduler: Job 0 finished: foreach at SparkCoreTemperatureCountThirdPartySequenceBroadCastExecutorMemOps.java:117, took 2.544404 s
#######################
 Transforming the pairedRDD considering the choice of keys and values , considering the query or the question you want to answer .  
 key is going to be the year , value is going to be the temperature readings for that year
#######################
######## REDUCE PHASE ###############
 Now going to run reduceByKey , which will aggregate the values corresponding to the keys from across the machines in the cluster , and then will run the aggregation logic as per the lambda function on the transformed pairedRDD 
#######################
21/03/01 15:20:37 INFO spark.SparkContext: Starting job: collect at SparkCoreTemperatureCountThirdPartySequenceBroadCastExecutorMemOps.java:141
21/03/01 15:20:37 INFO scheduler.DAGScheduler: Got job 1 (collect at SparkCoreTemperatureCountThirdPartySequenceBroadCastExecutorMemOps.java:141) with 2 output partitions
21/03/01 15:20:37 INFO scheduler.DAGScheduler: Final stage: ResultStage 1 (collect at SparkCoreTemperatureCountThirdPartySequenceBroadCastExecutorMemOps.java:141)
21/03/01 15:20:37 INFO scheduler.DAGScheduler: Parents of final stage: List()
21/03/01 15:20:37 INFO scheduler.DAGScheduler: Missing parents: List()
21/03/01 15:20:37 INFO scheduler.DAGScheduler: Submitting ResultStage 1 (MapPartitionsRDD[2] at map at SparkCoreTemperatureCountThirdPartySequenceBroadCastExecutorMemOps.java:131), which has no missing parents
21/03/01 15:20:37 INFO storage.MemoryStore: Block broadcast_3 stored as values in memory (estimated size 4.0 KB, free 529.7 MB)
21/03/01 15:20:37 INFO storage.MemoryStore: Block broadcast_3_piece0 stored as bytes in memory (estimated size 2.3 KB, free 529.7 MB)
21/03/01 15:20:37 INFO storage.BlockManagerInfo: Added broadcast_3_piece0 in memory on localhost:44489 (size: 2.3 KB, free: 530.0 MB)
21/03/01 15:20:37 INFO spark.SparkContext: Created broadcast 3 from broadcast at DAGScheduler.scala:1004
21/03/01 15:20:37 INFO scheduler.DAGScheduler: Submitting 2 missing tasks from ResultStage 1 (MapPartitionsRDD[2] at map at SparkCoreTemperatureCountThirdPartySequenceBroadCastExecutorMemOps.java:131) (first 15 tasks are for partitions Vector(0, 1))
21/03/01 15:20:37 INFO scheduler.TaskSchedulerImpl: Adding task set 1.0 with 2 tasks
21/03/01 15:20:37 INFO scheduler.TaskSetManager: Starting task 1.0 in stage 1.0 (TID 2, localhost, executor driver, partition 1, PROCESS_LOCAL, 2566 bytes)
21/03/01 15:20:37 INFO scheduler.TaskSetManager: Starting task 0.0 in stage 1.0 (TID 3, localhost, executor driver, partition 0, ANY, 2651 bytes)
21/03/01 15:20:37 INFO executor.Executor: Running task 0.0 in stage 1.0 (TID 3)
21/03/01 15:20:37 INFO executor.Executor: Running task 1.0 in stage 1.0 (TID 2)
21/03/01 15:20:37 INFO rdd.WholeTextFileRDD: Input split: Paths:/user/cloudera/xmlNonSeq/books.xml:0+797,/user/cloudera/xmlNonSeq/books1.xml:0+720
21/03/01 15:20:37 INFO rdd.WholeTextFileRDD: Input split: Paths:/user/cloudera/xmlNonSeq/books2.xml:0+738
@@@@@@@@@@@@@@ key metadata of file    @@@@@@@  @@@@@@@@@@@@@@hdfs://quickstart.cloudera:8020/user/cloudera/xmlNonSeq/books.xml
@@@@@@@@@@@@@@ content of the file   @@@@@@@  @@@@@@@@@@@@@@<?xml version="1.0" encoding="UTF-8"?>
<books>
   
   <book category="JAVA">
      <title lang="en">Learn Java in 24 Hours</title>
      <author>Robert</author>
      <year>2005</year>
      <price>30.00</price>
   </book>
   
   <book category="DOTNET">
      <title lang="en">Learn .Net in 24 hours</title>
      <author>Peter</author>
      <year>2011</year>
      <price>40.50</price>
   </book>
   
   <book category="XML">
      <title lang="en">Learn XQuery in 24 hours</title>
      <author>Robert</author>
      <author>Peter</author> 
      <year>2013</year>
      <price>50.00</price>
   </book>
   
   <book category="XML">
      <title lang="en">Learn XPath in 24 hours</title>
      <author>Jay Ban</author>
      <year>2010</year>
      <price>16.50</price>
   </book>
   
</books>

@@@@@@@@@@@@@@@ file written to executor working direcotry at path : @@@@@@@/home/cloudera/dev/practicexQueryFile.txt
@@@@@@@@@@@@@@@ file written to executor working direcotry at path : @@@@@@@/home/cloudera/dev/practicexmlFile0.6279881494664334.xml
@@@@@@@@@@@@@@ key metadata of file    @@@@@@@  @@@@@@@@@@@@@@hdfs://quickstart.cloudera:8020/user/cloudera/xmlNonSeq/books2.xml
@@@@@@@@@@@@@@ content of the file   @@@@@@@  @@@@@@@@@@@@@@<?xml version="1.0" encoding="UTF-8"?>
<books>   <book category="JAVA"><title lang="en">Learn Java in 24 Hours</title><author>Robert</author><year>2005</year><price>30.00</price></book><book category="DOTNET"><title lang="en">Learn .Net in 24 hours</title><author>Peter</author><year>2011</year><price>40.50</price></book>
   
   <book category="XML">
      <title lang="en">Modified Learn XQuery in 24 hours</title>
      <author>Robert</author>
      <author>Peter</author> 
      <year>2013</year>
      <price>50.00</price>
   </book>
   
   <book category="XML">
      <title lang="en">Modified Learn XPath in 24 hours</title>
      <author>Jay Ban</author>
      <year>2010</year>
      <price>16.50</price>
   </book>
   
</books>

@@@@@@@@@@@@@@@ file written to executor working direcotry at path : @@@@@@@/home/cloudera/dev/practicexQueryFile.txt
@@@@@@@@@@@@@@@ file written to executor working direcotry at path : @@@@@@@/home/cloudera/dev/practicexmlFile0.35382573939675466.xml
21/03/01 15:20:40 INFO spark.ContextCleaner: Cleaned accumulator 1
21/03/01 15:20:40 INFO storage.BlockManagerInfo: Removed broadcast_2_piece0 on localhost:44489 in memory (size: 2021.0 B, free: 530.0 MB)
$$$$$$$$$$$$ results  $$$$$$$$$$$$$$$$$$$$$$$$$
$$$$$$$$$$$$ results  $$$$$$$$$$$$$$$$$$$$$$$$$
$$$$$$$$$$$$ results  $$$$$$$$$$$$$$$$$$$$$$$$$
$$$$$$$$$$$$ results  $$$$$$$$$$$$$$$$$$$$$$$$$
$$$$$$$$$$$$ results  $$$$$$$$$$$$$$$$$$$$$$$$$
$$$$$$$$$$$$ results  $$$$$$$$$$$$$$$$$$$$$$$$$
$$$$$$$$$$$$ results  $$$$$$$$$$$$$$$$$$$$$$$$$
$$$$$$$$$$$$ results  $$$$$$$$$$$$$$$$$$$$$$$$$
<title lang="en">Learn .Net in 24 hours</title>

<title lang="en">Learn .Net in 24 hours</title>

$$$$$$$$$$$$ results  $$$$$$$$$$$$$$$$$$$$$$$$$
$$$$$$$$$$$$ results  $$$$$$$$$$$$$$$$$$$$$$$$$
$$$$$$$$$$$$ results  $$$$$$$$$$$$$$$$$$$$$$$$$
$$$$$$$$$$$$ results  $$$$$$$$$$$$$$$$$$$$$$$$$
$$$$$$$$$$$$ results  $$$$$$$$$$$$$$$$$$$$$$$$$
$$$$$$$$$$$$ results  $$$$$$$$$$$$$$$$$$$$$$$$$
$$$$$$$$$$$$ results  $$$$$$$$$$$$$$$$$$$$$$$$$
$$$$$$$$$$$$ results  $$$$$$$$$$$$$$$$$$$$$$$$$
<title lang="en">Modified Learn XQuery in 24 hours</title>

<title lang="en">Modified Learn XQuery in 24 hours</title>

21/03/01 15:20:40 INFO executor.Executor: Finished task 1.0 in stage 1.0 (TID 2). 2981 bytes result sent to driver
21/03/01 15:20:40 INFO scheduler.TaskSetManager: Finished task 1.0 in stage 1.0 (TID 2) in 3147 ms on localhost (executor driver) (1/2)
@@@@@@@@@@@@@@ key metadata of file    @@@@@@@  @@@@@@@@@@@@@@hdfs://quickstart.cloudera:8020/user/cloudera/xmlNonSeq/books1.xml
@@@@@@@@@@@@@@ content of the file   @@@@@@@  @@@@@@@@@@@@@@<?xml version="1.0" encoding="UTF-8"?>
<books>   <book category="JAVA"><title lang="en">Learn Java in 24 Hours</title><author>Robert</author><year>2005</year><price>30.00</price></book><book category="DOTNET"><title lang="en">Learn .Net in 24 hours</title><author>Peter</author><year>2011</year><price>40.50</price></book>
   
   <book category="XML">
      <title lang="en">Learn XQuery in 24 hours</title>
      <author>Robert</author>
      <author>Peter</author> 
      <year>2013</year>
      <price>50.00</price>
   </book>
   
   <book category="XML">
      <title lang="en">Learn XPath in 24 hours</title>
      <author>Jay Ban</author>
      <year>2010</year>
      <price>16.50</price>
   </book>
   
</books>

@@@@@@@@@@@@@@@ file written to executor working direcotry at path : @@@@@@@/home/cloudera/dev/practicexQueryFile.txt
@@@@@@@@@@@@@@@ file written to executor working direcotry at path : @@@@@@@/home/cloudera/dev/practicexmlFile0.49289190256650084.xml
$$$$$$$$$$$$ results  $$$$$$$$$$$$$$$$$$$$$$$$$
$$$$$$$$$$$$ results  $$$$$$$$$$$$$$$$$$$$$$$$$
$$$$$$$$$$$$ results  $$$$$$$$$$$$$$$$$$$$$$$$$
$$$$$$$$$$$$ results  $$$$$$$$$$$$$$$$$$$$$$$$$
<title lang="en">Learn .Net in 24 hours</title>

$$$$$$$$$$$$ results  $$$$$$$$$$$$$$$$$$$$$$$$$
$$$$$$$$$$$$ results  $$$$$$$$$$$$$$$$$$$$$$$$$
$$$$$$$$$$$$ results  $$$$$$$$$$$$$$$$$$$$$$$$$
$$$$$$$$$$$$ results  $$$$$$$$$$$$$$$$$$$$$$$$$
<title lang="en">Learn XQuery in 24 hours</title>

21/03/01 15:20:40 INFO executor.Executor: Finished task 0.0 in stage 1.0 (TID 3). 3959 bytes result sent to driver
21/03/01 15:20:40 INFO scheduler.TaskSetManager: Finished task 0.0 in stage 1.0 (TID 3) in 3287 ms on localhost (executor driver) (2/2)
21/03/01 15:20:40 INFO scheduler.DAGScheduler: ResultStage 1 (collect at SparkCoreTemperatureCountThirdPartySequenceBroadCastExecutorMemOps.java:141) finished in 3.290 s
21/03/01 15:20:40 INFO scheduler.TaskSchedulerImpl: Removed TaskSet 1.0, whose tasks have all completed, from pool 
21/03/01 15:20:40 INFO scheduler.DAGScheduler: Job 1 finished: collect at SparkCoreTemperatureCountThirdPartySequenceBroadCastExecutorMemOps.java:141, took 3.332404 s
21/03/01 15:20:40 INFO spark.SparkContext: Invoking stop() from shutdown hook
21/03/01 15:20:40 INFO handler.ContextHandler: stopped o.s.j.s.ServletContextHandler{/metrics/json,null}
21/03/01 15:20:40 INFO handler.ContextHandler: stopped o.s.j.s.ServletContextHandler{/stages/stage/kill,null}
21/03/01 15:20:40 INFO handler.ContextHandler: stopped o.s.j.s.ServletContextHandler{/api,null}
21/03/01 15:20:40 INFO handler.ContextHandler: stopped o.s.j.s.ServletContextHandler{/,null}
21/03/01 15:20:40 INFO handler.ContextHandler: stopped o.s.j.s.ServletContextHandler{/static,null}
21/03/01 15:20:40 INFO handler.ContextHandler: stopped o.s.j.s.ServletContextHandler{/executors/threadDump/json,null}
21/03/01 15:20:40 INFO handler.ContextHandler: stopped o.s.j.s.ServletContextHandler{/executors/threadDump,null}
21/03/01 15:20:40 INFO handler.ContextHandler: stopped o.s.j.s.ServletContextHandler{/executors/json,null}
21/03/01 15:20:40 INFO handler.ContextHandler: stopped o.s.j.s.ServletContextHandler{/executors,null}
21/03/01 15:20:40 INFO handler.ContextHandler: stopped o.s.j.s.ServletContextHandler{/environment/json,null}
21/03/01 15:20:40 INFO handler.ContextHandler: stopped o.s.j.s.ServletContextHandler{/environment,null}
21/03/01 15:20:40 INFO handler.ContextHandler: stopped o.s.j.s.ServletContextHandler{/storage/rdd/json,null}
21/03/01 15:20:40 INFO handler.ContextHandler: stopped o.s.j.s.ServletContextHandler{/storage/rdd,null}
21/03/01 15:20:40 INFO handler.ContextHandler: stopped o.s.j.s.ServletContextHandler{/storage/json,null}
21/03/01 15:20:40 INFO handler.ContextHandler: stopped o.s.j.s.ServletContextHandler{/storage,null}
21/03/01 15:20:40 INFO handler.ContextHandler: stopped o.s.j.s.ServletContextHandler{/stages/pool/json,null}
21/03/01 15:20:40 INFO handler.ContextHandler: stopped o.s.j.s.ServletContextHandler{/stages/pool,null}
21/03/01 15:20:40 INFO handler.ContextHandler: stopped o.s.j.s.ServletContextHandler{/stages/stage/json,null}
21/03/01 15:20:40 INFO handler.ContextHandler: stopped o.s.j.s.ServletContextHandler{/stages/stage,null}
21/03/01 15:20:40 INFO handler.ContextHandler: stopped o.s.j.s.ServletContextHandler{/stages/json,null}
21/03/01 15:20:40 INFO handler.ContextHandler: stopped o.s.j.s.ServletContextHandler{/stages,null}
21/03/01 15:20:40 INFO handler.ContextHandler: stopped o.s.j.s.ServletContextHandler{/jobs/job/json,null}
21/03/01 15:20:40 INFO handler.ContextHandler: stopped o.s.j.s.ServletContextHandler{/jobs/job,null}
21/03/01 15:20:40 INFO handler.ContextHandler: stopped o.s.j.s.ServletContextHandler{/jobs/json,null}
21/03/01 15:20:40 INFO handler.ContextHandler: stopped o.s.j.s.ServletContextHandler{/jobs,null}
21/03/01 15:20:40 INFO ui.SparkUI: Stopped Spark web UI at http://192.168.234.128:4040
21/03/01 15:20:40 INFO spark.MapOutputTrackerMasterEndpoint: MapOutputTrackerMasterEndpoint stopped!
21/03/01 15:20:40 INFO storage.MemoryStore: MemoryStore cleared
21/03/01 15:20:40 INFO storage.BlockManager: BlockManager stopped
21/03/01 15:20:40 INFO storage.BlockManagerMaster: BlockManagerMaster stopped
21/03/01 15:20:40 INFO scheduler.OutputCommitCoordinator$OutputCommitCoordinatorEndpoint: OutputCommitCoordinator stopped!
21/03/01 15:20:40 INFO spark.SparkContext: Successfully stopped SparkContext
21/03/01 15:20:40 INFO util.ShutdownHookManager: Shutdown hook called
21/03/01 15:20:41 INFO util.ShutdownHookManager: Deleting directory /tmp/spark-8939af7c-4858-482d-9a7d-d41e1e1bc577
21/03/01 15:20:41 INFO remote.RemoteActorRefProvider$RemotingTerminator: Shutting down remote daemon.
21/03/01 15:20:41 INFO remote.RemoteActorRefProvider$RemotingTerminator: Remote daemon shut down; proceeding with flushing remote transports.
21/03/01 15:20:41 INFO Remoting: Remoting shut down
21/03/01 15:20:41 INFO remote.RemoteActorRefProvider$RemotingTerminator: Remoting shut down.




	 */



}
