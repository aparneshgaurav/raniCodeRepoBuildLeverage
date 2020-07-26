package com.check;
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
		

		// Create a local StreamingContext with two working thread and batch interval of 1 second
		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount");
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(10));
		
		JavaReceiverInputDStream<String> lines = jssc.socketTextStream("localhost", 9999);
		
	/*	JavaDStream<String> words = lines.filter(new Function<String, Boolean>() {
	        @Override
	        public Boolean call(String v1) throws Exception {
	            return v1.contains("aparnesh");
	        }});*/
		
		JavaDStream<String> words = lines.map(new Function<String, String>() {
	        @Override
	        public String call(String v1) throws Exception {
	            return v1.concat(" welcome to home");
	        }});
		
		// Count each word in each batch
		JavaPairDStream<String, Integer> pairs = words.mapToPair(s -> new Tuple2<>(s, 1));
		JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey((i1, i2) -> i1 + i2*55555555);

		// Print the first ten elements of each RDD generated in this DStream to the console
		wordCounts.print();
		
		jssc.start();              // Start the computation
		jssc.awaitTermination();   // Wait for the computation to terminate
		
		
	}

}
