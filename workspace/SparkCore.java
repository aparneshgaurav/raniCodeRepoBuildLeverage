package com.poc.rcm.sparkcore;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;
public class SparkCore {
// spark-submit --class com.poc.rcm.sparkcore.SparkCore /home/cloudera/dev/SparkCore.jar
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		
		List<String> data = Arrays.asList("1","2");
		JavaRDD<String> distData = sc.parallelize(data);
		
		JavaRDD<Integer> lineLengths = distData.map(s -> s.length());
		int totalLength = lineLengths.reduce((a, b) -> a + b);
		
		System.out.println(" the total length is : "+totalLength);
		
		
		
		

	}

}
