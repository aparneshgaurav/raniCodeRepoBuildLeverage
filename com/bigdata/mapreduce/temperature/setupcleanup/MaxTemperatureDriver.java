package com.bigdata.mapreduce.temperature.setupcleanup;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;
/**
 * @description The class is used to run the map reduce code on the input data to generate the output data.
 * In totality , it is expected that the input file will be containing set of year and it's temperature 
 * recordings.
 * 
 * Also when running in training vm , it's needed to change domain from training
 * to localhost in both mapper and reducer.
 * 
 * Also one difference here is that , max temperature code here is having the setup and clean up 
 * methods , which are called once per mapper task which corresponds to each input split.
 * 
 * Ex : 
 * 
 * 1947 45
 * 1947 36
 * 1951 34
 * 1951 56
 * 1947 50
 * 1951 52
 * 
 * The expected output is the highest temperature reading recorded per year : 
 * 
 * Ex : 
 * 
 * 1947 50
 * 1951 56
 * 
 * @author Aparnesh.Gaurav , if you are using cloudera distro , the third party jars on eclipse have to be bound at /usr/lib/hadoop/client
 *
 */
public class MaxTemperatureDriver {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
		Logger logger = Logger.getLogger(MaxTemperatureDriver.class);
		logger.info("**********************execution started*****************************");
		// setting the service class.
		Job job = new Job();
		job.setJarByClass(MaxTemperatureDriver.class);
		job.setJobName("Max temperature per year");
		
		// Giving the input and the output files here , both in hdfs.
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		// setting the mapper and the reducer class.
		logger.info("*************specifying the mapper class in service ****************");
		job.setMapperClass( MaxTemperatureMapper.class);
		logger.info("*************specifying the reducer class in service ****************");
		job.setReducerClass( MaxTemperatureReducer.class);
		// setting the data type of the output key and the value.
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		// Getting the exception when the map reduce code is run.
		logger.info("**********************execution finished*****************************");
				
		System.exit(job.waitForCompletion(true) ? 0 :1);
	}
}
