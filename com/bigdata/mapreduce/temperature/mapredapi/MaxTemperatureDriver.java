package com.bigdata.mapreduce.temperature.mapredapi;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
//import org.apache.log4j.Logger;
/**
 * @description The class is used to run the map reduce code on the input data to generate the output data.
 * In totality , it is expected that the input file will be containing set of year and it's temperature 
 * recordings.
 * 
 * Also when running in training vm , it's needed to change domain from training
 * to localhost in both mapper and reducer.
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
 * @author Aparnesh.Gaurav
 *
 */
public class MaxTemperatureDriver {
	public static void main(String[] args){
//		Logger logger = Logger.getLogger(MaxTemperatureDriver.class);
//		logger.info("**********************execution started*****************************");
		// setting the service class.
		JobConf jobConf = new JobConf(MaxTemperatureDriver.class);
		jobConf.setJobName("Max temperature");
		// Giving the input and the output files here,both in hdfs.
		FileInputFormat.addInputPath(jobConf, new Path(args[0]));
		FileOutputFormat.setOutputPath(jobConf, new Path(args[1]));
		// setting the mapper and the reducer class.
//		logger.info("*************specifying the mapper class in service ****************");
		jobConf.setMapperClass(MaxTemperatureMapper.class);
//		logger.info("*************specifying the reducer class in service ****************");
		jobConf.setReducerClass(MaxTemperatureReducer.class);
		// setting the data type of the output key and the value.
		jobConf.setOutputKeyClass(Text.class);
		jobConf.setOutputValueClass(IntWritable.class);
		// Getting the exception when the map reduce code is run.
//		logger.info("**********************execution finished*****************************");
		try {
			JobClient.runJob(jobConf);
		}catch (IOException e) {
			e.printStackTrace();
		}

	}
}
