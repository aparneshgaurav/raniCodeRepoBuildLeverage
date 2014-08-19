package com.bigdata.mapreduce.minmaxcount.v2;

import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;
/**
 * @description 
 * 
 * Sample input dataset : 
 * 
127447.|199112.|14/04/01|2011/03/26|C|BHED253|2010-03-01|9999-12-31EOR
127448.1|199146.|24/02/01|2012/03/26|C|BHE3444|2010-04-01|9999-12-31EOR
127444.34|199112.|17/01/01|2014/03/26|C|BHED253|2012-04-01|9999-12-31EOR

EDA result : 



 * 
 * @author Aparnesh.Gaurav
 *
 */
public class MinMaxCountDriver {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
		Logger logger = Logger.getLogger(MinMaxCountDriver.class);
		logger.info("**********************execution started*****************************");
		// setting the service class.
		Job job = new Job();
		job.setJarByClass(MinMaxCountDriver.class);
		job.setJobName("Min max count job");
		
		// Giving the input and the output files here , both in hdfs.
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		// setting the mapper and the reducer class.
		logger.info("*************specifying the mapper class in service ****************");
		job.setMapperClass( MinMaxCountMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		logger.info("*************specifying the reducer class in service ****************");
		job.setReducerClass( MinMaxCountReducer.class);
		// setting the data type of the output key and the value.
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		// Getting the exception when the map reduce code is run.
		logger.info("**********************execution finished*****************************");
				
		System.exit(job.waitForCompletion(true) ? 0 :1);
	}
}
