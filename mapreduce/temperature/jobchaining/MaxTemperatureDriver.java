package com.bigdata.mapreduce.temperature.jobchaining;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.Logger;
/**
 * @description The class is used to run the map reduce code on the input data to generate the output data.
 * In totality , it is expected that the input file will be containing set of year and it's temperature 
 * recordings.
 * 
 * Also when running in training vm,it's needed to change domain from training
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
 * @author Aparnesh.Gaurav
 *
 */
public class MaxTemperatureDriver {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
		Logger logger = Logger.getLogger(MaxTemperatureDriver.class);
		logger.info("********************** execution started *****************************");
		
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: Max temeprature across years collating max year per year  <in> <out>");
			System.exit(2);
		}
        
		Job job1 = new Job(conf, "max temp per year");
 
		job1.setJarByClass(MaxTemperatureDriver.class);
 
		job1.setMapperClass(MaxTemperatureMapper.class);
		job1.setReducerClass(MaxTemperatureReducer.class);
 
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(IntWritable.class);
 
//		job1.setInputFormatClass(TextInputFormat.class);
//		job1.setOutputFormatClass(TextOutputFormat.class);
 
		FileInputFormat.addInputPath(job1, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job1, new Path("/user/musigma/temp1"));
 
		ControlledJob cJob1 = new ControlledJob(conf);
		cJob1.setJob(job1);
 
		Job job2 = new Job(conf, "max temp across years");
 
		job2.setJarByClass(MaxTemperatureDriver.class);
 
		job2.setMapperClass(MaxTemperatureAcrossYearsMapper.class);
		job2.setReducerClass(MaxTemperatureReducerAcrossYears.class);
 
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(IntWritable.class);
 
//		job2.setInputFormatClass(TextInputFormat.class);
//		job2.setOutputFormatClass(TextOutputFormat.class);
 
		FileInputFormat.addInputPath(job2, new Path("/user/musigma/temp1/part*"));
		FileOutputFormat.setOutputPath(job2, new Path(otherArgs[1]));
 
		ControlledJob cJob2 = new ControlledJob(conf);
		cJob2.setJob(job2);
 
		JobControl jobctrl = new JobControl("jobctrl");
		jobctrl.addJob(cJob1);
		jobctrl.addJob(cJob2);
		cJob2.addDependingJob(cJob1);
 
		Thread jobRunnerThread = new Thread(new JobRunner(jobctrl));
		jobRunnerThread.start();
 
		while (!jobctrl.allFinished()) {
			System.out.println("Still running...");
			Thread.sleep(5000);
		}
		System.out.println("done");
		jobctrl.stop();
 
		// Cleaning intermediate data.. can be ignored. ch
		FileSystem fs = new RawLocalFileSystem();
		fs.delete(new Path("/user/musigma/temp1/part*"), true);
		fs.close();
		// Getting the exception when the map reduce code is run.
		logger.info("**********************execution finished*****************************");
	}
}

class JobRunner implements Runnable {
	private JobControl control;
	public JobRunner(JobControl _control) {
		this.control = _control;
	}
	public void run() {
		this.control.run();
	}
}
