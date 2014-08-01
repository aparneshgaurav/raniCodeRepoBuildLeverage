package com.bigdata.mapreduce.seqtotext.beta1;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.bigdata.mapreduce.salesdata.TotalQuantityPerProductDriver;
import com.bigdata.mapreduce.salesdata.TotalQuantityPerProductMapper;
import com.bigdata.mapreduce.salesdata.TotalQuantityPerProductReducer;

public class SequenceToTextDriver extends Configured implements Tool {
	
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Configuration(), new SequenceToTextDriver(), args);
		System.exit(exitCode);
		
				
	}

	@Override
	public int run(String[] args) throws Exception {
		JobConf jobConf = new JobConf(super.getConf());
		jobConf.setJobName("test job");
		jobConf.setJarByClass(SequenceToTextDriver.class);
		// Giving the input and the output files here , both in hdfs.
		FileInputFormat.addInputPath(jobConf, new Path(args[0]));
		FileOutputFormat.setOutputPath(jobConf, new Path(args[1]));
		// setting the mapper and the reducer class.
		jobConf.setInputFormat(ZipFileInputFormat.class);
		jobConf.setMapperClass(SequenceToTextMapper.class);
		// setting the data type of the output key and the value.
		// Getting the exception when the map reduce code is run.
		try {
			JobClient.runJob(jobConf);
		}catch (IOException e) {
			e.printStackTrace();
		}
		return 0;
	}
}
