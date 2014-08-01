package com.bigdata.mapreduce.eda;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

// The driver class
public class EDA extends Configuration implements Tool {
	
	// Declaring logger to which will be used to print log messages with the jobs
	//private static final Logger sLogger = Logger.getLogger(EDA.class);
	
	// Function to check whether a value is of type numeric or character
	public static boolean isNumeric(String str)  {  
		try  {  
			Double.parseDouble(str);  
		} 
		catch(NumberFormatException nfe)  {  
			return false;  
		}  
		return true;  
	}
	
	@Override
	public Configuration getConf() {
		// TODO Auto-generated method stub
		return new Configuration();
	}

	@Override
	public void setConf(Configuration arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public int run(String[] arg) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = getConf();
		//conf.set("analysis", arg[0]);
		conf.set("columns", arg[2]);
		conf.set("columnSeparator", arg[3]);
		conf.set("catCutOff", arg[4]);
		conf.set("percentilePoints", arg[5]);
		Job job = new Job(conf, "muEDA - Exploratory Data Analysis");
		
		job.setJarByClass(EDA.class);
		job.setMapperClass(EDAMap.class);
		job.setPartitionerClass(EDAPartition.class);
		job.setReducerClass(EDAReduce.class);
		job.setNumReduceTasks(2);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapOutputKeyClass(EDAComparator.class);
		job.setMapOutputValueClass(Text.class);
		//job.setOutputFormatClass(EDAMultipleTextOutputFormat.class);
		MultipleOutputs.addNamedOutput(job, "cacheFile", 
				TextOutputFormat.class, Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "output1", 
				TextOutputFormat.class, Text.class, Text.class);
		
		FileInputFormat.addInputPath(job, new Path(arg[0]));
		FileOutputFormat.setOutputPath(job, new Path(arg[1]));
		
		if (job.waitForCompletion(true)) {
			FileSystem hdfs = FileSystem.get(conf);
			Path outputPath = FileOutputFormat.getOutputPath(job);
			Path successFile = new Path(outputPath, "_SUCCESS");
			Path logsFile = new Path(outputPath, "_logs");
			hdfs.delete(successFile);
			hdfs.delete(logsFile);
			
			Configuration conf1 = new Configuration();
			//conf1.set("analysis", arg[0]);
			conf1.set("columns", arg[2]);
			conf1.set("catCutOff", arg[4]);
			conf.set("percentilePoints", arg[5]);
			
			FileSystem f = FileSystem.get(conf1);
			FileStatus[] fStatus = f.listStatus(outputPath);
			for (FileStatus status : fStatus) {
				if (status.getPath().toString().contains("cacheFile")) 
					DistributedCache.addCacheFile(status.getPath().toUri(), conf1);
			}
			
			Job job1 = new Job(conf1, "muEDA - Exploratory Data Analysis");
			
			job1.setJarByClass(EDA.class);
			job1.setMapperClass(EDAMap2.class);
			job1.setPartitionerClass(EDAPartition2.class);
			job1.setReducerClass(EDAReduce2.class);
			job1.setNumReduceTasks(2);
			
			job1.setInputFormatClass(TextInputFormat.class);
			job1.setMapOutputKeyClass(EDAComparator.class);
			job1.setMapOutputValueClass(Text.class);
			MultipleOutputs.addNamedOutput(job1, "newCacheFile", 
					TextOutputFormat.class, Text.class, Text.class);
			MultipleOutputs.addNamedOutput(job1, "output2", 
					TextOutputFormat.class, Text.class, Text.class);
			MultipleOutputs.addNamedOutput(job1, "frequency", 
					TextOutputFormat.class, Text.class, Text.class);
		
			FileInputFormat.addInputPath(job1, new Path(arg[1]));
			FileOutputFormat.setOutputPath(job1, new Path(arg[1] + "_" + "result"));
			
			if (job1.waitForCompletion(true)) {
				FileSystem hdfs1 = FileSystem.get(conf1);
				Path outputPath1 = FileOutputFormat.getOutputPath(job1);
				Path successFile1 = new Path(outputPath1, "_SUCCESS");
				Path logsFile1 = new Path(outputPath1, "_logs");
				hdfs1.delete(successFile1);
				hdfs1.delete(logsFile1);
				
				Configuration conf2 = new Configuration();
				//conf2.set("analysis", arg[0]);
				conf2.set("columns", arg[2]);
				conf2.set("catCutOff", arg[4]);
				conf2.set("percentilePoints", arg[5]);
				
				FileSystem fs = FileSystem.get(conf2);
				FileStatus[] fileStatus = fs.listStatus(outputPath1);
				for (FileStatus status : fileStatus) {
					if (status.getPath().toString().contains("newCacheFile")) 
						DistributedCache.addCacheFile(status.getPath().toUri(), conf2);
				}
				
				FileSystem fs1 = FileSystem.get(conf2);
				FileStatus[] fileStatus1 = fs1.listStatus(outputPath1);
				
				Job job2 = new Job(conf2, "muEDA - Exploratory Data Analysis");
				
				job2.setJarByClass(EDA.class);
				job2.setMapperClass(EDAMap3.class);
				job2.setPartitionerClass(EDAPartition3.class);
				job2.setReducerClass(EDAReduce3.class);
				job2.setNumReduceTasks(2);
				
				job2.setInputFormatClass(TextInputFormat.class);
				job2.setMapOutputKeyClass(EDAComparator.class);
				job2.setMapOutputValueClass(Text.class);
				
				FileInputFormat.addInputPath(job2, new Path((arg[1] + "_" + "result")));
				FileOutputFormat.setOutputPath(job2, new Path(arg[1] + "_" + "final"));
				
				if (job2.waitForCompletion(true)) {
					
					//fs1.mkdirs(new Path(arg[1] + "_" + "final/univ"));
					fs1.mkdirs(new Path(arg[1] + "_" + "final/freq"));
					
					String srcPath = arg[1] + "_" + "final";
					String dstPath = srcPath + "/univariate.txt";
					String srcPath1 = arg[1] + "_" + "final/freq";
					String dstPath1 = arg[1] + "_" + "final/frequency.txt";
					//Configuration conf3 = new Configuration();
					try {
						
						for (FileStatus status : fStatus) {
							if (status.getPath().toString().contains("output1")) {
								FileUtil.copy(f, status.getPath(), f, new Path(srcPath), false, conf1);
							}
						}
						for (FileStatus status : fileStatus) {
							if (status.getPath().toString().contains("output2")) 
								FileUtil.copy(fs, status.getPath(), fs, new Path(srcPath), false, conf2);
						}
						
						FileSystem filesystem = FileSystem.get(conf);
						FileUtil.copyMerge(filesystem, new Path(srcPath), filesystem, 
								new Path(dstPath), false, conf, null);
						
						
						for (FileStatus status1 : fileStatus1) {
							if (status1.getPath().toString().contains("frequency")) 
								FileUtil.copy(fs1, status1.getPath(), fs1, new Path(srcPath1), false, conf2);
						}
						
						FileSystem filesystem1 = FileSystem.get(conf1);
						FileUtil.copyMerge(filesystem1, new Path(srcPath1), filesystem1, 
								new Path(dstPath1), false, conf1, null);
						
					
					} catch (IOException e) {
						System.out.println("Something wrong happened with the HDFS output" +
								" files. Please remove the HDFS output directory and" +
								"re-run the job.");
					}
					return 0;
				}
				else
					return 1;
			}
			else
				return 1;
		}
		else
			return 1;
	}
	
	public static void main(String[] arg) throws Exception {
		int results = ToolRunner.run(new Configuration(), new EDA(), arg);
	}
}
