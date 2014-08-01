package com.bigdata.mapreduce.minmaxcount;

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

columnAlphaNumeric : 2 : keyword :  14/04/01 : count :                 1
columnAlphaNumeric : 2 : keyword :  17/01/01 : count :                 1
columnAlphaNumeric : 2 : keyword :  24/02/01 : count :                 1
columnAlphaNumeric : 3 : keyword :  2011/03/26 : count :             1
columnAlphaNumeric : 3 : keyword :  2012/03/26 : count :             1
columnAlphaNumeric : 3 : keyword :  2014/03/26 : count :             1
columnAlphaNumeric : 4 : keyword :  C : count :                 3
columnAlphaNumeric : 5 : keyword :  BHE3444 : count : 1
columnAlphaNumeric : 5 : keyword :  BHED253 : count :                 2
columnAlphaNumeric : 6 : keyword :  2010-03-01 : count :             1
columnAlphaNumeric : 6 : keyword :  2010-04-01 : count :             1
columnAlphaNumeric : 6 : keyword :  2012-04-01 : count :             1
columnAlphaNumeric : 7 : keyword :  9999-12-31EOR : count :     3
columnNumeric : 0 :        : min : 127444.34: max : 127448.1: count : 3: sum : 382339.44
columnNumeric : 0 : dataPoint :  127444.34 : count :         1
columnNumeric : 0 : dataPoint :  127447. : count :              1
columnNumeric : 0 : dataPoint :  127448.1 : count :            1
columnNumeric : 1 :        : min : 199112.0: max : 199146.0: count : 3: sum : 597370.0
columnNumeric : 1 : dataPoint :  199112. : count :              2
columnNumeric : 1 : dataPoint :  199146. : count :              1

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
		job.setMapOutputValueClass(DoubleWritable.class);
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
