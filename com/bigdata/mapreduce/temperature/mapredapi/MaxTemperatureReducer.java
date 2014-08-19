package com.bigdata.mapreduce.temperature.mapredapi;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
//import org.apache.log4j.Logger;

/**
 * 
 * @author Aparnesh
 * @description The input key value is string and integer , string being the year and temperature
 * being the integer , same is the output format of the key value pair.
 * 
 *
 */
public class MaxTemperatureReducer extends MapReduceBase implements Reducer<Text, IntWritable
,Text, IntWritable>{
	/**
	 * @description The method takes the input as key and values for it.
	 * The maximum reading for those values is to be found and mapped to
	 * the same key and sent as an output.
	 * 
	 * Values received : it is the key which is the year and the set of values corresponding to the key.
	 * Here the set of values are processed to get the maximum value ie. the maximum temperature .Finally
	 * year as the key and maximum temperature as the value are sent to output.
	 * 
	 */
	@Override
	public void reduce(Text key, Iterator<IntWritable> values,
			OutputCollector<Text, IntWritable> output, Reporter reporter)
					throws IOException {
//		Logger logger = Logger.getLogger(MaxTemperatureReducer.class);
//		logger.info("############################## reduce method getting executed for a key ##########################");
		int maxValue = Integer.MIN_VALUE;
//		logger.info("*************reduce key is : ********************"+key);
		/**
		 * Iterating to get the highest of the values
		 */
		while(values.hasNext()){
			Integer val = values.next().get();
//			logger.info("*************list of values corresponding to the reducer key printed in loop : ***************"+val);
			maxValue = Math.max(maxValue,val);
		}
//		logger.info("*************max value in reduce phase  is : ********"+maxValue);
		output.collect(new Text(key),new IntWritable(maxValue));
	}
}
