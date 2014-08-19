package com.bigdata.mapreduce.salesdata;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;
/**
 * 
 * @author Aparnesh
 * @param The first two parameters are the key value being the input where key is the 
 * offset of the line being long and the data in the line being string. The last two
 * parameters are the key value paramters being sent as an output by the mapper function
 * which are string which is the year and integer which is the temperature in the year.
 * As an output , years as key can be repeated.
 * Also http://localhost:50030/jobhistory.jsp is the place where you look for the mapper
 * logs.
 *
 */
public class TotalQuantityPerProductMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
	/**
	 * @description : The mapper is used to take the input values in key,value format and send the output in 
	 * a key , value format to be shuffled.The last two
	 * parameters are the key value paramters being sent as an output by the mapper function
	 * which are string which is the year and integer which is the temperature in the year.
	 * In the current example , offset is the key and record/row of the input split is the value for that key.
	 */
	@Override
	public void map(LongWritable key, Text value,
			OutputCollector<Text, IntWritable> output, Reporter reporter)
					throws IOException {
		Logger logger = Logger.getLogger(TotalQuantityPerProductMapper.class);
		logger.info("############################## mapper method getting executed for a row ##############################");
		// fetching the line from the file. 		
		String line = value.toString();
		// fetching the year from the line obtained.
		String year = line.substring(0,4);
		// fetching the temperature reading for that year from the line
		String temperature = line.substring(5);
		// converting the string datatype into integer datatype for temperature
		Integer temperatureInt = Integer.parseInt(temperature);
		logger.info("****************mapper key is : ******************"+year);
		logger.info("****************mapper value is : ****************"+temperatureInt);
		// setting the key value to the ouput map
		output.collect(new Text(year),new IntWritable(temperatureInt));
	}
}
