package com.bigdata.mapreduce.temperature.setupcleanup;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
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
public class MaxTemperatureMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	public Integer mapperGlobalVariable ;
	Logger logger = Logger.getLogger(MaxTemperatureMapper.class);
	/**
	 * @description : setup method which is called once per mapper task.This method is executed 
	 * before mapper and cleanup methods.
	 */
	public void setup(Context context){
		try {
			mapperGlobalVariable = new Integer(1);
			logger.info("mapper global value in mapper setup  method is : "+mapperGlobalVariable);
			context.write(new Text("setupKeyMapper"),new IntWritable(mapperGlobalVariable));
			context.write(new Text("1950"),new IntWritable(50));
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	/**
	 * @description : mapper method is used to take the input values in key,value format and send the output in 
	 * a key , value format to be shuffled.The last two
	 * parameters are the key value paramters being sent as an output by the mapper function
	 * which are string which is the year and integer which is the temperature in the year.
	 * In the current example , offset is the key and record/row of the input split is the value for that key.
	 * It is called as per the number of rows/records found in the inputsplit corresponding to which the current
	 * mapper task is under exectuion.
	 */
	public void map(LongWritable key, Text value,
			Context context)
					throws IOException {
		
		logger.info("##############################mapper method getting executed for a row##############################");
		// fetching the line from the file. 		
		String line = value.toString();
		// fetching the year from the line obtained.
		String year = line.substring(0,4);
		// fetching the temperature reading for that year from the line
		String temperature = line.substring(5);
		// converting the string data type into integer data type for temperature
		Integer temperatureInt = Integer.parseInt(temperature);
		logger.info("****************mapper key is : ******************"+year);
		logger.info("****************mapper value is : ****************"+temperatureInt);
		mapperGlobalVariable ++;
		logger.info("mapper global value in mapper method is : "+mapperGlobalVariable);
		// setting the key value to the ouput map
		try {
			context.write(new Text(year),new IntWritable(temperatureInt));
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	/**
	 * @description : the clean up method is called when the mapper method is done executing , post that
	 * clean up method is called once per mapper task.
	 */
	public void cleanup(Context context){
		try {
			logger.info("mapper global value in mapper cleanup method is : "+mapperGlobalVariable);
//			context.write(new Text("cleanupKeyMapper"), new IntWritable(mapperGlobalVariable));
			context.write(new Text("1950"),new IntWritable(50));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
