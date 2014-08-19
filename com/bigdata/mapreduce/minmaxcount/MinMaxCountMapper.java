package com.bigdata.mapreduce.minmaxcount;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
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
public class MinMaxCountMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
	public Integer mapperGlobalVariable ;
	Logger logger = Logger.getLogger(MinMaxCountMapper.class);
	/**
	 * @description : setup method which is called once per mapper task.This method is executed 
	 * before mapper and cleanup methods.
	 */
	public void setup(Context context){

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
		// parsing the line
		String[] strArray = value.toString().split("\\|");

		/*String data1 = strArray[0];
		String data2 = strArray[1];


		Integer intData1 = Integer.parseInt(data1);
		Integer intData2 = Integer.parseInt(data2);

		logger.info("****************mapper value is : ****************"+intData1);
		logger.info("****************mapper value is : ****************"+intData2);

		try {
			context.write(new Text("column1"),new IntWritable(intData1));
			context.write(new Text("column2"),new IntWritable(intData2));
		} catch (InterruptedException e) {
			e.printStackTrace();
		}*/
		Pattern patternForNumeric = Pattern.compile("^[0-9]+(\\.[0-9]{0,6})?$");
		

		for(int i=0;i<strArray.length;i++){
			String data = strArray[i];
			 boolean isNumeric = patternForNumeric.matcher(data).find();
			/**
			 * Condition for numeric data
			 */
			if(isNumeric){
				Double doubleData = Double.parseDouble(data);
				logger.info("**************** mapper value is : "+doubleData);
				try{
					context.write(new Text(("columnNumeric : "+i+ " : ")), new DoubleWritable(doubleData));
					context.write(new Text(("columnNumeric : "+i+ " : dataPoint :  "+data+" : count : ")), new DoubleWritable(1));
				}catch(InterruptedException e){
					e.printStackTrace();
				}
			}
			/**
			 * Condition for non-numeric data
			 */
			else{
				try {
					context.write(new Text(("columnAlphaNumeric : "+i+ " : keyword :  "+data+" : count : ")), new DoubleWritable(1));
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}

		}
	}
	/**
	 * @description : the clean up method is called when the mapper method is done executing , post that
	 * clean up method is called once per mapper task.
	 */
	public void cleanup(Context context){

	}
}
