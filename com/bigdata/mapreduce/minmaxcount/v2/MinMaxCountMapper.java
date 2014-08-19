package com.bigdata.mapreduce.minmaxcount.v2;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
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
 * @param 
 *
 */
public class MinMaxCountMapper extends Mapper<LongWritable, Text, Text, Text> {
	public Integer mapperGlobalVariable ;
	public Set<Double> setOfDistinctValues;
	public Map<Integer,Set<Double>> mapOfColumnIdAndSetOfDistinctValues;
	Logger logger = Logger.getLogger(MinMaxCountMapper.class);
	/**
	 * @description : setup method which is called once per mapper task.This method is executed
	 * before mapper and cleanup methods.
	 */
	public void setup(Context context){
		mapOfColumnIdAndSetOfDistinctValues = new HashMap<Integer,Set<Double>>();
	}
	/**
	 * @description :
	 */
	public void map(LongWritable key, Text value,
			Context context)
					throws IOException {
		logger.info("##############################mapper method getting executed for a row##############################");
		String[] strArray = value.toString().split("\\|");
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
					context.write(new Text(("columnNumeric : "+(i+1)+ " : ")), new Text(doubleData.toString()));
					// context.write(new Text(("columnNumeric : "+i+ " : dataPoint :  "+data+" : count : ")), new DoubleWritable(1));
				}catch(InterruptedException e){
					e.printStackTrace();
				}
			}
			/**
			 * Condition for blank spaces
			 */
			else if (data.equalsIgnoreCase("")|(data.equalsIgnoreCase(" "))){
				try {
					context.write(new Text(("columnBlankCount : "+(i+1)+ " : ")), new Text("1.0"));
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			/**
			 * Condition for non-numeric data
			 */
			else{

				/**
				 * if alphaNumeric is a date of slash format
				 */
				if (data.contains("/")&&!(data.contains("EOR"))){
					try {
						context.write(new Text(("dateSlashFormat : "+(i+1)+ " : ")), new Text(data.toString()));
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
				/**
				 * if alphaNumeric is a date of hyphen format
				 */
				else if ((data.contains("-"))&&!(data.contains(":"))&&!(data.contains("EOR"))){
					try {
						context.write(new Text(("dateHyphenFormat : "+(i+1)+ " : ")), new Text(data.toString()));
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
				/**
				 * if alphaNumeric is a date of unix time stamp format
				 */
				else if ((data.contains("-"))&&(data.contains(":"))&&!(data.contains("EOR"))){
					try {
						context.write(new Text(("dateUnixFormat : "+(i+1)+ " : ")), new Text(data.toString()));
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
				/**
				 * if alphaNumeric is not a date
				 */
				//					if((!(data.contains("/")&&!(data.contains("-"))&&!(data.contains(":"))))){
				else{
					try {
						context.write(new Text(("columnAlphaNumeric : "+(i+1)+ " : ")), new Text(data.toString()));
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
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
