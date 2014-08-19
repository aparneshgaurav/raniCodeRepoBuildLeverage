package com.bigdata.mapreduce.minmaxcount.v1;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.log4j.Logger;
/**
 * 
 * @author Aparnesh
 * @description
 * 
 *
 */
public class MinMaxCountReducer extends  Reducer<Text, DoubleWritable
,Text, Text>{
	/**
	 * @description : setup method which is called once per reducer task.This method is executed 
	 * before reducer and cleanup methods.
	 */
	public void setup(Context context){

	}
	/**
	 * @description 
	 * 
	 */
	public void reduce(Text key,Iterable<DoubleWritable> values,
			Context context)
					throws IOException {
		Logger logger = Logger.getLogger(MinMaxCountReducer.class);
		logger.info("############################## reduce method getting executed for a key ##########################");
		/**
		 * Condtional check for numeric columns
		 * 
		 */
		if(key.toString().contains("columnNumeric")&&(!(key.toString().contains("dataPoint")))){
			logger.info("********************* numeric column analysis");
			/**
			 * Iterating to get the highest of the values
			 */
			Double minValue = Double.MAX_VALUE;
			Double maxValue = Double.MIN_VALUE;
			Integer count = 0;
			Double sum = 0.0;
			Set<Double> setOfUniqueNumericValues = new HashSet<Double>();
			for(DoubleWritable val : values){
				logger.info("************* value is :  ***************"+val);
				maxValue = Math.max(maxValue,val.get());
				minValue = Math.min(minValue, val.get());
				count = count + 1;
				sum = sum + val.get();
				setOfUniqueNumericValues.add(val.get());
			}
			logger.info("************* max value in reduce phase  is : ******** "+maxValue);

			logger.info("************* min value in reduce phase  is : ******** "+minValue);

			logger.info("***************** count of the values "+count);

			logger.info("**************** sum of the values is : "+sum);

			try {
				context.write(new Text(key),new Text(": min : "+minValue.toString()+": max : "+maxValue.toString()+": count : "+count.toString()+": sum : "+sum.toString() + " : uniqueCount : "+setOfUniqueNumericValues.size()));
				
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		/**
		 * Conditional check to render count of numeric data points
		 */
		/*else if(key.toString().contains("dataPoint")){
			Integer count = 0;
			for(DoubleWritable val : values){
				count = count + 1;
			}
			try {
				context.write(new Text(key), new Text(count.toString()));
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}*/
		/**
		 * Conditional check for alpha numeric columns along with their keywords
		 */
		else if(key.toString().contains("columnAlphaNumeric")){
			logger.info("********************* AlphaNumeric column analysis");
			Integer count = 0;
			for(DoubleWritable val : values){
				count++;
			}
			logger.info("**************** count in case of alphaNumeric columns : "+count);
			try {
				context.write(new Text(key), new Text(count.toString()));
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	/**
	 * @description : the clean up method is called when the reducer method is done executing , post that
	 * clean up method is called once per reducer task.
	 */
	public void cleanup(Context context){

	}
}
