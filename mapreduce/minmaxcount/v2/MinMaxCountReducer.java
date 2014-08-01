package com.bigdata.mapreduce.minmaxcount.v2;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
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
public class MinMaxCountReducer extends  Reducer<Text, Text
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
	public void reduce(Text key,Iterable<Text> values,
			Context context)
					throws IOException {
		Logger logger = Logger.getLogger(MinMaxCountReducer.class);
		logger.info("############################## reduce method getting executed for a key ##########################");
		/**
		 * Conditional check for numeric columns
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
			for(Text valString : values){
				logger.info("************* value is :  ***************"+valString);
				Double doubleData = Double.parseDouble(valString.toString());
				DoubleWritable doubleWritableData = new DoubleWritable(doubleData);
				maxValue = Math.max(maxValue,doubleWritableData.get());
				minValue = Math.min(minValue, doubleWritableData.get());
				count = count + 1;
				sum = sum + doubleWritableData.get();
				setOfUniqueNumericValues.add(doubleWritableData.get());
			}
			logger.info("************* max value in reduce phase  is : ******** "+maxValue);
			logger.info("************* min value in reduce phase  is : ******** "+minValue);
			logger.info("***************** count of the values "+count);
			logger.info("**************** sum of the values is : "+sum);
			String sampleNumericData = null;
			try {
				if(setOfUniqueNumericValues.size()>0){
					String[] numericValues = setOfUniqueNumericValues.toString().split(",");
					//					String sampleNumericData = (numericValues[0].substring(1)+","+numericValues[1].substring(0, numericValues[1].length()));
					if(numericValues.length>0&numericValues[0].length()>0){
						sampleNumericData = (numericValues[0].substring(1));
						context.write(new Text(key),new Text(": min : "+minValue.toString()+": max : "+maxValue.toString()+": count : "+count.toString()+": sum : "+sum.toString() + " : uniqueCount : "+setOfUniqueNumericValues.size()+" : SampleData : "+sampleNumericData));
					}else{
						context.write(new Text(key),new Text(": min : "+minValue.toString()+": max : "+maxValue.toString()+": count : "+count.toString()+": sum : "+sum.toString() + " : uniqueCount : "+setOfUniqueNumericValues.size()+" : SampleData : "+sampleNumericData));
					}
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		/**
		 * Conditional check for alpha numeric columns along with their keywords
		 */
		else if(key.toString().contains("columnAlphaNumeric")){
			logger.info("********************* AlphaNumeric column analysis");
			Integer count = 0;
			Set<String> setOfUniqueAlphaNumcericValues = new HashSet<String>();
			for(Text valString : values){
				setOfUniqueAlphaNumcericValues.add(valString.toString());
			}
			String sampleAlphaNumericValue = null;
			logger.info("**************** count in case of alphaNumeric columns : "+count);
			try {
				if(setOfUniqueAlphaNumcericValues.size()>0){
					String[] aplhaNumericValues = setOfUniqueAlphaNumcericValues.toString().split(",");
					if(aplhaNumericValues.length>0&aplhaNumericValues[0].length()>0){
						sampleAlphaNumericValue = (aplhaNumericValues[0].substring(1));
//					sampleNumericValue = (aplhaNumericValues[0].substring(1)+","+aplhaNumericValues[1].substring(0, aplhaNumericValues[1].length()));
					context.write(new Text(key), new Text(" : uniqueCountAlphaNumeric : "+setOfUniqueAlphaNumcericValues.size()+" : SampleData : "+sampleAlphaNumericValue));
					}else{
					context.write(new Text(key), new Text(" : uniqueCountAlphaNumeric : "+setOfUniqueAlphaNumcericValues.size()+" : SampleData : "+sampleAlphaNumericValue));
					}
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		/**
		 * Conditional check for blank spaces or empty regions
		 */
		else if(key.toString().contains("columnBlankCount")){
			logger.info("********************* AlphaNumeric column analysis");
			Integer count = 0;

			for(Text valString : values){
				count ++;
			}
			logger.info("**************** count in case of alphaNumeric columns : "+count);
			try {
				context.write(new Text(key), new Text(" : blank Count : "+count.toString()));
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		/**
		 * Conditional check for alpha numeric columns which are dates of slash format and computes min and max
		 */
		else if(key.toString().contains("dateSlashFormat")){
			logger.info("********************* AlphaNumeric column analysis");
			Integer count = 0;
			Set<String> setOfUniqueAlphaNumcericDateValues = new HashSet<String>();
			List<Date> dateCollections = new ArrayList<Date>();
			SimpleDateFormat df = new SimpleDateFormat("yyyy/MM/dd");

			for(Text valString : values){
				setOfUniqueAlphaNumcericDateValues.add(valString.toString());
				Date fromDate = new Date();
				try {
					fromDate = df.parse(valString.toString());
				} catch (ParseException e) {
					e.printStackTrace();
				}
				dateCollections.add(fromDate);
				Collections.sort(dateCollections);
			}
			String minDate = dateCollections.get(0).toString();
			String maxDate = dateCollections.get(dateCollections.size()-1).toString();
			logger.info("**************** count in case of alphaNumeric columns : "+count);
			try {
				if(setOfUniqueAlphaNumcericDateValues.size()>0){
					String[] aplhaNumericDateValues = setOfUniqueAlphaNumcericDateValues.toString().split(",");
					String sampleNumericValue = (aplhaNumericDateValues[0].substring(1)+","+aplhaNumericDateValues[1].substring(0, aplhaNumericDateValues[1].length()));
					context.write(new Text(key), new Text(" : uniqueCountAlphaNumericDate : "+setOfUniqueAlphaNumcericDateValues.size()+" : SampleData : "+sampleNumericValue+" : minDate : "+minDate.toString()+ " : maxDate : "+maxDate.toString()));
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		/**
		 * Conditional check for alpha numeric columns which are dates of hyphen format and computes min and max
		 */
		else if(key.toString().contains("dateHyphenFormat")){
			logger.info("********************* AlphaNumeric column analysis");
			Integer count = 0;
			Set<String> setOfUniqueAlphaNumcericDateValues = new HashSet<String>();
			List<Date> dateCollections = new ArrayList<Date>();
			SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");

			for(Text valString : values){
				setOfUniqueAlphaNumcericDateValues.add(valString.toString());
				Date fromDate = new Date();
				try {
					fromDate = df.parse(valString.toString());
				} catch (ParseException e) {
					e.printStackTrace();
				}
				dateCollections.add(fromDate);
				Collections.sort(dateCollections);
			}
			String minDate = dateCollections.get(0).toString();
			String maxDate = dateCollections.get(dateCollections.size()-1).toString();
			logger.info("**************** count in case of alphaNumeric columns : "+count);
			try {
				if(setOfUniqueAlphaNumcericDateValues.size()>0){
					String[] aplhaNumericDateValues = setOfUniqueAlphaNumcericDateValues.toString().split(",");
					String sampleNumericValue = (aplhaNumericDateValues[0].substring(1)+","+aplhaNumericDateValues[1].substring(0, aplhaNumericDateValues[1].length()));
					context.write(new Text(key), new Text(" : uniqueCountAlphaNumericDate : "+setOfUniqueAlphaNumcericDateValues.size()+" : SampleData : "+sampleNumericValue+" : minDate : "+minDate.toString()+ " : maxDate : "+maxDate.toString()));
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		/**
		 * Conditional check for alpha numeric columns which are dates of UnixFormat and computes min and max
		 */
		else if(key.toString().contains("dateUnixFormat")){
			logger.info("********************* AlphaNumeric column analysis");
			Integer count = 0;
			Set<String> setOfUniqueAlphaNumcericDateValues = new HashSet<String>();
			List<Date> dateCollections = new ArrayList<Date>();
			SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

			for(Text valString : values){
				setOfUniqueAlphaNumcericDateValues.add(valString.toString());
				Date fromDate = new Date();
				try {
					fromDate = df.parse(valString.toString());
				} catch (ParseException e) {
					e.printStackTrace();
				}
				dateCollections.add(fromDate);
				Collections.sort(dateCollections);
			}
			String minDate = dateCollections.get(0).toString();
			String maxDate = dateCollections.get(dateCollections.size()-1).toString();
			logger.info("**************** count in case of alphaNumeric columns : "+count);
			try {
				if(setOfUniqueAlphaNumcericDateValues.size()>0){
					String[] aplhaNumericDateValues = setOfUniqueAlphaNumcericDateValues.toString().split(",");
					String sampleNumericValue = (aplhaNumericDateValues[0].substring(1)+","+aplhaNumericDateValues[1].substring(0, aplhaNumericDateValues[1].length()));
					context.write(new Text(key), new Text(" : uniqueCountAlphaNumericDate : "+setOfUniqueAlphaNumcericDateValues.size()+" : SampleData : "+sampleNumericValue+" : minDate : "+minDate.toString()+ " : maxDate : "+maxDate.toString()));
				}
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
