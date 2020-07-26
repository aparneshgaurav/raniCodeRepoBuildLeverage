package com.check.mapreduce.setupcleanup;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.log4j.Logger;
/**
 * 
 * @author Aparnesh
 * @description The input key value is string and integer , string being the year and temperature
 * being the integer , same is the output format of the key value pair.
 * 
 *
 */
public class MaxTemperatureReducer extends  Reducer<Text, IntWritable
,Text, IntWritable>{
	Map<String,Integer> mapForHoldingMaxTemperaturePerYear;
	/**
	 * @description : setup method which is called once per reducer task.This method is executed 
	 * before reducer and cleanup methods.
	 */
	public void setup(Context context){
		try {
			mapForHoldingMaxTemperaturePerYear = new HashMap<String,Integer>();
			context.write(new Text("setupKeyReducer"),new IntWritable(1));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
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
	public void reduce(Text key, Iterable<IntWritable> values,
			Context context)
					throws IOException {
		Logger logger = Logger.getLogger(MaxTemperatureReducer.class);
		logger.info("############################## reduce method getting executed for a key ##########################");
		int maxValue = Integer.MIN_VALUE;
		logger.info("************* reduce key is : ********************"+key);
		/**
		 * Iterating to get the highest of the values
		 */
		for(IntWritable val : values){
			logger.info("*************list of values corresponding to the reducer key printed in loop : ***************"+val);
			maxValue = Math.max(maxValue,val.get());
		}
		logger.info("************* max value in reduce phase  is : ******** "+maxValue);
		try {
			context.write(new Text(key),new IntWritable(maxValue));
			mapForHoldingMaxTemperaturePerYear.put(key.toString(), maxValue);//1947,50 ; 1951,56
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	/**
	 * @description : the clean up method is called when the reducer method is done executing , post that
	 * clean up method is called once per reducer task.
	 */
	public void cleanup(Context context){
		try {
			String returnedKeyValuePair = computeMaxKeyValuePairForMap(mapForHoldingMaxTemperaturePerYear);
			String[] strArray = returnedKeyValuePair.split(",");
			String keyReceived = strArray[0];
			Integer valueReceived = Integer.parseInt(strArray[1]);
			context.write(new Text(keyReceived), new IntWritable(valueReceived));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	/**
	 * @description : To compute key value pair for the value selected with highest value pair
	 * @param map
	 */
	public static String  computeMaxKeyValuePairForMap(Map<String,Integer>  map){
		Set<String> keySet = map.keySet();
		List<Integer> listOfValues = new ArrayList<Integer>();
		Iterator<String> itr = keySet.iterator();
		while(itr.hasNext()){
			String key = itr.next();
			Integer value = map.get(key);
			listOfValues.add(value);
		}
		Collections.sort(listOfValues);
		System.out.println(listOfValues);
		Integer largestValue = listOfValues.get(listOfValues.size()-1);
		System.out.println("largest value is : "+largestValue);

		/**
		 * Iterate through the keyset , match the values for those keys of map against the largest value , then 
		 * store those particular keys or just one of the keys.
		 */
		String keyForLargestValue = null;
		Iterator<String> itr1 = keySet.iterator();
		while(itr1.hasNext()){
			String key = itr1.next();
			if(map.get(key)==largestValue){
				keyForLargestValue = key;
			}
		}
		System.out.println("selected key-value pair is : "+keyForLargestValue+ " : " + largestValue);
		return (keyForLargestValue+","+largestValue.toString());
	}
}

