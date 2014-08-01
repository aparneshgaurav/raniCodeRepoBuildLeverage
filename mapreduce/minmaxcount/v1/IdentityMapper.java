package com.bigdata.mapreduce.minmaxcount.v1;

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
public class IdentityMapper extends Mapper<LongWritable, Text, LongWritable , Text> {
	public Integer mapperGlobalVariable ;
	public Set<Double> setOfDistinctValues;
	public Map<Integer,Set<Double>> mapOfColumnIdAndSetOfDistinctValues;
	Logger logger = Logger.getLogger(IdentityMapper.class);
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
		try {
			context.write(key, value);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	/**
	 * @description : the clean up method is called when the mapper method is done executing , post that
	 * clean up method is called once per mapper task.
	 */
	public void cleanup(Context context){

	}
}
