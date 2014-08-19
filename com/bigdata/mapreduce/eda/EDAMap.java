package com.bigdata.mapreduce.eda;
import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/*
 * *************************************************************************** 
 * ************************** First map class ******************************** 
 * This class emits all the data for each column specified by user and also
 * computes the count, sum, max, min and level (distinct count) per mapper
 * for each column. It also checks whether any data in the column is non-numeric.
 * ***************************************************************************
 */
public class EDAMap extends Mapper<LongWritable, Text, EDAComparator, Text> {
	
	// Creating ArrayLists to store count, sum, max, min per column
	private static ArrayList<String> count = new ArrayList<String>();
	private static ArrayList<String> sum = new ArrayList<String>();
	private static ArrayList<String> max = new ArrayList<String>();
	private static ArrayList<String> min = new ArrayList<String>();
	private static ArrayList<Boolean> numericCheck = new ArrayList<Boolean>();
	private static ArrayList<Integer> finalLevel = new ArrayList<Integer>();
	
	// Creating a HashMap to store the distinct column data
	private static HashMap<String, Integer> level = new HashMap<String, Integer>();
	
	private static String columns;
	private static String colSep;
	
	// Setup method for the first job. This will be called only once
	public void setup(Context context) {
		
		// Getting the column names from the user
		columns = context.getConfiguration().get("columns");
		String columnArr[] = columns.split(",");
		
		colSep = context.getConfiguration().get("columnSeparator");
		
		for (int i = 0; i < columnArr.length; i++) {
			count.add("0");
			sum.add("0");
			max.add("0");
			min.add(Integer.MAX_VALUE + "");
			numericCheck.add(true);
			finalLevel.add(0);
		}			
	}
	
	Text one = new Text("1");
	
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {
		
		String line[] = value.toString().split(colSep);
		String columnArr[] = columns.split(",");
		double newCount = 0;
		double newSum = 0;
		double newMax = 0;
		double newMin = 0;
		
		for (int i = 0; i < columnArr.length; i++) {
			// Adding column data with frequency count into the HashMap
			if (level.get("col" + columnArr[i] + "-" + line[Integer.parseInt(columnArr[i])])
					!= null) {
				int freq = level.get("col" + columnArr[i] + "-" + line[Integer.parseInt(columnArr[i])]);
				level.put("col" + columnArr[i] + "-" + line[Integer.parseInt(columnArr[i])], freq + 1);
			}
			else
				level.put("col" + columnArr[i] + "-" + line[Integer.parseInt(columnArr[i])], 1);
			
			// Output of the column data as key with value as 1
			context.write(
				new EDAComparator("col" + columnArr[i] + "-" + line[Integer.parseInt(columnArr[i])]), 
				one);
			
			newCount = 1 + Double.parseDouble(count.get(i));
			count.set(i, newCount + "");
			
			// Checking if numeric data
			if (EDA.isNumeric(line[Integer.parseInt(columnArr[i])]) == true) {
				double data = Double.parseDouble(line[Integer.parseInt(columnArr[i])]);
				
				//newCount = 1 + Double.parseDouble(count.get(i));
				newSum = data + Double.parseDouble(sum.get(i));
				newMax = Math.max(data, Double.parseDouble(max.get(i)));
				newMin = Math.min(data, Double.parseDouble(min.get(i)));
				
				//count.set(i, newCount + "");
				sum.set(i, newSum + "");
				max.set(i, newMax + "");
				min.set(i, newMin + "");
			}
			else
				numericCheck.set(i, false);
		}
	}
	
	// Cleanup function which outputs the final count, sum, max, min, levels per mapper
	public void cleanup(Context context) 
			throws IOException, InterruptedException {
		
		String columnArr[] = columns.split(",");
		
		Set set = level.entrySet();
		
		int index = 0;
		for (String c : columnArr) {
			// Iterating over the HashMap to compute the levels per column
			Iterator itr = set.iterator();
			while (itr.hasNext()) {
				Map.Entry me = (Map.Entry)itr.next();
				if (me.getKey().toString().split("l")[1].split("-")[0].equals(c))
					finalLevel.set(index, finalLevel.get(index) + 1);
			}
			index++;
		}
		
		String totalCount = "";
		String totalSum = "";
		String finalMax = "";
		String finalMin = "";
		String finalNumericCheck = "";
		String totalLevel = "";
		
		// Arranging the output values for count, max, min, sum, levels in string format 
		for (int i = 0; i < count.size(); i++) {
			if (i == 0) {
				totalCount = count.get(i);
				totalSum = sum.get(i);
				finalMax = max.get(i);
				finalMin = min.get(i);
				finalNumericCheck = numericCheck.get(i).toString();
				totalLevel = finalLevel.get(i).toString();
			}
			else {
				totalCount = totalCount + "-" + count.get(i);
				totalSum = totalSum + "-" + sum.get(i);
				finalMax = finalMax + "-" + max.get(i);
				finalMin = finalMin + "-" + min.get(i);
				finalNumericCheck = finalNumericCheck + "-" + numericCheck.get(i);
				totalLevel = totalLevel + "-" + finalLevel.get(i);
			}
		}
		
		// Output for count, sum, max, min, levels per column
		context.write(new EDAComparator("count"), new Text(totalCount));
		context.write(new EDAComparator("sum"), new Text(totalSum));
		context.write(new EDAComparator("max"), new Text(finalMax));
		context.write(new EDAComparator("min"), new Text(finalMin));
		context.write(new EDAComparator("numericCheck"), new Text(finalNumericCheck));
		context.write(new EDAComparator("level"), new Text(totalLevel));
	}
}
