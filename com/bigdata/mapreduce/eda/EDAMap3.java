package com.bigdata.mapreduce.eda;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

/*
 * *************************************************************************** 
 * ************************** Third map class ******************************** 
 * ***************************************************************************
 */
public class EDAMap3 extends Mapper<LongWritable, Text, EDAComparator, Text> {
	
	public void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException {
		
		String lineArr[] = value.toString().split("\t");
		
		if (lineArr[0].split("-")[0].contains("col")) {
			context.write(new EDAComparator(lineArr[0]), new Text(lineArr[1]));
		}
	}
}