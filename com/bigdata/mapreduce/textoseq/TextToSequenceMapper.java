package com.bigdata.mapreduce.textoseq;
/*********************************************************************************************************
 **           Mapper 
 **           formatProject/FormatConverterTextToSequence/src/FormatConverterMapper.java 
 **           Reads text file and emits the contents out as key-value pairs
 *********************************************************************************************************/

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
public class TextToSequenceMapper extends
Mapper<LongWritable, Text, LongWritable, Text> {
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		context.write(key, value);
	}
}

