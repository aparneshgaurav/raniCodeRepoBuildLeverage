package com.bigdata.mapreduce.temperature.jobchaining;
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
public class MaxTemperatureAcrossYearsMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	public Integer mapperGlobalVariable ;
	Logger logger = Logger.getLogger(MaxTemperatureAcrossYearsMapper.class);
	
	/**
	 * @description : the input to this mapper is the year and it's highest temperature ,
	 * so the intermediate output generated is a hard coded key and that year's highest 
	 * temperature , key is going to be the same now as it is hard coded but the temperature
	 * readings are going to be the highest of each individual year.
	 */
	public void map(LongWritable key, Text value,
			Context context)
					throws IOException {
		
		logger.info("############################## mapper method getting executed for a row #############################");
		// fetching the line from the file. 		
		String line = value.toString();
		String[] strArray = line.split("\t");
		String year = strArray[0];
		Integer maxTempInThatYear = Integer.parseInt(strArray[1]);
		try {
			context.write(new Text("abc"),new IntWritable(maxTempInThatYear));
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
