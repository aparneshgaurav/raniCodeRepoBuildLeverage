package com.bigdata.mapreduce.eda;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Partitioner;

/*
 * *************************************************************************** 
 * ************************* Second partition class **************************
 * ***************************************************************************
 */
public class EDAPartition2 extends Partitioner<EDAComparator, Text> {

	private static String catCutOff = "";
	
	public void setup(Context context) {
		catCutOff = context.getConfiguration().get("catCutOff");
	}
	
	@Override
	public int getPartition(EDAComparator key, Text value, int numReduce) {
		
		if(key.toString().equals("varSum")) {
			int returnVal = Math.abs(key.hashCode()) % numReduce;
			if (returnVal <  0)
				return -returnVal;
			else
				return returnVal;
		}
		else {
			if (Double.parseDouble(key.toString().split("-")[2]) 
					< 10) {
				return Integer.parseInt(key.toString().split("l")[1].split("-")[0]) % numReduce;
			}
			else {
				int returnVal = Math.abs(key.hashCode()) % numReduce;
				if (returnVal <  0)
					return -returnVal;
				else
					return returnVal;
			}
		}
	}
		
}