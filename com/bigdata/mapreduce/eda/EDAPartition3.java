package com.bigdata.mapreduce.eda;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/*
 * *************************************************************************** 
 * ********************** Third job partition class ************************** 
 * ***************************************************************************
 * */

public class EDAPartition3 extends Partitioner<EDAComparator, Text> {

	@Override
	public int getPartition(EDAComparator key, Text value, int numReduce) {
	
		String str = key.toString().split("c")[0];
		return (Integer.parseInt(str)) % numReduce;
	}
		
}