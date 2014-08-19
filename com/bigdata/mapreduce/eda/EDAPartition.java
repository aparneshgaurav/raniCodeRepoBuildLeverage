package com.bigdata.mapreduce.eda;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;


/* Partition class for first MapReduce job. 
 * Sending count, sum, max, min, levels to a single reducer.
 */
public class EDAPartition extends Partitioner<EDAComparator, Text> {

	@Override
	public int getPartition(EDAComparator key, Text value, int numReduce) {
			
		if (key.toString().contains("col")) {
			int returnVal = Math.abs(key.hashCode()) % numReduce;
			if (returnVal <  0)
				return -returnVal;
			else
				return returnVal;
		}
		else {
			return 0 % numReduce;
		}
	}	
}
