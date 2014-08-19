package com.bigdata.mapreduce.eda;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

/*
 * *************************************************************************** 
 * ************************** Second map class ******************************* 
 * ***************************************************************************
 */
public class EDAMap2 extends Mapper<LongWritable, Text, EDAComparator, Text> {
	
	private static String dataPoint = "";
	private static String bucket = "";
	private static String max = "";
	private static String min = "";
	private static String mean = "";
	private static ArrayList<Double> varSum = new ArrayList<Double>();
	private static String totalLevel = "";
	private static String numericCheck = "";
	private static String columns = "";
	private static String catCutOff = "";
	
	// Second setup method which will be called only once
	public void setup(Context context) throws IOException, InterruptedException {
				
		columns = context.getConfiguration().get("columns");
		catCutOff = context.getConfiguration().get("catCutOff");
		String columnArr[] = columns.split(",");
		
		for (String col : columnArr) {
			varSum.add(0.0);
		}
				
		Path[] filePath = DistributedCache.getLocalCacheFiles(context.getConfiguration());
		for (int f = 0; f < filePath.length; f++) {
			BufferedReader br = new BufferedReader(new FileReader(filePath[f].toString()));
				
			String str = "";
			while((str = br.readLine()) != null) {
				// Extracting levels from cache file
				if (str.contains("levels")) {
					for (String s : str.split("\t")[1].split("-")) {
						if (totalLevel.equals(""))
							totalLevel = s;
						else
							totalLevel = totalLevel + "-" + s;
					}
				}
				// Extracting data points from cache file
				if (str.contains("dataPoint")) {
					for (String s : str.split("\t")[1].split("-")) {
						if (dataPoint.equals(""))
							dataPoint = s;
						else
							dataPoint = dataPoint + "-" + s;
					}	
				}
				// Extracting buckets from cache file
				if (str.contains("bucket")) {
					for (String s : str.split("\t")[1].split("-")) {
						if (bucket.equals(""))
							bucket = s;
						else
							bucket = bucket + "-" + s;
					}	
				}
				// Extracting max from cache file
				if (str.contains("max")) {
					for (String s : str.split("\t")[1].split("-")) {
						if (max.equals(""))
							max = s;
						else
							max = max + "-" + s;
					}	
				}
				//Extracting min from cache file
				if (str.contains("min")) {
					for (String s : str.split("\t")[1].split("-")) {
						if (min.equals(""))
							min = s;
						else
							min = min + "-" + s;
					}	
				}
				// Extracting mean from cache file
				if (str.contains("mean")) {
					for (String s : str.split("\t")[1].split("-")) {
						if (mean.equals(""))
							mean = s;
						else
							mean = mean + "-" + s;
					}	
				}
				// Extracting numeric check from cache file
				if (str.contains("numericCheck")) {
					for (String s : str.split("\t")[1].split("-")) {
						if (numericCheck.equals(""))
							numericCheck = s;
						else
							numericCheck = numericCheck + "-" + s;
					}
				}
			}
		}
	}
	
	// Map function for the second map class
	public void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException {
									
		String columnArr[] = columns.split(","); 
		String lineArr[] = value.toString().split("\t");
		
		if (lineArr[0].split("-")[0].contains("col")) {
			//double data = Double.parseDouble(lineArr[0].split("-")[1]);
			int i = 0;
			
			for (String col : columnArr) {
				if ((lineArr[0].split("l")[1].split("-")[0]).equals(col)) {
					
					/* Bucketing to be done only if the column contains all numeric data
					 * and if the cat.cut.off is acceptable for univariate analysis
					 */
					if (numericCheck.split("-")[i].equals("true")
							&& Double.parseDouble(totalLevel.split("-")[i]) 
							>= Double.parseDouble(catCutOff)) {
						double data = Double.parseDouble(lineArr[0].split("-")[1]);
						
						/* Computing the the mean squared difference and summing up
						 * for calculating the variance
						 */
						varSum.set(i, varSum.get(i) + ((data - Double.parseDouble(mean.split("-")[i]))
								*(data - Double.parseDouble(mean.split("-")[i]))));
					
						double maxPoint = Double.parseDouble(dataPoint.split("-")[i]) 
								+ Double.parseDouble(dataPoint.split("-")[i]);
						double minPoint = Double.parseDouble(min.split("-")[i]);
						double buck = Double.parseDouble(bucket.split("-")[i]);
							
						for (int j = 1; j <= buck; j++) {
							if (j == buck) {
								if (data <= Double.parseDouble(max.split("-")[i]) 
										&& data >= minPoint) {
									context.write(new EDAComparator(j + "" + lineArr[0] + "-" + 
										totalLevel.split("-")[i]), new Text(lineArr[1]));
								}
							}
							else {
								if (data <= maxPoint && data >= minPoint) {
									context.write(new EDAComparator(j + "" + lineArr[0] + "-" + 
											totalLevel.split("-")[i]), new Text(lineArr[1]));
								}
								minPoint = maxPoint + 1;
								maxPoint = maxPoint + 1 + Double.parseDouble(dataPoint.split("-")[i]);
							}
						}
					}
					else if (Double.parseDouble(totalLevel.split("-")[i]) 
							< Double.parseDouble(catCutOff)) {
						if ((lineArr[0].split("l")[1].split("-")[0]).equals(col)) {
							context.write(new EDAComparator(lineArr[0] + "-" + totalLevel.split("-")[i]), 
									new Text(lineArr[1]));
						}
					}
				}
				/*else if (Double.parseDouble(totalLevel.split("-")[i]) 
						< Double.parseDouble(catCutOff)) {
					if ((lineArr[0].split("l")[1].split("-")[0]).equals(col)) {
						context.write(new EDAComparator(lineArr[0] + "-" + totalLevel.split("-")[i]), 
								new Text(lineArr[1]));
					}
				}*/
				i++;
			}
		}
	}
	
	// Cleanup function to output the sum of the squared difference for variation
	public void cleanup(Context context) throws IOException, InterruptedException {
		String varianceSum = "";
		for (int i = 0; i < varSum.size(); i++) {
			if (numericCheck.split("-")[i].equals("true")
					&& Double.parseDouble(totalLevel.split("-")[i]) 
					>= Double.parseDouble(catCutOff)) {
				if (i == 0)
					varianceSum = varSum.get(i).toString();
				else
					varianceSum = varianceSum + "-" + varSum.get(i).toString();
			}
			else if (numericCheck.split("-")[i].equals("false")) {
				if (i == 0)
					varianceSum = "nonNumeric";
				else
					varianceSum = varianceSum + "-" + "nonNumeric";
			}
			else if (Double.parseDouble(totalLevel.split("-")[i]) 
					< Double.parseDouble(catCutOff)) {
				if (i == 0)
					varianceSum = "freq";
				else
					varianceSum = varianceSum + "-" + "freq";
			}
		}
		context.write(new EDAComparator("varSum"), new Text(varianceSum));
	}
}
