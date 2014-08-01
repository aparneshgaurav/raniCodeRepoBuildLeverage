package com.bigdata.mapreduce.eda;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;


/*
	 * *************************************************************************** 
	 * ************************* First reduce class ******************************
	 * This class computes the final count, sum, max, min, level for the columns
	 * and also check whether any column has non-numeric data. Finally, the mean
	 * and range is computed and with the found information, buckets for the 
	 * columns are formed based on which the data will be distributed in the 
	 * next jobs for computing percentiles. 
	 * ***************************************************************************
	 */
	public class EDAReduce extends Reducer<EDAComparator, Text, Text, Text> {
		
		// Declaring the global static variables for reduce
		private static ArrayList<String> count = new ArrayList<String>();
		private static ArrayList<String> sum = new ArrayList<String>();
		private static ArrayList<String> max = new ArrayList<String>();
		private static ArrayList<String> min = new ArrayList<String>();
		private static ArrayList<String> totalLevel = new ArrayList<String>();
		private static ArrayList<Boolean> numericCheck = new ArrayList<Boolean>();
		
		private static String columns;
		private static String catCutOff;
		private static String percentilePoints;
		private MultipleOutputs<Text, Text> mos;
		
		// Setup method which will be called only once
		public void setup(Context context) {
			
			mos = new MultipleOutputs<Text, Text>(context);
			columns = context.getConfiguration().get("columns");
			catCutOff = context.getConfiguration().get("catCutOff");
			percentilePoints = context.getConfiguration().get("percentilePoints");
			
			String columnArr[] = columns.split(",");
		
			for (int i = 0; i < columnArr.length; i++) {
				count.add("0");
				sum.add("0");
				max.add("0");
				min.add(Integer.MAX_VALUE + "");
				totalLevel.add("0");
				numericCheck.add(true);
			}
		}
		
		// The reduce method for the first reduce class
		public void reduce(EDAComparator key, Iterable<Text> values, Context context) 
				throws IOException, InterruptedException {
			
			String columnArr[] = columns.split(",");
			
			// Computing frequency
			if (key.toString().contains("col")) {
				double freqCount = 0;
				for (Text val : values) {
					freqCount += Double.parseDouble(val.toString());
				}
				context.write(new Text(key.toString()),  new Text(freqCount + ""));
			}
			
			// Computing the final numeric check for columns
			if (key.toString().equals("numericCheck")) {
				String finalNumericCheck = "";
				for (Text val : values) {
					for (int i = 0; i < val.toString().split("-").length; i++) {
						if (val.toString().split("-")[i].equals("false"))
							numericCheck.set(i, false);
						if (i == 0)
							finalNumericCheck += numericCheck.get(i);
						else
							finalNumericCheck = finalNumericCheck + "-" + numericCheck.get(i);
					}
				}
				mos.write("cacheFile", new Text("numericCheck"), new Text(finalNumericCheck));
			}
			
			
			// Computing the total count for each column
			if (key.toString().equals("count")) {
				double newCount = 0;
				String totalCount = "";
				for (Text val : values) {
					for (int i = 0; i < val.toString().split("-").length; i++) {
						newCount = Double.parseDouble(count.get(i)) 
								+ Double.parseDouble(val.toString().split("-")[i]);
						count.set(i, newCount + "");
						mos.write("output1", new Text("count-col" + columnArr[i]), 
								new Text(Math.round(Double.parseDouble(count.get(i))) + ""));
						if (i == 0)
							totalCount += count.get(i);
						else
							totalCount = totalCount + "-" + count.get(i);
					}
				}
				mos.write("cacheFile", new Text("count"), new Text(totalCount));
			}
			
			// Computing the total level for each column
			if (key.toString().equals("level")) {
				double newLevel = 0;
				String totalLevelOut = "";
				for (Text val : values) {
					for (int i = 0; i < val.toString().split("-").length; i++) {
						newLevel = Double.parseDouble(totalLevel.get(i)) 
								+ Double.parseDouble(val.toString().split("-")[i]);
						totalLevel.set(i, newLevel + "");
						mos.write("output1", new Text("levels-col" + columnArr[i]), 
								new Text(Math.round(Double.parseDouble(totalLevel.get(i))) + ""));
						if (i == 0)
							totalLevelOut += totalLevel.get(i);
						else
							totalLevelOut = totalLevelOut + "-" + totalLevel.get(i);
					}
				}
				mos.write("cacheFile", new Text("levels"), new Text(totalLevelOut));
			}
			
			// Computing the total sum for each column
			if (key.toString().equals("sum")) {
				double newSum = 0;
				String totalSum = "";
				for (Text val : values) {
					for (int i = 0; i < val.toString().split("-").length; i++) {
						if (numericCheck.get(i) == false)
							sum.set(i, "nonNumeric");
						else if (Double.parseDouble(totalLevel.get(i)) 
								< Double.parseDouble(catCutOff)) {
							sum.set(i, "freq");
						}
						else {
							newSum = Double.parseDouble(sum.get(i)) 
									+ Double.parseDouble(val.toString().split("-")[i]);
							sum.set(i, newSum + "");
							mos.write("output1", new Text("sum-col" + columnArr[i]), 
									new Text(sum.get(i)));
						}
						if (i == 0)
							totalSum += sum.get(i);
						else
							totalSum = totalSum + "-" + sum.get(i);						
					}
				}
				mos.write("cacheFile", new Text("sum"), new Text(totalSum));
			}
			
			// Computing the overall max for each column
			if (key.toString().equals("max")) {
				double newMax = 0;
				String finalMax = "";
				for (Text val : values) {
					for (int i = 0; i < val.toString().split("-").length; i++) {
						if (numericCheck.get(i) == false)
							max.set(i, "nonNumeric");
						else if (Double.parseDouble(totalLevel.get(i)) 
								< Double.parseDouble(catCutOff)) {
							max.set(i, "freq");
						}
						else {
							newMax = Math.max(Double.parseDouble(max.get(i)), 
									Double.parseDouble(val.toString().split("-")[i]));
							max.set(i, newMax + "");
							mos.write("output1", new Text("max-col" + columnArr[i]), 
									new Text(max.get(i)));
						}
						if (i == 0)
							finalMax += max.get(i);
						else
							finalMax = finalMax + "-" + max.get(i);
					}
				}
				mos.write("cacheFile", new Text("max"), new Text(finalMax));
			}
			
			// Computing the overall min for each column
			if (key.toString().equals("min")) {
				double newMin = 0;
				String finalMin = "";
				for (Text val : values) {
					for (int i = 0; i < val.toString().split("-").length; i++) {
						if (numericCheck.get(i) == false)
							min.set(i, "nonNumeric");
						else if (Double.parseDouble(totalLevel.get(i)) 
								< Double.parseDouble(catCutOff)) {
							min.set(i, "freq");
						}
						else {
							newMin = Math.min(Double.parseDouble(min.get(i)), 
									Double.parseDouble(val.toString().split("-")[i]));
							min.set(i, newMin + "");
							mos.write("output1", new Text("min-col" + columnArr[i]), 
									new Text(min.get(i)));
						}
						if (i == 0)
							finalMin += min.get(i);
						else
							finalMin = finalMin + "-" + min.get(i);
					}
				}
				mos.write("cacheFile", new Text("min"), new Text(finalMin));
			}
		}
		
		/* Cleanup method which will be called at the end to compute mean, range and buckets.
		 * This method will also compute the positions for percentiles. 
		 */
		public void cleanup(Context context) 
				throws IOException, InterruptedException {
			
			String columnArr[] = columns.split(",");
			String percentileArr[] = percentilePoints.split(",");
			
			if (count.get(0).equals("0") == false) {
				String mean = "";
				String medianPos = "";
				String q1Pos = "";
				String q2Pos = "";
				String q3Pos = "";
				String percentilePos = "";
				String range = "";
				String bucket = "";
				String dataPoint = "";
				
				double newMean = 0;
				double newMedianPos = 0;
				double newQ1Pos = 0;
				double newQ2Pos = 0;
				double newQ3Pos = 0;
				double newRange = 0;
				double newBucket = 0;
				double newDataPoint = 0;
				
				for (int i = 0; i < count.size(); i++) {
					/* Checking if column has any non-numeric data and level is
					 * greater than cut off value
					 */
					if (numericCheck.get(i) == true 
							&& Double.parseDouble(totalLevel.get(i)) 
							>= Double.parseDouble(catCutOff)) {
						
						// Computing mean
						newMean = Double.parseDouble(sum.get(i))/Double.parseDouble(count.get(i));
					
						// Computing median position
						if (Double.parseDouble(count.get(i)) % 2 != 0)
							newMedianPos = (Double.parseDouble(count.get(i)) + 1)/2;
						else
							newMedianPos = Double.parseDouble(count.get(i))/2;
					
						// Computing quartile positions
						newQ1Pos = (long) Math.round(Double.parseDouble(count.get(i))*0.25);
						newQ2Pos = (long) Math.round(Double.parseDouble(count.get(i))*0.5);
						newQ3Pos = (long) Math.round(Double.parseDouble(count.get(i))*0.75);
						
						// Computing percentile positions
						ArrayList<Long> tempPercentilePos = new ArrayList<Long>();
						for (int j = 0; j < percentileArr.length; j++) {
							double perc = Math.round(Double.parseDouble(count.get(i))
									* Double.parseDouble(percentileArr[j]));
							if (perc == 0)
								perc = 1;
							
							tempPercentilePos.add((long) perc);
						}
						
						// Computing range
						newRange = Double.parseDouble(max.get(i)) - Double.parseDouble(min.get(i));
						
						// Computing number of buckets and bucket points
						newBucket = (long) Math.round(
								(Math.log(Double.parseDouble(count.get(i)))/Math.log(2)));
						newDataPoint = Math.round(newRange/newBucket) + 1;
					
						if (i == 0) {
							mean = newMean + "";
							medianPos = newMedianPos + "";
							q1Pos = newQ1Pos + "";
							q2Pos = newQ2Pos + "";
							q3Pos = newQ3Pos + "";
							int j = 0;
							for (long p : tempPercentilePos) {
								if (j == 0)
									percentilePos = p + "";
								else
									percentilePos = percentilePos + "," + p;
								
								j++;
							}
							range = newRange + "";
							bucket = newBucket + "";
							dataPoint = newDataPoint + "";
						}
						else {
							mean = mean + "-" + newMean;
							medianPos = medianPos + "-" + newMedianPos;
							q1Pos = q1Pos + "-" + newQ1Pos;
							q2Pos = q2Pos + "-" + newQ2Pos;
							q3Pos = q3Pos + "-" + newQ3Pos;
							int j = 0;
							for (long p : tempPercentilePos) {
								if (j == 0)
									percentilePos = percentilePos + "-" + p;
								else
									percentilePos = percentilePos + "," + p;
								
								j++;
							}
							range = range + "-" + newRange;
							bucket = bucket + "-" + newBucket;
							dataPoint = dataPoint + "-" + newDataPoint;
						}
					
						mos.write("output1", new Text("mean-col" + columnArr[i]), 
								new Text(newMean + ""));
						mos.write("output1", new Text("range-col" + columnArr[i]), 
								new Text(newRange + ""));
					}
					// If non-numeric column, then the output is "nonNumeric" string
					else if (numericCheck.get(i) == false) {
						if (i == 0) {
							mean = "nonNumeric";
							medianPos = "nonNumeric";
							q1Pos = "nonNumeric";
							q2Pos = "nonNumeric";
							q3Pos = "nonNumeric";
							percentilePos = "nonNumeric";
							range = "nonNumeric";
							bucket = "nonNumeric";
							dataPoint = "nonNumeric";
						}
						else {
							mean = mean + "-" + "nonNumeric";
							medianPos = medianPos + "-" + "nonNumeric";
							q1Pos = q1Pos + "-" + "nonNumeric";
							q2Pos = q2Pos + "-" + "nonNumeric";
							q3Pos = q3Pos + "-" + "nonNumeric";
							percentilePos = percentilePos + "-" + "nonNumeric";
							range = range + "-" + "nonNumeric";
							bucket = bucket + "-" + "nonNumeric";
							dataPoint = dataPoint + "-" + "nonNumeric";
						}
						
						mos.write("output1", new Text("mean-col" + columnArr[i]), 
								new Text("nonNumeric"));
						mos.write("output1", new Text("range-col" + columnArr[i]), 
								new Text("nonNumeric"));
					}
					// If level is less than cut off value, then output is "freq" string
					else if (Double.parseDouble(totalLevel.get(i)) 
							< Double.parseDouble(catCutOff)) {
						if (i == 0) {
							mean = "freq";
							medianPos = "freq";
							q1Pos = "freq";
							q2Pos = "freq";
							q3Pos = "freq";
							percentilePos = "freq";
							range = "freq";
							bucket = "freq";
							dataPoint = "freq";
						}
						else {
							mean = mean + "-" + "freq";
							medianPos = medianPos + "-" + "freq";
							q1Pos = q1Pos + "-" + "freq";
							q2Pos = q2Pos + "-" + "freq";
							q3Pos = q3Pos + "-" + "freq";
							percentilePos = percentilePos + "-" + "freq";
							range = range + "-" + "freq";
							bucket = bucket + "-" + "freq";
							dataPoint = dataPoint + "-" + "freq";
						}
						
						mos.write("output1", new Text("mean-col" + columnArr[i]), 
								new Text("freq"));
						mos.write("output1", new Text("range-col" + columnArr[i]), 
								new Text("freq"));
					}
				}
				
				mos.write("cacheFile", new Text("mean"), new Text(mean));			
				mos.write("cacheFile", new Text("medianPos"), new Text(medianPos));
				mos.write("cacheFile", new Text("q1Pos"), new Text(q1Pos));
				mos.write("cacheFile", new Text("q2Pos"), new Text(q2Pos));
				mos.write("cacheFile", new Text("q3Pos"), new Text(q3Pos));
				mos.write("cacheFile", new Text("percentilePos"), new Text(percentilePos));
				mos.write("cacheFile", new Text("range"), new Text(range));
				mos.write("cacheFile", new Text("bucket"), new Text(bucket));
				mos.write("cacheFile", new Text("dataPoint"), new Text(dataPoint));
			}
			mos.close();
		}
	}