package com.bigdata.mapreduce.eda;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

/*
 * *************************************************************************** 
 * ************************** Third reduce class ***************************** 
 * ***************************************************************************
 * */
public class EDAReduce3 extends Reducer<EDAComparator, Text, Text, Text> {
	
	private static String medianPos = "";
	private static String q1Pos = ""; 
	private static String q2Pos = "";
	private static String q3Pos = "";
	private static String percentilePos = "";
	private static String bucketCount = "";
	private static String medianBucket = "";
	private static String q1Bucket = ""; 
	private static String q2Bucket = "";
	private static String q3Bucket = "";
	private static String percentileBucket = "";
	
	private static String q1 = "";
	private static String q3 = "";
	
	private static ArrayList<Double> posCount = new ArrayList<Double>();
	private static ArrayList<Double> q1posCount = new ArrayList<Double>();
	private static ArrayList<Double> q2posCount = new ArrayList<Double>();
	private static ArrayList<Double> q3posCount = new ArrayList<Double>();
	private static ArrayList<ArrayList<Double>> percentileposCount 
						= new ArrayList<ArrayList<Double>>();
	
	private static String columns = "";
	private static String percentilePoints = "";
	
	// Setup function for the third reduce class
	public void setup(Context context) throws IOException, InterruptedException {
		
		columns = context.getConfiguration().get("columns");
		percentilePoints = context.getConfiguration().get("percentilePoints");
		
		Path[] filePath = DistributedCache.getLocalCacheFiles(context.getConfiguration());
		for (int i = 0; i < filePath.length; i++) {
			BufferedReader br = new BufferedReader(new FileReader(filePath[i].toString()));
			
			/* Extracting median, quartiles, percentile positions and bucket count
			 * from cache file  
			 */
			String str = "";
			while((str = br.readLine()) != null) {
				if (str.contains("medianPos"))
					medianPos = str.split("\t")[1];
				if (str.contains("q1Pos"))
					q1Pos = str.split("\t")[1];
				if (str.contains("q2Pos"))
					q2Pos = str.split("\t")[1];
				if (str.contains("q3Pos"))
					q3Pos = str.split("\t")[1];
				if (str.contains("percentilePos"))
					percentilePos = str.split("\t")[1];
				if (str.contains("bucketCount")) {
					if (bucketCount.equals(""))
						bucketCount = str.split("\t")[1];
					else
						bucketCount = bucketCount + ":" + str.split("\t")[1];
				}
				// 2-8-5-3-3,6-5-0-1-0:0-1-2-1-0,9-0-1-2-1
			}
		}
		
		String finalBucket = "";
		//String bucketCountArr[] = bucketCount.split(",");
		String bucketCountArr[] = bucketCount.split(":")[0].split(",");
		
		for (int i = 0; i < bucketCountArr.length; i++) {
			if (bucketCountArr[i].equals("nonNumeric") == false
					&& bucketCountArr[i].equals("freq") == false) {
				//for (int j = 0; j < bucketCount.split(",")[i].split("-").length; j++) {
				for (int j = 0; j < bucketCountArr[i].split("-").length; j++) {
					double element = 0;
                    for (String k : bucketCount.split(":")) {
                            element += Double.parseDouble(k.split(",")[i].split("-")[j]);
                    }
                    if (j == 0)
                            finalBucket = finalBucket + element;
                    else if ((j == bucketCountArr[i].split("-").length - 1)
                    		&& (i != bucketCountArr.length - 1)) {
                    	finalBucket = finalBucket + "-" + element + ",";
                    }
                    else
                            finalBucket = finalBucket + "-" + element;
                    /*if (bucketCount.split(",")[i].split("-")[j].contains(":"))
                            break;*/
				}
				/*if (bucketCount.split(",")[i].contains(":"))
					break;
				else
					finalBucket = finalBucket + ",";*/
			}
			else {
				if (i == 0)
					finalBucket = bucketCountArr[i] + ",";
				else if (i == bucketCountArr.length - 1)
					finalBucket = finalBucket + bucketCountArr[i];
				else
					finalBucket = finalBucket + bucketCountArr[i] + ",";
			}
		}
		/*context.write(new Text("bucketCount"), new Text(bucketCount));
		context.write(new Text("finalBucket"), new Text(finalBucket));
		context.write(new Text("medianPos"), new Text(medianPos));*/
		
		String medianPosArr[] = medianPos.split("-");
		
		for (int i = 0; i < medianPosArr.length; i++) {
			// Computes only if numeric column and level is more than cut off value
			if (medianPosArr[i].equals("nonNumeric") == false 
					&& medianPosArr[i].equals("freq") == false) {
			
				double pos = Double.parseDouble(medianPos.split("-")[i]);
				double q1pos = Double.parseDouble(q1Pos.split("-")[i]);
				double q2pos = Double.parseDouble(q2Pos.split("-")[i]);
				double q3pos = Double.parseDouble(q3Pos.split("-")[i]);
				String percentilepos = percentilePos.split("-")[i];
				double count = 0;
				double q1count = 0;
				double q2count = 0;
				double q3count = 0;
				double percentilecount = 0;
				double previousCount = 0;
				double q1previousCount = 0;
				double q2previousCount = 0;
				double q3previousCount = 0;
				double percentilepreviousCount = 0;
				
				// Creating median bucket
				for (int j = 1; j <= finalBucket.split(",")[i].split("-").length; j++) {
					previousCount = count;
					count += Double.parseDouble(finalBucket.split(",")[i].split("-")[j-1]);
					if (count >= pos) {
						if (medianBucket.equals("")) {
							medianBucket = j + "";
							posCount.add(previousCount);
							break;
						}
						else {
							medianBucket = medianBucket + "-" + j;
							posCount.add(previousCount);
							break;
						}
					}
				}
				// Creating q1 bucket
				for (int j = 1; j <= finalBucket.split(",")[i].split("-").length; j++) {
					q1previousCount = q1count;
					q1count += Double.parseDouble(finalBucket.split(",")[i].split("-")[j-1]);
					if (q1count >= q1pos) {
						if (q1Bucket.equals("")) {
							q1Bucket = j + "";
							q1posCount.add(q1previousCount);
							break;
						}
						else {
							q1Bucket = q1Bucket + "-" + j;
							q1posCount.add(q1previousCount);
							break;
						}
					}
				}
				// Creating q2 bucket
				for (int j = 1; j <= finalBucket.split(",")[i].split("-").length; j++) {
					q2previousCount = q2count;
					q2count += Double.parseDouble(finalBucket.split(",")[i].split("-")[j-1]);
					if (q2count >= q2pos) {
						if (q2Bucket.equals("")) {
							q2Bucket = j + "";
							q2posCount.add(q2previousCount);
							break;
						}
						else {
							q2Bucket = q2Bucket + "-" + j;
							q2posCount.add(q2previousCount);
							break;
						}
					}
				}
				// Creating q3 bucket
				for (int j = 1; j <= finalBucket.split(",")[i].split("-").length; j++) {
					q3previousCount = q3count;
					q3count += Double.parseDouble(finalBucket.split(",")[i].split("-")[j-1]);
					if (q3count >= q3pos) {
						if (q3Bucket.equals("")) {
							q3Bucket = j + "";
							q3posCount.add(q1previousCount);
							break;
						}
						else {
							q3Bucket = q3Bucket + "-" + j;
							q3posCount.add(q3previousCount);
							break;
						}
					}
				}
				// Creating percentile bucket
				ArrayList<Double> percentilePosCountTemp = new ArrayList<Double>();
				for (int p = 0; p < percentilePos.split("-")[0].split(",").length; p++) {
					for (int j = 1; j <= finalBucket.split(",")[i].split("-").length; j++) {
						percentilepreviousCount = percentilecount;
						percentilecount += Double.parseDouble(finalBucket.split(",")[i].split("-")[j-1]);
						if (percentilecount >= Double.parseDouble(percentilepos.split(",")[p])) {
							if (percentileBucket.equals("") && p == 0) {
								percentileBucket = j + "";
								percentilePosCountTemp.add(percentilepreviousCount);
								break;
							}
							if ((percentileBucket.equals("") == false) && p == 0) {
								percentileBucket = percentileBucket + "-" + j;
								percentilePosCountTemp.add(percentilepreviousCount);
								break;
							}
							/*if ((p == (percentilePos.length() - 1)) && (i != (medianPosArr.length - 1))) {
								percentileBucket = percentileBucket + "," + j + "-";
								percentilePosCountTemp.add(percentilepreviousCount);
								break;
							}*/
							else {
								percentileBucket = percentileBucket + "," + j;
								percentilePosCountTemp.add(percentilepreviousCount);
								break;
							}
						}
					}
					percentilecount = 0;
					percentilepreviousCount = 0;
				}
				percentileposCount.add(percentilePosCountTemp);
			}
			else {
				if (medianBucket.equals("")) {
					medianBucket = medianPosArr[i];
				}
				else {
					medianBucket = medianBucket + "-" + medianPosArr[i];
				}
				if (q1Bucket.equals("")) {
					q1Bucket = medianPosArr[i];
				}
				else {
					q1Bucket = q1Bucket + "-" + medianPosArr[i];
				}
				if (q2Bucket.equals("")) {
					q2Bucket = medianPosArr[i];
				}
				else {
					q2Bucket = q2Bucket + "-" + medianPosArr[i];
				}
				if (q3Bucket.equals("")) {
					q3Bucket = medianPosArr[i];
				}
				else {
					q3Bucket = q3Bucket + "-" + medianPosArr[i];
				}
				if (percentileBucket.equals("")) {
					percentileBucket = medianPosArr[i];
				}
				/*else if (i == (medianPosArr.length - 1))
					percentileBucket = percentileBucket + medianPosArr[i];*/
				else {
					percentileBucket = percentileBucket + "-" + medianPosArr[i];
				}
				posCount.add(0.0);
				q1posCount.add(0.0);
				q2posCount.add(0.0);
				q3posCount.add(0.0);
				percentileposCount.add(posCount);
				
			}
		}
	}
	
	// Reduce function for the third Reduce class
	public void reduce(EDAComparator key, Iterable<Text> values, Context context) 
					throws IOException, InterruptedException {
		
		String columnArr[] = columns.split(",");
		double presentCount = 0;
		for (Text val : values) {
			presentCount = Double.parseDouble(val.toString());
		}
		
		String medianPosArr[] = medianPos.split("-");
		
		for (int i = 0; i < columnArr.length; i++) {
			if (Integer.parseInt(key.toString().split("l")[1].split("-")[0]) 
					== Integer.parseInt(columnArr[i])) {
				
				// Computes only if numeric column and level is more than cut off value
				if (medianPosArr[i].equals("nonNumeric") == false 
						&& medianPosArr[i].equals("freq") == false) {
				
					// Computing median
					if (Double.parseDouble(key.toString().split("c")[0]) 
							== Double.parseDouble(medianBucket.split("-")[i])) {
						double previousCount = posCount.get(i);
						double newCount = previousCount + presentCount;
						if (newCount >= Double.parseDouble(medianPos.split("-")[i])
								&& posCount.get(i) < Double.parseDouble(medianPos.split("-")[i])) {
							
							// Emitting the median output
							context.write(new Text("median-col" + columnArr[i]), 
									new Text(key.toString().split("-")[1]));
							posCount.set(i, newCount);
						}
						else {
							posCount.set(i, newCount);
						}
					}
					
					// Computing 1st quartile
					if (Integer.parseInt(key.toString().split("c")[0]) 
							== Integer.parseInt(q1Bucket.split("-")[i])) {
						double q1PreviousCount = q1posCount.get(i);
						double q1NewCount = q1PreviousCount + presentCount;
						if (q1NewCount >= Double.parseDouble(q1Pos.split("-")[i])
								&& q1posCount.get(i) < Double.parseDouble(q1Pos.split("-")[i])) {
							
							// Emitting the q1 output
							context.write(new Text("quartile1-col" + columnArr[i]), 
									new Text(key.toString().split("-")[1]));
							if (q1.equals(""))
								q1 = key.toString().split("-")[1];
							else
								q1 = q1 + "-" + key.toString().split("-")[1]; 
							q1posCount.set(i, q1NewCount);
						}
						else {
							q1posCount.set(i, q1NewCount);
						}
					}
					
					// Computing 2nd quartile
					if (Integer.parseInt(key.toString().split("c")[0]) 
							== Integer.parseInt(q2Bucket.split("-")[i])) {
						double q2PreviousCount = q2posCount.get(i);
						double q2NewCount = q2PreviousCount + presentCount;
						if (q2NewCount >= Double.parseDouble(q2Pos.split("-")[i])
								&& q2posCount.get(i) < Double.parseDouble(q2Pos.split("-")[i])) {
							
							// Emitting the q2 output
							context.write(new Text("quartile2-col" + columnArr[i]), 
									new Text(key.toString().split("-")[1]));
							q2posCount.set(i, q2NewCount);
						}
						else {
							q2posCount.set(i, q2NewCount);
						}
					}
					
					// Computing 3rd quartile
					if (Integer.parseInt(key.toString().split("c")[0]) 
							== Integer.parseInt(q3Bucket.split("-")[i])) {
						double q3PreviousCount = q3posCount.get(i);
						double q3NewCount = q3PreviousCount + presentCount;
						if (q3NewCount >= Double.parseDouble(q3Pos.split("-")[i])
								&& q3posCount.get(i) < Double.parseDouble(q3Pos.split("-")[i])) {
							
							// Emitting the q3 output
							context.write(new Text("quartile3-col" + columnArr[i]), 
									new Text(key.toString().split("-")[1]));
							if (q3.equals(""))
								q3 = key.toString().split("-")[1];
							else
								q3 = q3 + "-" + key.toString().split("-")[1];
							q3posCount.set(i, q3NewCount);
						}
						else {
							q3posCount.set(i, q3NewCount);
						}
					}
					
					// Computing percentiles
					for (int j = 0; j < percentileBucket.split("-")[i].split(",").length; j++) {
						if (Integer.parseInt(key.toString().split("c")[0]) 
							== Integer.parseInt(percentileBucket.split("-")[i].split(",")[j])) {
							
							double percentilePreviousCount = percentileposCount.get(i).get(j);
							double percentileNewCount = percentilePreviousCount + presentCount;
							if (percentileNewCount 
									>= Double.parseDouble(percentilePos.split("-")[i].split(",")[j])
									&& percentileposCount.get(i).get(j) 
									< Double.parseDouble(percentilePos.split("-")[i].split(",")[j])) {
								
								// Emitting the percentile point output
								context.write(new Text("percentile" + 
										percentilePoints.split(",")[j] + "-col" + columnArr[i]), 
										new Text(key.toString().split("-")[1]));
								
								percentileposCount.get(i).set(j, percentileNewCount);
							}
							else {
								percentileposCount.get(i).set(j, percentileNewCount);
							}
						}
					}
					/*context.write(new Text("medianBucket"), new Text(medianBucket));
					context.write(new Text("percentileBucket"), new Text(percentileBucket));*/
				}
				// If column is non-numeric or level is less that cut off value
				else {
					posCount.set(i, 0.0);
					q1posCount.set(i, 0.0);
					q2posCount.set(i, 0.0);
					q3posCount.set(i, 0.0);
					for (int j = 0; j < percentilePoints.split(",").length; j++) {
						percentileposCount.get(i).set(j, 0.0);
					}
				}
			}
		}
	}
}