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
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

/*
 * *************************************************************************** 
 * ************************** Second reduce class **************************** 
 * ***************************************************************************
 * */
public class EDAReduce2 extends Reducer<EDAComparator, Text, Text, Text> {
	
	private static String dataPoint = "";
	private static String bucket = "";
	private static String count = "";
	private static String level = "";
	private static String max = "";
	private static String min = "";
	private static String mean = "";
	private static String numericCheck = "";
	private static ArrayList<Double> variance = new ArrayList<Double>();
	private static ArrayList<Double> stdDeviation = new ArrayList<Double>();
	private static ArrayList<Double> stdErrMean = new ArrayList<Double>();
	private static ArrayList<Double> coeffVar = new ArrayList<Double>();
	private static String medianPos = "";
	private static String q1Pos = "";
	private static String q2Pos = "";
	private static String q3Pos = "";
	private static String percentilePos = "";
	
	private static String columns = "";
	private static String catCutOff = "";
	private static ArrayList<String> bucketCount = new ArrayList<String>();
	
	private static ArrayList<Double> cumFreq = new ArrayList<Double>();
	private static ArrayList<Double> modeCheck = new ArrayList<Double>();
	private static ArrayList<String> mode = new ArrayList<String>();
	
	private MultipleOutputs<Text, Text> mos;
	
	// Setup method for second reduce class
	public void setup(Context context) throws IOException, InterruptedException {
		
		mos = new MultipleOutputs<Text, Text>(context);
		columns = context.getConfiguration().get("columns");
		catCutOff = context.getConfiguration().get("catCutOff");
		String columnArr[] = columns.split(",");
		
		for (String col : columnArr) {
			variance.add(0.0);
			stdDeviation.add(0.0);
			stdErrMean.add(0.0);
			coeffVar.add(0.0);
		}
		
		for (int i = 0; i < columnArr.length; i++) {
			cumFreq.add(0.0);
			modeCheck.add(Double.MIN_VALUE);
			mode.add(" ");
		}
		
		Path[] filePath = DistributedCache.getLocalCacheFiles(context.getConfiguration());
		for (int f = 0; f < filePath.length; f++) {
			BufferedReader br = new BufferedReader(new FileReader(filePath[f].toString()));
		
			String str = "";
			while((str = br.readLine()) != null) {
				
				// Extracting numericCheck from cache file
				if (str.contains("numericCheck")) {
					for (String s : str.split("\t")[1].split("-")) {
						if (numericCheck.equals(""))
							numericCheck = s;
						else
							numericCheck = numericCheck + "-" + s;
					}
				}
				
				// Extracting count from cache file
				if (str.contains("count")) {
					for (String s : str.split("\t")[1].split("-")) {
						if (count.equals(""))
							count = s;
						else
							count = count + "-" + s;
					}
				}
				
				// Extracting level from cache file
				if (str.contains("levels")) {
					for (String s : str.split("\t")[1].split("-")) {
						if (level.equals(""))
							level = s;
						else
							level = level + "-" + s;
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
				
				// Extracting bucket from cache file
				if (str.contains("bucket")) {
					for (String s : str.split("\t")[1].split("-")) {
						if (bucket.equals(""))
							bucket = s;
						else
							bucket = bucket + "-" + s;
					}
					
					String bucketCountCol = "";
					String bucketArr[] = bucket.split("-");
					
					for (int i = 0; i < bucketArr.length; i++) {
						if (bucketArr[i].equals("nonNumeric") 
								|| bucketArr[i].equals("freq")) {
							bucketCount.add(bucketArr[i]);
						}
						else {
							for (int j = 0; j < Double.parseDouble(bucket.split("-")[i]); j++) {
								if (j == 0)
									bucketCountCol = "0";
								else
									bucketCountCol = bucketCountCol + "-" + "0";
							}
							bucketCount.add(bucketCountCol);
						}
					}
				}
				
				// Extracting medianPos from cache file
				if (str.contains("medianPos")) {
					for (String s : str.split("\t")[1].split("-")) {
						if (medianPos.equals(""))
							medianPos = s;
						else
							medianPos = medianPos + "-" + s;
					}
				}
				// Extracting q1Pos from cache file
				if (str.contains("q1Pos")) {
					for (String s : str.split("\t")[1].split("-")) {
						if (q1Pos.equals(""))
							q1Pos = s;
						else
							q1Pos = q1Pos + "-" + s;
					}
				}
				// Extracting q2Pos from cache file
				if (str.contains("q2Pos")) {
					for (String s : str.split("\t")[1].split("-")) {
						if (q2Pos.equals(""))
							q2Pos = s;
						else
							q2Pos = q2Pos + "-" + s;
					}
				}
				// Extracting q3Pos from cache file
				if (str.contains("q3Pos")) {
					for (String s : str.split("\t")[1].split("-")) {
						if (q3Pos.equals(""))
							q3Pos = s;
						else
							q3Pos = q3Pos + "-" + s;
					}
				}
				// Extracting percentilePos from cache file
				if (str.contains("percentilePos")) {
					for (String s : str.split("\t")[1].split("-")) {
						if (percentilePos.equals(""))
							percentilePos = s;
						else
							percentilePos = percentilePos + "-" + s;
					}
				}
			}
		}
	}
	
	// Reduce function for the second reduce class
	public void reduce(EDAComparator inputKey, Iterable<Text> values, Context context) 
			throws IOException, InterruptedException {
		
		String columnArr[] = columns.split(",");
		
		// Computes final variance, SD, std err mean, coeff of variation
		if (inputKey.toString().equals("varSum")) {
			ArrayList<Boolean> numeric = new ArrayList<Boolean>();
			ArrayList<String> analyse = new ArrayList<String>(); 
			for (Text val : values) {
				int i = 0;
				for (String s : val.toString().split("-")) {
					numeric.add(true);
					analyse.add("univ");
					/* Checking if numeric column and cat.cut.off range is for
					 * univariate analysis
					 */
					if (s.equals("nonNumeric") == false 
							&& s.equals("freq") == false) {
						variance.set(i, variance.get(i) + Double.parseDouble(s));
					}
					else if (s.equals("nonNumeric"))
						numeric.set(i, false);
					else if (s.equals("freq"))
						analyse.set(i, "freq");
					i++;
				}
			}
			
			int i = 0;
			for (double d : variance) {
				/* Compute only if numeric column and cat.cut.off range is for
				 * univariate analysis
				 */
				if (numeric.get(i) == true && analyse.get(i).equals("univ")) {
					variance.set(i, variance.get(i)/Double.parseDouble(count.split("-")[i]));
					stdDeviation.set(i, Math.sqrt(variance.get(i)));
					stdErrMean.set(i, stdDeviation.get(i)/Math.sqrt(Double.parseDouble(
							count.split("-")[i])));
					coeffVar.set(i, (stdDeviation.get(i)/Double.parseDouble(mean.split("-")[i]))
							*100);
					Double tStatistic = Double.parseDouble(mean.split("-")[i])
							* (Math.sqrt(Double.parseDouble(count.split("-")[i]))/stdDeviation.get(i));
			
					mos.write("output2", new Text("variance-col" + columnArr[i]), 
							new Text(variance.get(i).toString()));
					mos.write("output2", new Text("stdDeviation-col" + columnArr[i]), 
							new Text(stdDeviation.get(i).toString()));
					mos.write("output2", new Text("stdErrMean-col" + columnArr[i]), 
							new Text(stdErrMean.get(i).toString()));
					mos.write("output2", new Text("coeffVar-col" + columnArr[i]), 
							new Text(coeffVar.get(i).toString()));
					mos.write("output2", new Text("tStatistic-col" + columnArr[i]), 
							new Text(tStatistic.toString()));
				}
				// If non-numeric column, then the output values are "nonNumeric" string
				else if (numeric.get(i) == false) {
					mos.write("output2", new Text("variance-col" + columnArr[i]), 
							new Text("nonNumeric"));
					mos.write("output2", new Text("stdDeviation-col" + columnArr[i]), 
							new Text("nonNumeric"));
					mos.write("output2", new Text("stdErrMean-col" + columnArr[i]), 
							new Text("nonNumeric"));
					mos.write("output2", new Text("coeffVar-col" + columnArr[i]), 
							new Text("nonNumeric"));
					mos.write("output2", new Text("tStatistic-col" + columnArr[i]), 
							new Text("nonNumeric"));
				}
				/* If cat.cut.off is for frequency analysis, then output values
				 * are "freq" string
				 */
				else if (analyse.get(i).equals("freq")) {
					mos.write("output2", new Text("variance-col" + columnArr[i]), 
							new Text("freq"));
					mos.write("output2", new Text("stdDeviation-col" + columnArr[i]), 
							new Text("freq"));
					mos.write("output2", new Text("stdErrMean-col" + columnArr[i]), 
							new Text("freq"));
					mos.write("output2", new Text("coeffVar-col" + columnArr[i]), 
							new Text("freq"));
					mos.write("output2", new Text("tStatistic-col" + columnArr[i]), 
							new Text("freq"));
				}
					
				i++;
			}
		}
		else {
			String key = inputKey.toString().split("-")[0] + "-" + inputKey.toString().split("-")[1];
			
			String outVal = "";
			for (Text val : values) {
				outVal = val.toString();
			}
			
			// If level is more than cut off value, then forming bucket
			if (Double.parseDouble(inputKey.toString().split("-")[2]) 
					>= Double.parseDouble(catCutOff)) {
				for (int i = 0; i < columnArr.length; i++) {
					if ((key.split("l")[1].split("-")[0]).equals(columnArr[i])) {
						if (numericCheck.split("-")[i].equals("true")) {
							String elementArr[] = bucketCount.get(i).split("-");
							String element = "";
							double newElement = 0;
							
							for (int j = 1; j <= elementArr.length; j++) {
								if (j == Double.parseDouble(key.split("c")[0])) {
									newElement = Double.parseDouble(elementArr[j-1]) 
											+ Double.parseDouble(outVal);
									if (j == 1)
										element = newElement + "";
									else
										element = element + "-" + newElement + "";
								}
								else {
									if (j == 1)
										element = elementArr[j-1];
									else
										element = element + "-" + elementArr[j-1];
								}
							}	
							bucketCount.set(i, element);
							context.write(new Text(key), new Text(outVal));
						}
					}
				} 
			}
			// If level is less than the cut off value, then performing frequency analysis
			else if (Double.parseDouble(inputKey.toString().split("-")[2]) 
					< Double.parseDouble(catCutOff)) {
				//int i = 0;
				for (String s : columnArr) {
					if ((key.split("l")[1].split("-")[0]).equals(s)) {
						
						int i = Integer.parseInt(s);
						
						// Computing cumFreq, freq% and cumFreq%
						cumFreq.set(i, Double.parseDouble(outVal) + cumFreq.get(i));
						double freqPercent = Double.parseDouble(outVal)
								/Double.parseDouble(count.split("-")[i]);
						double cumFreqPercent = cumFreq.get(i)
								/Double.parseDouble(count.split("-")[i]);
						
						// Computing mode
						if (Double.parseDouble(outVal) > modeCheck.get(i)) {
							modeCheck.set(i, Double.parseDouble(outVal));
							mode.set(i, key.split("-")[1]);
						}
						
						mos.write("frequency", 
								new Text("freq" + key.split("-")[1] + "-" + "col" + s), 
								new Text(Math.round(Double.parseDouble(outVal)) + "," + freqPercent + "," + 
										(int) Math.round(cumFreq.get(i)) + "," + 
										cumFreqPercent));
						
						break;
					}
					//i++;
				}
			}
		}
	}
	
	public void cleanup(Context context) throws IOException, InterruptedException {
		
		String newBucketCount = "";
		for (int i = 0; i < bucketCount.size(); i++) {
			if (i == 0)
				newBucketCount = bucketCount.get(i);
			else
				newBucketCount = newBucketCount + "," + bucketCount.get(i);
		}
		mos.write("newCacheFile", new Text("bucketCount"), new Text(newBucketCount));
		mos.write("newCacheFile", new Text("medianPos"), new Text(medianPos));
		mos.write("newCacheFile", new Text("q1Pos"), new Text(q1Pos));
		mos.write("newCacheFile", new Text("q2Pos"), new Text(q2Pos));
		mos.write("newCacheFile", new Text("q3Pos"), new Text(q3Pos));
		mos.write("newCacheFile", new Text("percentilePos"), new Text(percentilePos));
		
		String columnArr[] = columns.split(",");
		
		for (int i = 0; i < columnArr.length; i++) {
			if (mode.get(i).equals(" ") == false) {
				mos.write("frequency", new Text("mode-col" + columnArr[i]), new Text(mode.get(i)));
				mos.write("frequency", new Text("count-col" + columnArr[i]), 
						new Text(Math.round(Double.parseDouble(count.split("-")[i])) + ""));
				mos.write("frequency", new Text("levels-col" + columnArr[i]), 
						new Text(Math.round(Double.parseDouble(level.split("-")[i])) + ""));
			}
		}
		mos.close();
	}
}