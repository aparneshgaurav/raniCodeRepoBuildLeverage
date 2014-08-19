/**
 * Copyright 2011 Michael Cutler <m@cotdp.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.bigdata.mapreduce.seqtotext.beta1;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;

/**
 * Extends the basic FileInputFormat class provided by Apache Hadoop to accept ZIP files. It should be noted that ZIP
 * files are not 'splittable' and each ZIP file will be processed by a single Mapper.
 */
public class ZipFileInputFormat
    extends FileInputFormat<Text, BytesWritable>
{
    /** See the comments on the setLenient() method */
    private static boolean isLenient = false;
    
    /**
     * ZIP files are not splitable
     */


    /**
     * Create the ZipFileRecordReader to parse the file
     */
   
    
    /**
     * 
     * @param lenient
     */
    public static void setLenient( boolean lenient )
    {
        isLenient = lenient;
    }
    
    public static boolean getLenient()
    {
        return isLenient;
    }

	@Override
	public org.apache.hadoop.mapred.RecordReader<Text, BytesWritable> getRecordReader(
			org.apache.hadoop.mapred.InputSplit inputSplit, JobConf jobConf,
			Reporter arg2) throws IOException{
		// TODO Auto-generated method stub
			FileSplit fileSplit = (FileSplit) inputSplit;
			Configuration conf = jobConf;
			
			ZipFileRecordReader zipFileRecordReader = null;
					try {
						zipFileRecordReader = new ZipFileRecordReader( conf,fileSplit);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
//		zipFileRecordReader = new ZipFileRecordReader();
		return zipFileRecordReader;
	}
}
