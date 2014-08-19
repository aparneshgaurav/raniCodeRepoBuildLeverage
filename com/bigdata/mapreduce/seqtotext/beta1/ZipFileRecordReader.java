/**
 * Copyright 2011 Michael Cutler <m@cotdp.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *	 http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.bigdata.mapreduce.seqtotext.beta1;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.tools.tar.TarEntry;
import org.apache.tools.tar.TarInputStream;

/**
 * This RecordReader implementation extracts individual files from a ZIP
 * file and hands them over to the Mapper. The "key" is the decompressed
 * file name, the "value" is the file contents.
 */
public class ZipFileRecordReader implements RecordReader<Text, BytesWritable>
{
	/** InputStream used to read the ZIP file from the FileSystem */
	private FSDataInputStream fsin;

	/** ZIP file parser/decompresser */
	//    private ZipInputStream zip;
	/**
	 * Tar file parser
	 */
	private TarInputStream tar;
	/** Uncompressed file name */
	private Text currentKey;

	/** Uncompressed file contents */
	private BytesWritable currentValue;

	/** Used to indicate progress */
	private boolean isFinished = false;

	/**
	 * Initialise and open the ZIP file from the FileSystem
	 */public ZipFileRecordReader(){
		 
	 }
	public ZipFileRecordReader( Configuration conf, org.apache.hadoop.mapred.FileSplit split )
			throws IOException, InterruptedException
			{
		Path path = split.getPath();
		FileSystem fs = path.getFileSystem( conf );
		System.out.println("file system replication : "+fs.getDefaultReplication());
		// Open the stream
		fsin = fs.open( path );
		// zip = new ZipInputStream(fsin);
		tar = new TarInputStream(fsin);
		System.out.println("tar input stream is : "+tar.toString());
			}

	/**
	 * This is where the magic happens, each ZipEntry is decompressed and
	 * readied for the Mapper. The contents of each file is held *in memory*
	 * in a BytesWritable object.
	 * 
	 * If the ZipFileInputFormat has been set to Lenient (not the default),
	 * certain exceptions will be gracefully ignored to prevent a larger job
	 * from failing.
	 */


	/**
	 * Rather than calculating progress, we just keep it simple
	 */


	/**
	 * Returns the current key (name of the zipped file)
	 */



	/**
	 * Close quietly,ignoring any exceptions
	 */
	@Override
	public void close()
			throws IOException
			{
		try { tar.close(); } catch ( Exception ignore ) { }
		try { fsin.close(); } catch ( Exception ignore ) { }
			}

	@Override
	public Text createKey() {
		// TODO Auto-generated method stub
		return currentKey;
	}

	@Override
	public BytesWritable createValue() {
		// TODO Auto-generated method stub
		return currentValue;
	}

	@Override
	public long getPos() throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public boolean next(Text arg0, BytesWritable arg1) throws IOException {
		// TODO Auto-generated method stub
		//      ZipEntry entry = null;
		TarEntry entry = null;
		try
		{
			System.out.println("here the latest tar status is : "+tar.getRecordSize());
			entry = tar.getNextEntry();
		}
		catch (Exception e)
		{
			if (ZipFileInputFormat.getLenient() == false )
				throw e;
		}

		// Sanity check
		if ( entry == null )
		{
			isFinished = true;
			return false;
		}

		// Filename
		currentKey = new Text(entry.getName());

		// Read the file contents
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		byte[] temp = new byte[8192];
		while ( true )
		{
			int bytesRead = 0;
			try
			{
				bytesRead = tar.read( temp, 0, 8192 );
			}
			catch ( EOFException e )
			{
				if ( ZipFileInputFormat.getLenient() == false )
					throw e;
				return false;
			}
			if ( bytesRead > 0 )
				bos.write( temp, 0, bytesRead );
			else
				break;
		}
		tar.close();

		// Uncompressed contents
		currentValue = new BytesWritable( bos.toByteArray() );
		return true;

	}

	@Override
	public float getProgress() throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}
}
