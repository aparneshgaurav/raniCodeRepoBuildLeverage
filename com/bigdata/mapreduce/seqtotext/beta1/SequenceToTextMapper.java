package com.bigdata.mapreduce.seqtotext.beta1;
/*********************************************************************************************************
 **           Mapper 
 **           formatProject/FormatConverterTextToSequence/src/FormatConverterMapper.java 
 **           Reads text file and emits the contents out as key-value pairs
 *********************************************************************************************************/

import java.io.IOException;
import java.io.StringReader;
import java.io.Writer;
import java.text.SimpleDateFormat;
import java.util.Date;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
public class SequenceToTextMapper implements Mapper<Text, BytesWritable, Text, Text> {
	Logger logger = Logger.getLogger(SequenceToTextMapper.class);
	
	public static Document loadXMLFromString(String xml) throws Exception
	{
		DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
		DocumentBuilder builder = factory.newDocumentBuilder();
		InputSource is = new InputSource(new StringReader(xml));
		return builder.parse(is);
	}

	private static String getTagValue(String sTag, Element eElement) {

                NodeList nlList = eElement.getElementsByTagName(sTag).item(0).getChildNodes();
                Node nValue = (Node) nlList.item(0);

		String returnValue = "";
                if(nlList.getLength() != 0 ) {

                        returnValue = nValue.getNodeValue();
                }
                //return nValue.getNodeValue();
                return returnValue;
        }

	@Override
	public void configure(JobConf arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void map(Text key, BytesWritable value,
			OutputCollector<Text, Text> context, Reporter arg3) throws IOException {
		// TODO Auto-generated method stub
		byte[] arrayOfBytes = value.getBytes();
        String xmlString = new String(arrayOfBytes);
        context.collect(key, new Text(xmlString));
	}
}

