package com.poc.rcm.sparkcore;

import java.io.FileNotFoundException;

import javax.xml.xquery.XQException;

import com.xml.parser.xQueryParser;

/*Since that method , execute was static marked , hence you called from class name . 
Else had it not been static , you would have created an object and then called that method . 
*/

public class RunXQueryParser {
	
	public static void main(String[] args) throws FileNotFoundException, XQException{
		System.out.println("hello");
		xQueryParser.execute();
	}

}
