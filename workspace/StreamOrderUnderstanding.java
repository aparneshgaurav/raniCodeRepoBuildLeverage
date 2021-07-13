package com.poc.rcm.java.eight;

import java.util.ArrayList;
import java.util.List;

public class StreamOrderUnderstanding {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		List<String> listOfString = new ArrayList<String>();
		listOfString.add("z");
		listOfString.add("d");
		listOfString.add("t");
		listOfString.add("i");
		
		listOfString.stream()
	    .filter(record
	    		-> 
	    {
	        System.out.println("filter: " + record);
	        return true;
	    }
	    ).forEach(record
	    		->
	    {
	    	System.out.println("forEach: " + record);
	    });

	}

}
/*
filter: z
forEach: z
filter: d
forEach: d
filter: t
forEach: t
filter: i
forEach: i
*/