package com.poc.rcm.java.eight;

import java.util.Arrays;
import java.util.List;

public class ForEachDemo {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		List<String> names = Arrays.asList("Alex", "Brian", "Charles");
	     
		names.forEach(
				record
				->
			    System.out.println(record)
				);
	}

}


/*output
Alex
Brian
Charles*/