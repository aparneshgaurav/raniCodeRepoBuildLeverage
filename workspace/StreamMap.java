package com.poc.rcm.java.eight;

import java.util.Arrays;
import java.util.List;

public class StreamMap {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		List<String> listOfValues = Arrays.asList("abc", "", "bc", "", "abcd","", "jkl");

		//get count of empty string
		int count = (int) listOfValues.stream().
				filter(
						record 
						->
						{
						Boolean bool = false;
						bool = record.isEmpty();
						return bool; // for filter , it returns true bool 
						}
						)
						.count();
		System.out.println(count);

	}

}
