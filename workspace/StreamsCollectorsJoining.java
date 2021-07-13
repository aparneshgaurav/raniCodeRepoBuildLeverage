package com.poc.rcm.java.eight;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class StreamsCollectorsJoining {
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		List<String>strings = Arrays.asList("abc","","bc","efg","abcd","","jkl");
		List<String> filtered = strings.stream().filter
				(
						string
						->
						{
							Boolean bool = false;
						bool = string.isEmpty();
						return !bool;
						}
						).collect(Collectors.toList());

		System.out.println("Filtered List here : " + filtered);
		
		
		String mergedString = strings.stream().filter
				(string 
						->
				{
					Boolean bool = false;
					bool = string.isEmpty();
					return !bool;
				}
						).collect(Collectors.joining(","));
		System.out.println("Merged String: " + mergedString);

	}

}
