package com.poc.rcm.java.eight;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import com.kenai.constantine.platform.darwin.Sysconf;

public class StreamMapping {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		List<Integer> numbers = Arrays.asList(3, 2, 2, 3, -7, 3, 5);

		//get list of unique squares
		List<Integer> squaresList = numbers.stream().
				map( 
						record
						->
						{
							if(record < 0){
						return record*record*100;
							}
							else{
								return record*record;
							}
						
						}
						)
						.distinct()
						.collect(Collectors.toList());
		System.out.println(squaresList);
	}
    

}
