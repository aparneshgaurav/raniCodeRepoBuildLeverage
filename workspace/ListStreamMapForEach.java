package com.poc.rcm.java.eight;

import java.util.LinkedList;
import java.util.List;
import java.util.function.Consumer;

public class ListStreamMapForEach {

	public static void main(String[] args) {
		String alphabet = "a";
		// TODO Auto-generated method stub
		List<String> listHere = new LinkedList<String>();
		listHere.add("apple");
		listHere.add("cat");
		listHere.add("ant");
		
		listHere.stream().map(record
				->
		{
			record = "hello " + record;
			return record;
		}
				).forEach(record
						->
				{
					System.out.println("printing the  records with transformation "+record);
				});
		

	}
	
	

}
