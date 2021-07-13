package com.poc.rcm.java.eight;

import java.util.LinkedList;
import java.util.List;
import java.util.function.Consumer;

public class ListStreamFilterAndMapForEach {

	public static void main(String[] args) {
		String alphabet = "a";
		// TODO Auto-generated method stub
		List<String> listHere = new LinkedList<String>();
		listHere.add("apple");
		listHere.add("cat");
		listHere.add("ant");
		
		listHere.stream().filter(record
				->
		{
			Boolean bool = false;
			bool = record.startsWith(alphabet);
			return bool;
		}
				).map(record
						->
				{
					record = record.toUpperCase();
					return record;
				}
						).forEach(record
						->
				{
					System.out.println("printing the filtered records with albhabet : "+alphabet+" : "+record);
				}
						
						);
		

	}
	
	

}
