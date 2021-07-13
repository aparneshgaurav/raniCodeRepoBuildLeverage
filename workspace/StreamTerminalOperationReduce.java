package com.poc.rcm.java.eight;

import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

public class StreamTerminalOperationReduce {

	public static void main(String[] args) {
		String alphabet = "a";
		// TODO Auto-generated method stub
		List<String> listHere = new LinkedList<String>();
		listHere.add("apple");
		listHere.add("cat");
		listHere.add("ant");
		
		/**
		 * doing reduce terminal operation and printing if reduce has some data with it 
		 */
		
		listHere.stream().map(record
				->
		{
			record = "hello " + record;
			return record;
		}
				).reduce( (record1,record2)
						->
				{
				return record1+"#"+record2;
				}		
						
						).ifPresent(record
								->
						{
							System.out.println(record);
						}
								);
		
		/**
		 * using Optional<String>
		 */
		
		Optional<String> data = listHere.stream().map(record
				->
		{
			record = "hello optional " + record;
			return record;
		}
				).reduce( (record1,record2)
						->
				{
				return record1+"#"+record2;
				}		
						
						);
		
		data.ifPresent(record
				->
		{
		System.out.println(record);
		}		
				);
		
	

	}
	
	Consumer<String> action = record
			->
	{
		System.out.println(record);
	};
	

}
