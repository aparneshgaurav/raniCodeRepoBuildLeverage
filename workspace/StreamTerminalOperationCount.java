package com.poc.rcm.java.eight;

import java.util.LinkedList;
import java.util.List;
import java.util.function.Consumer;

public class StreamTerminalOperationCount {

	public static void main(String[] args) {
		String alphabet = "a";
		// TODO Auto-generated method stub
		List<String> listHere = new LinkedList<String>();
		listHere.add("apple");
		listHere.add("cat");
		listHere.add("ant");
		
		Long matchedCount = listHere.stream().filter(record
				->
		{
			Boolean bool = false;
			bool = record.startsWith(alphabet);
			return bool;
		}
				).count();
		System.out.println("matchedCount"+matchedCount);

	}
	
	

}
