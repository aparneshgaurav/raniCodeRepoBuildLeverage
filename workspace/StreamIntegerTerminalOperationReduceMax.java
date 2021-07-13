package com.poc.rcm.java.eight;

import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

public class StreamIntegerTerminalOperationReduceMax {

	public static void main(String[] args) {
		String alphabet = "a";
		// TODO Auto-generated method stub
		List<Integer> listHere = new LinkedList<Integer>();
		listHere.add(10);
		listHere.add(70);
		listHere.add(30);
		
		/**
		 * doing reduce terminal operation and printing if reduce has some data with it 
		 */
		
		listHere.stream().map(record
				->
		{
			record = 100 * record;
			return record;
		}
				).reduce( (record1,record2)
						->
				{
				if(record1 > record2){
					return record1;
				}
				else{
					return record2;
				}
				}		
						
						).ifPresent(record
								->
						{
							System.out.println(record);
						}
								);
		
	}
}
