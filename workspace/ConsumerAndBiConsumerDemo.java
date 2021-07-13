package com.poc.rcm.java.eight;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

 

public class ConsumerAndBiConsumerDemo {

	/**
	 * commneted interface codes tell about the internal implementation of the 
	 * methods as objects that we are seeing in this code leveraging Consumer 
	 * interfaces , both single and parameterized . 
	 * @param args
	 */
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		
		 
		List<String> names = Arrays.asList("Alex", "Brian", "Charles");
		 
		/*Consumer<String> makeUpperCase = new Consumer<String>()
		{
		    @Override
		    public void accept(String paramString) 
		    {
		        System.out.println(paramString.toUpperCase());
		    }
		};*/
		Consumer<String> makeUpperCase = record
				->
		{
			System.out.println(record.toUpperCase()+" : converted to upper case : ");
		};
		
		java.util.function.BiConsumer<String, Integer > action = (a, b) -> 
		{ 
		    System.out.println("Key is : " + a); 
		    System.out.println("Value is : " + b); 
		}; 
		
		System.out.println(names);
		names.forEach(makeUpperCase);
		
		Map<String,Integer> nameAndAgeCombo = new HashMap<String,Integer>();
		nameAndAgeCombo.put("Alex", 21);
		nameAndAgeCombo.put("Brian", 22);
		nameAndAgeCombo.put("charles", 23);
		nameAndAgeCombo.forEach(action);
	}

}
