package com.poc.rcm.java.eight;

import java.util.HashMap;
import java.util.Map;


public class MapForEach {
	
	public static void main(String[] args){
		Map<String, String> map = new HashMap<String,String>();
		 
		map.put("A", "Alex");
		map.put("B", "Brian");
		map.put("C", "Charles");
		 
		map.forEach(
				(key,value) 
				->
				{
		    System.out.println("key : "+key);
		    System.out.println("value : "+value);
				});
	}

}
