package com.check.mapreduce.temperature;



import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class MaxKeyValuePairForMap {
	public static void main(String[] args){
		System.out.println("hello");
		Map<String,Integer> map = new HashMap<String, Integer>();
		map.put("a", 1);
		map.put("bfg", 25);
		map.put("c", 3);
		map.put("c", 4);
		computeMaxKeyValuePairForMap(map);
	}

	public static void computeMaxKeyValuePairForMap(Map<String,Integer>  map){
		Set<String> keySet = map.keySet();
		List<Integer> listOfValues = new ArrayList<Integer>();
		Iterator<String> itr = keySet.iterator();
		while(itr.hasNext()){
			String key = itr.next();
			Integer value = map.get(key);
			listOfValues.add(value);
		}
		Collections.sort(listOfValues);
		System.out.println(listOfValues);
		Integer largestValue = listOfValues.get(listOfValues.size()-1);
		System.out.println("largest value is : "+largestValue);

		/**
		 * Iterate through the keyset , match the values for those keys of map against the largest value , then 
		 * store those particular keys or just one of the keys.
		 */
		String keyForLargestValue = null;
		Iterator<String> itr1 = keySet.iterator();
		while(itr1.hasNext()){
			String key = itr1.next();
			if(map.get(key)==largestValue){
				keyForLargestValue = key;
			}
		}
		System.out.println("selected key-value pair is : "+keyForLargestValue+ " : " + largestValue);
	}
}

