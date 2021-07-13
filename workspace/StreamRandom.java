package com.poc.rcm.java.eight;

import java.util.Random;

public class StreamRandom {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Random random = new Random();
		random.ints().limit(10).map(record1
				->
		{
			
			if(record1<0){
				System.out.println("negative records : "+record1);
				return record1 = record1*-1;
			}
			else{
				return record1;
			}
			
		}
				
				).sorted().forEach
		(record
				->
		{
			System.out.println("finally processed records : "+record);
		}
				);
	}

}
