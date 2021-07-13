package com.poc.rcm.java.eight;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collector;
import java.util.stream.Collectors;

public class Stream {
	
	/**
	 * Stream concepts . 
	 * If  a video has to be entirely loaded to be played , it's the concept of collection . 
	 * If the part of video downloaded is being played , this is called the concept of stream . 
	 * Streams generally has collection or i/o source as their source . 
	 * In streams , the computation is on source , at demand . 
	 * It has a producer consumer relationship with collection being the producer and logics of filter , 
	 * maps , aggregations being the consumer side .
	 * Nested steram operations do iterative iteration rather than collections doing explicit iteration . 
	 * Hence instead of doing iteration over the entire collection , the iterative iteration is done on 
	 * the elements of streams ( in that time or volume based stream portion ) and results are returned .
	 * Pipelining of operations supported with collect method marked as terminal operation . 
	 * Until collect , it is all lazily computed and stored and gets eagerly evaluated once it 
	 * sees collect method . 
	 * stream is written with lambda in mind , lambda works on streams
	 * @param args
	 */

	public static void main(String[] args) {
		// TODO Auto-generated method stub
			List<String> list = new ArrayList<String>();
			list.add("hello world");
			list.add("hi world");
			list.add("hello inida world");
			list.add("hello to the world world");
			
			List<String> refinedList = list.stream().filter(
					record
					->
					{
					Boolean bool = false;	
					bool = record.startsWith("hello");
					return bool;
					}
					).collect(Collectors.toList());
			System.out.println(refinedList);
	}

}

/**
 * output
[hello world, hello inida world, hello to the world world]
 * 
 * 
 * 
 * 
 */
