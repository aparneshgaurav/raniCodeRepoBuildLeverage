package com.poc.rcm.java.eight;



public class FunctionalInterface implements Interface {
	
	public void move(){
		System.out.println("Custom implementation , this is getting printed when "
				+ "the move method of implementing class has it's own custom implementation");
	}
	
	/*public void move(){
		System.out.println("Custom implementation , this is getting printed when "
				+ "the move method of implementing class has it's own custom implementation");
	}*/

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		FunctionalInterface functionalInterface = new FunctionalInterface();
		functionalInterface.move();
	}

}


/**
 * output case 1 : having default interface method implementation
 * hello , its  moving
 * 
 * output case 2 : 
 * Custom implementation , this is getting printed when the move method of implementing class has it's own custom implementation

 * 
 * 
 */
