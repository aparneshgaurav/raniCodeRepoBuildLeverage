package com.check;


public class LambdaAndInterfaces {
	

	public static void main(String[] args){
		
		IntBank sumOperation = (a,b) -> (a+b);
		IntBank multiplyOperation = (a,b) -> (a*b);
		
		LambdaAndInterfaces lambdaAndInterfaces = new LambdaAndInterfaces();
		Integer sum = lambdaAndInterfaces.operate(1,2,sumOperation);
		System.out.println("sum:"+sum);
		Integer product =lambdaAndInterfaces.operate(2, 5 ,multiplyOperation);
		System.out.println("product"+product);
		
	}
	
	public Integer operate(Integer a , Integer b , IntBank intBankRef){
	    return intBankRef.doBank(a, b);
	}
	
	
	 
}

 interface IntBank{
	
	public Integer doBank(Integer a , Integer b);
	
}
