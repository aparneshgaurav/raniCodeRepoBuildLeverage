


import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public class FuncPassingAsParamAndLambda {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		List<String> listOfString = new LinkedList<String>();
		listOfString.add("bat");
		listOfString.add("apple");
		
		System.out.println(listOfString);
		listOfString = sortList(listOfString);
		System.out.println(listOfString);

	}
	
	public static List<String> sortList(List<String> listOfString){
	
		Collections.sort(listOfString,(param1,param2) -> param1.compareTo(param2));
		return listOfString;
		
	}

}

