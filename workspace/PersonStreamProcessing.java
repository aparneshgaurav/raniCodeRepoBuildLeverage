package com.poc.rcm.java.eight;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

class Person {
    String name;
    int age;

    
    
    public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public int getAge() {
		return age;
	}

	public void setAge(int age) {
		this.age = age;
	}

	Person(String name, int age) {
        this.name = name;
        this.age = age;
    }

    @Override
    public String toString() {
        return name;
    }
}

public class PersonStreamProcessing {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		List<Person> persons =
			    Arrays.asList(
			        new Person("Max", 18),
			        new Person("Peter", 23),
			        new Person("Pamela", 23),
			        new Person("David", 12));
		
		List<Person> filtered =
			    persons
			        .stream()
			        .filter(record
			        		-> 
			        {
			        	Boolean bool = false;
			        	bool = record.getName().startsWith("D");
			        	return bool;
			        })
			        .collect(Collectors.toList());

			System.out.println(filtered);    // [Peter, Pamela]
	}

}
