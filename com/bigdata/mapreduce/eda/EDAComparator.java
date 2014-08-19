package com.bigdata.mapreduce.eda;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;


// Custom comparator class used to sort by a part of key
public class EDAComparator implements WritableComparable<EDAComparator> {
	private Text value;
		
	public EDAComparator() {
		set(new Text());
	}
		
	public EDAComparator(String value) {
		set(new Text(value));
	}
	
	public EDAComparator(Text value) {
		set(value);
	}
	
	public void set(Text value) {
		this.value = value;
	}
	
	public Text get() {
		return value;
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		value.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		value.readFields(in);
	}
	
	@Override
	public int hashCode() {
		return value.hashCode();
	}
	
	@Override
	public boolean equals(Object o) {
		if (o instanceof EDAComparator) {
			EDAComparator ec = (EDAComparator) o;
			return value.equals(ec.value);
	    }
	    return false;
	}
	
	@Override
	public String toString() {
		return value + "";
	}
		
	@Override
    public int compareTo(EDAComparator ec) {
    	String thisValue = this.value.toString();
    	String thatValue = ((EDAComparator)ec).value.toString();
    	if (thisValue.split("-")[0].contains("col")
    			&& thatValue.split("-")[0].contains("col")) {
    		try {
    			long oneValue = Long.parseLong(thisValue.split("-")[1]);
    			long anotherValue = Long.parseLong(thatValue.split("-")[1]);
    			return (oneValue<anotherValue ? -1 : (oneValue==anotherValue ? 0 : 1));
    		}
    		catch (NumberFormatException ne) {
    			return value.compareTo(ec.value);
    		}
    	}
    	else
    		return value.compareTo(ec.value);
    }
}