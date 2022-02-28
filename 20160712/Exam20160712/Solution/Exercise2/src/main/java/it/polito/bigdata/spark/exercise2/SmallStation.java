package it.polito.bigdata.spark.exercise2;

import org.apache.spark.api.java.function.Function;

@SuppressWarnings("serial")
public class SmallStation implements Function<String, Boolean> {

	public Boolean call(String line)  {
		
		String[] fields=line.split(",");
		
		int totalNumSlots=Integer.parseInt(fields[4]);
		
		if (totalNumSlots<5)
			return true;
		else
			return false;
	}

}
