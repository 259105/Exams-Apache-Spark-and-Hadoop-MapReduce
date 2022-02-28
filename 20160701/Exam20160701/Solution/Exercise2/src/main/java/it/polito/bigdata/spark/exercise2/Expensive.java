package it.polito.bigdata.spark.exercise2;

import org.apache.spark.api.java.function.Function;

@SuppressWarnings("serial")
public class Expensive implements Function<String, Boolean> {

	@Override
	public Boolean call(String line) {
		String[] fields=line.split(",");
		
		// fields[3] = suggested price 
		float suggestedPrice=Float.parseFloat(fields[3]);
		
		if (suggestedPrice>30)
			return true;
		else
			return false;
	}

}
