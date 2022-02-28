package it.polito.bigdata.spark.exercise2;

import org.apache.spark.api.java.function.Function;

@SuppressWarnings("serial")
public class BookIdFromPurchase implements Function<String, String> {

	@Override
	public String call(String line) {
		String[] fields=line.split(",");
		
		// fields[1] = bookid 
		return fields[1];
	}

}
