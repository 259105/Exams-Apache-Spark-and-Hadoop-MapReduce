package it.polito.bigdata.spark.exercise2;

import org.apache.spark.api.java.function.Function;

@SuppressWarnings("serial")
public class ExtractBid implements Function<String, String> {

	@Override
	public String call(String book) throws Exception {
		// B1020,The Body in the Library,Crime,Dodd and Company,1942
		String[] fields = book.split(",");

		return new String(fields[0]);
	}

}
