package it.polito.bigdata.spark.exercise2;

import org.apache.spark.api.java.function.Function;

@SuppressWarnings("serial")
public class GermanAirport implements Function<String, Boolean> {

	@Override
	public Boolean call(String line) throws Exception {
		String[] fields = line.split(",");

		if (fields[3].compareTo("Germany") == 0)
			return true;
		else
			return false;
	}

}
