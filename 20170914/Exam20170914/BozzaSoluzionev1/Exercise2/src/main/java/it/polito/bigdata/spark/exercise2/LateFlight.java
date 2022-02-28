package it.polito.bigdata.spark.exercise2;

import org.apache.spark.api.java.function.Function;

@SuppressWarnings("serial")
public class LateFlight implements Function<String, Boolean> {

	@Override
	public Boolean call(String line) throws Exception {
		String[] fields = line.split(",");

		int delay = Integer.parseInt(fields[7]);

		if (delay >= 15)
			return true;
		else
			return false;

	}

}
