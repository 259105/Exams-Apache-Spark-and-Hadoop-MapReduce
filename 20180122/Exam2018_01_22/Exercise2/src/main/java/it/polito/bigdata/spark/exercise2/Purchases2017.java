package it.polito.bigdata.spark.exercise2;

import org.apache.spark.api.java.function.Function;

@SuppressWarnings("serial")
public class Purchases2017 implements Function<String, Boolean> {

	@Override
	public Boolean call(String purchase) throws Exception {

		// customer1,B1020,20170502_13:10,25.00

		String[] fields = purchase.split(",");
		if (fields[2].startsWith("2017") == true)
			return true;
		else
			return false;
	}

}
