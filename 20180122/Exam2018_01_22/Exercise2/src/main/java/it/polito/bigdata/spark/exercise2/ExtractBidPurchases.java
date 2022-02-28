package it.polito.bigdata.spark.exercise2;

import org.apache.spark.api.java.function.Function;

@SuppressWarnings("serial")
public class ExtractBidPurchases implements Function<String, String> {

	@Override
	public String call(String purchase) throws Exception {
		// customer1,B1020,20170502_13:10,25.00

		String[] fields = purchase.split(",");

		return new String(fields[1]);
	}

}
