package it.polito.bigdata.spark.exercise2;

import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

@SuppressWarnings("serial")
public class BidPricePrice implements PairFunction<String, String, MinMax> {

	@Override
	public Tuple2<String, MinMax> call(String purchase) throws Exception {

		// customer1,B1020,20170502_13:10,25.00

		String[] fields = purchase.split(",");

		String bid = new String(fields[1]);
		double price = Double.parseDouble(fields[3]);

		MinMax pricePrice = new MinMax();
		pricePrice.min = price;
		pricePrice.max = price;

		return new Tuple2<String, MinMax>(bid, pricePrice);
	}

}
