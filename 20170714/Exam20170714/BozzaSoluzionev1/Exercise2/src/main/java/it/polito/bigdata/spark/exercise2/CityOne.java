package it.polito.bigdata.spark.exercise2;

import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

@SuppressWarnings("serial")
public class CityOne implements PairFunction<String, String, Integer> {

	@Override
	public Tuple2<String, Integer> call(String line) throws Exception {
		// 2016/07/20,Turin,Italy,32.5,26.0
		String fields[] = line.split(",");

		String city = fields[1];

		return new Tuple2<String, Integer>(city, new Integer(1));
	}

}
