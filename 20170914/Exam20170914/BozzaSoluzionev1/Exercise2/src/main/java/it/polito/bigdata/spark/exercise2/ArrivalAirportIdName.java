package it.polito.bigdata.spark.exercise2;

import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

@SuppressWarnings("serial")
public class ArrivalAirportIdName implements PairFunction<String, String, String> {

	@Override
	public Tuple2<String, String> call(String airport) throws Exception {
		String[] fields = airport.split(",");

		String airportId = fields[0];
		String name = fields[1];

		return new Tuple2<String, String>(new String(airportId), new String(name));
	}

}
