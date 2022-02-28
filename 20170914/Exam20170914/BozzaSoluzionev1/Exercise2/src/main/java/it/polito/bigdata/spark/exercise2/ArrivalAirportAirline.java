package it.polito.bigdata.spark.exercise2;

import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

@SuppressWarnings("serial")
public class ArrivalAirportAirline implements PairFunction<String, String, String> {

	@Override
	public Tuple2<String, String> call(String flight) throws Exception {
		String[] fields = flight.split(",");

		String airline = fields[1];
		String arrivalAirportId = fields[6];

		return new Tuple2<String, String>(new String(arrivalAirportId), new String(airline));
	}

}
