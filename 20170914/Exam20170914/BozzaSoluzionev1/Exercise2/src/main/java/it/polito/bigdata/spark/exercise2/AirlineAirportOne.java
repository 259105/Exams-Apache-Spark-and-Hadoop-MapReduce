package it.polito.bigdata.spark.exercise2;

import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

@SuppressWarnings("serial")
public class AirlineAirportOne implements PairFunction<Tuple2<String, Tuple2<String, String>>, String, Integer> {

	@Override
	public Tuple2<String, Integer> call(Tuple2<String, Tuple2<String, String>> value) throws Exception {
		// key = airport id
		// value = (airline, airport name)
		String airline = value._2()._1();
		String airportName = value._2()._2();

		return new Tuple2<String, Integer>(new String(airline + "," + airportName), new Integer(1));
	}

}
