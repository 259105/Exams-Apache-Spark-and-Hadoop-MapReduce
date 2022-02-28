package it.polito.bigdata.spark.exercise2;

import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

@SuppressWarnings("serial")
public class OverloadedRoute implements Function<Tuple2<String, Counter>, Boolean> {

	@Override
	public Boolean call(Tuple2<String, Counter> routeCounters) throws Exception {

		double percFullyBooked = (double) routeCounters._2().numFullyBooked / (double) routeCounters._2().numFlights;
		double percCancelled = (double) routeCounters._2().numCancelled / (double) routeCounters._2().numFlights;

		if (percFullyBooked >= 0.99 && percCancelled >= 0.05)
			return true;
		else
			return false;
	}

}
