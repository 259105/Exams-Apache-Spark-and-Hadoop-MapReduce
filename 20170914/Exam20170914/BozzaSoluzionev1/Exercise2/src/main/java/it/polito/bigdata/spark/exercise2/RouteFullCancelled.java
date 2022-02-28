package it.polito.bigdata.spark.exercise2;

import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

@SuppressWarnings("serial")
public class RouteFullCancelled implements PairFunction<String, String, Counter> {

	@Override
	public Tuple2<String, Counter> call(String flight) throws Exception {
		String[] fields = flight.split(",");

		String route = new String(fields[5] + "," + fields[6]);

		String cancelled = fields[8];
		int num_of_seats = Integer.parseInt(fields[9]);
		int num_of_booked_seats = Integer.parseInt(fields[10]);

		Counter counters = new Counter();

		if (num_of_seats == num_of_booked_seats)
			counters.numFullyBooked = 1;
		else
			counters.numFullyBooked = 0;

		if (cancelled.compareTo("yes") == 0)
			counters.numCancelled = 1;
		else
			counters.numCancelled = 0;

		counters.numFlights = 1;

		return new Tuple2<String, Counter>(route, counters);
	}

}
