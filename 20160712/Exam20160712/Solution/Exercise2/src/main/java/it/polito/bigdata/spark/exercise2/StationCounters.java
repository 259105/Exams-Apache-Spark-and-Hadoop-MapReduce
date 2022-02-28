package it.polito.bigdata.spark.exercise2;

import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

@SuppressWarnings("serial")
public class StationCounters implements PairFunction<String, String, Integer> {

	@Override
	public Tuple2<String, Integer> call(String line) {
		String[] fields=line.split(",");

		// fields[0] = stationId
		// fields[5] = numFreeSlots
		
		int numFreeSlots=Integer.parseInt(fields[5]);

		if (numFreeSlots>=3)
			return new Tuple2<String, Integer>(fields[0], new Integer(0));
		else
			return new Tuple2<String, Integer>(fields[0], new Integer(1));
	}

}
