package it.polito.bigdata.spark.exercise2;

import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

@SuppressWarnings("serial")
public class StationIdOne implements PairFunction<String, String, Integer> {

	@Override
	public Tuple2<String, Integer> call(String reading) throws Exception {
		
		// Emit stationid, 1
		// station1,2016/03/07,17,15,29.2,15.1
		String[] fields=reading.split(",");
		
		String stationId=fields[0];
	
		return new Tuple2<String, Integer>(stationId, new Integer(1));
	}

}
