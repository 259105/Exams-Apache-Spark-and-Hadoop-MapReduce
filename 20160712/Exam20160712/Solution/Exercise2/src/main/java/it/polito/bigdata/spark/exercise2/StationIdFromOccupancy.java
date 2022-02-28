package it.polito.bigdata.spark.exercise2;

import org.apache.spark.api.java.function.Function;

@SuppressWarnings("serial")
public class StationIdFromOccupancy implements Function<String, String> {

	@Override
	public String call(String line) {

		String[] fields=line.split(",");
		
		String stationId=fields[0];
		
		return stationId;
	}

}
