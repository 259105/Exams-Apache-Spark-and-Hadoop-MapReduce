package it.polito.bigdata.spark.exercise2;

import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

@SuppressWarnings("serial")
public class StationCounters implements PairFunction<String, String, Integer> {

	private Double PM25th_limit;
	
	public StationCounters(Double PM25th) {
		this.PM25th_limit=PM25th;
	}

	@Override
	public Tuple2<String, Integer> call(String reading) throws Exception {
		// station1,2016/03/07,17,15,29.2,15.1
		String[] fields=reading.split(",");
		
		String stationId=fields[0];
		Double PM25=Double.parseDouble(fields[5]);
		
		if (PM25<=PM25th_limit) {
			return new Tuple2<String, Integer>(stationId, new Integer(1));
		} 
		else {
			return new Tuple2<String, Integer>(stationId, new Integer(0));
		}
		
	}

}
