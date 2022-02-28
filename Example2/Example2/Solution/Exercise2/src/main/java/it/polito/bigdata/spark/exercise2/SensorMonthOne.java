package it.polito.bigdata.spark.exercise2;

import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

@SuppressWarnings("serial")
public class SensorMonthOne implements PairFunction<String, String, Integer> {

	@Override
	public Tuple2<String, Integer> call(String PM10reading)  {
		
		// sensor#1,1-10-2014,15.3
		String sensorid;
		String month;
		String year;
		
		// fields[0] = sensorid
		// fields[1] = date
		String[] fields=PM10reading.split(",");

		sensorid=fields[0];
		month=fields[1].split("-")[0];
		year=fields[1].split("-")[2];
		
		return new Tuple2<String, Integer>(new String(sensorid+"_"+month+"-"+year),new Integer(1));
	}

}
