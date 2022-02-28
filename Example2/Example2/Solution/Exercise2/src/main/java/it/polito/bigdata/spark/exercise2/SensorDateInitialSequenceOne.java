package it.polito.bigdata.spark.exercise2;

import java.util.ArrayList;
import java.util.Iterator;

import org.apache.spark.api.java.function.PairFlatMapFunction;

import scala.Tuple2;

@SuppressWarnings("serial")
public class SensorDateInitialSequenceOne implements PairFlatMapFunction<String, String, Integer> {

	public Iterator<Tuple2<String, Integer>> call(String PM10reading)  {
		
		ArrayList<Tuple2<String, Integer>> initialDates=new ArrayList<Tuple2<String, Integer>>();
		
		// sensor#1,January-10-2014,15.3
		String[] fields = PM10reading.split(",");
		
		// fields[0] = sensordid
		// fields[1] = date
		// Return 
		// - date,1
		// - date-1,1
		// - date-2,1
		String sensorid=fields[0];
		String date=fields[1];
		initialDates.add(new Tuple2<String, Integer>(date+"_"+sensorid,new Integer(1)));

		initialDates.add(new Tuple2<String, Integer>(DateTool.dateOffset(date, -1)+"_"+sensorid,new Integer(1)));

		initialDates.add(new Tuple2<String, Integer>(DateTool.dateOffset(date, -2)+"_"+sensorid,new Integer(1)));
		
		return initialDates.iterator();
	}

}
