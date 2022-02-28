package it.polito.bigdata.spark.exercise2;

import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

@SuppressWarnings("serial")
public class AlwaysPolluted implements Function<Tuple2<String, Integer>, Boolean> {

	@Override
	public Boolean call(Tuple2<String, Integer> stationCounters) {
		
		// Check if (number of lines with PM2.5<=0PM2.5th_limit) is 0 
		if (stationCounters._2()==0)
			return true;
		else
			return false;
	}

}
