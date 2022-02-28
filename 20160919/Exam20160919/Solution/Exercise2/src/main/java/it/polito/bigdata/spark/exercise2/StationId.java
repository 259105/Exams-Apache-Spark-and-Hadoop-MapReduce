package it.polito.bigdata.spark.exercise2;

import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

@SuppressWarnings("serial")
public class StationId implements Function<Tuple2<String, Integer>, String> {

	@Override
	public String call(Tuple2<String, Integer> stationCount) throws Exception {
		return stationCount._1();
	}

}
