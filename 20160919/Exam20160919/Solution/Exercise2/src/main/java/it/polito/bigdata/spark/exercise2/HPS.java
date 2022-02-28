package it.polito.bigdata.spark.exercise2;

import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

@SuppressWarnings("serial")
public class HPS implements Function<Tuple2<String, Integer>, Boolean> {

	@Override
	public Boolean call(Tuple2<String, Integer> stationCount) throws Exception {
		
		if (stationCount._2()>45)
			return true;
		else
			return false;
	}

}
