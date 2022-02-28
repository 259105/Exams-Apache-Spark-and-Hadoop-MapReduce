package it.polito.bigdata.spark.exercise2;

import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

@SuppressWarnings("serial")
public class HotCity implements Function<Tuple2<String, Tuple2<CityMaxTempCount, MaxTempCount>>, Boolean> {

	@Override
	public Boolean call(Tuple2<String, Tuple2<CityMaxTempCount, MaxTempCount>> values) throws Exception {
		double avgCity = values._2()._1().maxTemp / (double) values._2()._1().count;

		double avgCountry = values._2()._2().maxTemp / (double) values._2()._2().count;

		if (avgCity > 5 + avgCountry) {
			return true;
		} else {
			return false;
		}
	}

}
