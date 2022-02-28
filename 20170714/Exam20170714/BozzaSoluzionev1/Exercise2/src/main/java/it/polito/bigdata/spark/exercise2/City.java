package it.polito.bigdata.spark.exercise2;

import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

@SuppressWarnings("serial")
public class City implements Function<Tuple2<String, Tuple2<CityMaxTempCount, MaxTempCount>>, String> {

	@Override
	public String call(Tuple2<String, Tuple2<CityMaxTempCount, MaxTempCount>> value) throws Exception {
		return new String(value._2()._1().city);
	}

}
