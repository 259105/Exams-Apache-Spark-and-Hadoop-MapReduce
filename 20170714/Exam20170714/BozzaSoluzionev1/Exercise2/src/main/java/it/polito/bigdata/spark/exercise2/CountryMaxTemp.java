package it.polito.bigdata.spark.exercise2;

import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

@SuppressWarnings("serial")
public class CountryMaxTemp implements PairFunction<Tuple2<String, MaxTempCount>, String, MaxTempCount> {

	@Override
	public Tuple2<String, MaxTempCount> call(Tuple2<String, MaxTempCount> cityStatistics) throws Exception {

		return new Tuple2<String, MaxTempCount>(cityStatistics._1().split("-")[1],
				new MaxTempCount(cityStatistics._2().maxTemp, cityStatistics._2().count));

	}

}
