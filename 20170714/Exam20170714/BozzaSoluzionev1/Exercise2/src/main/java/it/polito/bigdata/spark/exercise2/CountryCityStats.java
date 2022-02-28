package it.polito.bigdata.spark.exercise2;

import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

@SuppressWarnings("serial")
public class CountryCityStats implements PairFunction<Tuple2<String, MaxTempCount>, String, CityMaxTempCount> {

	@Override
	public Tuple2<String, CityMaxTempCount> call(Tuple2<String, MaxTempCount> value) throws Exception {
		String fields[] = value._1().split("-");

		String city = fields[0];
		String country = fields[1];

		
		return new Tuple2<String, CityMaxTempCount>(new String(country),
				new CityMaxTempCount(city, value._2().maxTemp, value._2().count));
	}

}
