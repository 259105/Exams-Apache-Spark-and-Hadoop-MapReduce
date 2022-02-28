package it.polito.bigdata.spark.exercise2;

import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

@SuppressWarnings("serial")
public class CityCountryMaxTemp implements PairFunction<String, String, MaxTempCount> {

	@Override
	public Tuple2<String, MaxTempCount> call(String line) throws Exception {
		// 2016/07/20,Turin,Italy,32.5,26.0
		String fields[] = line.split(",");

		String city = fields[1];
		String country = fields[2];

		double maxTemp = Double.parseDouble(fields[3]);

		return new Tuple2<String, MaxTempCount>(new String(city + "-" + country), new MaxTempCount(maxTemp, 1));
	}

}
