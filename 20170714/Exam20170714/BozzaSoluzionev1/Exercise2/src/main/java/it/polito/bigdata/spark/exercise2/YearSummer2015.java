package it.polito.bigdata.spark.exercise2;

import org.apache.spark.api.java.function.Function;

@SuppressWarnings("serial")
public class YearSummer2015 implements Function<String, Boolean> {

	@Override
	public Boolean call(String line) throws Exception {
		// 2016/07/20,Turin,Italy,32.5,26.0
		String fields[] = line.split(",");

		String date = fields[0];

		if (date.compareTo("2015/06/01") >= 0 && date.compareTo("2015/08/31") <= 0) {
			return true;
		} else {
			return false;
		}
	}

}
