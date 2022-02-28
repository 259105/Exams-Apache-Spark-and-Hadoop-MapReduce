package it.polito.bigdata.spark.exercise2;

import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

@SuppressWarnings("serial")
public class UserOne implements PairFunction<String, String, Integer> {

	@Override
	public Tuple2<String, Integer> call(String line) {
		String[] fields=line.split(",");
		// fields[0]=userid

		// Return a pair (userid, 1)
		return new Tuple2<String, Integer>(fields[0], new Integer(1));
	}

}
