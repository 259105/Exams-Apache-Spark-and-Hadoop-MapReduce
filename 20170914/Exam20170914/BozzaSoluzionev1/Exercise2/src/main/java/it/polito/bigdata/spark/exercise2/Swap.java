package it.polito.bigdata.spark.exercise2;

import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

@SuppressWarnings("serial")
public class Swap implements PairFunction<Tuple2<String, Integer>, Integer, String> {

	@Override
	public Tuple2<Integer, String> call(Tuple2<String, Integer> pair) throws Exception {
		return new Tuple2<Integer, String>(new Integer(pair._2()), new String(pair._1()));
	}

}
