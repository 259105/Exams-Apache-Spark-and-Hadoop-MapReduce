package it.polito.bigdata.spark.exercise2;

import org.apache.spark.api.java.function.Function2;

@SuppressWarnings("serial")
public class Sum implements Function2<Integer, Integer, Integer> {

	@Override
	public Integer call(Integer v1, Integer v2) throws Exception {
		return new Integer(v1 + v2);
	}

}
