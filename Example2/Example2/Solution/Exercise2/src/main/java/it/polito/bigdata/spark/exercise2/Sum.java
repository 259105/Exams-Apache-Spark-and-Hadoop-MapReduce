package it.polito.bigdata.spark.exercise2;

import org.apache.spark.api.java.function.Function2;

@SuppressWarnings("serial")
public class Sum implements Function2<Integer, Integer, Integer> {

	public Integer call(Integer num1, Integer num2) {
		return num1+num2;
	}

}
