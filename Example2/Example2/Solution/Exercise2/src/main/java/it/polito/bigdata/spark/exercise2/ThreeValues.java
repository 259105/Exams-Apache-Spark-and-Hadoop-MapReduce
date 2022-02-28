package it.polito.bigdata.spark.exercise2;

import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

@SuppressWarnings("serial")
public class ThreeValues implements Function<Tuple2<String, Integer>, Boolean> {

	public Boolean call(Tuple2<String, Integer> InitiaDateCount) {
		if (InitiaDateCount._2()==3)
			return true;
		else
			return false;
	}

}
