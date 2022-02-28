package it.polito.bigdata.spark.exercise2;

import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

@SuppressWarnings("serial")
public class AnomalousPrice implements Function<Tuple2<String, MinMax>, Boolean> {

	@Override
	public Boolean call(Tuple2<String, MinMax> bidMinMax) throws Exception {
		if ((bidMinMax._2().max - bidMinMax._2().min) > 15)
			return true;
		else
			return false;
	}

}
