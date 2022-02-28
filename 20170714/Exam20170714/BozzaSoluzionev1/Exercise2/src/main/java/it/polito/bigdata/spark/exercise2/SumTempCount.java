package it.polito.bigdata.spark.exercise2;

import org.apache.spark.api.java.function.Function2;

@SuppressWarnings("serial")
public class SumTempCount implements Function2<MaxTempCount, MaxTempCount, MaxTempCount> {

	@Override
	public MaxTempCount call(MaxTempCount v1, MaxTempCount v2) throws Exception {
		return new MaxTempCount(v1.maxTemp + v2.maxTemp, v1.count + v2.count);
	}

}
