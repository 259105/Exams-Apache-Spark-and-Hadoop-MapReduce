package it.polito.bigdata.spark.exercise2;

import org.apache.spark.api.java.function.Function2;

@SuppressWarnings("serial")
public class ReduceMinMax implements Function2<MinMax, MinMax, MinMax> {

	@Override
	public MinMax call(MinMax prices1, MinMax prices2) throws Exception {
		MinMax newMinMax = new MinMax();

		if (prices1.min < prices2.min)
			newMinMax.min = prices1.min;
		else
			newMinMax.min = prices2.min;

		if (prices1.max > prices2.max)
			newMinMax.max = prices1.max;
		else
			newMinMax.max = prices2.max;

		return newMinMax;
	}

}
