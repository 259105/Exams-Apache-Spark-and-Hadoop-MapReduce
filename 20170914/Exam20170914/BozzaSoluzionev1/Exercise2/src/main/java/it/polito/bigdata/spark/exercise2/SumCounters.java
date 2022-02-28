package it.polito.bigdata.spark.exercise2;

import org.apache.spark.api.java.function.Function2;

@SuppressWarnings("serial")
public class SumCounters implements Function2<Counter, Counter, Counter> {

	@Override
	public Counter call(Counter count1, Counter count2) throws Exception {
		Counter count = new Counter();
		count.numFullyBooked = count1.numFullyBooked + count2.numFullyBooked;
		count.numCancelled = count1.numCancelled + count2.numCancelled;
		count.numFlights = count1.numFlights + count2.numFlights;

		return count;
	}

}
