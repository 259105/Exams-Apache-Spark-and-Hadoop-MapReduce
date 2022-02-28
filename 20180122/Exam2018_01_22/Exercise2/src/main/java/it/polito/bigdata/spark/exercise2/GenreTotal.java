package it.polito.bigdata.spark.exercise2;

import org.apache.spark.api.java.function.Function2;

@SuppressWarnings("serial")
public class GenreTotal implements Function2<Counter, Counter, Counter> {

	@Override
	public Counter call(Counter count1, Counter count2) throws Exception {
		Counter count = new Counter();

		count.numBooks = count1.numBooks + count2.numBooks;
		count.numNeverPurchased = count1.numNeverPurchased + count2.numNeverPurchased;
		
		return count;
	}

}
