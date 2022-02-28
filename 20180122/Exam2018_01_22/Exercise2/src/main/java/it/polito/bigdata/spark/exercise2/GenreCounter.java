package it.polito.bigdata.spark.exercise2;

import java.util.List;

import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

@SuppressWarnings("serial")
public class GenreCounter implements PairFunction<String, String, Counter> {

	List<String> localNeverPurchased;

	public GenreCounter(List<String> localNeverPurchased) {
		this.localNeverPurchased = localNeverPurchased;
	}

	@Override
	public Tuple2<String, Counter> call(String book) throws Exception {
		// B1020,The Body in the Library,Crime,Dodd and Company,1942
		String[] fields = book.split(",");
		String bid = fields[0];
		String genre = fields[2];

		Counter count = new Counter();
		count.numBooks = 1;

		if (localNeverPurchased.contains(bid))
			count.numNeverPurchased = 1;
		else
			count.numNeverPurchased = 0;

		return new Tuple2<String, Counter>(genre, count);
	}

}
