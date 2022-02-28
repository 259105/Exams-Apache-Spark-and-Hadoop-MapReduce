package it.polito.bigdata.spark.exercise2;

import java.io.Serializable;

@SuppressWarnings("serial")
public class Counter implements Serializable {
	public int numBooks;
	public int numNeverPurchased;

	public String toString() {
		return new String(""+100.0*(double)numNeverPurchased/(double)numBooks);
	}
}
