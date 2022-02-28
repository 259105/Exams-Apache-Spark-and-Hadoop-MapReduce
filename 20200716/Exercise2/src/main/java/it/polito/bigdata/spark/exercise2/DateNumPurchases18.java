package it.polito.bigdata.spark.exercise2;

import java.io.Serializable;

@SuppressWarnings("serial")
public class DateNumPurchases18  implements Serializable {
	public String date;
	public int numPurchases18;
	
	DateNumPurchases18(String date, int numPurchases18) {
		this.date=date;
		this.numPurchases18=numPurchases18;
	}

}
