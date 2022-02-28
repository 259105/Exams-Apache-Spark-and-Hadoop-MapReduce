package it.polito.bigdata.spark.exercise2;

import java.io.Serializable;

@SuppressWarnings("serial")
public class FirstDatePriceLastDatePrice implements Serializable {

	public String firstdate;
	public Double firstprice;

	public String lastdate;
	public Double lastprice;

	public FirstDatePriceLastDatePrice(String firstdate, Double firstprice, String lastdate, Double lastprice) {
		this.firstdate = firstdate;
		this.firstprice = firstprice;
		this.lastdate = lastdate;
		this.lastprice = lastprice;
	}
}
