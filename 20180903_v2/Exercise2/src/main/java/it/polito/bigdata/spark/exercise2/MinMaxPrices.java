package it.polito.bigdata.spark.exercise2;

import java.io.Serializable;

@SuppressWarnings("serial")
public class MinMaxPrices implements Serializable {
	private double minPrice;
	private double maxPrice;

	public MinMaxPrices(double minPrice, double maxPrice) {
		this.minPrice = minPrice;
		this.maxPrice = maxPrice;
	}

	public double getMinPrice() {
		return minPrice;
	}

	public void setMinPrice(double minPrice) {
		this.minPrice = minPrice;
	}

	public double getMaxPrice() {
		return maxPrice;
	}

	public void setMaxPrice(double maxPrice) {
		this.maxPrice = maxPrice;
	}

	public String toString() {
		return new String(minPrice+" "+maxPrice);
	}

}
