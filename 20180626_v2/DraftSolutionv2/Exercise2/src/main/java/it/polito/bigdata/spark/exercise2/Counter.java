package it.polito.bigdata.spark.exercise2;

import java.io.Serializable;

@SuppressWarnings("serial")
public class Counter implements Serializable {

	private int count;
	private double sumPrec;
	private double sumWindSpeed;

	public Counter(int count, double sumPrec, double sumWindSpeed) {
		this.count = count;
		this.sumPrec = sumPrec;
		this.sumWindSpeed = sumWindSpeed;
	}

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}

	public double getSumPrec() {
		return sumPrec;
	}

	public void setSumPrec(double sumPrec) {
		this.sumPrec = sumPrec;
	}

	public double getSumWindSpeed() {
		return sumWindSpeed;
	}

	public void setSumWindSpeed(double sumWindSpeed) {
		this.sumWindSpeed = sumWindSpeed;
	}

}
