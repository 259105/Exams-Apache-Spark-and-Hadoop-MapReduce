package it.polito.bigdata.spark.exercise2;

import java.io.Serializable;

@SuppressWarnings("serial")
public class CounterHighLowWindSpeed implements Serializable {
	private int highWindSpeed;
	private int lowWindSpeed;

	public CounterHighLowWindSpeed(int highWindSpeed, int lowWindSpeed) {
		this.highWindSpeed = highWindSpeed;
		this.lowWindSpeed = lowWindSpeed;
	}

	public int getHighWindSpeed() {
		return highWindSpeed;
	}

	public void setHighWindSpeed(int highWindSpeed) {
		this.highWindSpeed = highWindSpeed;
	}

	public int getLowWindSpeed() {
		return lowWindSpeed;
	}

	public void setLowWindSpeed(int lowWindSpeed) {
		this.lowWindSpeed = lowWindSpeed;
	}

	public String toString() {
		return new String(highWindSpeed + "_" + lowWindSpeed);
	}
}
