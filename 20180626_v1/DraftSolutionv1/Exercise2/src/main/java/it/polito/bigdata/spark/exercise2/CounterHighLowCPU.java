package it.polito.bigdata.spark.exercise2;

import java.io.Serializable;

@SuppressWarnings("serial")
public class CounterHighLowCPU implements Serializable {
	private int highCPU;
	private int lowCPU;

	public CounterHighLowCPU(int highCPU, int lowCPU) {
		this.highCPU = highCPU;
		this.lowCPU = lowCPU;
	}

	public int getHighCPU() {
		return highCPU;
	}

	public void setHighCPU(int highCPU) {
		this.highCPU = highCPU;
	}

	public int getLowCPU() {
		return lowCPU;
	}

	public void setLowCPU(int lowCPU) {
		this.lowCPU = lowCPU;
	}

	public String toString() {
		return new String(highCPU + "_" + lowCPU);
	}
}
