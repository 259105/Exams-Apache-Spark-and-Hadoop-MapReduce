package it.polito.bigdata.spark.exercise2;

import java.io.Serializable;

@SuppressWarnings("serial")
public class Counter implements Serializable {

	private int count;
	private double sumCPUutil;
	private double sumRAMutil;

	public Counter(int count, double sumCPUutil, double sumRAMutil) {
		this.count = count;
		this.sumCPUutil = sumCPUutil;
		this.sumRAMutil = sumRAMutil;
	}

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}

	public double getSumCPUutil() {
		return sumCPUutil;
	}

	public void setSumCPUutil(double sumCPUutil) {
		this.sumCPUutil = sumCPUutil;
	}

	public double getSumRAMutil() {
		return sumRAMutil;
	}

	public void setSumRAMutil(double sumRAMutil) {
		this.sumRAMutil = sumRAMutil;
	}

}
