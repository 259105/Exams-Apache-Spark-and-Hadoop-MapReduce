package it.polito.bigdata.spark.exercise2;

import java.io.Serializable;

@SuppressWarnings("serial")
public class NumFaultsMaxDuration implements Serializable {
	private int numFaults;
	private int maxDuration;

	public NumFaultsMaxDuration(int numFaults, int maxDuration) {
		this.numFaults = numFaults;
		this.maxDuration = maxDuration;
	}

	public int getNumFaults() {
		return numFaults;
	}

	public void setNumFaults(int numFaults) {
		this.numFaults = numFaults;
	}

	public int getMaxDuration() {
		return maxDuration;
	}

	public void setMaxDuration(int maxDuration) {
		this.maxDuration = maxDuration;
	}
	
	public String toString() {
		return new String("num. faults:"+numFaults+ " max duration:"+maxDuration);
	}
	

}
