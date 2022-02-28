package it.polito.bigdata.spark.exercise2;

import java.io.Serializable;

@SuppressWarnings("serial")
public class NumFailuresDowntime implements Serializable {
	private int numFailures;
	private int downtimeMinutes;

	public NumFailuresDowntime(int numFailures, int downtimeMinutes) {
		this.numFailures = numFailures;
		this.downtimeMinutes = downtimeMinutes;
	}

	public int getNumFailures() {
		return numFailures;
	}

	public void setNumFailures(int numFailures) {
		this.numFailures = numFailures;
	}

	public int getDowntimeMinutes() {
		return downtimeMinutes;
	}

	public void setDowntimeMinutes(int downtimeMinutes) {
		this.downtimeMinutes = downtimeMinutes;
	}

	public String toString() {
		return new String("num. failures:" + numFailures + " tot. downtime:" + downtimeMinutes);
	}

}
