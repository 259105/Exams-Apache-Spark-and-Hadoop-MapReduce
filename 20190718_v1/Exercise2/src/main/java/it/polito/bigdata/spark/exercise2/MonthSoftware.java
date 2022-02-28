package it.polito.bigdata.spark.exercise2;

import java.io.Serializable;

@SuppressWarnings("serial")
public class MonthSoftware implements Serializable {
	private int month;
	private String software;

	public MonthSoftware(int month, String software) {
		this.month = month;
		this.software = software;
	}

	public int getMonth() {
		return month;
	}

	public void setMonth(int month) {
		this.month = month;
	}

	public String getSoftware() {
		return software;
	}

	public void setSoftware(String software) {
		this.software = software;
	}

	public String toString() {
		return month + "," + software;
	}

	/*
	 * @Override public int compareTo(MonthSoftware obj2) {
	 * 
	 * if (this.month > obj2.month) return 1; else if (this.month < obj2.month)
	 * return -1; else return this.software.compareTo(obj2.software); }
	 * 
	 * @Override public int hashCode() { return (this.month +
	 * this.software).hashCode(); }
	 */

	// The objects of this class are used as keys of JavaPairRDDs. Hence, custom
	// versions of the methods equals and hashCode must be defined
	public boolean equals(Object o2) {
		String v1 = new String(month + software);
		String v2 = new String(((MonthSoftware) o2).getMonth() + ((MonthSoftware) o2).getSoftware());

		if (v1.compareTo(v2) == 0)
			return true;
		else
			return false;
	}

	@Override
	public int hashCode() {
		return (new String(this.month + this.software)).hashCode();
	}

}
