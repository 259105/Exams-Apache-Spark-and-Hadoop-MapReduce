package it.polito.bigdata.spark.exercise2;

import java.io.Serializable;

@SuppressWarnings("serial")
public class CountTaxiBusstop implements Serializable {
	private int numTaxiPOIs;
	private int numBusstopPOIs;

	public CountTaxiBusstop(int numTaxiPOIs, int numBusstopPOIs) {
		this.numTaxiPOIs = numTaxiPOIs;
		this.numBusstopPOIs = numBusstopPOIs;

	}

	public int getNumTaxiPOIs() {
		return numTaxiPOIs;
	}

	public void setNumTaxiPOIs(int numTaxiPOIs) {
		this.numTaxiPOIs = numTaxiPOIs;
	}

	public int getNumBusstopPOIs() {
		return numBusstopPOIs;
	}

	public void setNumBusstopPOIs(int numBusstopPOIs) {
		this.numBusstopPOIs = numBusstopPOIs;
	}

	public String toString() {
		return new String(this.numTaxiPOIs + " " + this.numBusstopPOIs);
	}

}
