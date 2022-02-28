package it.polito.bigdata.spark.exercise2;

import java.io.Serializable;

@SuppressWarnings("serial")
public class Counter implements Serializable {
	private int numPatchesWindows;
	private int numPatchesUbuntu;

	public Counter(int numPatchesWindows, int numPatchesUbuntu) {
		this.numPatchesWindows = numPatchesWindows;
		this.numPatchesUbuntu = numPatchesUbuntu;
	}

	public int getNumPatchesWindows() {
		return numPatchesWindows;
	}

	public void setNumPatchesWindows(int numPatchesWindows) {
		this.numPatchesWindows = numPatchesWindows;
	}

	public int getNumPatchesUbuntu() {
		return numPatchesUbuntu;
	}

	public void setNumPatchesUbuntu(int numPatchesUbuntu) {
		this.numPatchesUbuntu = numPatchesUbuntu;
	}

}
