package it.polito.bigdata.spark.exercise2;

import java.io.Serializable;

@SuppressWarnings("serial")
public class Counter implements Serializable {
	private int numCommitsSpark;
	private int numCommitsFlink;

	public Counter(int numCommitsSpark, int numCommitsFlink) {
		this.numCommitsSpark = numCommitsSpark;
		this.numCommitsFlink = numCommitsFlink;
	}

	public int getNumCommitsSpark() {
		return numCommitsSpark;
	}

	public void setNumCommitsSpark(int numCommitsSpark) {
		this.numCommitsSpark = numCommitsSpark;
	}

	public int getNumCommitsFlink() {
		return numCommitsFlink;
	}

	public void setNumCommitsFlink(int numCommitsFlink) {
		this.numCommitsFlink = numCommitsFlink;
	}

}
