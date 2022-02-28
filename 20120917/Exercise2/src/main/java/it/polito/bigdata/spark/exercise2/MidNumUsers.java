package it.polito.bigdata.spark.exercise2;

import java.io.Serializable;

@SuppressWarnings("serial")
public class MidNumUsers implements Serializable {
	public String mid;
	public int numUsers;

	MidNumUsers(String mid, int numUsers) {
		this.mid = mid;
		this.numUsers = numUsers;
	}

}
