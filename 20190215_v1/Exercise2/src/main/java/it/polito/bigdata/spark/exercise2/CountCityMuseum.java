package it.polito.bigdata.spark.exercise2;

import java.io.Serializable;

@SuppressWarnings("serial")
public class CountCityMuseum implements Serializable {
	private int numCities;
	private int numMuseumPOIs;
	
	public CountCityMuseum(int numCities, int numMuseumPOIs) {
		this.setNumCities(numCities);
		this.setNumMuseumPOIs(numMuseumPOIs);

	}

	public int getNumCities() {
		return numCities;
	}

	public void setNumCities(int numCities) {
		this.numCities = numCities;
	}

	public int getNumMuseumPOIs() {
		return numMuseumPOIs;
	}

	public void setNumMuseumPOIs(int numMuseumPOIs) {
		this.numMuseumPOIs = numMuseumPOIs;
	}


	public String toString() {
		return new String(this.numCities + " " + this.numMuseumPOIs);
	}

}
