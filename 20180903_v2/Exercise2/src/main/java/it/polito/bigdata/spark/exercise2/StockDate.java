package it.polito.bigdata.spark.exercise2;

import java.io.Serializable;

@SuppressWarnings("serial")
public class StockDate implements Serializable {
	private String stockId;
	private String date;

	public StockDate(String stockId, String date) {
		this.stockId=stockId;
		this.date=date;
	}

	public String getStockId() {
		return stockId;
	}

	public void setStockId(String stockId) {
		this.stockId = stockId;
	}

	public String getDate() {
		return date;
	}

	public void setDate(String date) {
		this.date = date;
	}

	public String toString() {
		return new String(stockId + "," + date);
	}

	// The objects of this class are used as keys of JavaPairRDDs. Hence, custom versions of the methods equals and hashCode must be defined
	public boolean equals(Object o2) {
		String v1 = new String(stockId + date);
		String v2 = new String(((StockDate)o2).getStockId() + ((StockDate)o2).getDate());

		if (v1.compareTo(v2) == 0)
			return true;
		else
			return false;
	}

	@Override
	public int hashCode() {
		return (new String(stockId + date)).hashCode();
	}

}
