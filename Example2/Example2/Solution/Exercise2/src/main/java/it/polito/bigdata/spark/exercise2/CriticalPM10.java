package it.polito.bigdata.spark.exercise2;

import org.apache.spark.api.java.function.Function;

@SuppressWarnings("serial")
public class CriticalPM10 implements Function<String, Boolean> {

	private double PM10threshold;
	
	public CriticalPM10(double PM10th) {
		PM10threshold=PM10th;
	}
	
	public Boolean call(String line)  {

		// fields[2] = PM10 value
		String[] fields=line.split(",");
		if (Double.parseDouble(fields[2])>PM10threshold)
			return true;
		else
			return false;
	}

}
