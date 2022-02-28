package it.polito.bigdata.spark.exercise2;

import org.apache.spark.api.java.function.Function;

@SuppressWarnings("serial")
public class GreaterPM10th implements Function<String, Boolean> {

	private Double PM10th_limit;
	
	public GreaterPM10th(Double PM10th) {
		PM10th_limit=PM10th;
	}

	@Override
	public Boolean call(String reading) throws Exception {
		
		// station1,2016/03/07,17,15,29.2,15.1
		String[] fields=reading.split(",");
		
		String year=fields[1].substring(0, 4);
	
		Double PM10=Double.parseDouble(fields[4]);
		
		if (year.equals("2015")==true && PM10>PM10th_limit)
			return true;
		else
			return false;
	}

}
