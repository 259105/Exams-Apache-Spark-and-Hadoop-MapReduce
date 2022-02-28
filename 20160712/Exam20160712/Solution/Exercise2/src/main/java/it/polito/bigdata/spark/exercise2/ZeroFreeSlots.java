package it.polito.bigdata.spark.exercise2;

import org.apache.spark.api.java.function.Function;

@SuppressWarnings("serial")
public class ZeroFreeSlots implements Function<String, Boolean> {

	@Override
	public Boolean call(String line)  {
		String[] fields=line.split(",");
		
		int numFreeSlots=Integer.parseInt(fields[5]);
		
		if (numFreeSlots==0)
			return true;
		else
			return false;
	}

}
