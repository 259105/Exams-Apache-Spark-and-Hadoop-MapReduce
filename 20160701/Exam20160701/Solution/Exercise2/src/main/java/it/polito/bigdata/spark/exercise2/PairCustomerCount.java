package it.polito.bigdata.spark.exercise2;

import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

@SuppressWarnings("serial")
public class PairCustomerCount implements PairFunction<String, String, ComplexCount> {

	@Override
	public Tuple2<String, ComplexCount> call(String line) {
		ComplexCount localCount;
		
		String[] fields=line.split(",");

		// fields[0] = customerid
		// fields[3] = price
		
		float price=Float.parseFloat(fields[3]);

		localCount=new ComplexCount();
		
		localCount.numPurchases=1;
		
		if (price<10)
			localCount.numCheapPurchases=1;
		else
			localCount.numCheapPurchases=0;
			
			
		return new Tuple2<String, ComplexCount>(fields[0], localCount);
	}

}
