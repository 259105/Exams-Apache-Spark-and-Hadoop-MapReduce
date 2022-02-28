package it.polito.bigdata.spark.exercise2;

import org.apache.spark.api.java.function.Function2;

@SuppressWarnings("serial")
public class Sum implements Function2<ComplexCount, ComplexCount, ComplexCount> {

	@Override
	public ComplexCount call(ComplexCount count1, ComplexCount count2) throws Exception {

		ComplexCount localCount=new ComplexCount();

		localCount.numCheapPurchases=count1.numCheapPurchases+count2.numCheapPurchases;
		localCount.numPurchases=count1.numPurchases+count2.numPurchases;
		
		return localCount;
	}

}
