package it.polito.bigdata.spark.exercise2;

import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

@SuppressWarnings("serial")
public class Propensity implements Function<Tuple2<String, ComplexCount>, Boolean> {

	private double threshold; 
	
	public Propensity(double th) {
		threshold=th;
	}
	
	@Override
	public Boolean call(Tuple2<String, ComplexCount> count)  {

		if (((double)count._2().numCheapPurchases)>=((double)count._2().numPurchases)*threshold) 
			return true;
		else
			return false;
		
	}

}
