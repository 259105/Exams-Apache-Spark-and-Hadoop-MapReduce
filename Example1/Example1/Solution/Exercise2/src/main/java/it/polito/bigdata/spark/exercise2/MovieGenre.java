package it.polito.bigdata.spark.exercise2;

import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

@SuppressWarnings("serial")
public class MovieGenre implements PairFunction<String, String, String> {

	@Override
	public Tuple2<String, String> call(String line) {
		
		String[] fields=line.split(",");
		Tuple2<String, String> movieGenre=new Tuple2<String,String>(fields[0],fields[2]); 
		
		return movieGenre;
	}

}
