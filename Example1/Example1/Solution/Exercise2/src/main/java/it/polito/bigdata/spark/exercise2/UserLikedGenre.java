package it.polito.bigdata.spark.exercise2;

import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

@SuppressWarnings("serial")
public class UserLikedGenre implements PairFunction<String, String, String> {

	@Override
	public Tuple2<String, String> call(String line) throws Exception {
		
		String[] fields=line.split(",");
		Tuple2<String, String> userGenre=new Tuple2<String,String>(fields[0],fields[1]); 
		
		return userGenre;
	}

}
