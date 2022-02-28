package it.polito.bigdata.spark.exercise2;

import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

@SuppressWarnings("serial")
public class UserWatchedGenre implements PairFunction<Tuple2<String, Tuple2<String, String>>, String, String> {

	@Override
	public Tuple2<String, String> call(Tuple2<String, Tuple2<String, String>> userMovie) {

		// movieid - userid - genre
		Tuple2<String, String> movieGenre=
				new Tuple2<String, String>(userMovie._2()._1(), userMovie._2()._2());
		
		return movieGenre;
	}

}
