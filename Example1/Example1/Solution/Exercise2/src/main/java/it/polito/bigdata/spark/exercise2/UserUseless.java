package it.polito.bigdata.spark.exercise2;

import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

@SuppressWarnings("serial")
public class UserUseless implements Function<Tuple2<String, Integer>, Boolean> {

	private long numGenres;
	
	public UserUseless(long numGen)
	{
		this.numGenres=numGen;
	}
	
	@Override
	public Boolean call(Tuple2<String, Integer> userNumLikedGenres) {
		
		// Select the user if he/she likes more than 90% of the genres
		// The number of liked genres is in userNumLikedGenres._2();
		if ((double)userNumLikedGenres._2()>0.9*(double)numGenres)
			return true;
		else
			return false;
	}

}
