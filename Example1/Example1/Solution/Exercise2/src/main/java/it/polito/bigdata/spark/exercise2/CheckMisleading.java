package it.polito.bigdata.spark.exercise2;

import java.util.ArrayList;

import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

@SuppressWarnings("serial")
public class CheckMisleading implements Function<Tuple2<String, Tuple2<Iterable<String>, Iterable<String>>>, Boolean> {

	double threshold;
	
	public CheckMisleading(double th)
	{
		this.threshold=th;
	}
	
	@Override
	public Boolean call(Tuple2<String, Tuple2<Iterable<String>, Iterable<String>>> listWatchedLikedGenres) {

		// listWatchedLikedGenres
		// listWatchedLikedGenres._1() is the userid
		// listWatchedLikedGenres._2()._1() is the list of watched genres
		// listWatchedLikedGenres._2()._2() is the list of liked genres
		
		// Store in main memory the list of liked genres
		ArrayList<String> likedGenres=new ArrayList<String>();
		
		// Iterate over the liked genres
		for (String likedGenre: listWatchedLikedGenres._2()._2()) {
			likedGenres.add(likedGenre);
		}
		

		// Count how many of watched genres are in the liked genres
		int notLiked=0;
		// Count also the total number of watched movies
		int totalWatched=0;
		// Iterate over the watched genres
		for (String watchedGenre: listWatchedLikedGenres._2()._1()) {
			totalWatched++;
			// if the current genre is not in the liked ones
			// increment notLiked 
			if (likedGenres.contains(watchedGenre)==false) {
				notLiked++;
			}
		}
		
		// Check if the number of watched movie with a genre that is 
		// not in the liked ones is greater that threshold% of the 
		// total number of watched movies
		if ((double)notLiked>threshold*(double)totalWatched) 
			return true;
		else
			return false;
	}

}



