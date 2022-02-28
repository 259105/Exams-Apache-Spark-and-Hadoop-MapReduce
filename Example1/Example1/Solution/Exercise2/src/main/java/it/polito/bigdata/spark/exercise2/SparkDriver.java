package it.polito.bigdata.spark.exercise2;

import org.apache.spark.api.java.*;

import scala.Tuple2;

import org.apache.spark.SparkConf;
	
public class SparkDriver {
	
	
	public static void main(String[] args) {

		String inputPathWatched;
		String inputPathPreferences;
		String inputPathMovies;
		String outputPathUseless;
		String outputPathMisleading;

		double threshold;
		
		inputPathWatched=args[0];
		inputPathPreferences=args[1];
		inputPathMovies=args[2];
		threshold=Double.parseDouble(args[3]);
		outputPathUseless=args[4];
		outputPathMisleading=args[5];
	
		// Create a configuration object and set the name of the application
		SparkConf conf=new SparkConf().setAppName("Spark Exam 1 - Exercise #2");
		
		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);

		// *****************************************
		// Exercise 2 - Part A
		// *****************************************

		// Read the content of movies.txt
		JavaRDD<String> moviesRDD = sc.textFile(inputPathMovies).cache();

		// Select only the genre attribute
		JavaRDD<String> genresRDD = moviesRDD.map(new SelectGenre()); 
		
		// Count the number of possible genres (=distinct genres in genresRDD)
		JavaRDD<String> distinctGenresRDD = genresRDD.distinct();
		long numGenres=distinctGenresRDD.count();
		
		// Count the number of liked genres for each user
		// It is like a "word count" problem where each user 
		// is a "word" and the number of occurrences is given 
		// by the number of "genres" associated with him/her,
		// i.e., the number of liked genres for each user 
		// is equal to the number of lines of preferencesProfile 
		// containing the user

		// Read the content of preferencesProfile.txt
		JavaRDD<String> preferencesRDD=sc.textFile(inputPathPreferences).cache();
		
		// Generate one pair userid,1 for each line of preferencesProfile.txt 
		JavaPairRDD<String,Integer> userOneRDD=preferencesRDD.mapToPair(new UserOne());

		// Reduce by key (=userid) to compute the number of liked genres for
		// each user
		// The output RDD contains one pair (userid, num liked genres) for each user 
		JavaPairRDD<String,Integer> usersNumLikedGenres=userOneRDD.reduceByKey(new Sum());
		
		// Select the user with more than 90% of the possible genes
		JavaPairRDD<String,Integer> usersUselessProfile=usersNumLikedGenres.filter(new UserUseless(numGenres));

		// Select only the userids of the selected users
		JavaRDD<String> userIdsUseless=usersUselessProfile.keys();
		
		// Store the selected users in the output folder
		userIdsUseless.saveAsTextFile(outputPathUseless);
		
		
		// *****************************************
		// Exercise 2 - Part B
		// *****************************************

		// Read the content of the watched movies file
		JavaRDD<String> watchedRDD = sc.textFile(inputPathWatched);
		
		// Select only userid and movieid
		// Define a JavaPairRDD with movieid as key and userid as value
		JavaPairRDD<String,String> movieUserPairRDD=watchedRDD.mapToPair(new MovieUser());

		// Read the content of the movies file
		// The content of movies is already in moviesRDD 
		
		// Select only movieid and genre
		// Define a JavaPairRDD with movieid as key and genre as value
		JavaPairRDD<String,String> movieGenrePairRDD=moviesRDD.mapToPair(new MovieGenre());
		
		// Join watched movie with movies
		// The output pairs are pairs of type (key=movied, value=Tuple2<userid,genre>)
		// It is used to associated each user with the genres of the movies he/she watched
		JavaPairRDD<String,Tuple2<String,String>> joinWatchedGenreRDD = movieUserPairRDD.join(movieGenrePairRDD);

		// We are interested only in the values of joinWatchedGenreRDD
		// They associate users with the genres of the movies they watched
		// Define a new PairRDD with userid (as key) and genre (as value)
		JavaPairRDD<String,String> usersWatchedGenresRDD = joinWatchedGenreRDD.mapToPair(new UserWatchedGenre());
		
		// Read the content of the preferences/liked genres
		// The content of preferencesProfile is already in preferencesRDD 
		
		// Define a JavaPairRDD with userid as key and genre as value
		// Associate each user with the genres he/she likes
		JavaPairRDD<String,String> userLikedGenresRDD = preferencesRDD.mapToPair(new UserLikedGenre());
		
		// Cogroup the lists of watched and liked genres for each user
		// There is one pair for each userid
		// The key is the userid. The value is a Tuple2 containing 
		// the list of watched genres and the list of liked genres 
		// the value contains the list of watched genres and the list of liked genres
		JavaPairRDD<String,Tuple2<Iterable<String>,Iterable<String>>> userWatchedLikedGenres = usersWatchedGenresRDD.cogroup(userLikedGenresRDD);
		
		// Filter the users with a misleading profile
		// For each user, the filter compares the list of watched genres and the list of liked genres
		// The to two lists of one single user are "small". 
		// Hence, the comparison can be executed in main memory 
		JavaPairRDD<String,Tuple2<Iterable<String>,Iterable<String>>> misleadingUsersListsRDD=userWatchedLikedGenres.filter(new CheckMisleading(threshold));
		
		// Select only the userid of the users with a misleading profile
		JavaRDD<String> misleadingUsersRDD=misleadingUsersListsRDD.keys();
		
		// Store the result in the HDFS folder
		misleadingUsersRDD.saveAsTextFile(outputPathMisleading);
		
		// Close the Spark context
		sc.close(); 
	}
}
