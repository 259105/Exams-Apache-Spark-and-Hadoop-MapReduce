package it.polito.bigdata.spark.exercise2;

import org.apache.spark.api.java.*;

import scala.Tuple2;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;

public class SparkDriver {

	public static void main(String[] args) {

		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);

		String inUsers;
		String inMovies;
		String inWatchedMovies;
		String outputPathPart1;
		String outputPathPart2;

		inUsers = "exam_ex2_data/Users.txt";
		inMovies = "exam_ex2_data/Movies.txt";
		inWatchedMovies = "exam_ex2_data/WatchedMovies.txt";

		outputPathPart1 = "outPart1/";
		outputPathPart2 = "outPart2/";

		// Create a configuration object and set the name of the application
		SparkConf conf = new SparkConf().setAppName("Spark Exam - Exercise #2").setMaster("local");

		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);

		// *****************************************
		// Exercise 2 - Part 1
		// *****************************************

		// Read the content of WatchedMovies
		JavaRDD<String> inWatchedMoviesRDD = sc.textFile(inWatchedMovies).cache();

		// Data of the last 5 years
		JavaRDD<String> inWatchedMovies5YearsRDD = inWatchedMoviesRDD.filter(line -> {
			// PaoloG76,MID124,2018/06/01_14:18,2018/06/01_16:10
			String[] fields = line.split(",");
			String startTime = fields[2];

			if (startTime.compareTo("2015/09/17") >= 0 && startTime.compareTo("2020/09/16") <= 0)
				return true;
			else
				return false;
		});

		// Compute the number of distinct years in which the movie has been watched

		// Extract pairs (mid, year)
		// Remove duplicated pairs
		JavaPairRDD<String, Integer> movieYearRDD = inWatchedMovies5YearsRDD.mapToPair(line -> {
			String[] fields = line.split(",");
			String mid = fields[1];
			Integer year = Integer.parseInt(fields[2].split("/")[0]);

			return new Tuple2<String, Integer>(mid, year);
		}).distinct();

		// Map pairs to pairs (mid, +1)
		JavaPairRDD<String, Integer> movieOneRDD = movieYearRDD.mapValues(year -> 1);

		// Count the number of distinct years for each mid
		JavaPairRDD<String, Integer> movieNumDistinctYearsRDD = movieOneRDD.reduceByKey((v1, v2) -> v1 + v2);

		// Select only the movies associated with one single year
		JavaPairRDD<String, Integer> movieOneYearRDD = movieNumDistinctYearsRDD.filter(pair -> pair._2() == 1);

		// Compute the number of times each movie was watched in each year

		// Extract pairs (mid+year, +1)
		JavaPairRDD<String, Integer> movieYearOneRDD = inWatchedMovies5YearsRDD.mapToPair(line -> {
			String[] fields = line.split(",");
			String mid = fields[1];
			Integer year = Integer.parseInt(fields[2].split("/")[0]);

			return new Tuple2<String, Integer>(new String(mid + "_" + year), 1);
		});

		// Sum values for each combination mid+year
		JavaPairRDD<String, Integer> movieYearNumVisRDD = movieYearOneRDD.reduceByKey((v1, v2) -> v1 + v2);

		// Select only the combinations mid+year occurring at least 1000 times
		JavaPairRDD<String, Integer> selectedMovieYearNumVisRDD = movieYearNumVisRDD.filter(pair -> pair._2() >= 4);
		// .filter(pair -> pair._2() >= 1000);

		// Map pairs to pairs (mid, year)
		JavaPairRDD<String, Integer> movieYear1000RDD = selectedMovieYearNumVisRDD.mapToPair(pair -> {
			String[] fields = pair._1().split("_");
			String mid = fields[0];
			Integer year = Integer.parseInt(fields[1]);

			return new Tuple2<String, Integer>(mid, year);
		});

		// Join the content of movieOneYearRDD and movieYear1000RDD to select the movies
		// satisfying both constraints
		JavaPairRDD<String, Tuple2<Integer, Integer>> joinRDD = movieOneYearRDD.join(movieYear1000RDD);

		// Store only the combination (mid,year)
		JavaPairRDD<String, Integer> selecteMidYearRDD = joinRDD
				.mapToPair(pair -> new Tuple2<String, Integer>(pair._1(), pair._2()._2()));

		selecteMidYearRDD.saveAsTextFile(outputPathPart1);

		// *****************************************
		// Exercise 2 - Part 2
		// *****************************************

		// For each combination movie+year, compute the number of distinct users who
		// watched that movie in that year

		// Extract combinations mid+year+username and remove duplicates
		JavaRDD<String> midYearUserRDD = inWatchedMoviesRDD.map(line -> {
			// PaoloG76,MID124,2018/06/01_14:18,2018/06/01_16:10
			String[] fields = line.split(",");
			String username = fields[0];
			String mid = fields[1];
			Integer year = Integer.parseInt(fields[2].split("/")[0]);

			return mid + "_" + year + "_" + username;
		}).distinct();

		// Extract pairs (mid+year, +1)
		JavaPairRDD<String, Integer> midYearOneRDD = midYearUserRDD.mapToPair(line -> {
			String[] fields = line.split("_");
			String mid = fields[0];
			String year = fields[1];

			return new Tuple2<String, Integer>(mid + "_" + year, 1);
		});

		// Sum ones to count the number of distinct users for each combination mid+year
		JavaPairRDD<String, Integer> midYearNumUsersRDD = midYearOneRDD.reduceByKey((v1, v2) -> v1 + v2);
		
		midYearNumUsersRDD.saveAsTextFile("temp/");

		// Compute the maximum number of distinct users for each year (i.e., the highest
		// popularity for each year)

		// Map each pair to a new pair
		// key = year
		// value = number of distinct users
		// Then, compute the maximum value for each key
		JavaPairRDD<String, Integer> yearMaxPopularityRDD = midYearNumUsersRDD.mapToPair(pair -> {
			String[] fields = pair._1().split("_");

			String year = fields[1];

			Integer numDistinctUsers = pair._2();

			return new Tuple2<String, Integer>(year, numDistinctUsers);
		}).reduceByKey((v1, v2) -> {
			if (v1 > v2)
				return v1;
			else
				return v2;
		});

		// Now we must join the content of midYearNumUsersRDD with that of
		// yearMaxPopularityRDD
		// to select the most popular movie(s) for each year

		// Map each pair to a pair
		// key = year
		// value = mid,number of distinct users
		JavaPairRDD<String, MidNumUsers> yearMidDistinctUsersRDD = midYearNumUsersRDD.mapToPair(pair -> {
			String[] fields = pair._1().split("_");
			String mid = fields[0];
			String year = fields[1];

			Integer numDistinctUsers = pair._2();

			return new Tuple2<String, MidNumUsers>(year, new MidNumUsers(mid, numDistinctUsers));
		});

		// Join yearMidDistinctUsersRDD and yearMaxPopularityRDD
		JavaPairRDD<String, Tuple2<MidNumUsers, Integer>> joinByYearRDD = yearMidDistinctUsersRDD
				.join(yearMaxPopularityRDD);

		// Select only the most popular movie for each year
		JavaPairRDD<String, Tuple2<MidNumUsers, Integer>> mostPopularMovieYearRDD = joinByYearRDD.filter(pair -> {
			if (pair._2()._1().numUsers == pair._2()._2())
				return true;
			else
				return false;
		});

		// Extract pairs (movie, +1)
		JavaPairRDD<String, Integer> movieMostPopularOneRDD = mostPopularMovieYearRDD.mapToPair(pair -> {
			String mid = pair._2()._1().mid;
			return new Tuple2<String, Integer>(mid, 1);
		});

		// Count the number of times each movie has been the most popular
		JavaPairRDD<String, Integer> movieMostPopularTimesRDD = movieMostPopularOneRDD.reduceByKey((v1, v2) -> v1 + v2);

		// Select and store only the movies that have been the most popular in at least two years
		JavaPairRDD<String, Integer> selectedMoviesRDD = movieMostPopularTimesRDD.filter(pair -> pair._2()>=2);
		
		selectedMoviesRDD.keys().saveAsTextFile(outputPathPart2);
		
		sc.close();
	}
}
