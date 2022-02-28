package it.polito.bigdata.spark.exercise2;

import org.apache.spark.api.java.*;

import scala.Tuple2;

import java.util.List;

import org.apache.spark.SparkConf;

public class SparkDriver {

	public static void main(String[] args) {

		String inputPathBooks;
		String inputPathPurchases;
		String outputPathPartA;
		String outputPathPartB;

		inputPathBooks = args[0];
		inputPathPurchases = args[1];
		outputPathPartA = args[2];
		outputPathPartB = args[3];

		// Create a configuration object and set the name of the application
		SparkConf conf = new SparkConf().setAppName("Spark Exam 2018_01_22 - Exercise #2");

		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);

		// *****************************************
		// Exercise 2 - Part A
		// *****************************************

		// Read the content of Purchases.txt
		JavaRDD<String> purchases = sc.textFile(inputPathPurchases);

		// Select purchases 2017
		JavaRDD<String> purchases2017 = purchases.filter(new Purchases2017()).cache();

		// Compute max and min price for each book
		JavaPairRDD<String, MinMax> bidPrice = purchases2017.mapToPair(new BidPricePrice());

		JavaPairRDD<String, MinMax> bibMinMax = bidPrice.reduceByKey(new ReduceMinMax());

		// Select books having max-min>15
		JavaPairRDD<String, MinMax> booksAnomalousPrice = bibMinMax.filter(new AnomalousPrice());

		booksAnomalousPrice.saveAsTextFile(outputPathPartA);

		// *****************************************
		// Exercise 2 - Part B
		// *****************************************
		// Read the content of Books.txt
		JavaRDD<String> books = sc.textFile(inputPathBooks);

		// Select BIDS of all books
		JavaRDD<String> bids = books.map(new ExtractBid());

		// Select BIDS of the books with at least one purchase in 2017
		JavaRDD<String> bidsAtLeastOnePurchase = purchases2017.map(new ExtractBidPurchases());

		// Select the bids of the never purchased books (in 2017)
		JavaRDD<String> neverPurchased = bids.subtract(bidsAtLeastOnePurchase);

		// The number of never purchased books in 2017 is small enough to be
		// stored in a local variable (this assumption is reported in the
		// problem spcification)
		List<String> localNeverPurchased = neverPurchased.collect();

		// Map each book to Pair(genre, (+1,0/1))
		// 1 = never purchased book 2017
		// 0 = purchased book 2017
		JavaPairRDD<String, Counter> genreCounter = books.mapToPair(new GenreCounter(localNeverPurchased));
		
		// Count the number of total and never purchased books for each genre
		JavaPairRDD<String, Counter> genreTotalCounts = genreCounter.reduceByKey(new GenreTotal());
		
		genreTotalCounts.saveAsTextFile(outputPathPartB);

		sc.close();
	}
}
