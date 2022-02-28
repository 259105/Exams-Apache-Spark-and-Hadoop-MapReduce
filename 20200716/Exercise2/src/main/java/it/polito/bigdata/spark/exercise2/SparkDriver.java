package it.polito.bigdata.spark.exercise2;

import org.apache.spark.api.java.*;

import scala.Tuple2;

import java.util.ArrayList;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;

public class SparkDriver {

	
	public static void main(String[] args) {

		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);

		//String inputPathBooks;
		String inputPathPurchases;
		String outputPathPart1;
		String outputPathPart2;

		//inputPathBooks = "exam_ex2_data/Books.txt";
		inputPathPurchases = "exam_ex2_data/Purchases.txt";
		outputPathPart1 = "outPart1/";
		outputPathPart2 = "outPart2/";

		// Create a configuration object and set the name of the application
		SparkConf conf = new SparkConf().setAppName("Spark Exam - Exercise #2").setMaster("local");

		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);

		// *****************************************
		// Exercise 2 - Part 1
		// *****************************************

		// Read the content of Purchases.txt
		JavaRDD<String> purchasesRDD = sc.textFile(inputPathPurchases);

		// Select only the purchases related to year 2018
		JavaRDD<String> purchases2018RDD = purchasesRDD.filter(line -> {
			String[] fields = line.split(",");
			String date = fields[2];

			if (date.startsWith("2018"))
				return true;
			else
				return false;
		});

		// Emit a pair from each input element
		// - key = bid+date
		// - value = +1
		JavaPairRDD<String, Integer> bidDatePurchases2018RDD = purchases2018RDD.mapToPair(line -> {
			String[] fields = line.split(",");
			String bid = fields[1];
			String date = fields[2];

			return new Tuple2<String, Integer>(bid + "_" + date, 1);
		});

		// Count the number of purchases for each bid in each date (i.e., num. daily
		// purchases)
		JavaPairRDD<String, Integer> bidDailyPurchases2018RDD = bidDatePurchases2018RDD
				.reduceByKey((v1, v2) -> v1 + v2).cache();

		// Select the maximum number of daily purchases for each book

		// Emit a pair from each input element
		// - key = bid
		// - value = num. daily purchases
		JavaPairRDD<String, Integer> bidNumPurchases2018RDD = bidDailyPurchases2018RDD.mapToPair(pair -> {
			String bidDate = pair._1();
			int numDailyPurchases = pair._2();
			// Select the bid part
			String bid = bidDate.split("_")[0];

			return new Tuple2<String, Integer>(bid, numDailyPurchases);
		}).cache();

		// Compute the maximum value for each bid
		JavaPairRDD<String, Integer> bidMaxDailyPurchases2018RDD = bidNumPurchases2018RDD
				.reduceByKey((v1, v2) -> {
					if (v1 > v2)
						return v1;
					else
						return v2;
				});
		
		// Store the result in the first output folder
		bidMaxDailyPurchases2018RDD.saveAsTextFile(outputPathPart1);

		
		// *****************************************
		// Exercise 2 - Part 2
		// *****************************************
		
		// Compute the total number of purchases for each book in year 2018
		JavaPairRDD<String, Integer> bidTotPurchases2018RDD = bidNumPurchases2018RDD
				.reduceByKey((v1, v2) -> v1+v2);
		
		// Select for each book the dates with more that 0.1*total num. purchases book 2018 
		
		// Emit one pair from each input pair
		// - key = bid
		// - value = (date, num. daily purchases 2018)
		
		JavaPairRDD<String, DateNumPurchases18> bidDateNumPurchasesRDD = bidDailyPurchases2018RDD
				.mapToPair(pair -> {
					String bidDate = pair._1();
					int numDailyPurchases = pair._2();
					// Select bid and date
					String[] fields = bidDate.split("_");
					String bid = fields[0];
					String date = fields[1];

					return new Tuple2<String, DateNumPurchases18>(bid, 
							new DateNumPurchases18(date, numDailyPurchases));
	
				}); 
		
		// Join bidDateNumPurchasesRDD with bidTotPurchases2018RDD
		JavaPairRDD<String, Tuple2<DateNumPurchases18, Integer>>  bidDateDailyandYearlyNumPurchasesRDD = 
				bidDateNumPurchasesRDD.join(bidTotPurchases2018RDD);
		
		// Select for each book the dates with more that 0.1*total num. purchases book 2018 
		JavaPairRDD<String, Tuple2<DateNumPurchases18, Integer>>  selectedbidDateDailyandYearlyNumPurchasesRDD = 
				bidDateDailyandYearlyNumPurchasesRDD.filter(pair ->
				{
					int dailyPurchases = pair._2()._1().numPurchases18;
					int yearlyPurchases = pair._2()._2();
					
					if (dailyPurchases>0.1*yearlyPurchases)
						return true;
					else
						return false;
				});
		
		// Create/count the elements of the windows of three consecutive dates for each book
		// For each input pair return three pairs
		// - (bid+date, +1)
		// - (bid+date-1, +1)
		// - (bid+date-2, +1)
		JavaPairRDD<String,Integer> bidDateElementRDD =  selectedbidDateDailyandYearlyNumPurchasesRDD
				.flatMapToPair(pair -> 
				{
					ArrayList<Tuple2<String, Integer>> elements = new ArrayList<Tuple2<String, Integer>>();
					
					String bid = pair._1();
					String date = pair._2()._1().date;

					String previousDate = DateTool.previousDeltaDate(date,1); 
					String twoDaysAgoDate = DateTool.previousDeltaDate(date,2); 
					
					elements.add(new Tuple2<String, Integer>(bid+","+date, 1));
					elements.add(new Tuple2<String, Integer>(bid+","+previousDate, 1));
					elements.add(new Tuple2<String, Integer>(bid+","+twoDaysAgoDate, 1));
					
					return elements.iterator();
				});


		// Count the number of dates for each window
		JavaPairRDD<String,Integer> bidDateNumElementsRDD =bidDateElementRDD.reduceByKey((v1,v2)-> v1+v2);
		
		// If the number of dates is 3 it means that that window must be selected 
		JavaPairRDD<String,Integer> selectedWindowsRDD = bidDateNumElementsRDD
				.filter(pair -> pair._2().intValue()==3);

		// Store the combinations (BID,first date) in the second output folder
		selectedWindowsRDD.keys().saveAsTextFile(outputPathPart2);

		sc.close();
	}
}
