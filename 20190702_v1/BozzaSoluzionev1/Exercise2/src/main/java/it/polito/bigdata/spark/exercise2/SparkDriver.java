package it.polito.bigdata.spark.exercise2;

import org.apache.spark.api.java.*;

import scala.Tuple2;

import org.apache.spark.SparkConf;

public class SparkDriver {

	public static void main(String[] args) {

		String inputBicycles;
		String inputFailures;
		String outputPathPartA;
		String outputPathPartB;

		inputBicycles = args[0];
		inputFailures = args[1];
		outputPathPartA = args[2];
		outputPathPartB = args[3];

		// Create a configuration object and set the name of the application
		SparkConf conf = new SparkConf().setAppName("Spark Exam 2019_07_02 - Exercise #2");

		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);

		// *****************************************
		// Exercise 2 - Part A
		// *****************************************

		// Read the content of Bicycles_Failures.txt
		JavaRDD<String> failuresRDD = sc.textFile(inputFailures);

		// Select failures associated with year 2018
		// Example: 2018/03/01_15:40,BID13,Wheel
		JavaRDD<String> failures2018RDD = failuresRDD.filter(line -> {
			if (line.startsWith("2018") == true)
				return true;
			else
				return false;
		}).cache();

		// Select wheel failures
		// I used to filter in order to cache the output of the first filter that is
		// used by the second part of the code (Part B).
		// Example: 2018/03/01_15:40,BID13,Wheel
		JavaRDD<String> wheelFailures2018RDD = failures2018RDD.filter(line -> {
			String[] fields = line.split(",");
			String component = fields[2];
			if (component.compareTo("Wheel") == 0)
				return true;
			else
				return false;
		});

		// Map each input line to a pair
		// key = BID_Month
		// value = 1
		JavaPairRDD<String, Integer> BIDMonthFailure = wheelFailures2018RDD.mapToPair(line -> {

			// Example: 2018/03/01_15:40,BID13,Wheel
			String[] fields = line.split(",");
			String month = fields[0].split("/")[1];
			String BID = fields[1];

			return new Tuple2<String, Integer>(new String(BID + "_" + month), 1);

		});

		// Count the number of wheel failures for each bid_month (i.e., for each bid in
		// each month of year 2018)
		JavaPairRDD<String, Integer> BIDMonthNumFailure = BIDMonthFailure.reduceByKey((v1, v2) -> new Integer(v1 + v2));

		// Select the pairs with number of failures > 2
		JavaPairRDD<String, Integer> BIDMonthNumFailureMoreThan2 = BIDMonthNumFailure.filter(pair -> {
			int numFailures = pair._2();

			if (numFailures > 2)
				return true;
			else
				return false;
		});

		// The BIDs of the selected pairs are the BIDs of the bicycles with at least one
		// month in which those bikes had more than 2 wheel failures in year 2018
		// Extract the BID part of the key
		JavaRDD<String> selectedBIDs = BIDMonthNumFailureMoreThan2
				.map((Tuple2<String, Integer> BIDMonthNumFailures) -> {
					String BID = BIDMonthNumFailures._1().split("_")[0];

					return BID;
				});

		// The same bicycle might had more than 2 wheel failures in two different months
		// of year 2018
		// Hence, duplicated must be removed

		selectedBIDs.distinct().saveAsTextFile(outputPathPartA);

		// *****************************************
		// Exercise 2 - Part B
		// *****************************************

		// Compute the number of failures for each bicycle in year 2018

		// Map each input line to a pair
		// key = BID
		// value = 1
		JavaPairRDD<String, Integer> BIDFailure = failures2018RDD.mapToPair(line -> {

			// Example: 2018/03/01_15:40,BID13,Wheel
			String[] fields = line.split(",");
			String BID = fields[1];

			return new Tuple2<String, Integer>(BID, 1);
		});

		// Compute the number of failures for each bicycle in year 2018
		JavaPairRDD<String, Integer> BIDNumFailuresRDD = BIDFailure.reduceByKey((v1, v2) -> (v1 + v2));

		// Select the BIDs of the bicycles with more than 20 failures
		JavaPairRDD<String, Integer> BIDNumFailuresMoreThan20 = BIDNumFailuresRDD
				.filter((Tuple2<String, Integer> BidNumFailures) -> {
					int numFailures = BidNumFailures._2();

					if (numFailures > 20) {
						return true;
					} else {
						return false;
					}
				});

		// Retrieve the cities of the selected BIDs

		// Read the content of bicycles.txt
		JavaRDD<String> bicyclesRDD = sc.textFile(inputBicycles).cache();

		// Map each input line to a pair
		// Key: BID
		// Value: City
		JavaPairRDD<String, String> BIDCities = bicyclesRDD.mapToPair(line -> {
			// Example: BID13,BianchiCompany,Turin,Italy
			String[] fields = line.split(",");
			String BID = fields[0];
			String city = fields[2];

			return new Tuple2<String, String>(BID, city);

		});

		// Join BIDNumFailuresMoreThan20 with BIDCities
		// The result is:
		// key: BID
		// value: numFailues, city
		JavaPairRDD<String, Tuple2<Integer, String>> BIDNumFailuresCity = BIDNumFailuresMoreThan20.join(BIDCities);

		// Select the cities for which there is at least one bicycle with more than 20
		// failures.
		JavaRDD<String> citiesToBeRemoved = BIDNumFailuresCity.map(
				(Tuple2<String, Tuple2<Integer, String>> pairBIDNumFailuresCity) -> pairBIDNumFailuresCity._2()._2());

		// Select all the cities managed by PoliBike.
		// Remove duplicates.
		JavaRDD<String> allCities = bicyclesRDD.map(line -> {
			// Example: BID13,BianchiCompany,Turin,Italy
			String[] fields = line.split(",");
			String city = fields[2];

			return city;
		}).distinct();

		// Select the cities with all the bicycles with at most 20 failures (i.e., those
		// that are never associated with a bicycle with more than 20 failures
		JavaRDD<String> selectedCities = allCities.subtract(citiesToBeRemoved).cache();

		// Save the selected cities
		selectedCities.saveAsTextFile(outputPathPartB);

		// Print on the standard output the number of select cities
		System.out.println(selectedCities.count());

		sc.close();
	}
}
