package it.polito.bigdata.spark.exercise2;

import org.apache.spark.api.java.*;

import scala.Tuple2;

import org.apache.spark.SparkConf;

public class SparkDriver {

	public static void main(String[] args) {

		String inputPathFlights;
		String inputPathAirports;
		String outputPathPartA;
		String outputPathPartB;

		inputPathFlights = args[0];
		inputPathAirports = args[1];
		outputPathPartA = args[2];
		outputPathPartB = args[3];

		// Create a configuration object and set the name of the application
		SparkConf conf = new SparkConf().setAppName("Spark Exam 2016_09_14 - Exercise #2");

		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);

		// *****************************************
		// Exercise 2 - Part A
		// *****************************************

		// Read the content of Flights.txt
		JavaRDD<String> flights = sc.textFile(inputPathFlights);

		// Select only the flights with delay>=15
		JavaRDD<String> lateFlights = flights.filter(new LateFlight());

		// Read the content of Airports.txt
		JavaRDD<String> airports = sc.textFile(inputPathAirports);

		// Select only the airports located in Germany
		JavaRDD<String> germanAirports = airports.filter(new GermanAirport());

		// Join late flights with the subset of selected arrival airports (it
		// means we are selecting
		// only the flights with an arrival airport located in Germany)

		// Before performing join, create the pairs
		// - (arrival airport id, airline)
		// - (airport id, airport name)
		JavaPairRDD<String, String> arrivalAirportId_Airline = lateFlights.mapToPair(new ArrivalAirportAirline());

		JavaPairRDD<String, String> airportId_Name = germanAirports.mapToPair(new ArrivalAirportIdName());

		// key = airport id
		// value = (airline, airport name)
		JavaPairRDD<String, Tuple2<String, String>> selectedFlights = arrivalAirportId_Airline.join(airportId_Name);

		// create the pairs (airline-arrival airport name, +1)
		JavaPairRDD<String, Integer> airlineAirportOne = selectedFlights.mapToPair(new AirlineAirportOne());

		// count
		JavaPairRDD<String, Integer> airlineAirportCounts = airlineAirportOne.reduceByKey(new Sum());

		// swap
		JavaPairRDD<Integer, String> countAirlineAirport = airlineAirportCounts.mapToPair(new Swap());

		// sort by decreasing number of late flights
		JavaPairRDD<Integer, String> sortedAirlineAirport = countAirlineAirport.sortByKey(false);

		// select values and store then in the first output folder
		sortedAirlineAirport.saveAsTextFile(outputPathPartA);

		// *****************************************
		// Exercise 2 - Part B
		// *****************************************

		// Map flights to pairs
		// key: departure_airport_id,arrival_airport_id
		// value: two "counters": fully booked(0/1) - cancelled(0/1) - num
		// flights (+1)
		JavaPairRDD<String, Counter> fullyBookedOrCancelledOnes = flights.mapToPair(new RouteFullCancelled());

		// Count the number of fully booked, cancelled flights, and total
		// flights per route
		JavaPairRDD<String, Counter> fullyBookedOrCancelledCounters = fullyBookedOrCancelledOnes
				.reduceByKey(new SumCounters());

		// Select only the routes with at least 99% fully booked flights and at
		// least 5% cancelled flights
		JavaPairRDD<String, Counter> selectedRoutes = fullyBookedOrCancelledCounters.filter(new OverloadedRoute());

		selectedRoutes.keys().saveAsTextFile(outputPathPartB);

		sc.close();
	}
}
