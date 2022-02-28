package it.polito.bigdata.spark.exercise2;

import org.apache.spark.api.java.*;

import org.apache.spark.SparkConf;
	
public class SparkDriver {
	
	
	public static void main(String[] args) {

		String inputPathReadings;
		String outputPathPartA;
		String outputPathPartB;
		Double pm10th_limit;
		Double pm25th_limit;
		
		inputPathReadings=args[0];
		outputPathPartA=args[1];
		outputPathPartB=args[2];
		pm10th_limit=Double.parseDouble(args[3]);
		pm25th_limit=Double.parseDouble(args[4]);
		
	
		// Create a configuration object and set the name of the application
		SparkConf conf=new SparkConf().setAppName("Spark Exam 2016_09_19 - Exercise #2");
		
		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);

		// *****************************************
		// Exercise 2 - Part A
		// *****************************************


		// Read the content of ReadingsPerMonitoringStations.txt
		JavaRDD<String> readingsRDD = sc.textFile(inputPathReadings);

		// Select the reading of year 2015 with PM10>PM10th_limit
		JavaRDD<String> readings2015HighPM10RDD = readingsRDD.filter(new GreaterPM10th(pm10th_limit));

		// Map to pairs stationid, 1
		JavaPairRDD<String,Integer> stationOnesPairRDD=readings2015HighPM10RDD.mapToPair(new StationIdOne());

		// Count lines per stations
		JavaPairRDD<String,Integer> stationsCountsRDD=stationOnesPairRDD.reduceByKey(new SumOnes());
		
		// Select Highly Polluted Stations
		JavaPairRDD<String,Integer> highlyPollutedStations=stationsCountsRDD.filter(new HPS());
		
		// Select the ids of the stations
		// Store the selected stations in the output folder
		highlyPollutedStations.keys().saveAsTextFile(outputPathPartA);
		
		// *****************************************
		// Exercise 2 - Part B
		// *****************************************
		// Count for each station the number of lines of ReadingsPerMonitoringStations.txt
		// associated with that station with 
		// PM2.5<=PM2.5th_limit
		// If the value is 0 it means that station is a "always polluted station" because
		// the value of PM2.5 was always above the user provided  threshold	for that station

		// For each line of ReadingsPerMonitoringStations.txt, the program emits one pair
		// key = stationId
		// value = 1 if PM2.5<=0PM2.5th_limit
		// 		 = 0 if PM2.5>PM2.5th_limit
		
		JavaPairRDD<String,Integer> StationsCountersRDD=readingsRDD.mapToPair(new StationCounters(pm25th_limit));
		
		// Count the total number of lines and the number of lines with PM2.5>=PM2.5th_limit
		// for each station
		JavaPairRDD<String,Integer> TotalStationsCountersRDD=StationsCountersRDD.reduceByKey(new Sum());
		
		// Select the stations with 
		// (total number of lines)=(number of lines with PM2.5>=PM2.5th_limit)
		JavaPairRDD<String,Integer> SelectedStations=TotalStationsCountersRDD.filter(new AlwaysPolluted());
		
		// Select the keys and store them
		SelectedStations.keys().saveAsTextFile(outputPathPartB);

		// Close the Spark context
		sc.close(); 
	}
}
