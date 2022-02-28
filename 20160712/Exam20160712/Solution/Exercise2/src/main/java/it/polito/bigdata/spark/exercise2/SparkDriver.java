package it.polito.bigdata.spark.exercise2;

import org.apache.spark.api.java.*;

import org.apache.spark.SparkConf;
	
public class SparkDriver {
	
	
	public static void main(String[] args) {

		String inputPathStations;
		String inputPathStationsOccupacy;
		String outputPathPartA;
		String outputPathPartB;
		
		inputPathStations=args[0];
		inputPathStationsOccupacy=args[1];
		outputPathPartA=args[2];
		outputPathPartB=args[3];
		
	
		// Create a configuration object and set the name of the application
		SparkConf conf=new SparkConf().setAppName("Spark Exam 2016_07_12 - Exercise #2");
		
		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);

		// *****************************************
		// Exercise 2 - Part A
		// *****************************************

		// Read the content of Stations.txt
		JavaRDD<String> stationsRDD = sc.textFile(inputPathStations);

		// Select small stations
		JavaRDD<String> smallStationsRDD = stationsRDD.filter(new SmallStation());
		
		// Select the ids of the small stations
		JavaRDD<String> IDsSmallStationsRDD = smallStationsRDD.map(new StationId());		
		
		// Read the content of StationsOccupacy.txt
		JavaRDD<String> stationsOccupacyRDD = sc.textFile(inputPathStationsOccupacy).cache();

		// Select the lines with numFreeSlots = 0
		JavaRDD<String> criticalLinesRDD = stationsOccupacyRDD.filter(new ZeroFreeSlots());
		
		
		// Select the ids of the stations associated with a critical situation
		// i.e., the potentil critical stations
		JavaRDD<String> IDsCriticalStationsRDD = criticalLinesRDD.map(new StationIdFromOccupancy());		
		
		// Compute the intersection between IDsSmallStationsRDD and IDsCriticalStationsRDD  
		// to obtain the set of potential critical small stations
		JavaRDD<String> SmallPotentialCriticalStationsRDD = IDsSmallStationsRDD.intersection(IDsCriticalStationsRDD);
		
		// Store the selected stations in the output folder
		SmallPotentialCriticalStationsRDD.saveAsTextFile(outputPathPartA);
		
		// *****************************************
		// Exercise 2 - Part B
		// *****************************************
		// Count for each station the number of lines 
		// of StationOccupancy.txt associated with that station with numFreeSlots<3
		// This is a word count-like problem. 
		// If the count for one station is 0 it means 
		// that it  has  always  had  at  least  3  free  slots 
		// (based on the historical data stored in StationsOccupancy.txt)

		// For each line of StationsOccupancy.txt, the program emits one pair
		// key = stationId
		// value = 0 if numFreeSlots>=3
		// 		 = 1 if numFreeSlots<3
		
		JavaPairRDD<String,Integer> StationsCountersRDD=stationsOccupacyRDD.mapToPair(new StationCounters());
		
		// Count the total number of lines with numFreeSlots<3 for each station
		JavaPairRDD<String,Integer> TotalStationsCountersRDD=StationsCountersRDD.reduceByKey(new Sum());
		
		// Select the stations with 
		// (number of lines with numFreeSlots<3) == 0
		JavaPairRDD<String,Integer> SelectedStations=TotalStationsCountersRDD.filter(new WellSized());
		
		// Select the keys and store them
		SelectedStations.keys().saveAsTextFile(outputPathPartB);

		// Close the Spark context
		sc.close(); 
	}
}
