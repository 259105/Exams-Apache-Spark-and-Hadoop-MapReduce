package it.polito.bigdata.spark.exercise2;

import org.apache.spark.api.java.*;

import scala.Tuple2;

import org.apache.spark.SparkConf;

public class SparkDriver {

	@SuppressWarnings("resource")
	public static void main(String[] args) {

		String inputPathRobots;
		String inputPathFaults;
		String outputPathPartA;
		String outputPathPartB;

		inputPathRobots = args[0];
		inputPathFaults = args[1];
		outputPathPartA = args[2];
		outputPathPartB = args[3];

		// Create a configuration object and set the name of the application
		SparkConf conf = new SparkConf().setAppName("Spark Exam 2018_07_16 - Exercise #2 - v2");

		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);

		// *****************************************
		// Exercise 2 - Part A
		// *****************************************

		// Read the content of robots.txt
		JavaRDD<String> robots = sc.textFile(inputPathRobots);

		// Map each input line to a pair
		// key = RID
		// value = PlantID
		JavaPairRDD<String, String> ridPlantID = robots.mapToPair(line -> {
			// RID,PlantID,MAC
			String[] fields = line.split(",");
			String rid = fields[0];
			String plantID = fields[1];

			return new Tuple2<String, String>(rid, plantID);
		});

		// Read the content of Faults.txt
		JavaRDD<String> faults = sc.textFile(inputPathFaults);

		// Select data related to the first semester of year 2015
		// RID,FaultTypeCode,FaultDuration,Date,Time
		// Example: R5,FCode122,20,2017/05/02,06:40:51

		JavaRDD<String> faultsISem2015 = faults.filter(line -> {
			String[] fields = line.split(",");
			String date = fields[3];

			if (date.compareTo("2015/01/01") >= 0 && date.compareTo("2015/06/30") <= 0)
				return true;
			else
				return false;
		}).cache();

		// Map each input line to a pair
		// key = RID
		// value = 1
		JavaPairRDD<String, Integer> ridOne = faultsISem2015.mapToPair(line -> {
			// RID,PlantID,MAC
			// R2,PID20,02:42:3e:aa:a4:d9
			String[] fields = line.split(",");
			String rid = fields[0];

			return new Tuple2<String, Integer>(rid, 1);
		});

		// Compute the number of failures per RID.
		// Those values can be used to compute the total number of failures per PlantID
		// This pre-computation allows reducing the number of input records of the next
		// join
		JavaPairRDD<String, Integer> ridNumFailures = ridOne.reduceByKey((v1, v2) -> v1 + v2);

		// Join ridPlantID and ridNumFailures
		// Returned pairs
		// key = RID
		// value = Tuple2<PlantID, num. failures for the RID>
		JavaPairRDD<String, Tuple2<String, Integer>> ridPlantIDRIDsFailures = ridPlantID.join(ridNumFailures);

		// Map each input element to a pair
		// key = PlantID
		// value = num. failures for the RID
		JavaPairRDD<String, Integer> plantIDOne = ridPlantIDRIDsFailures
				.mapToPair(pair -> new Tuple2<String, Integer>(pair._2()._1(), pair._2()._2()));

		// Count the number of element for each PlantID
		JavaPairRDD<String, Integer> planIDnumFaults = plantIDOne.reduceByKey((v1, v2) -> v1 + v2);

		// Select only the production plants with at least 180 failures
		JavaPairRDD<String, Integer> selectedPlantIDnumFaults = planIDnumFaults.filter(element -> {
			if (element._2() >= 180)
				return true;
			else
				return false;
		});

		selectedPlantIDnumFaults.saveAsTextFile(outputPathPartA);

		// *****************************************
		// Exercise 2 - Part B
		// *****************************************

		// Map each input line to a pair
		// key = (RID_month)
		// value = (1,FaultDuration)
		JavaPairRDD<String, NumFaultsMaxDuration> ridMonthStatistics = faultsISem2015.mapToPair(line -> {
			// RID,FaultTypeCode,FaultDuration,Date,Time
			String[] fields = line.split(",");
			String rid = fields[0];
			int duration = Integer.parseInt(fields[2]);
			String month = fields[3].split("/")[1];

			return new Tuple2<String, NumFaultsMaxDuration>(new String(rid + "_" + month),
					new NumFaultsMaxDuration(1, duration));
		});

		// Count the number of faults in each month for each each robot
		// Compute also the local maximum value of FaultDuration to create a
		// preaggregation/precomputation of max FaultDuration for each robot
		// For each RID+month we have the total number of faults and the
		// maximum value of FaultDuration in that month for that robot
		JavaPairRDD<String, NumFaultsMaxDuration> ridMonthAggregateStatistics = ridMonthStatistics
				.reduceByKey((v1, v2) -> {
					int maxDuration;
					if (v1.getMaxDuration() > v2.getMaxDuration())
						maxDuration = v1.getMaxDuration();
					else
						maxDuration = v2.getMaxDuration();

					return new NumFaultsMaxDuration(v1.getNumFaults() + v2.getNumFaults(), maxDuration);
				});

		// Select only the month with at least five failures
		JavaPairRDD<String, NumFaultsMaxDuration> ridMonthAggregateStatisticsSelected = ridMonthAggregateStatistics
				.filter(pair -> {
					if (pair._2().getNumFaults() >= 5)
						return true;
					else
						return false;
				});

		// Map each input element to a pair
		// key = RID
		// value = (1, partialMaxFaultDuration)
		JavaPairRDD<String, NumFaultsMaxDuration> ridPairs = ridMonthAggregateStatisticsSelected.mapToPair(element -> {

			String rid = element._1().split("_")[0];
			int maxFaultDurationMonth = element._2().getMaxDuration();

			return new Tuple2<String, NumFaultsMaxDuration>(rid, new NumFaultsMaxDuration(1, maxFaultDurationMonth));
		});

		// Apply reduceByKey to compute the total number of months with at least
		// 5 faults for each robot and also the maximum FaultDuration for each
		// robot
		JavaPairRDD<String, NumFaultsMaxDuration> ridNumMonthsWithFailuresAndTotalDowntime = ridPairs
				.reduceByKey((v1, v2) -> {
					int maxDuration;
					if (v1.getMaxDuration() > v2.getMaxDuration())
						maxDuration = v1.getMaxDuration();
					else
						maxDuration = v2.getMaxDuration();

					return new NumFaultsMaxDuration(v1.getNumFaults() + v2.getNumFaults(), maxDuration);
				});

		// Select only the robots with 6 months with at least five failures
		// and at the max(FaultDuration)>120 minutes
		JavaPairRDD<String, NumFaultsMaxDuration> selectedRobots = ridNumMonthsWithFailuresAndTotalDowntime
				.filter(element -> {
					int monthsWith5Failures = element._2().getNumFaults();
					int maxFaultDurationIsem2015 = element._2().getMaxDuration();

					if (monthsWith5Failures == 6 && maxFaultDurationIsem2015 > 120)
						return true;
					else
						return false;

				});

		// Save the SIDs of the selected servers
		selectedRobots.keys().saveAsTextFile(outputPathPartB);

		sc.close();
	}
}
