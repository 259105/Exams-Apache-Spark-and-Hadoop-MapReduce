package it.polito.bigdata.spark.exercise2;

import org.apache.spark.api.java.*;

import scala.Tuple2;

import org.apache.spark.SparkConf;

public class SparkDriver {

	@SuppressWarnings("resource")
	public static void main(String[] args) {

		String inputPathServers;
		String inputPathFailures;
		String outputPathPartA;
		String outputPathPartB;

		inputPathServers = args[0];
		inputPathFailures = args[1];
		outputPathPartA = args[2];
		outputPathPartB = args[3];

		// Create a configuration object and set the name of the application
		SparkConf conf = new SparkConf().setAppName("Spark Exam 2018_07_16 - Exercise #2");

		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);

		// *****************************************
		// Exercise 2 - Part A
		// *****************************************

		// Read the content of servers.txt
		JavaRDD<String> servers = sc.textFile(inputPathServers);

		// Map each input line to a pair
		// key = SID
		// value = DataCenterID
		JavaPairRDD<String, String> sidDataCenter = servers.mapToPair(line -> {
			// SID,IP,DateCenterID
			String[] fields = line.split(",");
			String sid = fields[0];
			String dataCenterId = fields[2];

			return new Tuple2<String, String>(sid, dataCenterId);
		});

		// Read the content of Failures.txt
		JavaRDD<String> failures = sc.textFile(inputPathFailures);

		// Select data related to year 2017
		// Date,Time,SID,FailureType,Downtime
		// Example: 2016/05/01,05:40:31,S2,hard_drive,5

		JavaRDD<String> failures2017 = failures.filter(line -> {
			if (line.startsWith("2017") == true)
				return true;
			else
				return false;
		}).cache();

		// Map each input line to a pair
		// key = SID
		// value = 1
		JavaPairRDD<String, Integer> sidOne = failures2017.mapToPair(line -> {
			// Date,Time,SID,FailureType,Downtime
			String[] fields = line.split(",");
			String sid = fields[2];

			return new Tuple2<String, Integer>(sid, 1);
		});

		// Compute the number of failures per SID. 
		// Those values can be used to compute the total number of failures per data center
		// This pre-computation allows reducing the number of input records of the next join   
		JavaPairRDD<String, Integer> sidNumFailures = sidOne.reduceByKey((v1, v2) -> v1 + v2);

		// Join sidDataCenter and sidNumFailures
		// Returned pairs
		// key = SID
		// value = Tuple2<DataCenterID, num. failures for the server>
		JavaPairRDD<String, Tuple2<String, Integer>> sidDataCenterIdFailuresServer = sidDataCenter.join(sidNumFailures);

		// Map each input element to a pair
		// key = DataCenterId
		// value = num. failures for the server
		JavaPairRDD<String, Integer> dataCenterIdSIDsFailures = sidDataCenterIdFailuresServer
				.mapToPair(pair -> new Tuple2<String, Integer>(pair._2()._1(), pair._2()._2()));

		// Count the number of failures for each dataCenterId
		JavaPairRDD<String, Integer> dataCenterIdnumFailures = dataCenterIdSIDsFailures.reduceByKey((v1, v2) -> v1 + v2);

		// Select only the data center with at least 365 failures
		JavaPairRDD<String, Integer> selectedDataCenterIdnumFailures = dataCenterIdnumFailures.filter(element -> {
			if (element._2() >= 365)
				return true;
			else
				return false;
		});

		selectedDataCenterIdnumFailures.saveAsTextFile(outputPathPartA);

		// *****************************************
		// Exercise 2 - Part B
		// *****************************************

		// Map each input line to a pair
		// key = (SID_month)
		// value = (1,Downtime)
		JavaPairRDD<String, NumFailuresDowntime> sidMonthStatistics = failures2017.mapToPair(line -> {
			// Date,Time,SID,FailureType,Downtime
			String[] fields = line.split(",");
			String sid = fields[2];
			int downtime = Integer.parseInt(fields[4]);
			String month = fields[0].split("/")[1];

			return new Tuple2<String, NumFailuresDowntime>(new String(sid + "_" + month),
					new NumFailuresDowntime(1, downtime));
		});

		// Count the number of failures in each month for each each server
		// Sum also the value of downtime to create a
		// preaggregation/precomputation of
		// total downtime for each server
		// For each SID+month we have the total number of failures and the
		// downtime minutes in that month for that server
		JavaPairRDD<String, NumFailuresDowntime> sidMonthAggregateStatistics = sidMonthStatistics
				.reduceByKey((v1, v2) -> new NumFailuresDowntime(v1.getNumFailures() + v2.getNumFailures(),
						v1.getDowntimeMinutes() + v2.getDowntimeMinutes()));

		// Select only the month with at least two failures
		JavaPairRDD<String, NumFailuresDowntime> sidMonthAggregateStatisticsSelected = sidMonthAggregateStatistics
				.filter(pair -> {
					if (pair._2().getNumFailures() >= 2)
						return true;
					else
						return false;
				});

		// Map each input element to a pair
		// key = SID
		// value = (1, partialDowntime)
		JavaPairRDD<String, NumFailuresDowntime> sidPairs = sidMonthAggregateStatisticsSelected.mapToPair(element -> {

			String sid = element._1().split("_")[0];
			int downtimeMonth = element._2().getDowntimeMinutes();

			return new Tuple2<String, NumFailuresDowntime>(sid, new NumFailuresDowntime(1, downtimeMonth));
		});

		// Apply reduceByKey to compute the total number of months with at least
		// two failures for each server
		// and also the total downtime minutes for each server
		JavaPairRDD<String, NumFailuresDowntime> sidNumMonthsWithFailuresAndTotalDowntime = sidPairs
				.reduceByKey((v1, v2) -> new NumFailuresDowntime(v1.getNumFailures() + v2.getNumFailures(),
						v1.getDowntimeMinutes() + v2.getDowntimeMinutes()));

		// Select only the servers with 12 months with at least two failures
		// and at least 1440 minutes of total downtime
		JavaPairRDD<String, NumFailuresDowntime> selectedServers = sidNumMonthsWithFailuresAndTotalDowntime
				.filter(element -> {
					int monthsWith2Failures = element._2().getNumFailures();
					int downtimeMinutes2017 = element._2().getDowntimeMinutes();

					if (monthsWith2Failures == 12 && downtimeMinutes2017 > 1440)
						return true;
					else
						return false;

				});

		// Save the SIDs of the selected servers
		selectedServers.keys().saveAsTextFile(outputPathPartB);

		sc.close();
	}
}
