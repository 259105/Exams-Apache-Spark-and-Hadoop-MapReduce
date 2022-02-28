package it.polito.bigdata.spark.exercise2;

import org.apache.spark.api.java.*;

import scala.Tuple2;

import org.apache.spark.SparkConf;

public class SparkDriver {

	public static void main(String[] args) {

		String inputServers;
		String inputAnomalies;
		String outputPathPartA;
		String outputPathPartB;

		inputServers = args[0];
		inputAnomalies = args[1];
		outputPathPartA = args[2];
		outputPathPartB = args[3];

		// Create a configuration object and set the name of the application
		SparkConf conf = new SparkConf().setAppName("Spark Exam 2019_07_02 - Exercise #2 v2");

		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);

		// *****************************************
		// Exercise 2 - Part A
		// *****************************************

		// Read the content of Servers_TemperatureAnomalies.txt
		JavaRDD<String> anomaliesRDD = sc.textFile(inputAnomalies);

		// Select anomalies associated with years 2010-2018
		// Example: SID13,2018/03/01_15:40,110
		JavaRDD<String> anomalies2010_2018RDD = anomaliesRDD.filter(line -> {
			String[] fields = line.split(",");
			int year = Integer.parseInt(fields[1].split("/")[0]);

			if (year >= 2010 && year <= 2018)
				return true;
			else
				return false;
		}).cache();

		// Select anomalies with temperature > 100°C
		// I used to filter in order to cache the output of the first filter that is
		// used by the second part of the code (Part B).
		// Example: SID13,2018/03/01_15:40,110
		JavaRDD<String> anomalies2010_2018RDDMoreThan100 = anomalies2010_2018RDD.filter(line -> {
			String[] fields = line.split(",");
			double temperature = Double.parseDouble(fields[2]);
			if (temperature > 100)
				return true;
			else
				return false;
		});

		// Map each input line to a pair
		// key = SID_Year
		// value = 1
		JavaPairRDD<String, Integer> SIDYearAnomaly = anomalies2010_2018RDDMoreThan100.mapToPair(line -> {

			// Example: SID13,2018/03/01_15:40,110
			String[] fields = line.split(",");
			String SID = fields[0];
			String year = fields[1].split("/")[0];

			return new Tuple2<String, Integer>(new String(SID + "_" + year), 1);

		});

		// Count the number of temperatures > 100°C for each SID_Year (i.e., for each
		// sid in each year between 2010-2018)
		JavaPairRDD<String, Integer> SIDYearNumAnomalies = SIDYearAnomaly.reduceByKey((v1, v2) -> new Integer(v1 + v2));

		// Select the pairs with number of anomalies > 50
		JavaPairRDD<String, Integer> SIDYearNumAnomaliesMoreThan50 = SIDYearNumAnomalies.filter(pair -> {
			int numFailures = pair._2();

			if (numFailures > 50)
				return true;
			else
				return false;
		});

		// The SIDs of the selected pairs are the SIDs of the servers with at least one
		// year in which those servers had more than 50 anomalies with temperature >
		// 100°C in years 2010-2018
		// Extract the SID part of the key
		JavaRDD<String> selectedSIDs = SIDYearNumAnomaliesMoreThan50
				.map((Tuple2<String, Integer> SIDYearNumAnomalyTemps) -> {
					String SID = SIDYearNumAnomalyTemps._1().split("_")[0];

					return SID;
				});

		// The same server might had more than 50 anomalies in two different years
		// of years 2010-2018
		// Hence, duplicated must be removed

		selectedSIDs.distinct().saveAsTextFile(outputPathPartA);

		// *****************************************
		// Exercise 2 - Part B
		// *****************************************

		// Compute the number of anomalies for each server in the period 2010-2018

		// Map each input line to a pair
		// key = SID
		// value = 1
		JavaPairRDD<String, Integer> SIDAnomaly = anomalies2010_2018RDD.mapToPair(line -> {

			// Example: SID13,2018/03/01_15:40,110
			String[] fields = line.split(",");
			String SID = fields[0];

			return new Tuple2<String, Integer>(SID, 1);
		});

		// Compute the number of anomalies for each server in the period 2010-2018
		JavaPairRDD<String, Integer> SIDNumAnomaliesRDD = SIDAnomaly.reduceByKey((v1, v2) -> (v1 + v2));

		// Select the SIDs of the servers with more than 10 anomalies
		JavaPairRDD<String, Integer> SIDNumAnomaliesMoreThan10 = SIDNumAnomaliesRDD
				.filter((Tuple2<String, Integer> SidNumAnomalies) -> {
					int numAnomalies = SidNumAnomalies._2();

					if (numAnomalies > 10) {
						return true;
					} else {
						return false;
					}
				});

		// Retrieve the data centers of the selected SIDs

		// Read the content of servers.txt
		JavaRDD<String> serversRDD = sc.textFile(inputServers).cache();

		// Map each input line to a pair
		// Key: SID
		// Value: Data center ID
		JavaPairRDD<String, String> SIDDataCenters = serversRDD.mapToPair(line -> {
			// Example: SID13,PentiumV,DC10,Barcelona,Spain
			String[] fields = line.split(",");
			String SID = fields[0];
			String DataCenterID = fields[2];

			return new Tuple2<String, String>(SID, DataCenterID);

		});

		// Join SIDNumAnomaliesMoreThan10 with SIDDataCenters
		// The result is:
		// key: SID
		// value: numAnomalies, Data Center ID
		JavaPairRDD<String, Tuple2<Integer, String>> SIDNumAnomaliesDataCenter = SIDNumAnomaliesMoreThan10
				.join(SIDDataCenters);

		// Select the data centers for which there is at least one server with more than
		// 10 anomalies.
		JavaRDD<String> dataCentersToBeRemoved = SIDNumAnomaliesDataCenter.map(
				(Tuple2<String, Tuple2<Integer, String>> pairSIDNumAnmaliesDataCenter) -> pairSIDNumAnmaliesDataCenter._2()._2());

		// Select all the data centers managed by PoliData.
		// Remove duplicates.
		JavaRDD<String> allServers = serversRDD.map(line -> {
			// Example: SID13,PentiumV,DC10,Barcelona,Spain
			String[] fields = line.split(",");
			String dataCenter = fields[2];

			return dataCenter;
		}).distinct();

		// Select the data centers with all the servers with at most 10 anomalies (i.e., those
		// that are never associated with a server with more than 10 anomalies)
		JavaRDD<String> selectedDataCenters = allServers.subtract(dataCentersToBeRemoved).cache();

		// Save the selected data centers
		selectedDataCenters.saveAsTextFile(outputPathPartB);

		// Print on the standard output the number of select data centers
		System.out.println(selectedDataCenters.count());

		sc.close();
	}
}
