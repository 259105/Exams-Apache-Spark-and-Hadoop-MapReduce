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

		String inputPathServers;
		String inputPathPatchesServers;
		String outputPathPart1;
		String outputPathPart2;

		inputPathServers = "exam_ex2_data/Servers.txt";
		inputPathPatchesServers = "exam_ex2_data/PatchedServers.txt";
		outputPathPart1 = "outPart1/";
		outputPathPart2 = "outPart2/";

		// Create a configuration object and set the name of the application
		SparkConf conf = new SparkConf().setAppName("Spark Exam - Exercise #2").setMaster("local");

		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);

		/* Write your code here */

		// *****************************************
		// Exercise 2 - Part 1
		// *****************************************

		// Read the content of PatchesServers.txt
		JavaRDD<String> patchesRDD = sc.textFile(inputPathPatchesServers).cache();

		// Select only the patches applied in 2018 or 2019
		JavaRDD<String> patches1819RDD = patchesRDD.filter(line -> {
			String[] fields = line.split(",");
			String date = fields[2];

			if (date.startsWith("2018") || date.startsWith("2019"))
				return true;
			else
				return false;
		});

		// Emit a pair from each input element
		// - key = sid
		// - value = (2018-0/1, 2017-0/1)
		JavaPairRDD<String, Count1819> sid1819RDD = patches1819RDD.mapToPair(line -> {
			String[] fields = line.split(",");
			String sid = fields[0];
			String date = fields[2];

			Count1819 counts;

			if (date.startsWith("2018"))
				counts = new Count1819(1, 0);
			else
				counts = new Count1819(0, 1);

			return new Tuple2<String, Count1819>(sid, counts);
		});

		// Count the number of patches in the two years for each sid
		JavaPairRDD<String, Count1819> sidTot1819RDD = sid1819RDD.reduceByKey((c1, c2) -> {
			return new Count1819(c1.count18 + c2.count18, c1.count19 + c2.count19);
		});

		// Select only the SID with num. patche 2019 < 0.5*num. patches 2018
		JavaPairRDD<String, Count1819> selectedSIDsRDD = sidTot1819RDD.filter(pair -> {
			if (pair._2().count19 < 0.5 * pair._2().count18)
				return true;
			else
				return false;
		});

		// Read the content of Servers.txt and create a PairRDD containing pairs
		// (sid,model)
		JavaRDD<String> serversRDD = sc.textFile(inputPathServers);

		// Map input lines to pairs (sid, model)
		JavaPairRDD<String, String> sidModelRDD = serversRDD.mapToPair(line -> {
			String[] fields = line.split(",");
			String sid = fields[0];
			String model = fields[1];

			return new Tuple2<String, String>(sid, model);
		}).cache();

		// Join the content of sidModelRDD and selectedSIDsRDD to select the
		// model of the selected servers
		JavaPairRDD<String, Tuple2<String, Count1819>> selectedSIDModelCountsRDD = 
				sidModelRDD.join(selectedSIDsRDD);

		// Extract only sid and model.
		// Map the value part to a string containing only the model of the server
		JavaPairRDD<String, String> selectedSidAndModelRDD = 
				selectedSIDModelCountsRDD.mapValues(value -> value._1());

		// Store the result in the first output folder
		selectedSidAndModelRDD.saveAsTextFile(outputPathPart1);

		// *****************************************
		// Exercise 2 - Part 2
		// *****************************************

		// Count the number of applied patches for each server in each date

		// Map each input line to a pair:
		// - key: sid+date
		// - value: +1
		JavaPairRDD<String, Integer> siddateRDD = patchesRDD.mapToPair(line -> {
			String[] fields = line.split(",");

			String sid = fields[0];
			String date = fields[2];

			return new Tuple2<String, Integer>(new String(sid + "_" + date), 1);
		});

		// Sum the ones to count the number of patches in each date for each server
		JavaPairRDD<String, Integer> siddateNumPatchesRDD = siddateRDD.reduceByKey((v1, v2) -> v1 + v2);

		// Select the combinations server,date with at least 2 patches
		// These are the servers to exclude from the final result
		JavaPairRDD<String, Integer> siddateToDiscardRDD = siddateNumPatchesRDD.filter(pair -> {
			if (pair._2() >= 2)
				return true;
			else
				return false;
		});

		// Map each of the selected pairs to a new pair with
		// - key = sid
		// - value = an empty string
		JavaPairRDD<String, String> sidToDiscardRDD = siddateToDiscardRDD.mapToPair(pair -> {
			String sid = pair._1().split("_")[0];

			return new Tuple2<String, String>(sid, "");
		});

		// Select only the servers that are not occurring in sidToDiscardRDD
		// If the key of a pair that occurs in sidModelRDD does not occur as key in the
		// pairs of sidToDiscardRDD, that server (pair) must be selected
		JavaPairRDD<String, String> selectedSidModelRDD = sidModelRDD.subtractByKey(sidToDiscardRDD);

		// Store the result in the second output folder
		selectedSidModelRDD.saveAsTextFile(outputPathPart2);
		
		// Select only the models of the selected servers and count the number of distinct models.
		JavaRDD<String> modelsRDD = selectedSidModelRDD.values();
		
		// Print the the number of distinct models on the standard output of the driver.
		System.out.println("Number of distinct (selected) models: "+modelsRDD.distinct().count());

		sc.close();
	}
}
