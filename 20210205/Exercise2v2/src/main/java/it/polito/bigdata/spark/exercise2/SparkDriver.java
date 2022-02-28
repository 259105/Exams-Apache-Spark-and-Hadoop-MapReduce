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

		String inProdPlants;
		String inRobots;
		String inFailures;
		String outputPathPart1;
		String outputPathPart2;
		inProdPlants = "exam_ex2_data/ProductionPlants.txt";
		inRobots = "exam_ex2_data/Robots.txt";
		inFailures = "exam_ex2_data/Failures.txt";

		outputPathPart1 = "outPart1/";
		outputPathPart2 = "outPart2/";
		// Create a configuration object and set the name of the application
		SparkConf conf = new SparkConf().setAppName("Spark Exam - Exercise #2").setMaster("local");
		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);

		/**********/
		/* Part 1 */
		/**********/

		// Read the content of Failures
		JavaRDD<String> failuresRDD = sc.textFile(inFailures);

		// Select only the failures related to year 2020
		JavaRDD<String> failures2020RDD = failuresRDD.filter(line -> {
			// R1,FCode122,2020/05/01,06:40:51
			String[] fields = line.split(",");
			String date = fields[2];

			return date.startsWith("2020");
		});

		// Count the number of failures for each robot
		JavaPairRDD<String, Integer> ridOneRDD = failures2020RDD.mapToPair(line -> {
			// R1,FCode122,2020/05/01,06:40:51
			String[] fields = line.split(",");
			String rid = fields[0];

			return new Tuple2<String, Integer>(new String(rid), +1);
		});

		JavaPairRDD<String, Integer> ridNumFailuresRDD = ridOneRDD.reduceByKey((v1, v2) -> v1 + v2).cache();

		// Read the content of Robots
		JavaRDD<String> robotsRDD = sc.textFile(inRobots);

		// Map each input line to a pair (RID, ProdPlantID)
		JavaPairRDD<String, String> ridPlantIDRDD = robotsRDD.mapToPair(line -> {
			// R1,PID1,130.192.20.21
			String[] fields = line.split(",");
			String rid = fields[0];
			String plantID = fields[1];

			return new Tuple2<String, String>(new String(rid), new String(plantID));
		});

		// Join ridPlantIDRDD with ridNumFailuresRDD
		JavaPairRDD<String, Tuple2<String, Integer>> joinPlantsRidsWithFailuresRDD = ridPlantIDRDD
				.join(ridNumFailuresRDD).cache();

		// Select only the robots with at least 50 failures
		JavaPairRDD<String, Tuple2<String, Integer>> selectedRidNumFailuresRDD = joinPlantsRidsWithFailuresRDD
				.filter(pair -> pair._2()._2() >= 50);

		// Extract the list of plantIDs
		JavaRDD<String> plantIDsRDD = selectedRidNumFailuresRDD.map(pair -> pair._2()._1());

		// Remove duplicates and store the result on the output folder
		plantIDsRDD.distinct().saveAsTextFile(outputPathPart1);

		/**********/
		/* Part 2 */
		/**********/

		// Map each pair to a new pair (plantId, +1)
		JavaPairRDD<String, Integer> plantIdOneRDD = joinPlantsRidsWithFailuresRDD
				.mapToPair(pair -> new Tuple2<String, Integer>(pair._2()._1(), +1));

		// Count for each production plant the number of robots with at least one
		// failure
		JavaPairRDD<String, Integer> plantIdNumRobotsWithFailuresRDD = plantIdOneRDD.reduceByKey((v1, v2) -> v1 + v2);

		// Read the content of ProductionPlants
		JavaRDD<String> prodPlantsRDD = sc.textFile(inProdPlants);

		// Map each input line of ProductionPlants to a pair (PlantID, 0)
		JavaPairRDD<String, Integer> prodPlantZeroRDD = prodPlantsRDD.mapToPair(line -> {
			// PID1,Turin,Italy
			String[] fields = line.split(",");
			String plantID = fields[0];

			return new Tuple2<String, Integer>(new String(plantID), 0);
		}).cache();

		// Select the production plants with all robots without failures in year 2020
		JavaPairRDD<String, Integer> prodPlantsNoFailuresRDD = prodPlantZeroRDD
				.subtractByKey(plantIdNumRobotsWithFailuresRDD);

		// Union prodPlantsNoFailuresRDD with plantIdNumRobotsWithFailuresRDD to compute
		// the final result
		JavaPairRDD<String, Integer> finalResPartIIRDD = prodPlantsNoFailuresRDD.union(plantIdNumRobotsWithFailuresRDD);

		// Store the result
		finalResPartIIRDD.saveAsTextFile(outputPathPart2);

		sc.close();
	}
}
