package it.polito.bigdata.spark.example;

import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import scala.Tuple2;

public class SparkDriver {

	public static void main(String[] args) {
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);

		String actionsPath, appsPath;
		String output1Path, output2Path;

		actionsPath = args[0];
		appsPath = args[1];
		output1Path = args[2];
		output2Path = args[3];

		// Create a configuration object and set the name of the application
		SparkConf conf = new SparkConf().setAppName("Exam 20220202 - Exercise #2").setMaster("local");

		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);

		// Part 1
		// Actions line format: UserID, AppID, Timestamp, Action
		// timestamp format: YYYY/MM/DD-HH:MM:SS
		JavaRDD<String> actionsRDD = sc.textFile(actionsPath);

		// apps line format: AppId, Name, Price, Category, Company
		JavaRDD<String> appsRDD = sc.textFile(appsPath);

		// filter actions performed in 2021
		// since we are interested only in installation and removal actions, a condition
		// on the type of action is introduced
		JavaRDD<String> actions2021 = actionsRDD.filter(line -> {
			String[] fields = line.split(",");
			String action = fields[3];

			int year = Integer.parseInt(fields[2].split("/")[0]);
			return year == 2021 && (action.equals("Install") || action.equals("Remove"));
		});

		// Create a PairRDD in which
		// key = appId, Month
		// value = install/removal
		// where value is either +1 or -1, depending on the type of action the line is
		// associated with
		// Then use reduceByKey to count the difference between the number of
		// installation and removals of
		// a certain app in a specific month of 2021
		JavaPairRDD<Tuple2<String, String>, Integer> appMonthInstallRemoval = actions2021.mapToPair(line -> {
			String[] fields = line.split(",");
			String appId = fields[1];
			String month = fields[2].split("/")[1];
			String action = fields[3];

			Integer value;

			if (action.equals("Install"))
				value = 1;
			else
				value = 0;

			Tuple2<String, String> key = new Tuple2<>(appId, month);

			return new Tuple2<>(key, value);
		}).reduceByKey((v1, v2) -> v1 + v2);

		// filter only those apps and months in which #installations > #removals (i.e,
		// those pairs with value>0)
		JavaPairRDD<Tuple2<String, String>, Integer> appMonthWithMoreInstall = appMonthInstallRemoval
				.filter(pair -> pair._2() > 0);

		// transform the ((appId, Month), n. installation-n. removals)) into the
		// following pairRDD
		// key = appId
		// value = +1
		// and use reduceByKey to count the number of months in 2021 that the app of
		// interest (key) had #install > #removals
		// use then a filter to select only those apps for which count == 12.
		JavaPairRDD<String, Integer> appAll2021 = appMonthWithMoreInstall.mapToPair(it -> new Tuple2<>(it._1()._1(), 1))
				.reduceByKey((it1, it2) -> it1 + it2).filter(it -> it._2() == 12);

		// Starting from the content of Apps.txt, obtain a pairRDD in which
		// key = appId
		// value = app name
		JavaPairRDD<String, String> appIdName = appsRDD.mapToPair(line -> {
			String[] fields = line.split(",");
			return new Tuple2<>(fields[0], fields[1]);
		});

		// join the two RDDs and keep only the fields of interest to get the final
		// result
		JavaPairRDD<String, String> res1 = appAll2021.join(appIdName).mapToPair(t -> new Tuple2<>(t._1(), t._2()._2()));

		res1.saveAsTextFile(output1Path);

		// Part 2
		// Filter only "Install" actions
		JavaRDD<String> actionsInstall = actionsRDD.filter(line -> {
			String[] fields = line.split(",");
			String action = fields[3];

			return action.equals("Install");
		}).cache();

		// Select installations after 2021/12/31
		// Extract the associated pairs (AppId, UserId)
		// Remove duplicates
		JavaPairRDD<String, String> appsUsersAfter = actionsInstall.filter(line -> {
			String[] fields = line.split(",");
			String date = fields[2].split("-")[0];

			return date.compareTo("2021/12/31") > 0;
		}).mapToPair(line -> {
			String[] fields = line.split(",");
			String userId = fields[0];
			String appId = fields[1];

			return new Tuple2<String, String>(appId, userId);
		}).distinct();

		// Select installations before 2022/01/01
		// Extract the associated pairs (AppId, UserId)
		JavaPairRDD<String, String> appsUsersBefore = actionsInstall.filter(line -> {
			String[] fields = line.split(",");
			String date = fields[2].split("-")[0];

			return date.compareTo("2022/01/01") < 0;
		}).mapToPair(line -> {
			String[] fields = line.split(",");
			String userId = fields[0];
			String appId = fields[1];

			return new Tuple2<String, String>(appId, userId);
		});

		// Keep for each app only the new users after 2021/12/31
		JavaPairRDD<String, String> appsNewUsersAfter = appsUsersAfter.subtract(appsUsersBefore);

		// count for each app, the number of new distinct users after 31/12/2021
		// map input pair to new pairs
		// key: appId
		// value: +1
		// then use reduceByKey to count the number of new users for each app
		JavaPairRDD<String, Integer> newInstall2022PerApp = appsNewUsersAfter.mapValues(value -> 1)
				.reduceByKey((i1, i2) -> i1 + i2).cache();

		// extract the maximum number of new distinct users after 31/12/2021
		int maxInstallations = newInstall2022PerApp.values().reduce((i1, i2) -> Math.max(i1, i2));

		// select only the apps that achieved the maximum number of new distinct users
		JavaRDD<String> res2 = newInstall2022PerApp.filter(t -> t._2().equals(maxInstallations)).keys();

		res2.saveAsTextFile(output2Path);

		sc.close();
	}
}
