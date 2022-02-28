package it.polito.bigdata.spark;

import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import scala.Tuple2;

public class SparkDriver {

	public static void main(String[] args) {

		// The following two lines are used to switch off some verbose log messages
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);

		String purchaseInputPath, catalogInputPath;
		String outputPath1, outputPath2;

		purchaseInputPath = args[0];
		catalogInputPath = args[1];
		outputPath1 = args[2];
		outputPath2 = args[3];

		// Create a configuration object and set the name of the application
		SparkConf conf = new SparkConf().setAppName("Spark - Exam20220221");

		// Use the following command to create the SparkConf object if you want to run
		// your application inside Eclipse.
		// Remember to remove .setMaster("local") before running your application on the
		// cluster
		// SparkConf conf=new SparkConf().setAppName("Spark Lab #5").setMaster("local");

		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);

		// Input format: Timestamp,Username,itemID,SalePrice
		JavaRDD<String> purchaseRDD = sc.textFile(purchaseInputPath);
		// Input format: itemID,Name,Category,Timestamp
		JavaRDD<String> catalogRDD = sc.textFile(catalogInputPath);

		// Part 1
		// Filter only the years 2020 and 2021 and cache this RDD, so that it can be
		// used for the second part
		JavaRDD<String> purchase20_21RDD = purchaseRDD
				.filter(line -> line.startsWith("2020/") || line.startsWith("2021/")).cache();

		// Map into a pairRDD with:
		// key = (itemId, yearOfPurchase)
		// value = +1
		JavaPairRDD<Tuple2<String, String>, Integer> itemsYearsNumPurchsRDD = purchase20_21RDD.mapToPair(line -> {
			String fields[] = line.split(",");
			String year = fields[0].split("/")[0];
			String itemId = fields[2];

			return new Tuple2<Tuple2<String, String>, Integer>(new Tuple2<String, String>(itemId, year), 1);
		});

		// Sum the number of purchases for each item in each year
		// key = (itemId, yearOfPurchase)
		// value = number of purchases
		// Then, select the years for which the item was sold at least 10K times
		JavaPairRDD<Tuple2<String, String>, Integer> validItemsSoldPerYear = itemsYearsNumPurchsRDD
				.reduceByKey((i1, i2) -> i1 + i2).filter(it -> it._2() >= 10000);

		// Transform validItemsSoldPerYear into the following pairRDD:
		// key = itemId
		// value = +1
		// and use reduceByKey to count how many years (considering only 2020 and 2021)
		// the items was sold at least 10K times
		// and finally verify that such count is == 2
		JavaRDD<String> resPart1 = validItemsSoldPerYear.mapToPair(pair -> {
			String itemId = pair._1()._1();
			return (new Tuple2<>(itemId, 1));
		}).reduceByKey((i1, i2) -> i1 + i2).filter(it -> it._2() == 2).keys();

		resPart1.saveAsTextFile(outputPath1);

		// Part 2
		// Start from previously cached RDD 
		// and consider only purchases made in 2020
		// Map the considered pairRDD into
		// key = (itemId, month)
		// value = userId
		// and perform a distinct operation to consider purchases made in each month of
		// 2020 by distinct users.
		JavaPairRDD<Tuple2<String, String>, String> distinctPurchasesPerMoth2021RDD = purchase20_21RDD
				.filter(line -> line.startsWith("2020")).mapToPair(line -> {
					String fields[] = line.split(",");
					String month = fields[0].split("/")[1];
					String userId = fields[1];
					String itemId = fields[2];

					return new Tuple2<>(new Tuple2<>(itemId, month), userId);
				}).distinct();

		// Count the number of distinct customers for each item+month
		// by first mapping the RDD into the following pair:
		// key = (itemId, month)
		// value = +1
		// and then use a reduceByKey to sum
		// and filter only those months for which the distinct customers were >= 10
		JavaPairRDD<Tuple2<String, String>, Integer> purchasesMoreThan9 = distinctPurchasesPerMoth2021RDD
				.mapToPair(it -> new Tuple2<>(it._1(), 1))
				.reduceByKey((i1, i2) -> i1 + i2)
				.filter(it -> it._2() >= 10);

		// Count the number of months in 2020 in which each itemId was bought by >= 10
		// distinct users
		// key = itemId
		// value = +1
		// Use reduceByKey to count for each item the number of months of 2020 for which
		// distinct customers were >= 10
		JavaPairRDD<String, Integer> itemsNumMonthsPurchasesMoreThan9 = purchasesMoreThan9
				.mapToPair(it -> new Tuple2<>(it._1()._1(), 1))
				.reduceByKey((i1, i2) -> i1 + i2);

		// Select the items with more than 10 months with number of distinct customers
		// >=10
		// These items must be discarded because they have at least 11 months each one with 
		// num. distinct customers  >= 10. In other words, they have at most one month with less than 
		// 10 distinct customers.
		JavaPairRDD<String, Integer> itemsWithFewPurchases = itemsNumMonthsPurchasesMoreThan9
				.filter(it -> it._2() > 10);

		
		
		// Filter only items which were inserted in catalog before 01/01/2021 and
		// Map the catalogRDD into a pairRDD with
		// key = itemId
		// value = Category
		JavaPairRDD<String, String> itemCategoryRDD = catalogRDD.filter(line -> {
			String[] fields = line.split(",");
			String firstTimeInCatalog = fields[3];

			return firstTimeInCatalog.compareTo("2021/01/01") <= 0;
		}).mapToPair(line -> {
			String[] fields = line.split(",");
			String itemId = fields[0];
			String category = fields[2];

			return new Tuple2<>(itemId, category);
		});

		// Select the items occurring in itemCategoryRDD but not in
		// itemsWithFewPurchases
		// We need to use this approach in order to consider for each item also the months without sales
		// (i.e., without customers). A month without sales has less than 10 distinct customers.
		JavaPairRDD<String, String> resPart2 = itemCategoryRDD.subtractByKey(itemsWithFewPurchases);

		resPart2.saveAsTextFile(outputPath2);

		sc.close();
	}
}
