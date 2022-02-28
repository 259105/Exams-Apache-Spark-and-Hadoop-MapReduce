package it.polito.bigdata.spark.exercise2;

import org.apache.spark.api.java.*;

import scala.Tuple2;
import org.apache.spark.SparkConf;

public class SparkDriver {

	public static void main(String[] args) {

		String inputPathPOIs;
		String outputPathPartA;
		String outputPathPartB;

		inputPathPOIs = args[0];
		outputPathPartA = args[1];
		outputPathPartB = args[2];

		// Create a configuration object and set the name of the application
		SparkConf conf = new SparkConf().setAppName("Spark Exam 2019_02_15 - Exercise #2");

		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);

		// *****************************************
		// Exercise 2 - Part A
		// *****************************************

		/*
		 * Italian cities with taxis but without buses. Considering only the Italian
		 * cities, the application must select the Italian cities with at least one
		 * "taxi" POI (subcategory="Taxi") but without "Busstop" POIs
		 * (subcategory="Busstop"). The application stores in the first HDFS output
		 * folder the selected cities, one city per line.
		 */

		// Read the content of POIs.txt
		// POI_ID,latitude,longitude,city,country,category,subcategory
		JavaRDD<String> pois = sc.textFile(inputPathPOIs);

		// Select only the Italian cities
		JavaRDD<String> poisItaly = pois.filter(line -> {
			String[] fields = line.split(",");
			String country = fields[4];

			if (country.equals("Italy") == true)
				return true;
			else
				return false;
		}).cache();

		// Select only the POIs with subcategory="Taxi" or subcategory="Busstop"
		JavaRDD<String> poisItalySelectedSubcategories = poisItaly.filter(line -> {
			String[] fields = line.split(",");
			String subcategory = fields[6];

			if (subcategory.equals("taxi") == true || subcategory.equals("busstop") == true)
				return true;
			else
				return false;
		});

		// Generate for each input line a pair with
		// key = city
		// value = (taxi=0/1,busstop=0/1) (object of type CountTaxiBusstop)

		JavaPairRDD<String, CountTaxiBusstop> cityTaxiBusstop = poisItalySelectedSubcategories.mapToPair(line -> {
			String[] fields = line.split(",");

			String city = fields[3];
			String subcategory = fields[6];

			CountTaxiBusstop value;

			if (subcategory.equals("taxi")) {
				// Taxi POI associated with city
				value = new CountTaxiBusstop(1, 0);
			} else {
				// Busstop POI associated with city
				value = new CountTaxiBusstop(0, 1);
			}

			return new Tuple2<String, CountTaxiBusstop>(city, value);
		});

		// For each Italian city, compute
		// - number of taxi POIs
		// - number of Busstop POIs
		JavaPairRDD<String, CountTaxiBusstop> cityNumTaxiNumBusstop = cityTaxiBusstop.reduceByKey((v1, v2) -> {
			return new CountTaxiBusstop(v1.getNumTaxiPOIs() + v2.getNumTaxiPOIs(),
					v1.getNumBusstopPOIs() + v2.getNumBusstopPOIs());
		});

		// Select only the element with
		// - number of taxi POIs >=1
		// - number of Busstop =0
		JavaPairRDD<String, CountTaxiBusstop> selectedCityNumTaxiNumBusstop = cityNumTaxiNumBusstop
				.filter(cityCounterTaxiBusstop -> {
					if (cityCounterTaxiBusstop._2().getNumTaxiPOIs() >= 1
							&& cityCounterTaxiBusstop._2().getNumBusstopPOIs() == 0)
						return true;
					else
						return false;
				});

		// Save the selected cities = the keys of selectedCityNumTaxiNumBusstop
		selectedCityNumTaxiNumBusstop.keys().saveAsTextFile(outputPathPartA);

		// *****************************************
		// Exercise 2 - Part B
		// *****************************************

		/*
		 * Italian cities with "many" museums with respect to the other Italian cities.
		 * Considering only the Italian cities, the application must select the Italian
		 * cities with a number of "museum" POIs (subcategory="museum") greater than the
		 * average number of "museum" POIs per city in Italy. The application stores in
		 * the second HDFS output folder the selected cities, one city per line.
		 */

		// Count the number of museum POIs for each Italian city

		// Generate for each input line a pair with
		// key = city
		// value = 1 if subcategory="museum"
		// value = 0 if subcategory!="museum"

		JavaPairRDD<String, Integer> cityMuseumPOIs = poisItaly.mapToPair(line -> {
			String[] fields = line.split(",");

			String city = fields[3];
			String subcategory = fields[6];

			if (subcategory.equals("museum") == true) { // museum POI
				return new Tuple2<String, Integer>(city, 1);
			} else { // non-museum POI
				return new Tuple2<String, Integer>(city, 0);
			}
		});

		// Count the number of museum POIs for each Italian city
		JavaPairRDD<String, Integer> cityNumMuseumPOIs = cityMuseumPOIs.reduceByKey((v1, v2) -> v1 + v2);

		// Each input element represents
		// - an italian city
		// - the number of museum POIs for that city
		// Emit one new element of type CountCityMuseum with
		// - numCities = 1
		// - numMuseumPOIs (for that city) = value of the input pair
		JavaRDD<CountCityMuseum> numCitiesNumMuseumPOIs = cityNumMuseumPOIs
				.map(pair -> new CountCityMuseum(1, pair._2()));

		// Compute total number of Italian cities and total number of "museum" POIs the
		// Italy (i.e., in the Italian cities)
		CountCityMuseum totalNumCitiesNumMuseumPOIs = numCitiesNumMuseumPOIs
				.reduce((CountCityMuseumV1, CountCityMuseumV2) -> new CountCityMuseum(
						CountCityMuseumV1.getNumCities() + CountCityMuseumV2.getNumCities(),
						CountCityMuseumV1.getNumMuseumPOIs() + CountCityMuseumV2.getNumMuseumPOIs()));

		// Compute average number of "museum" POIs per city in Italy
		double average = (double) totalNumCitiesNumMuseumPOIs.getNumMuseumPOIs()
				/ (double) totalNumCitiesNumMuseumPOIs.getNumCities();

		// Select only the Italian cities with a number of "museum" POIs
		// (subcategory="museum") greater than the average number of "museum" POIs per
		// city in Italy.
		JavaPairRDD<String, Integer> selectedCityNumMuseumPOIs = cityNumMuseumPOIs.filter(inputPair -> {
			if (inputPair._2() > average)
				return true;
			else
				return false;
		});

		// Store the selected cities
		selectedCityNumMuseumPOIs.keys().saveAsTextFile(outputPathPartB);

		sc.close();
	}
}
