package it.polito.bigdata.spark.exercise2;

import org.apache.spark.api.java.*;

import scala.Tuple2;

import java.util.ArrayList;

import org.apache.spark.SparkConf;

public class SparkDriver {

	@SuppressWarnings("resource")
	public static void main(String[] args) {

		String inputPathPrices;
		String outputPathPartA;
		String outputPathPartB;

		inputPathPrices = args[0];
		outputPathPartA = args[1];
		outputPathPartB = args[2];

		// Create a configuration object and set the name of the application
		SparkConf conf = new SparkConf().setAppName("Spark Exam 2018_09_03 - Exercise #2 - v2");

		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);

		// *****************************************
		// Exercise 2 - Part A
		// *****************************************

		// Read the content of stocks_prices.txt
		JavaRDD<String> stockPrices = sc.textFile(inputPathPrices);

		// Select only year 2014 data
		JavaRDD<String> stockPrices2014 = stockPrices.filter(line -> {
			String[] fields = line.split(",");
			String date = fields[0];

			if (date.startsWith("2014") == true)
				return true;
			else
				return false;
		});

		// Generate for each input line (of year 2014) a pair with
		// key = stockId,Date (object of type StockDate)
		// value = price,price (object of type MinMaxPrices)

		JavaPairRDD<StockDate, MinMaxPrices> stockDate_Price = stockPrices2014.mapToPair(line -> {
			String[] fields = line.split(",");

			String date = fields[0];
			String stockId = fields[2];
			double price = Double.parseDouble(fields[3]);

			StockDate key = new StockDate(stockId, date);

			MinMaxPrices value = new MinMaxPrices(price, price);

			return new Tuple2<StockDate, MinMaxPrices>(key, value);
		});

		// Compute max price and min price for each stockId+date
		JavaPairRDD<StockDate, MinMaxPrices> stockDate_MinandMaxPrice = stockDate_Price.reduceByKey((v1, v2) -> {

			double minPrice;
			double maxPrice;

			if (v1.getMinPrice() < v2.getMinPrice())
				minPrice = v1.getMinPrice();
			else
				minPrice = v2.getMinPrice();

			if (v1.getMaxPrice() > v2.getMaxPrice())
				maxPrice = v1.getMaxPrice();
			else
				maxPrice = v2.getMaxPrice();

			MinMaxPrices value = new MinMaxPrices(minPrice, maxPrice);

			return value;
		});

		// Compute daily variation for each stockId+date
		JavaPairRDD<StockDate, Double> stockDate_DailyVariation = stockDate_MinandMaxPrice
				.mapValues(minmax -> new Double(minmax.getMaxPrice() - minmax.getMinPrice())).cache();

		// Select only the element with a daily variation < 5
		JavaPairRDD<StockDate, Double> stockDate_DailyVariationLess5 = stockDate_DailyVariation
				.filter(pairVariation -> pairVariation._2() < 5);

		// Count the number of dates with daily variation < 5 for each stockId
		// Map each input element to a pair (stockId, +1)
		JavaPairRDD<String, Integer> stockIdOne = stockDate_DailyVariationLess5
				.mapToPair(stockDateVariation -> new Tuple2<String, Integer>(stockDateVariation._1().getStockId(),
						new Integer(1)));

		// ReduceByKey to count the number of dates for each stockId
		JavaPairRDD<String, Integer> stockIdNumDates = stockIdOne.reduceByKey((v1, v2) -> v1 + v2);

		// Save result
		stockIdNumDates.saveAsTextFile(outputPathPartA);

		// *****************************************
		// Exercise 2 - Part B
		// *****************************************

		// Consider stockDate_DailyVariation
		// It contains for each stockid+Date the daily variation.
		// Each element of stockDate_DailyVariation is the first date of a
		// sequence of two dates and also the second date of another sequence of
		// two dates. 
		// Emit for each input element (stockid+Date,DailyVariation) two pairs:
		// 1 - key=stockId+Date - value=DailyVariation at date Date -> This is the first
		// element of the sequence of two dates starting at date "Date" associated with stockId. 
		// 2 - key=stockId+(Date-1) - value=DailyVariation at date Date -> This is the
		// second element of the sequence of two dates starting at date "Date-1" associated with stockId 
		JavaPairRDD<StockDate, Double> stockIdSequenceFirstDate_DailyVariations = stockDate_DailyVariation
				.flatMapToPair(stockIdDateDailyVariation -> {

					String stockId = stockIdDateDailyVariation._1().getStockId();
					String date = stockIdDateDailyVariation._1().getDate();
					String previousDate = DateTool.previousDate(date);

					Double dailyVariation = new Double(stockIdDateDailyVariation._2());

					ArrayList<Tuple2<StockDate, Double>> returnedPairs = new ArrayList<Tuple2<StockDate, Double>>();

					StockDate stockIdDate = new StockDate(stockId, date);
					StockDate stockIdPreviousDate = new StockDate(stockId, previousDate);

					// Add the pair key=stockId+Date - value=Date+DailyVariation
					returnedPairs.add(new Tuple2<StockDate, Double>(stockIdDate, dailyVariation));

					// Add the pair key=stockId+(Date-1) -
					// value=Date+DailyVariation
					returnedPairs
							.add(new Tuple2<StockDate, Double>(stockIdPreviousDate, dailyVariation));

					return returnedPairs.iterator();
				});

		// Apply groupBykey to generate all the "sequences" of two dates for
		// each stock with the associated dailyVariation values
		// Each returned pair has the following content:
		// - key = StockId+first date of the sequence of two dates
		// - value = daily variations for stockId for the two dates associated with this sequence of two dates     
		
		JavaPairRDD<StockDate, Iterable<Double>> stockIdTwoConsecutiveDatesDailyVariations = stockIdSequenceFirstDate_DailyVariations
				.groupByKey();

		// Select only the "unstable trends"
		JavaPairRDD<StockDate, Iterable<Double>> stockIDUnstableTrends = stockIdTwoConsecutiveDatesDailyVariations
				.filter(inputSequence -> {

					// Select the two daily variations and compute the absolute
					// difference between those two values
					Double dailyVariation1 = null;
					Double dailyVariation2 = null;

					// Iterate over the values and store the two daily values
					for (Double value : inputSequence._2()) {
						if (dailyVariation1 == null) { // This is the first
														// value of the iterable
							dailyVariation1 = new Double(value);
						} else {// There are at most two values. This is the
								// second one
							dailyVariation2 = new Double(value);
						}
					}

					// Check if there are at two daily variation values and if
					// their absolute difference is greater than 1 
					if (dailyVariation2 != null
							&& Math.abs(dailyVariation1 - dailyVariation2) > 1)
						return true;
					else
						return false;
				});

		// Select the keys and store them
		stockIDUnstableTrends.keys().saveAsTextFile(outputPathPartB);

		sc.close();
	}
}

