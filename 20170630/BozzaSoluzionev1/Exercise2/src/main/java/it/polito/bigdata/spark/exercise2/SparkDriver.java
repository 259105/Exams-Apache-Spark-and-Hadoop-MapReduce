package it.polito.bigdata.spark.exercise2;

import org.apache.spark.api.java.*;

import scala.Tuple2;

import org.apache.spark.SparkConf;

public class SparkDriver {

	public static void main(String[] args) {

		String inputPathPrices;
		String outputPathPartA;
		String outputPathPartB;
		int nw;

		inputPathPrices = args[0];
		nw = Integer.parseInt(args[1]);
		outputPathPartA = args[2];
		outputPathPartB = args[3];

		// Create a configuration object and set the name of the application
		SparkConf conf = new SparkConf().setAppName("Spark Exam 2016_06_30 - Exercise #2");

		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);

		// *****************************************
		// Exercise 2 - Part A
		// *****************************************

		// Read the content of Prices.txt
		JavaRDD<String> prices = sc.textFile(inputPathPrices);

		// Select data of year 2016
		JavaRDD<String> prices2016 = prices.filter(line -> {

			// Input format: stockId,date,hour:minute,price

			String fields[] = line.split(",");

			String date = fields[1];
			String year = date.split("/")[0];

			// Select data of year 2016
			if (year.compareTo("2016") == 0) {
				return true;
			} else {
				return false;
			}
		});

		// Extract one pair (stockId_date, price) for each input line
		JavaPairRDD<String, Double> stockDate_Price = prices2016.mapToPair(line -> {
			// Input format: stockId,date,hour:minute,price

			// Extract one pair (stockId_date, price) for each input line
			String fields[] = line.split(",");
			String stockId = fields[0];
			String date = fields[1];
			Double price = new Double(fields[3]);

			return new Tuple2<String, Double>(stockId + "_" + date, new Double(price));
		}).cache();

		// Select the lowest price for each pair (stockId, date)
		JavaPairRDD<String, Double> stockDate_LowestPrice = stockDate_Price.reduceByKey((price1, price2) -> {
			if (price1.doubleValue() < price2.doubleValue()) {
				return new Double(price1);
			} else {
				return new Double(price2);
			}
		});

		JavaPairRDD<String, Double> sortedStockDate_LowestPrice = stockDate_LowestPrice.sortByKey();

		// Store the selected users in the output folder
		sortedStockDate_LowestPrice.saveAsTextFile(outputPathPartA);

		// *****************************************
		// Exercise 2 - Part B
		// *****************************************

		// stockDate_Price already contains one element for each pair
		// (stockId_date, price) for each input line

		// Select the highest price for each pair (stockId, date)
		JavaPairRDD<String, Double> stockDate_HighestPrice = stockDate_Price
				.reduceByKey((Double price1, Double price2) -> {
					if (price1.doubleValue() > price2.doubleValue()) {
						return new Double(price1);
					} else {
						return new Double(price2);
					}
				});

		// Map each input pair to a new pair
		// Input pair: (stockId_date, highest_price)
		// New pair: ((stockId_weekNumber, (firstdate_highestPrice,
		// lastdate_highestPrice))
		JavaPairRDD<String, FirstDatePriceLastDatePrice> stockId_WeekNumber_date_Price = stockDate_HighestPrice
				.mapToPair((Tuple2<String, Double> input) -> {
					// Input pair: (stockId_date, highest_price)
					// New pair: ((stockId_weekNumber, date_highestPrice)
					String keys[] = input._1().split("_");
					String stockId = keys[0];
					String date = keys[1];
					Integer weekNum = DateTool.weekNumber(date);

					Double price = input._2();
					FirstDatePriceLastDatePrice dp = new 
				FirstDatePriceLastDatePrice(date, price, date, price);

					return new Tuple2<String, FirstDatePriceLastDatePrice>(stockId + "_" + weekNum, dp);
				});

		// ReduceByKey to select for each key the first date (and the associated
		// highestPrice) and the last date (and the associated highestprice)
		JavaPairRDD<String, FirstDatePriceLastDatePrice> stockId_WeekNumber_firstdate_lastdate = stockId_WeekNumber_date_Price
				.reduceByKey((FirstDatePriceLastDatePrice dp1, FirstDatePriceLastDatePrice dp2) -> {
					String firstdate;
					Double firstprice;
					String lastdate;
					Double lastprice;

					// Select the first date comparing the first dates of the two input elements
					// and the associated price
					if (dp1.firstdate.compareTo(dp2.firstdate) < 0) {
						firstdate = dp1.firstdate;
						firstprice = dp1.firstprice;
					} else {
						firstdate = dp2.firstdate;
						firstprice = dp2.firstprice;
					}

					// Select the last date comparing the last dates of the two input elements
					// and the associated price
					if (dp1.lastdate.compareTo(dp2.lastdate) > 0) {
						lastdate = dp1.lastdate;
						lastprice = dp1.lastprice;
					} else {
						lastdate = dp2.lastdate;
						lastprice = dp2.lastprice;
					}

					return new FirstDatePriceLastDatePrice(firstdate, firstprice, lastdate, lastprice);

				});

		// Select pairs associate with positive weeks
		JavaPairRDD<String, FirstDatePriceLastDatePrice> stockId_WeekNumber_PositiveWeeks = stockId_WeekNumber_firstdate_lastdate
				.filter((Tuple2<String, FirstDatePriceLastDatePrice> inputPair) -> {
					// Check if the week is "positive"
					if (inputPair._2().lastprice.doubleValue() > inputPair._2().firstprice.doubleValue()) {
						return true;
					} else {
						return false;
					}
				});
		
		// Count the number of positive weeks for each stock

		// Map each input pair to a new pair
		// Input pair: (stockId_WeekNumber,	(firstdate_highestPrice,lastdate_highestPrice)
		// New pair: ((stockId, 1)
		JavaPairRDD<String, Integer> stockId_One = stockId_WeekNumber_PositiveWeeks
				.mapToPair((Tuple2<String, FirstDatePriceLastDatePrice> inputPair) -> {
					String stockId_WeekNumber = inputPair._1();

					String fields[] = stockId_WeekNumber.split("_");

					String stockId = fields[0];

					return new Tuple2<String, Integer>(new String(stockId), new Integer(1));
				});

		// Count ones per stock
		JavaPairRDD<String, Integer> stockId_Count = stockId_One
				.reduceByKey((value1, value2) -> new Integer(value1 + value2));

		// Filter stocks base on the number of "positive weeks"
		JavaPairRDD<String, Integer> frequentStockId_Count = stockId_Count
				.filter((Tuple2<String, Integer> inputPair) -> {
					if (inputPair._2().compareTo(nw) >= 0) {
						return true;
					} else {
						return false;
					}
				});

		frequentStockId_Count.keys().saveAsTextFile(outputPathPartB);

		sc.close();
	}
}
