package it.polito.bigdata.spark.exercise2;

import org.apache.spark.api.java.*;

import scala.Tuple2;

import org.apache.spark.SparkConf;

public class SparkDriver {

	@SuppressWarnings("resource")
	public static void main(String[] args) {

		double precThr;
		double windThr;
		String inputPathPrices;
		String outputPathPartA;
		String outputPathPartB;

		precThr = Double.parseDouble(args[0]);
		windThr = Double.parseDouble(args[1]);
		inputPathPrices = args[2];
		outputPathPartA = args[3];
		outputPathPartB = args[4];

		// Create a configuration object and set the name of the application
		SparkConf conf = new SparkConf().setAppName("Spark Exam 2018_06_26 - Exercise #2 - v2");

		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);

		// *****************************************
		// Exercise 2 - Part A
		// *****************************************

		// Read the content of ClimateData.txt
		JavaRDD<String> measurements = sc.textFile(inputPathPrices);

		// Select data related to April and May 2018
		// Example: Sens2,2018/03/01,15:40,0.10,8.5
		JavaRDD<String> measurementsAprilMay2018 = measurements.filter(line -> {
			String[] fields = line.split(",");
			String date = fields[1];
			if (date.startsWith("2018/04") == true || date.startsWith("2018/05") == true)
				return true;
			else
				return false;
		}).cache();

		// Map each input line to a pair
		// key = SID-Hour
		// value = (1, precipitation, windSpeed)
		JavaPairRDD<String, Counter> sidHourCounter = measurementsAprilMay2018.mapToPair(line -> {
			String[] fields = line.split(",");

			String time = fields[2];
			String hour = time.split(":")[0];

			String sid = fields[0];
			double prec = Double.parseDouble(fields[3]);
			double windSpeed = Double.parseDouble(fields[4]);

			Counter counter = new Counter(1, prec, windSpeed);

			return new Tuple2<String, Counter>(new String(sid + "-" + hour), counter);

		});

		// Sum the three part of the Counter objects
		// Sum count
		// Sum prec
		// Sum windSpeed
		JavaPairRDD<String, Counter> sidHourAvg = sidHourCounter
				.reduceByKey((Counter c1, Counter c2) -> new Counter(c1.getCount() + c2.getCount(),
						c1.getSumPrec() + c2.getSumPrec(), c1.getSumWindSpeed() + c2.getSumWindSpeed()));

		// Filter pairs by applying the two thresholds
		JavaPairRDD<String, Counter> selectedVsidHour = sidHourAvg.filter(pair -> {
			double avgPrec = pair._2().getSumPrec() / (double) pair._2().getCount();
			double avgWindSpeed = pair._2().getSumWindSpeed() / (double) pair._2().getCount();

			if (avgPrec < precThr && avgWindSpeed < windThr)
				return true;
			else
				return false;
		});

		selectedVsidHour.keys().saveAsTextFile(outputPathPartA);

		// *****************************************
		// Exercise 2 - Part B
		// *****************************************

		// Map each input line to a pair
		// key = SID+date+hour
		// value = WindSpeed
		// Example: Sens2,2018/03/01,15:40,0.10,8.5
		JavaPairRDD<String, Double> sidDateHourWindSpeed = measurementsAprilMay2018.mapToPair(line -> {
			String[] fields = line.split(",");
			String sid = fields[0];
			String date = fields[1];
			String time = fields[2];
			String hour = time.split(":")[0];
			Double windSpeed = new Double(fields[4]);

			return new Tuple2<String, Double>(new String(sid + "_" + date + "_" + hour), windSpeed);
		});

		// Compute min wind speed for each key (SID+date+hour)
		JavaPairRDD<String, Double> sidDateHourMinWindSpeed = sidDateHourWindSpeed
				.reduceByKey((windSpeed1, windSpeed2) -> {
					if (windSpeed1 < windSpeed2)
						return windSpeed1;
					else
						return windSpeed2;
				});

		// Select only the elements with min wind speed<1 or >40
		JavaPairRDD<String, Double> sidDateHourMinWindSpeedHighLow = sidDateHourMinWindSpeed.filter(pair -> {
			Double minWindSpeed = pair._2();
			if (minWindSpeed < 1.0 || minWindSpeed > 40.0)
				return true;
			else
				return false;
		});

		// Return one pair of each input element
		// key = SID+date
		// value = (>40, <1)
		JavaPairRDD<String, CounterHighLowWindSpeed> sidDateHighLow = sidDateHourMinWindSpeedHighLow.mapToPair(pair -> {
			String[] fields = pair._1().split("_");
			String sid = fields[0];
			String date = fields[1];

			Double minWindSpeed = pair._2();
			if (minWindSpeed > 40.0)
				return new Tuple2<String, CounterHighLowWindSpeed>(new String(sid + "-" + date),
						new CounterHighLowWindSpeed(1, 0));
			else
				return new Tuple2<String, CounterHighLowWindSpeed>(new String(sid + "-" + date),
						new CounterHighLowWindSpeed(0, 1));
		});

		// Compute how many hours with minWindSpeed>40 and how many hours
		// with minWindSpeed<1
		// Sum the two counters of the CounterHighLowWindSpeed objects
		JavaPairRDD<String, CounterHighLowWindSpeed> sidDateSumHighLow = sidDateHighLow
				.reduceByKey((CounterHighLowWindSpeed c1, CounterHighLowWindSpeed c2) -> new CounterHighLowWindSpeed(
						c1.getHighWindSpeed() + c2.getHighWindSpeed(), c1.getLowWindSpeed() + c2.getLowWindSpeed()));

		// Select only the pairs SID+date for which
		// (Num. of hours with max CPU utilization greater than 40) >= 5 hours
		// (Num. of hours with max CPU utilization less than 1) >= 5 hours
		JavaPairRDD<String, CounterHighLowWindSpeed> selectedSidDateSumHighLow = sidDateSumHighLow.filter(pair -> {
			CounterHighLowWindSpeed counterHighLow = pair._2();
			if (counterHighLow.getHighWindSpeed() >= 5 && counterHighLow.getLowWindSpeed() >= 5)
				return true;
			else
				return false;
		});

		// Save the selected pairs (SID+Date)
		selectedSidDateSumHighLow.keys().saveAsTextFile(outputPathPartB);

		sc.close();
	}
}
