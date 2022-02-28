package it.polito.bigdata.spark.exercise2;

import org.apache.spark.api.java.*;

import scala.Tuple2;

import org.apache.spark.SparkConf;

public class SparkDriver {

	@SuppressWarnings("resource")
	public static void main(String[] args) {

		double CPUthr;
		double RAMthr;
		String inputPathPrices;
		String outputPathPartA;
		String outputPathPartB;

		CPUthr = Double.parseDouble(args[0]);
		RAMthr = Double.parseDouble(args[1]);
		inputPathPrices = args[2];
		outputPathPartA = args[3];
		outputPathPartB = args[4];

		// Create a configuration object and set the name of the application
		SparkConf conf = new SparkConf().setAppName("Spark Exam 2018_06_26 - Exercise #2");

		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);

		// *****************************************
		// Exercise 2 - Part A
		// *****************************************

		// Read the content of PerformanceLog.txt
		JavaRDD<String> statistics = sc.textFile(inputPathPrices);

		// Select data related to May 2018
		// Example: 2018/05/01,15:40,VS1,10.5,0.5
		JavaRDD<String> statisticsMay2018 = statistics.filter(line -> {
			if (line.startsWith("2018/05") == true)
				return true;
			else
				return false;
		}).cache();

		// Map each input line to a pair
		// key = VSID_Hour
		// value = (1, CPUUtiliation, RAMUtiliation)
		JavaPairRDD<String, Counter> vsidHourCounter = statisticsMay2018.mapToPair(line -> {
			String[] fields = line.split(",");

			String time = fields[1];
			String hour = time.split(":")[0];

			String vsid = fields[2];
			double cpuUtil = Double.parseDouble(fields[3]);
			double ramUtil = Double.parseDouble(fields[4]);

			Counter counter = new Counter(1, cpuUtil, ramUtil);

			return new Tuple2<String, Counter>(new String(vsid + "_" + hour), counter);

		});

		// Sum the three part of the Counter objects
		// Sum count
		// Sum CPUutilization
		// Sum RAMutilization
		JavaPairRDD<String, Counter> vsidHourAvg = vsidHourCounter
				.reduceByKey((Counter c1, Counter c2) -> new Counter(c1.getCount() + c2.getCount(),
						c1.getSumCPUutil() + c2.getSumCPUutil(), c1.getSumRAMutil() + c2.getSumRAMutil()));

		// Filter pairs by applying the two thresholds
		JavaPairRDD<String, Counter> selectedVsidHour = vsidHourAvg.filter(pair -> {
			double avgCPU = pair._2().getSumCPUutil() / (double) pair._2().getCount();
			double avgRAM = pair._2().getSumRAMutil() / (double) pair._2().getCount();

			if (avgCPU > CPUthr && avgRAM > RAMthr)
				return true;
			else
				return false;
		});

		selectedVsidHour.keys().saveAsTextFile(outputPathPartA);

		// *****************************************
		// Exercise 2 - Part B
		// *****************************************

		// Map each input line to a pair
		// key = VSID+date+hour
		// value = CPUUtilization
		// Example of input line: 2018/05/01,15:40,VS1,10.5,0.5
		JavaPairRDD<String, Double> vsidDateHourCPUutil = statisticsMay2018.mapToPair(line -> {
			String[] fields = line.split(",");
			String date = fields[0];
			String time = fields[1];
			String hour = time.split(":")[0];
			String vsid = fields[2];
			Double cpuUtil = new Double(fields[3]);

			return new Tuple2<String, Double>(new String(vsid + "_" + date + "_" + hour), cpuUtil);
		});

		// Compute max CPUUtilization for each key (VSID+date+hour)
		JavaPairRDD<String, Double> vsidDateHourMaxCPUutil = vsidDateHourCPUutil.reduceByKey((cpuUtil1, cpuUtil2) -> {
			if (cpuUtil1 > cpuUtil2)
				return cpuUtil1;
			else
				return cpuUtil2;
		});

		// Select only the lines with maxCPUUtilization>90 or <10
		JavaPairRDD<String, Double> vsidDateHourMaxCPUHighLow = vsidDateHourMaxCPUutil.filter(pair -> {
			Double maxCPUUtil = pair._2();
			if (maxCPUUtil > 90.0 || maxCPUUtil < 10.0)
				return true;
			else
				return false;
		});

		// Return one pair of each input element
		// key = VSID+date
		// value = (>90, <10)
		JavaPairRDD<String, CounterHighLowCPU> vsidDateHighLow = vsidDateHourMaxCPUHighLow.mapToPair(pair -> {
			String[] fields = pair._1().split("_");
			String vsid = fields[0];
			String date = fields[1];

			Double maxCPUUtil = pair._2();
			if (maxCPUUtil > 90.0)
				return new Tuple2<String, CounterHighLowCPU>(new String(vsid + "_" + date),
						new CounterHighLowCPU(1, 0));
			else
				return new Tuple2<String, CounterHighLowCPU>(new String(vsid + "_" + date),
						new CounterHighLowCPU(0, 1));
		});

		// Compute how many hours with maxCPUutilization>90 and how many hours
		// with maxCPUutilization<10
		// Sum the two counters of the CounterHighLowCPU objects
		JavaPairRDD<String, CounterHighLowCPU> vsidDateSumHighLow = vsidDateHighLow.reduceByKey(
				(CounterHighLowCPU c1, CounterHighLowCPU c2) -> new CounterHighLowCPU(c1.getHighCPU() + c2.getHighCPU(),
						c1.getLowCPU() + c2.getLowCPU()));

		// Select only the pairs VSID+date for which
		// (Num. of hours with max CPU utilization greater than 90.0) >= 10
		// hours
		// (Num. of hours with max CPU utilization less than 10.0) >= 10 hours
		JavaPairRDD<String, CounterHighLowCPU> selectedVsidDateSumHighLow = vsidDateSumHighLow.filter(pair -> {
			CounterHighLowCPU counterHighLow = pair._2();
			if (counterHighLow.getHighCPU() >= 8 && counterHighLow.getLowCPU() >= 8)
				return true;
			else
				return false;
		});

		// Save the selected pairs (VSID+Date)
		selectedVsidDateSumHighLow.keys().saveAsTextFile(outputPathPartB);

		sc.close();
	}
}
