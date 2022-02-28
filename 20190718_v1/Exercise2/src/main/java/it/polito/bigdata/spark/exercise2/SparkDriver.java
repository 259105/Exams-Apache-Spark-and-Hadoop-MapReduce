package it.polito.bigdata.spark.exercise2;

import org.apache.spark.api.java.*;

import scala.Tuple2;

import java.util.ArrayList;

import org.apache.spark.SparkConf;

public class SparkDriver {

	public static void main(String[] args) {

		String inputPatches;
		String outputPathPartA;
		String outputPathPartB;

		inputPatches = args[0];
		outputPathPartA = args[1];
		outputPathPartB = args[2];

		// Create a configuration object and set the name of the application
		SparkConf conf = new SparkConf().setAppName("Spark Exam 2019_07_18 - Exercise #2");

		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);

		// *****************************************
		// Exercise 2 - Part A
		// *****************************************

		// Read the content of Patches.txt
		JavaRDD<String> patchesRDD = sc.textFile(inputPatches).cache();

		// Select only patches associated with year 2017 and (Windows 10 or Ubuntu 18.04
		JavaRDD<String> patchesRDD2017Windows_Ubuntu = patchesRDD.filter(line -> {
			// PID7000,2017/10/01,Windows 10,OS patch
			String[] fields = line.split(",");

			String date = fields[1];
			String software = fields[2];

			if (date.startsWith("2017") && (software.equals("Windows 10") || software.equals("Ubuntu 18.04")))
				return true;
			else
				return false;
		});

		// Emit pair (month, (Windows, Ubuntu))
		// Windows = 1 is the patch is associated with Windows 10, 0 Otherwise
		// Ubuntu = 1 is the patch is associated with Ubuntu 18.04, 0 Otherwise
		JavaPairRDD<Integer, Counter> MonthCountersRDD = patchesRDD2017Windows_Ubuntu.mapToPair(line -> {
			// PID7000,2017/10/01,Windows 10,OS patch
			String[] fields = line.split(",");

			String software = fields[2];
			Integer month = Integer.parseInt(fields[1].split("/")[1]);

			if (software.equals("Windows 10"))
				return new Tuple2<Integer, Counter>(month, new Counter(1, 0));
			else // Ubuntu 18.04
				return new Tuple2<Integer, Counter>(month, new Counter(0, 1));
		});

		// Compute the number of patches for each month for Windows and Ubuntu
		JavaPairRDD<Integer, Counter> monthNumPatches = MonthCountersRDD
				.reduceByKey((c1, c2) -> new Counter(c1.getNumPatchesWindows() + c2.getNumPatchesWindows(),
						c1.getNumPatchesUbuntu() + c2.getNumPatchesUbuntu()));

		// Store month,"W" if #patches windows > #patches ubuntu
		// Store month,"U" if #patches windows < #patches ubuntu
		// Store nothing if #patches windows = #patches ubuntu

		// Select the months with #windows>#ubuntu or #windows<#ubuntu
		JavaPairRDD<Integer, Counter> selectedMonth = monthNumPatches.filter(pair -> {
			Counter counter = pair._2();

			if (counter.getNumPatchesWindows() != counter.getNumPatchesUbuntu())
				return true;
			else
				return false;
		});

		JavaPairRDD<Integer, String> monthSoftware = selectedMonth.mapToPair(pair -> {
			Counter counter = pair._2();
			if (counter.getNumPatchesWindows() > counter.getNumPatchesUbuntu())
				return new Tuple2<Integer, String>(pair._1(), "W");
			else
				return new Tuple2<Integer, String>(pair._1(), "U");
		});

		monthSoftware.saveAsTextFile(outputPathPartA);

		// *****************************************
		// Exercise 2 - Part B
		// *****************************************

		// Select only patches associated with year 2018
		JavaRDD<String> patchesRDD2018 = patchesRDD.filter(line -> {
			// PID7000,2017/10/01,Windows 10,OS patch
			String[] fields = line.split(",");

			String date = fields[1];

			if (date.startsWith("2018"))
				return true;
			else
				return false;
		});

		// Count the number of patches per month_software
		// Emit pair (month+software, 1)
		JavaPairRDD<MonthSoftware, Integer> monthSoftwarePatch = patchesRDD2018.mapToPair(line -> {
			// PID7000,2017/10/01,Windows 10,OS patch
			String[] fields = line.split(",");

			int month = Integer.parseInt(fields[1].split("/")[1]);
			String software = fields[2];

			return new Tuple2<MonthSoftware, Integer>(new MonthSoftware(month, software), 1);

		});

		// Count the number of patches per month_software
		JavaPairRDD<MonthSoftware, Integer> monthSoftwareNumPatches = monthSoftwarePatch
				.reduceByKey((v1, v2) -> new Integer(v1 + v2));

		// For each software, select only the months with at least 4 patches
		JavaPairRDD<MonthSoftware, Integer> selectedMonthSoftwareNumPatches = monthSoftwareNumPatches.filter(pair -> {
			int numPatches = pair._2();
			if (numPatches >= 4)
				return true;
			else
				return false;
		});

		// Create the elements of the windows
		// Each month is part of at most 3 windows
		// - The window starting in the considered month
		// - The window starting in month-1
		// - The window starting in month-2
		// We are interested only in the windows completely included in 2018.
		// For each input element return (at most) 3 pair
		// (month + software, 1) -> fist element of the window starting at
		// month
		// (month-1+ software, 1) -> second element of the window starting at month-1 //
		// only if month-1>=1
		// (month-2 + software, 1) -> third element of the window starting at month-2 //
		// only if month-2>=1
		// The value is 1 before the next step consists in counting the number of
		// elements in each window
		JavaPairRDD<MonthSoftware, Integer> windowsElementsMonthSoftwareOne = selectedMonthSoftwareNumPatches
				.flatMapToPair(pair -> {

					ArrayList<Tuple2<MonthSoftware, Integer>> elements = new ArrayList<Tuple2<MonthSoftware, Integer>>();

					MonthSoftware currentMonthSoftware = pair._1();

					int currentMonth = currentMonthSoftware.getMonth();
					String currentSoftware = currentMonthSoftware.getSoftware();

					// The current pair is the first element of the window associated with the
					// current software starting at the current month
					elements.add(
							new Tuple2<MonthSoftware, Integer>(new MonthSoftware(currentMonth, currentSoftware), 1));

					// The current pair is the second element of the window associated with the
					// current software starting at the current month-1
					// If currentMonth-1 is less than or equal to 0 it means the starting time is
					// associated with a window starting in the previous year. Hence, it is useless
					if (currentMonth - 1 > 0)
						elements.add(new Tuple2<MonthSoftware, Integer>(
								new MonthSoftware(currentMonth - 1, currentSoftware), 1));

					// The current pair is the third element of the window associated with the
					// current software starting at the current month-2
					// If currentMonth-2 is less than or equal to 0 it means the starting time is
					// associated with a window starting in the previous year. Hence, it is useless
					if (currentMonth - 2 > 0)
						elements.add(new Tuple2<MonthSoftware, Integer>(
								new MonthSoftware(currentMonth - 2, currentSoftware), 1));

					return elements.iterator();

				});

		// Count the number of elements (i.e., months) in each window
		JavaPairRDD<MonthSoftware, Integer> numMonthsPerWindowsSoftware = windowsElementsMonthSoftwareOne
				.reduceByKey((v1, v2) -> v1 + v2);

		// Check if the number of elements is equal to 3. This means all the three
		// months of the window are present (i.e., this is a window of three consecutive
		// months with more than 4 patches for the software associated with this window) 

		JavaPairRDD<MonthSoftware, Integer> selectedWindowsSoftware = numMonthsPerWindowsSoftware.filter( pair -> pair._2()==3);

		// Save the first month and the software of the selected windows
		selectedWindowsSoftware.keys().saveAsTextFile(outputPathPartB);

		// Map each window to software, apply distinct and then count the number of elements=num. of softwares associated with at least one of the selected windows 
		JavaRDD<String> selectedSoftwares = selectedWindowsSoftware.map(pair -> pair._1().getSoftware()).distinct();
		
		// Print the number of softwares associated with the selected windows
		System.out.println(selectedSoftwares.count());
		
		sc.close();
	}
}
