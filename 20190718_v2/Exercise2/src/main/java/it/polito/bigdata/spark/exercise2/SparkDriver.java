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
		SparkConf conf = new SparkConf().setAppName("Spark Exam 2019_07_18 - Exercise #2 v2");

		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);

		// *****************************************
		// Exercise 2 - Part A
		// *****************************************

		// Read the content of Commitstxt
		JavaRDD<String> commitsRDD = sc.textFile(inputPatches).cache();

		// Select only commits associated with June 2019 and (Apache Spark  or Apache Flink) 
		JavaRDD<String> commitsRDDJune2019Spark_Flink = commitsRDD.filter(line -> {
			// CID1201,2017/05/02_17:24,Apache Spark,JohnP,[SQL] Group By
			String[] fields = line.split(",");

			String date = fields[1];
			String project = fields[2];

			if (date.startsWith("2019/06") && (project.equals("Apache Spark") || project.equals("Apache Flink")))
				return true;
			else
				return false;
		});

		// Emit pair (date, (Apache Spark, Apache Flink))
		// Apache Spark = 1 is the commit is associated with Apache Spark, 0 Otherwise
		// Apache Flink = 1 is the commit is associated with Apache Flink, 0 Otherwise
		JavaPairRDD<String, Counter> DateCountersRDD = commitsRDDJune2019Spark_Flink.mapToPair(line -> {
			// CID1201,2017/05/02_17:24,Apache Spark,JohnP,[SQL] Group By
			String[] fields = line.split(",");

			String date = fields[1].split("_")[0];
			String project = fields[2];

			if (project.equals("Apache Spark"))
				return new Tuple2<String, Counter>(date, new Counter(1, 0));
			else // Apache Flink
				return new Tuple2<String, Counter>(date, new Counter(0, 1));
		});

		// Compute the number of commits for each date for Apache Spark and Apache Flink
		JavaPairRDD<String, Counter> dateNumCommits = DateCountersRDD
				.reduceByKey((c1, c2) -> new Counter(c1.getNumCommitsSpark() + c2.getNumCommitsSpark(),
						c1.getNumCommitsFlink() + c2.getNumCommitsFlink()));

		// Store date,"AS" if #commits Apache Spark > #commits Apache Flink
		// Store date,"AF" if #commits Apache Spark < #commits Apache Flink
		// Store nothing   if #commits Apache Spark = #commits Apache Flink

		// Select the dates with #Apache Spark > #Apache Flink or #Apache Spark < #Apache Flink
		JavaPairRDD<String, Counter> selectedDates = dateNumCommits.filter(pair -> {
			Counter counter = pair._2();

			if (counter.getNumCommitsSpark() != counter.getNumCommitsFlink())
				return true;
			else
				return false;
		});

		JavaPairRDD<String, String> dateProject = selectedDates.mapToPair(pair -> {
			Counter counter = pair._2();
			if (counter.getNumCommitsSpark() > counter.getNumCommitsFlink())
				return new Tuple2<String, String>(pair._1(), "AS");
			else
				return new Tuple2<String, String>(pair._1(), "AF");
		});

		dateProject.saveAsTextFile(outputPathPartA);

		// *****************************************
		// Exercise 2 - Part B
		// *****************************************

		// Select only commits associated with year 2017
		JavaRDD<String> commitsRDD2017 = commitsRDD.filter(line -> {
			// CID1201,2017/05/02_17:24,Apache Spark,JohnP,[SQL] Group By
			String[] fields = line.split(",");

			String date = fields[1];

			if (date.startsWith("2017"))
				return true;
			else
				return false;
		});

		// Count the number of commits per month_project
		// Emit pair (month+project, 1)
		JavaPairRDD<MonthProject, Integer> monthProjectCommit = commitsRDD2017.mapToPair(line -> {
			// CID1201,2017/05/02_17:24,Apache Spark,JohnP,[SQL] Group By
			String[] fields = line.split(",");

			int month = Integer.parseInt(fields[1].split("/")[1]);
			String project = fields[2];

			return new Tuple2<MonthProject, Integer>(new MonthProject(month, project), 1);

		});

		// Count the number of commits per month_project
		JavaPairRDD<MonthProject, Integer> monthProjectNumCommits = monthProjectCommit
				.reduceByKey((v1, v2) -> new Integer(v1 + v2));

		// For each project, select only the months with at least 20 commits
		JavaPairRDD<MonthProject, Integer> selectedMonthProjectNumCommits = monthProjectNumCommits.filter(pair -> {
			int numPatches = pair._2();
			if (numPatches >= 20)
				return true;
			else
				return false;
		});

		// Create the elements of the windows
		// Each month is part of at most 3 windows
		// - The window starting in the considered month
		// - The window starting in month-1
		// - The window starting in month-2
		// We are interested only in the windows completely included in 2017.
		// For each input element return (at most) 3 pair
		// (month + project, 1) -> fist element of the window starting at
		// month
		// (month-1 + project, 1) -> second element of the window starting at month-1 //
		// only if month-1>=1
		// (month-2 + project, 1) -> third element of the window starting at month-2 //
		// only if month-2>=1
		// The value is 1 before the next step consists in counting the number of
		// elements in each window
		JavaPairRDD<MonthProject, Integer> windowsElementsMonthProjectOne = selectedMonthProjectNumCommits
				.flatMapToPair(pair -> {

					ArrayList<Tuple2<MonthProject, Integer>> elements = new ArrayList<Tuple2<MonthProject, Integer>>();

					MonthProject currentMonthProject = pair._1();

					int currentMonth = currentMonthProject.getMonth();
					String currentProject = currentMonthProject.getProject();

					// The current pair is the first element of the window associated with the
					// current project starting at the current month
					elements.add(
							new Tuple2<MonthProject, Integer>(new MonthProject(currentMonth, currentProject), 1));

					// The current pair is the second element of the window associated with the
					// current project starting at the current month-1
					// If currentMonth-1 is less than or equal to 0 it means the starting time is
					// associated with a window starting in the previous year. Hence, it is useless
					if (currentMonth - 1 > 0)
						elements.add(new Tuple2<MonthProject, Integer>(
								new MonthProject(currentMonth - 1, currentProject), 1));

					// The current pair is the third element of the window associated with the
					// current project starting at the current month-2
					// If currentMonth-2 is less than or equal to 0 it means the starting time is
					// associated with a window starting in the previous year. Hence, it is useless
					if (currentMonth - 2 > 0)
						elements.add(new Tuple2<MonthProject, Integer>(
								new MonthProject(currentMonth - 2, currentProject), 1));

					return elements.iterator();

				});

		// Count the number of elements (i.e., months) in each window
		JavaPairRDD<MonthProject, Integer> numMonthsPerWindowsProject = windowsElementsMonthProjectOne
				.reduceByKey((v1, v2) -> v1 + v2);

		// Check if the number of elements is equal to 3. This means all the three
		// months of the window are present (i.e., this is a window of three consecutive
		// months with more than 20 commits for the project associated with this window) 

		JavaPairRDD<MonthProject, Integer> selectedWindowsProject = numMonthsPerWindowsProject.filter( pair -> pair._2()==3).cache();

		// Save the first month and the project of the selected windows
		selectedWindowsProject.keys().saveAsTextFile(outputPathPartB);

		// Map each window to project, apply distinct and then count the number of elements=num. of projects associated with at least one of the selected windows 
		JavaRDD<String> selectedProjects = selectedWindowsProject.map(pair -> pair._1().getProject()).distinct();
		
		// Print the number of projects associated with the selected windows
		System.out.println(selectedProjects.count());
		
		sc.close();
	}
}
