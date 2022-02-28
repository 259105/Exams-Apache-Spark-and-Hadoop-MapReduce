package it.polito.bigdata.spark.exercise2;

import org.apache.spark.api.java.*;

import org.apache.spark.SparkConf;
	
public class SparkDriver {
	
	
	public static void main(String[] args) {

		String inputPathPM10readings;
		String outputPathMonthlyStatistics;
		String outputPathCriticalPeriods;

		double PM10threshold;
		
		inputPathPM10readings=args[0];
		PM10threshold=Double.parseDouble(args[1]);
		outputPathMonthlyStatistics=args[2];
		outputPathCriticalPeriods=args[3];
	
		// Create a configuration object and set the name of the application
		SparkConf conf=new SparkConf().setAppName("Spark Exam 2 - Exercise #2");
		
		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);

		// *****************************************
		// Exercise 2 - Part A
		// *****************************************
		// Compute, for each pair (sensorid, month), 
		// the number of days with a critical PM10 value and store the result in an HDFS folder. 
		// Each line of the output file contains a 
		// pair (month, number of critical days for that month).
		
		// Read the content of PM10Readings.txt
		JavaRDD<String> PM10readingsRDD = sc.textFile(inputPathPM10readings);
		
		// Select the critical readings/lines
		JavaRDD<String> criticalPM10readingsRDD = PM10readingsRDD.filter(new CriticalPM10(PM10threshold)).cache();
		
		// Count the number of critical days (lines) for each pair (sensorid,month)
		// Define a JavaPairRDD with sensorid_month as key and 1 as value
		JavaPairRDD<String,Integer> sensorMonthdatesCriticalPM10=criticalPM10readingsRDD.mapToPair(new SensorMonthOne());

		// Use reduce by key to compute the number of critical days for each pair (sensorid,month)  
		JavaPairRDD<String,Integer> sensorMonthdatesNumDays=sensorMonthdatesCriticalPM10.reduceByKey(new Sum());
		
		sensorMonthdatesNumDays.saveAsTextFile(outputPathMonthlyStatistics);

		// *****************************************
		// Exercise 2 - Part B
		// *****************************************
		// Select, for each sensor, the dates associated with critical PM10 values
		// The critical readings are already available in criticalPM10readingsRDD.
		// Each line associated with a critical PM10 value can be part of 
		// three critical time periods (composed of three consecutive dates). 
		// Suppose dateCurrent is the date of the current line.
		// The three potential critical time periods containing dateCurrent are:  
		// - dateCurrent, dateCurrent+1, dateCurrent+2
		// - dateCurrent-1, dateCurrent, dateCurrent+1
		// - dateCurrent-2, dateCurrent-1, dateCurrent
		// For each line emits three pairs:
		// - dateCurrent_sensorid, 1, 
		// - dateCurrent-1_sensorid, 1
		// - dateCurrent-2_sensorid, 1
		// The sensorid is needed because the same date can be associated with many sensors
		JavaPairRDD<String,Integer> sensorDateInitialSequenceRDD=criticalPM10readingsRDD.flatMapToPair(new SensorDateInitialSequenceOne());
		
		// Count the number of ones for each key. If the sum is 3 it means 
		// that the dates key, key+1, and key+2, for the sensor sensorid, are all associated 
		// with a critical PM10 value. Hence, key, key+2 is a critical time period for sensor sensordid
		JavaPairRDD<String,Integer> DateInitialSequenceCountRDD=sensorDateInitialSequenceRDD.reduceByKey(new Sum());

		// Select the critical time periods (i.e., the ones with count = 3 
		JavaPairRDD<String,Integer> DateInitialCriticalSequenceCountRDD=DateInitialSequenceCountRDD.filter(new ThreeValues());
		
		// Get only the dates and store them
		JavaRDD<String> DateInitialCriticalSequenceRDD=DateInitialCriticalSequenceCountRDD.keys();
				
		DateInitialCriticalSequenceRDD.saveAsTextFile(outputPathCriticalPeriods);
		
		// Close the Spark context
		sc.close(); 
	}
}
