package it.polito.bigdata.hadoop.exercise1;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Exercise 1 - Mapper
 */

class MapperBigData extends
		Mapper<LongWritable, // Input key type
				Text, // Input value type
				Text, // Output key type
				IntWritable> {// Output value type

	protected void map(LongWritable key, // Input key type
			Text value, // Input value type
			Context context) throws IOException, InterruptedException {

		// Split data
		// AI1103,Alitalia,2016/03/01,15:35,17:10,TRN,CDG,5,no,150,143
		String[] fields = value.toString().split(",");

		String departureAirport = fields[5];
		String date = fields[2];
		String cancelled = fields[8];
		int flag;

		// Select only the flights of the last year
		if (date.compareTo("2016/09/01") >= 0 && date.compareTo("2017/08/31") <= 0) {
			// If the flight was cancelled emit 1.
			// 0 otherwise.
			if (cancelled.compareTo("yes") == 0)
				flag = 1;
			else
				flag = 0;

			// emit the pair (departure airport, flag)
			context.write(new Text(departureAirport), new IntWritable(flag));

		}

	}
}
