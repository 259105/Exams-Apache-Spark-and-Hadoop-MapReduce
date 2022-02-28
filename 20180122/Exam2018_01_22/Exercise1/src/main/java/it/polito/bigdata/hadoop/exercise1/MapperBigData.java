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
		// customer1,B1020,20170502_13:10,25.00
		String[] fields = value.toString().split(",");

		String bid = fields[1];
		String date = fields[2];

		// Select only the flights of the last year
		if (date.startsWith("2016") == true) {
			// emit the pair (departure airport, flag)
			context.write(new Text(bid), new IntWritable(1));

		}

	}
}
