package it.polito.bigdata.hadoop.exercise1;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Exercise 1 - Mapper
 */
class MapperBigData extends Mapper<LongWritable, // Input key type
		Text, // Input value type
		Text, // Output key type
		IntWritable> {// Output value type

	protected void map(LongWritable key, // Input key type
			Text value, // Input value type
			Context context) throws IOException, InterruptedException {

		// Split data
		// Customerid,BID,date,price
		String[] fields = value.toString().split(",");
		
		String customerId = fields[0];
		String bid = fields[1];
		String date =  fields[2];

		// Select only the purchases of year 2018
		if (date.startsWith("2018")) {
			// emit the pair (customerId+BID, +1)
			context.write(new Text(customerId+"_"+bid), new IntWritable(1));
		}
	}
}
