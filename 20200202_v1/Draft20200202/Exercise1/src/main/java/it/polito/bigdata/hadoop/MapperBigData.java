package it.polito.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Basic MapReduce Project - Mapper
 */
class MapperBigData extends Mapper<LongWritable, // Input key type
		Text, // Input value type
		Text, // Output key type
		IntWritable> { // Output value type

	protected void map(LongWritable key, // Input key type
			Text value, // Input value type
			Context context) throws IOException, InterruptedException {

		// Input string format:
		// AppId, Name, Price, Category, Company
		String[] fields = value.toString().split(",");
		String category = fields[3];
		String company = fields[4];
		double price = Double.parseDouble(fields[2]);

		// Select only apps of the category Game.
		if (category.toLowerCase().equals("game")) {
			// If the app is free, emit +1
			if (price == 0)
				context.write(new Text(company), new IntWritable(1));
			else // If the app is not free, emit -1
				context.write(new Text(company), new IntWritable(-1));
		}
	}
}
