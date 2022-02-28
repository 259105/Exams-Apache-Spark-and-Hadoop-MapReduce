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
		// SID,PID,Date
		String[] fields = value.toString().split(",");
		
		String sid = fields[0];
		String date = fields[2];

		// Select only patches applied in 2017 or 2018
		if (date.startsWith("2017")) {
			// emit the pair (SID, 2017)
			context.write(new Text(sid), new IntWritable(2017));
		}
		
		if (date.startsWith("2018")) {
			// emit the pair (SID, 2018)
			context.write(new Text(sid), new IntWritable(2018));
		}
	}
}
