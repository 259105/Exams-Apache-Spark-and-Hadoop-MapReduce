package it.polito.bigdata.hadoop.exercise1;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
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
		// PID,Date,ApplicationName,BriefDescription
		String[] fields = value.toString().split(",");

		int year = Integer.parseInt(fields[1].split("/")[0]);
		String application = fields[2];

		// Select only years 2017 and 2018
		if (year==2017 || year==2018) {
			// emit the pair (application,year)
			context.write(new Text(application), new IntWritable(year));
			
		}

	}
}
