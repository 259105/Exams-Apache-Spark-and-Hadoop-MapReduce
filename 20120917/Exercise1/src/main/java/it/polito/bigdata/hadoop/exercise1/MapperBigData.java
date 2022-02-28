package it.polito.bigdata.hadoop.exercise1;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Exercise 1 - Mapper
 */
class MapperBigData extends Mapper<LongWritable, // Input key type
		Text, // Input value type
		Text, // Output key type
		Text> {// Output value type

	protected void map(LongWritable key, // Input key type
			Text value, // Input value type
			Context context) throws IOException, InterruptedException {

		// Split data
		// PaoloG76,MID124,2018/06/01_14:18,2018/06/01_16:10
		String[] fields = value.toString().split(",");

		String username = fields[0];
		String mid = fields[1];
		String startTime = fields[2];

		// Select only the lines associated with year 2019
		if (startTime.startsWith("2019")) {
			// emit the pair (mid,username)
			context.write(new Text(mid), new Text(username));
		}
	}
}
