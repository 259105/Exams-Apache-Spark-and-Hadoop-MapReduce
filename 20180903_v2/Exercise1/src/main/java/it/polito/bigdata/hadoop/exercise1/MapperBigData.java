package it.polito.bigdata.hadoop.exercise1;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Exercise 1 - Mapper
 */
class MapperBigData extends
		Mapper<LongWritable, // Input key type
				Text, // Input value type
				NullWritable, // Output key type
				Text> {// Output value type

	protected void map(LongWritable key, // Input key type
			Text value, // Input value type
			Context context) throws IOException, InterruptedException {

		// Split value
		// date,hour:minute,stockId,price
		// Example: 2014/04/01,10:02,TSLA,51.24
		String[] fields = value.toString().split(",");

		String date = fields[0];
		String hourAndMinute = fields[1];
		String stockId = fields[2];
		String price = fields[3];

		// Select only stockId TSLA and year 2014
		if (date.startsWith("2014") == true && stockId.equals("TSLA")) {
			// emit pair (null, date+","+hourAndMinutes+","+price)
			context.write(NullWritable.get(), new Text(date + "," + hourAndMinute + "," + price));
		}
	}
}
