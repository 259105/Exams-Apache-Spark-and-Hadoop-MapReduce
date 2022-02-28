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
		// CID,Timestamp,Project,ContributorNickname,Description
		String[] fields = value.toString().split(",");

		int year = Integer.parseInt(fields[1].split("/")[0]);
		int month = Integer.parseInt(fields[1].split("/")[1]);
		String contributor = fields[3];

		// Select only commits associated with May 2019 and June 2019
		if (year == 2019 && (month == 5 || month == 6)) {
			// emit the pair (contributor,month)
			context.write(new Text(contributor), new IntWritable(month));

		}

	}
}
