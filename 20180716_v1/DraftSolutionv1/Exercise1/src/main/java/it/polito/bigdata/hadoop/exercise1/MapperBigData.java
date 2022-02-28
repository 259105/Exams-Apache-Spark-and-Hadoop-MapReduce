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
		// Date,Time,SID,FailureType,Downtime
		// Example: 2016/05/01,05:40:31,S2,hard_drive,5
		String[] fields = value.toString().split(",");

		String date = fields[0];
		String sid = fields[2];
		String failureType = fields[3];


		// Select only April 2016
		if (date.startsWith("2016/04") == true) {
			// emit the pair (sid, 1) if the failute type is hard drive 
			// emit the pair (sid, 2) if the failute type is RAM
			// emit nothing in the other cases
			if (failureType.compareTo("hard_drive")==0)
				context.write(new Text(sid), new IntWritable(1));
			else
				if (failureType.compareTo("RAM")==0)
					context.write(new Text(sid), new IntWritable(2));

		}
	}
}
