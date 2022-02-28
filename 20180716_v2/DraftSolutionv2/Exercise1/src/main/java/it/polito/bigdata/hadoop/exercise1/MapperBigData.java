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
		// RID,FaultTypeCode,FaultDuration,Date,Time
		// Example: R5,FCode122,20,2017/05/02,06:40:51
		String[] fields = value.toString().split(",");

		String rid = fields[0];
		String failureTypeCode = fields[1];
		String date = fields[3];

		// Select only FaultTypeCode = FCode100
		if (failureTypeCode.compareTo("FCode100") == 0) {
			// emit the pair (rid, 1) if the month is January 2015
			// emit the pair (rid, 2) if the month is February 2015
			// emit nothing in the other cases
			if (date.startsWith("2015/01") == true)
				context.write(new Text(rid), new IntWritable(1));
			else if (date.startsWith("2015/02") == true)
				context.write(new Text(rid), new IntWritable(2));

		}
	}
}
