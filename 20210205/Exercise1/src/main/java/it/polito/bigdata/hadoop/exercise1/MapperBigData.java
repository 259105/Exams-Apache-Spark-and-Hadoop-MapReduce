package it.polito.bigdata.hadoop.exercise1;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Exercise 1 - Mapper
 */

class MapperBigData extends Mapper<LongWritable, // Input key type
		Text, // Input value type
		Text, // Output key type
		NullWritable> {// Output value type

	protected void map(LongWritable key, // Input key type
			Text value, // Input value type
			Context context) throws IOException, InterruptedException {

		// Split data
		// RID,FailureTypeCode,Date,Time
		String[] fields = value.toString().split(",");

		String rid = fields[0];
		String failureTypeCode = fields[1];

		// Select only the lines associated with failure type FCode122
		if (failureTypeCode.compareTo("FCode122") == 0) {
			// emit the pair (rid, NullWritable)
			context.write(new Text(rid), NullWritable.get());
		}
	}
}
