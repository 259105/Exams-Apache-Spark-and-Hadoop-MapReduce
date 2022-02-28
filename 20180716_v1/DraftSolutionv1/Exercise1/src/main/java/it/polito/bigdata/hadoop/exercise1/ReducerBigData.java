package it.polito.bigdata.hadoop.exercise1;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Exercise 1 - Reducer
 */
class ReducerBigData extends
		Reducer<Text, // Input key type
				IntWritable, // Input value type
				Text, // Output key type
				NullWritable> { // Output value type

	@Override
	protected void reduce(Text key, // Input key type
			Iterable<IntWritable> values, // Input value type
			Context context) throws IOException, InterruptedException {

		Boolean hardDriveFailure = false;
		Boolean RAMFailure = false;

		// Iterate over the set of values and check if there is at least one
		// hard drive failure and at least one RAM failure
		for (IntWritable failureType : values) {
			// Only two types of failures are sent to the reducer
			// 1 -> hard drive failure
			// 2 -> RAM failure
			if (failureType.get() == 1)
				hardDriveFailure = true;
			else
				RAMFailure = true;
		}

		// Check if there is at least one hard drive failure and at least one
		// RAM failure
		if (hardDriveFailure == true && RAMFailure == true) {
			context.write(key, NullWritable.get());
		}
	}
}
