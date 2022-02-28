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

		Boolean januaryFault = false;
		Boolean februaryFault = false;

		// Iterate over the set of values and check if there is at least one
		// time January 2015 and at least one time February 2015
		for (IntWritable failureType : values) {
			// Only two types of failures are sent to the reducer
			// 1 -> January 2015
			// 2 -> February 2015
			if (failureType.get() == 1)
				januaryFault = true;
			else
				februaryFault = true;
		}

		// Check if there is at least one time January 2015 and at least one
		// time February 2015
		if (januaryFault == true && februaryFault == true) {
			context.write(key, NullWritable.get());
		}
	}
}
