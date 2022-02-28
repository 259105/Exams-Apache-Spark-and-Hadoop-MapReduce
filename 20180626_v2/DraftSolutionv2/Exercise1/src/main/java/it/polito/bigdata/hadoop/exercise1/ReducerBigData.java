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

		int sum = 0;

		// Iterate over the set of values and sum them
		for (IntWritable one : values) {
			sum = sum + one.get();
		}

		if (sum >= 1000) {
			context.write(key, NullWritable.get());
		}
	}
}
