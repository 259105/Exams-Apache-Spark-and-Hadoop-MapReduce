package it.polito.bigdata.hadoop.exercise1;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Exercise 1 - Reducer
 */
class ReducerBigData extends Reducer<Text, // Input key type
		IntWritable, // Input value type
		Text, // Output key type
		NullWritable> { // Output value type

	@Override
	protected void reduce(Text key, // Input key type
			Iterable<IntWritable> values, // Input value type
			Context context) throws IOException, InterruptedException {

		int numOccurrences2017 = 0;
		int numOccurrences2018 = 0;

		// Iterate over the set of values and count the number of occurrences of year
		// 2017 and the number of occurrences of year 2018.
		// The first value is the number of patches for the current software application
		// during year 2017 and
		// the second value is the number of patches for the current software
		// application during year 2018

		for (IntWritable yearPatch : values) {
			if (yearPatch.get() == 2017) {
				numOccurrences2017++;
			} else {
				numOccurrences2018++;
			}
		}

		// Check if numOccurrences2018 is greater than numOccurrences2017
		if (numOccurrences2018 > numOccurrences2017) {
			// Emit (ApplicationName,NullWritable)
			context.write(new Text(key.toString()), NullWritable.get());
		}
	}
}
