package it.polito.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Basic MapReduce Project - Reducer
 */
class ReducerBigData extends Reducer<IntWritable, // Input key type
		IntWritable, // Input value type
		IntWritable, // Output key type
		IntWritable> { // Output value type

	private int maxCount;
	private int maxYear;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		maxCount = -1;
		maxYear = Integer.MAX_VALUE;
	}

	@Override
	protected void reduce(IntWritable key, // Input key type
			Iterable<IntWritable> values, // Input value type
			Context context) throws IOException, InterruptedException {

		int numOccurrences = 0;
		int currentYear = key.get();

		// Compute the number of occurrences of the current year
		for (IntWritable value : values) {
			numOccurrences = numOccurrences + value.get();
		}

		// Check if this is the local maximum
		if (maxCount < numOccurrences || (maxCount == numOccurrences && currentYear < maxYear)) {
			maxCount = numOccurrences;
			maxYear = currentYear;
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		// Emit the local maximum and the associated year
		context.write(new IntWritable(maxCount), new IntWritable(maxYear));
	}
}
