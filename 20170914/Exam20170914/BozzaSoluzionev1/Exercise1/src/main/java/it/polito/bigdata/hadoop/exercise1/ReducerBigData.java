package it.polito.bigdata.hadoop.exercise1;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Exercise 1 - Reducer
 */
class ReducerBigData extends
		Reducer<Text, // Input key type
				IntWritable, // Input value type
				Text, // Output key type
				DoubleWritable> { // Output value type

	@Override
	protected void reduce(Text key, // Input key type
			Iterable<IntWritable> values, // Input value type
			Context context) throws IOException, InterruptedException {

		int numFlights = 0;
		int numCancelledFlights = 0;
		double percCancelled;

		// Iterate over the set of values and check if the flag is 1 (cancelled)
		// or 0 (not cancelled)
		for (IntWritable flag : values) {

			numFlights++;

			if (flag.get() == 1)
				numCancelledFlights++;
		}

		percCancelled = (double) numCancelledFlights / (double) numFlights;

		if (percCancelled > 0.01) {
			context.write(key, new DoubleWritable(100.0 * percCancelled));
		}
	}
}
