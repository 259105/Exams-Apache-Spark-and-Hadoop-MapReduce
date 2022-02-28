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

		int numOccurrencesMay = 0;
		int numOccurrencesJune = 0;

		// Iterate over the set of values and count the number of occurrences of May and June. 
		// The first value is the number of commits for the current contributor
		// during May 2019 and the second value is the number of commits for the contributor
		// during June 2019

		for (IntWritable monthCommit : values) {
			if (monthCommit.get() == 05) {
				numOccurrencesMay++;
			} else {
				numOccurrencesJune++;
			}
		}

		// Check if numOccurrencesJune is greater than numOccurrencesMay
		if (numOccurrencesJune > numOccurrencesMay) {
			// Emit (Contributor,NullWritable)
			context.write(new Text(key.toString()), NullWritable.get());
		}
	}
}
