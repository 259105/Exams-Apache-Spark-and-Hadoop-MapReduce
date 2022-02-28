package it.polito.bigdata.hadoop.exercise1;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Exercise 1 - Reducer
 */
class ReducerBigData extends
		Reducer<Text, // Input key type
				Text, // Input value type
				Text, // Output key type
				NullWritable> { // Output value type

	@Override
	protected void reduce(Text key, // Input key type
			Iterable<Text> values, // Input value type
			Context context) throws IOException, InterruptedException {

		boolean greaterThan35 = false;
		boolean lessThanMinus20 = false;

		// Iterate over the set of values and check if there is at least
		// one "g" and at least on "l"
		for (Text flags : values) {

			if (flags.toString().compareTo("g") == 0) {
				greaterThan35 = true;
			}

			if (flags.toString().compareTo("l") == 0) {
				lessThanMinus20 = true;
			}
		}

		if (greaterThan35 == true && lessThanMinus20 == true) {
			context.write(key, NullWritable.get());
		}
	}
}
