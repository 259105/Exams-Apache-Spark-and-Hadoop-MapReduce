package it.polito.bigdata.hadoop.exercise1;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Exercise 1 - Reducer
 */
class ReducerBigData extends Reducer<Text, // Input key type
		Text, // Input value type
		Text, // Output key type
		NullWritable> { // Output value type

	@Override
	protected void reduce(Text key, // Input key type
			Iterable<Text> values, // Input value type
			Context context) throws IOException, InterruptedException {

		boolean oneUsername = true;
		String firstUsername = null;

		// Iterate over the set of values and check if there are at least two usernames
		for (Text value : values) {
			String username = value.toString();

			if (firstUsername == null) {
				firstUsername = username;
			} else {
				if (firstUsername.compareTo(username)!=0)
					oneUsername = false;
			}

		}

		if (oneUsername == true) {
			// Emit MID\tNullWritable
			context.write(new Text(key), NullWritable.get());
		}
	}
}
