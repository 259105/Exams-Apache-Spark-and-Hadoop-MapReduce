package it.polito.bigdata.hadoop.exercise1;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Exercise 1 - Reducer
 */
class ReducerBigData extends Reducer<Text, // Input key type
		NullWritable, // Input value type
		IntWritable, // Output key type
		NullWritable> { // Output value type

	int numRIDs;

	protected void setup(Context context) throws IOException, InterruptedException {
		// Initialize the variable used to count the number of distinct RIDs
		numRIDs = 0;
	}

	@Override
	protected void reduce(Text key, // Input key type
			Iterable<NullWritable> values, // Input value type
			Context context) throws IOException, InterruptedException {

		numRIDs++;
	}

	protected void cleanup(Context context) throws IOException, InterruptedException {
		// Emit numRIDs\tNullWritable
		context.write(new IntWritable(numRIDs), NullWritable.get());
	}

}
