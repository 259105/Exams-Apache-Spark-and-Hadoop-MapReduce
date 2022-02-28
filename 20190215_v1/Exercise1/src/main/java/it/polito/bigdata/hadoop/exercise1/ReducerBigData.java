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

		// Iterate over the set of values and compute
		// - total number of "turism" POIs = number of input values
		// - total number of "museum" POIs = number of ones = sum of values
		int numTurismPOIs = 0;
		int numMuseumPOIs = 0;

		for (IntWritable value : values) {
			numTurismPOIs++;
			numMuseumPOIs = numMuseumPOIs + value.get();
		}

		// Emit the city only if
		// - total number of "turism" POIs > 1000
		// - total number of "museum" POIs >= 20
		if (numTurismPOIs > 1000 && numMuseumPOIs >= 20)
			context.write(new Text(key), NullWritable.get());
	}
}




// 		if (numTurismPOIs >2 && numMuseumPOIs >= 2)
