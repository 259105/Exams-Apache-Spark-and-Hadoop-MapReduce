package it.polito.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Basic MapReduce Project - Reducer
 */
class Reducer2BigData extends Reducer<NullWritable, // Input key type
		Text, // Input value type
		IntWritable, // Output key type
		IntWritable> { // Output value type

	@Override
	protected void reduce(NullWritable key, // Input key type
			Iterable<Text> values, // Input value type
			Context context) throws IOException, InterruptedException {

		int maxCount = -1;
		int maxYear = Integer.MAX_VALUE;

		int currentYear;
		int numOccurrences;

		for (Text val : values) {
			String[] fields = val.toString().split("_");
			currentYear = Integer.parseInt(fields[0]);
			numOccurrences = Integer.parseInt(fields[1]);

			// Check if this is the glabal maximum
			if (maxCount < numOccurrences || (maxCount == numOccurrences && currentYear < maxYear)) {
				maxCount = numOccurrences;
				maxYear = currentYear;
			}
		}

		// Store the result
		context.write(new IntWritable(maxYear), new IntWritable(maxCount));
	}
}
