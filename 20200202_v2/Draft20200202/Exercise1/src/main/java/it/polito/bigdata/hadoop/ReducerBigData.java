package it.polito.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Basic MapReduce Project - Reducer
 */
class ReducerBigData extends Reducer<Text, // Input key type
		IntWritable, // Input value type
		Text, // Output key type
		IntWritable> { // Output value type

	@Override

	protected void reduce(Text key, // Input key type
			Iterable<IntWritable> values, // Input value type
			Context context) throws IOException, InterruptedException {

		int totalApps = 0;
		int diffFreeNonFree = 0;

		// Sum values and count the number of apps.
		for (IntWritable val : values) {
			diffFreeNonFree = diffFreeNonFree + val.get();
			totalApps++;
		}

		// If sum>0 then the number of free apps is greater than the number of non-free
		// apps
		if (diffFreeNonFree > 0)
			context.write(key, new IntWritable(totalApps));
	}
}
