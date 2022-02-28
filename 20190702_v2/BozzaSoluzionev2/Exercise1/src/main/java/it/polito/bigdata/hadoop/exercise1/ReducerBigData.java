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

		Boolean allEqual = true;
		String CPUversion = null;

		// Iterate over the set of values and check if they are all associated with the
		// same CPU version
		for (Text currentCPUversion : values) {

			if (CPUversion != null && CPUversion.compareTo(currentCPUversion.toString()) != 0) {
				// There are at least two different CPU versions for the current data center. It must
				// be discarded.
				allEqual = false;
			}

			CPUversion = currentCPUversion.toString();
		}

		if (allEqual == true) {
			// All the values are associated with the same CPU version for this data center. The
			// data center is selected.
			context.write(new Text(key.toString() + "," + CPUversion), NullWritable.get());
		}
	}
}
