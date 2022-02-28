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

		int num2017 = 0;
		int num2018 = 0;

		// Iterate over the set of values and count the number of patches in year 2017 and 2018
		// for the current sid
		for (IntWritable year: values) {
			if (year.get()==2017)
				num2017++;
			else
				num2018++;
		}
		
		if (num2017>=30 || num2018>=30) {
			// Emit sid,NullWritable
			context.write(key, NullWritable.get());
		}
	}
}
