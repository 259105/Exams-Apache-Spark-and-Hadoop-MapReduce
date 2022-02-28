package it.polito.bigdata.hadoop.exercise1;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Exercise 1 - Reducer
 */
class ReducerBigData2 extends
		Reducer<Text, // Input key type
				IntWritable, // Input value type
				Text, // Output key type
				IntWritable> { // Output value type

	int highestNumPurchases;
	String bidHighestNumPurchases;

	@Override
	protected void setup(Context context) {
		highestNumPurchases = Integer.MIN_VALUE;
		bidHighestNumPurchases = null;
	}

	@Override
	protected void reduce(Text key, // Input key type
			Iterable<IntWritable> values, // Input value type
			Context context) throws IOException, InterruptedException {

		int numPurchases = 0;

		String bid = new String(key.toString());

		// Iterate over the set of values and count the number of purchses for
		// this book
		for (IntWritable value : values) {
			numPurchases = numPurchases + value.get();
		}

		if (bidHighestNumPurchases == null || highestNumPurchases < numPurchases
				|| (highestNumPurchases == numPurchases && bid.compareTo(bidHighestNumPurchases) < 0)) {
			bidHighestNumPurchases = bid;
			highestNumPurchases = numPurchases;
		}

	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		context.write(new Text(bidHighestNumPurchases), new IntWritable(highestNumPurchases));
	}
}
