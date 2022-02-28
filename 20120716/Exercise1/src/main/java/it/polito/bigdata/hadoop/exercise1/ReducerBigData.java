package it.polito.bigdata.hadoop.exercise1;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Exercise 1 - Reducer
 */
class ReducerBigData extends Reducer<Text, // Input key type
		IntWritable, // Input value type
		Text, // Output key type
		Text> { // Output value type

	@Override
	protected void reduce(Text key, // Input key type
			Iterable<IntWritable> values, // Input value type
			Context context) throws IOException, InterruptedException {

		int numPurchases = 0;

		// Iterate over the set of values and count the number of purchases for the current
		// combination CustomerID+BID
		for (IntWritable value: values) {
			numPurchases=numPurchases+value.get();
		}
		
		if (numPurchases>=2) {
			// Emit CustomerID\tBID
			String[] fields = key.toString().split("_");
			String customerID = fields[0];  
			String bid = fields[1];
			context.write(new Text(customerID), new Text(bid));
		}
	}
}
