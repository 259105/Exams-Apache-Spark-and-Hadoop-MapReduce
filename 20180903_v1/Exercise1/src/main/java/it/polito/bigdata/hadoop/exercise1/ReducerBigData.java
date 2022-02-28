package it.polito.bigdata.hadoop.exercise1;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Exercise 1 - Reducer
 */
class ReducerBigData extends
		Reducer<NullWritable, // Input key type
				Text, // Input value type
				DoubleWritable, // Output key type
				Text> { // Output value type

	
	@Override
	protected void reduce(NullWritable key, // Input key type
			Iterable<Text> values, // Input value type
			Context context) throws IOException, InterruptedException {

		double highestPrice = Double.MIN_VALUE;
		String firstTimestampHighestPrice = null;

		// Iterate over the set of values and compute the global 
		// maximum price and the first date it occurs
		for (Text value : values) {
			// Split value
			// localmaximumprice,timestamp
			String[] fields = value.toString().split(",");

			double price = Double.parseDouble(fields[0]);
			String timestamp = fields[1];

			if (price >= highestPrice) {
				// Update highestPrice
				highestPrice = price;

				String currentTimestamp = timestamp;

				// Check if also the date must be updated
				if (firstTimestampHighestPrice == null || 
					(currentTimestamp.compareTo(firstTimestampHighestPrice) < 0))
					firstTimestampHighestPrice = currentTimestamp;

			}
		}

		// Emit the result
		context.write(new DoubleWritable(highestPrice), new Text(firstTimestampHighestPrice));
	}
}
