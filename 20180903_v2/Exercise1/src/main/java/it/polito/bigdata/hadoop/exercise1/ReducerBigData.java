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

		double lowestPrice = Double.MAX_VALUE;
		String lastTimestampLowestPrice = null;

		// Iterate over the set of values and compute the lowest price and the
		// last date it occurs
		for (Text value : values) {
			// Split value
			// date,hour:minute,price
			// Example: 2015/05/21,15:05,45.32
			String[] fields = value.toString().split(",");

			String date = fields[0];
			String hourAndMinute = fields[1];
			double price = Double.parseDouble(fields[2]);

			if (price <= lowestPrice) {
				// Update highestPrice
				lowestPrice = price;

				String currentTimestamp = new String(date + "," + hourAndMinute);

				// Check if also the date must be updated
				if (lastTimestampLowestPrice == null || (currentTimestamp.compareTo(lastTimestampLowestPrice) > 0))
					lastTimestampLowestPrice = currentTimestamp;

			}
		}

		// Emit the result
		context.write(new DoubleWritable(lowestPrice), new Text(lastTimestampLowestPrice));
	}
}
