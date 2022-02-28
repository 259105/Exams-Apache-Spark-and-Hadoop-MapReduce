package it.polito.bigdata.hadoop.exercise1;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Exercise 1 - Reducer
 * 
 */
class ReducerBigData extends
		Reducer<Text, // Input key type
				DoubleWritable, // Input value type
				Text, // Output key type
				Text> { // Output value type

	@Override
	protected void reduce(Text key, // Input key type
			Iterable<DoubleWritable> values, // Input value type
			Context context) throws IOException, InterruptedException {

		double highestPrice = Double.MIN_VALUE;
		double lowestPrice = Double.MAX_VALUE;

		// Iterate over the set of values and compute max and min prices
		for (DoubleWritable price : values) {
			if (price.get() > highestPrice) {
				highestPrice = price.get();
			}

			if (price.get() < lowestPrice) {
				lowestPrice = price.get();
			}
		}

		double absDiff = highestPrice - lowestPrice;
		double monthlyPercPriceVariation = 
				100.0 * (highestPrice - lowestPrice) / lowestPrice;

		if (monthlyPercPriceVariation > 5) {
			context.write(key, 
			new Text(absDiff + "," + monthlyPercPriceVariation));
		}
	}
}
