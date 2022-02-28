package it.polito.bigdata.hadoop.exercise1;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Exercise 1 - Mapper
 */
class MapperBigData extends Mapper<LongWritable, // Input key type
		Text, // Input value type
		NullWritable, // Output key type
		Text> {// Output value type

	double highestPrice = Double.MIN_VALUE;
	String firstTimestampHighestPrice = null;
	

	protected void map(LongWritable key, // Input key type
			Text value, // Input value type
			Context context) throws IOException, InterruptedException {

		// Split value
		// stockId,date,hour:minute,price
		// Example: GOOG,2015/05/21,15:05,45.32
		String[] fields = value.toString().split(",");

		String stockId = fields[0];
		String date = fields[1];
		String hourAndMinute = fields[2];
		double price = Double.parseDouble(fields[3]);

		// Select only stockId GOOG and year 2017
		if (date.startsWith("2017") == true && stockId.equals("GOOG")) {
			
			if (price >= highestPrice) {
				// Update highestPrice
				highestPrice = price;

				String currentTimestamp = new String(date + "_" + hourAndMinute);

				// Check if also the date must be updated
				if (firstTimestampHighestPrice == null || 
					(currentTimestamp.compareTo(firstTimestampHighestPrice) < 0))
					firstTimestampHighestPrice = currentTimestamp;
			}
		}
	}
	
	protected void cleanup(Context context) throws IOException, InterruptedException {
		// emit the local top 1 price and the associated date
		if (firstTimestampHighestPrice!=null)
			context.write(NullWritable.get(), new Text (highestPrice+","+firstTimestampHighestPrice));
	}

}
