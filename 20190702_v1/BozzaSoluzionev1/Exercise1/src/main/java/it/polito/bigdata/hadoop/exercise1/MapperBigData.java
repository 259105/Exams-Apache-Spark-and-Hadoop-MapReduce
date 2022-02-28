package it.polito.bigdata.hadoop.exercise1;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Exercise 1 - Mapper
 */
class MapperBigData extends Mapper<LongWritable, // Input key type
		Text, // Input value type
		Text, // Output key type
		Text> {// Output value type

	protected void map(LongWritable key, // Input key type
			Text value, // Input value type
			Context context) throws IOException, InterruptedException {

		// Split data
		// BID,BicycleManufacturer,City,Country
		String[] fields = value.toString().split(",");

		String manufacturer = fields[1];
		String city = fields[2];
		String country = fields[3];

		// Select only Italian cities
		if (country.compareTo("Italy") == 0) {
			// emit the pair (City,BicycleManufacturer)
			context.write(new Text(city), new Text(manufacturer));

		}
	}
}
