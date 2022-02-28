package it.polito.bigdata.hadoop.exercise1;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Exercise 1 - Mapper
 */
class MapperBigData extends Mapper<LongWritable, // Input key type
		Text, // Input value type
		Text, // Output key type
		IntWritable> {// Output value type

	protected void map(LongWritable key, // Input key type
			Text value, // Input value type
			Context context) throws IOException, InterruptedException {

		// Split record
		// POI_ID,latitude,longitude,city,country,category,subcategory
		// Example: P101,45.0621644,7.578633,Turin,Italy,shop,shoes
		String[] fields = value.toString().split(",");

		String city = fields[3];
		String country = fields[4];
		String category = fields[5];
		String subcategory = fields[6];

		// Select only POIs of the Italian cities
		// and only "tourism" POIs
		if (country.equals("Italy") == true && category.equals("tourism")) {
			// Check the subcategory
			if (subcategory.contentEquals("museum")) {
				// emit pair (city, 1)
				// -- One new tourism POI
				// -- One new museum POI
				context.write(new Text(city), new IntWritable(1));

			} else {
				// emit pair (city, 0))
				// -- One new tourism POI
				// -- This POI is not a museum POI
				context.write(new Text(city), new IntWritable(0));

			}
		}
	}
}
