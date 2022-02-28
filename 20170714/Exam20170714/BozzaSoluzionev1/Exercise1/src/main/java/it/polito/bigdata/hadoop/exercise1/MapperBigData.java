package it.polito.bigdata.hadoop.exercise1;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Exercise 1 - Mapper
 */

class MapperBigData extends
		Mapper<LongWritable, // Input key type
				Text, // Input value type
				Text, // Output key type
				Text> {// Output value type

	protected void map(LongWritable key, // Input key type
			Text value, // Input value type
			Context context) throws IOException, InterruptedException {

		// Split data
		// date_reading,city,country,max_temperature,min_temperature
		// 2016/07/20,Turin,Italy,32.5,26.0
		String[] fields = value.toString().split(",");

		String city = fields[1];
		Double maxTemp = Double.parseDouble(fields[3]);
		Double minTemp = Double.parseDouble(fields[4]);

		if (maxTemp.doubleValue() > 35) {
			// emit the pair (city, "g") -> g = greater than 35
			context.write(new Text(city), new Text("g"));
		}

		if (minTemp.doubleValue() < -20) {
			// emit the pair (city, "l") -> l = less than -20
			context.write(new Text(city), new Text("l"));
		}

	}
}
