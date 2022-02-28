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
		// SID13,PentiumV,DC10,Turin,Italy
		String[] fields = value.toString().split(",");

		String CPUversion = fields[1];
		String datacenter = fields[2];
		String country = fields[4];

		// Select only Italian cities
		if (country.compareTo("Spain") == 0) {
			// emit the pair (DataCenter,CPUVersion)
			context.write(new Text(datacenter), new Text(CPUversion));

		}
	}
}
