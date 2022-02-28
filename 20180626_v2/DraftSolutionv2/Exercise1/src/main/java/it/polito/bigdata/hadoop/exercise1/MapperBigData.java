package it.polito.bigdata.hadoop.exercise1;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
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
				IntWritable> {// Output value type

	protected void map(LongWritable key, // Input key type
			Text value, // Input value type
			Context context) throws IOException, InterruptedException {

		// Split data
		// SID,Timestamp,Precipitation_mm,WindSpeed
		// Timestamp = yyyy/mm/dd,hh:mm
		// Example: Sens2,2018/03/01,15:40,0.10,8.5
		String[] fields = value.toString().split(",");

		String sid = fields[0];
		String date = fields[1];
		String time = fields[2];
		Double windSpeed = Double.parseDouble(fields[4]);

		String[] yyyymmdd = date.split("/");
		String year = yyyymmdd[0];
		String month = yyyymmdd[1];
		int hour = Integer.parseInt(time.split(":")[0]);

		// Select only April 2018 from 8:00 to 11:59 and the lines with
		// wind speed < 1.0
		if (year.compareTo("2018") == 0 && month.compareTo("04") == 0 && hour >= 8 && hour <= 11 && windSpeed < 1.0) {
			// emit the pair (vsid, 1)
			context.write(new Text(sid), new IntWritable(1));

		}
	}
}
