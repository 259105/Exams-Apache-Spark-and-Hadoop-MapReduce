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
		// Timestamp,VSID,CPUUtilization%,RAMUtilization%
		// Timestamp format:yyyy/mm/dd_hh:mm
		// Example: 2018/05/01,15:40,VS1,10.5,0.5
		String[] fields = value.toString().split(",");

		String date = fields[0];
		String time=fields[1];
		String vsid = fields[2];
		Double cpuUtil = Double.parseDouble(fields[3]);

		String[] yyyymmdd = date.split("/");
		String year = yyyymmdd[0];
		String month = yyyymmdd[1];
		int hour = Integer.parseInt(time.split(":")[0]);

		// Select only May 2018 from 9:00 to 17:59 and the lines with
		// CPUUtilization%>99.8
		if (year.compareTo("2018") == 0 && month.compareTo("05") == 0 && hour >= 9 && hour <= 17 && cpuUtil > 99.8) {
			// emit the pair (vsid, 1)
			context.write(new Text(vsid), new IntWritable(1));

		}
	}
}
