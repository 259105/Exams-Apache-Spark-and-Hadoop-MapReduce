package it.polito.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Basic MapReduce Project - Mapper
 */
class MapperBigData extends Mapper<LongWritable, Text, Text, Text> {

	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String[] fields = value.toString().split(",");
		String modelID = fields[2];
		String eu = fields[6];
		String date = fields[3];
		String year = date.split("/")[0];

		if (year.equals("2020"))
			context.write(new Text(modelID), new Text(eu));
	}
}
