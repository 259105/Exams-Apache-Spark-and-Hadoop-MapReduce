package it.polito.bigdata.hadoop.exercise1;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper #2
 */
class MapperBigData2 extends
		Mapper<LongWritable, // Input key type
				Text, // Input value type
				Text, // Output key type
				IntWritable> {// Output value type

	protected void map(LongWritable key, // Input key type
			Text value, // Input value type
			Context context) throws IOException, InterruptedException {

		// Text(bidHighestNumPurchases), new IntWritable(highestNumPurchases));
		String[] fields = value.toString().split("\\t");

		String bid = fields[0];
		int numPurchases = Integer.parseInt(fields[1]);

		context.write(new Text(bid), new IntWritable(numPurchases));
	}

}
