package it.polito.bigdata.hadoop.exercise1;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
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
				DoubleWritable> {// Output value type

	protected void map(LongWritable key, // Input key type
			Text value, // Input value type
			Context context) throws IOException, InterruptedException {

		// Split data
		// stockId,date,hour:minute,price
		// Example: FCAU,2016/06/20,16:10,10.43
		String[] fields = value.toString().split(",");

		String stockId = fields[0];

		String date = fields[1];
		String year = date.split("/")[0];
		String month = date.split("/")[1];

		Double price = Double.parseDouble(fields[3]);

		if (year.compareTo("2016") == 0) {
			// emit the pair (stockid_month, price)
			context.write(new Text(stockId + "_" + month), 
					new DoubleWritable(price));

		}
	}
}
