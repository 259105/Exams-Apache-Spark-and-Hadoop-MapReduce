package it.polito.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Basic MapReduce Project - Reducer
 */
class ReducerBigData extends Reducer<Text, Text, Text, NullWritable> {

	@Override
	protected void reduce(Text key, // Input key type
			Iterable<Text> values, // Input value type
			Context context) throws IOException, InterruptedException {

		int countEU = 0;
		int countExtraEU = 0;

		for (Text value : values) {
			String isEu = value.toString();
			if (isEu.equals("T"))
				countEU += 1;
			else
				countExtraEU += 1;
		}

		if (countEU >= 10000 && countExtraEU >= 10000) // set to 100 for testing
			context.write(key, NullWritable.get());
	}
}
