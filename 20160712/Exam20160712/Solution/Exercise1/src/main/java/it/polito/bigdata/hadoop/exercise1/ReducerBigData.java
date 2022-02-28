package it.polito.bigdata.hadoop.exercise1;
import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reducer
 */
class ReducerBigData extends Reducer<
                Text,           // Input key type
                IntWritable,  // Input value type
                Text,           // Output key type
                IntWritable> {  // Output value type
    
	
	
    @Override
    protected void reduce(
        Text key, // Input key type
        Iterable<IntWritable> values, // Input value type
        Context context) throws IOException, InterruptedException {

    	// key = zone
    	// values = list of ones

    	int sum=0;
    	
        for (IntWritable value : values) {
        	sum=sum+value.get();
        }

        context.write(key, new IntWritable(new Integer(sum)));
    }
}
