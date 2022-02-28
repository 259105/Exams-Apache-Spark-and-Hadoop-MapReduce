package it.polito.bigdata.hadoop.exercise1;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reducer
 */
class Reducer1BigData extends Reducer<
                Text,           // Input key type
                IntWritable,  // Input value type
                Text,           // Output key type
                IntWritable> {  // Output value type
    
	
	
    @Override
    protected void reduce(
        Text key, // Input key type
        Iterable<IntWritable> values, // Input value type
        Context context) throws IOException, InterruptedException {

    	// key = movieid
    	// values = list of ones associated with the movieid
    	// if a combiner is used values can contain also values greater than 1

    	// Count the total number of occurrences of the current movie
        // Iterate over the set of values and sum them
    	int sum=0;
    	
        for (IntWritable value : values) {
        	sum=sum+value.get();
        }

        // Emit pair (movieid, total number of occurrences)
        context.write(key, new IntWritable(sum));
    }
}
