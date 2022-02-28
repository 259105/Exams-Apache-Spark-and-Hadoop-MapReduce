package it.polito.bigdata.hadoop.exercise1;
import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reducer
 */
class ReducerBigData extends Reducer<
                Text,           // Input key type
                FloatWritable,  // Input value type
                Text,           // Output key type
                NullWritable> {  // Output value type
    
	
	
    @Override
    protected void reduce(
        Text key, // Input key type
        Iterable<FloatWritable> values, // Input value type
        Context context) throws IOException, InterruptedException {

    	// key = bookid
    	// values = list of prices of the purchases associated with the current book

    	// Count the gross revenue of the current book by summing the prices associated
    	// with the book
        // Iterate over the set of values and sum them
    	float sum=0;
    	
        for (FloatWritable value : values) {
        	sum=sum+value.get();
        }

        // Check if the current book is a best seller
        // If the book is a best seller, emit the pair (bookid,null)
        if (sum>=1000000)
        {
        	context.write(key, NullWritable.get());
        }
    }
}
