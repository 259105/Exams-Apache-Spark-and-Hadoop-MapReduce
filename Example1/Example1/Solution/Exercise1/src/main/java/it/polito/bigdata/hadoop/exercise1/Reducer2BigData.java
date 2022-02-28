package it.polito.bigdata.hadoop.exercise1;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reducer
 */
class Reducer2BigData extends Reducer<
                NullWritable,           // Input key type
                Text,  // Input value type
                NullWritable,           // Output key type
                Text> {  // Output value type
    
	
	
	// The reduce method is called only once in this approach
	// All the key-value pairs emitted by the mappers as the 
	// same key (NullWritable.get())
    @Override
    protected void reduce(
        NullWritable key, // Input key type
        Iterable<Text> values, // Input value type
        Context context) throws IOException, InterruptedException {

    	String movieid;
    	int occurrences;
		
    	MovieOccurrences top1;
    	
    	top1=null;
    	
        // Iterate over the set of values and select the top 1
        for (Text value : values) {

        	String[] record=value.toString().split("_"); 
    		
        	
    		// record[0] = movieid
    		// record[1] = num occurrencies of movieid 

    		movieid=record[0];
    		occurrences=Integer.parseInt(record[1]);

    		// Check if the occurrences of this movie is greater than the current top
    		if (top1==null || top1.occurrences<occurrences)
    		{
    			top1=new MovieOccurrences();
    			top1.movieid=new String(movieid);
    			top1.occurrences=occurrences;
    		}
        }

		// Emit the key associated with the top movie 
    	context.write(NullWritable.get(), new Text(top1.movieid));
    }
}
