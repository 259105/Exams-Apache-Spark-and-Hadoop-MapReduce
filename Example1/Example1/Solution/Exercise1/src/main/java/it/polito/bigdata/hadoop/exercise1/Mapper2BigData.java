package it.polito.bigdata.hadoop.exercise1;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper
 */
class Mapper2BigData extends Mapper<
                    Text, // Input key type
                    Text,         // Input value type
                    NullWritable, // Output key type
                    Text> {// Output value type
	
	// Store the record (movied, num occurrences) of the most "frequent" movie
	MovieOccurrences top1;
	
    protected void setup(Context context)
    {
    	top1=null;
    }
	
    protected void map(
    		Text key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

    		// key = movieid
    		// value = num occurrencies of movieid 
    	
    		int occurrences=Integer.parseInt(value.toString());

    		// Check if the occurrences of this movie is greater than the current top
    		if (top1==null || top1.occurrences<occurrences)
    		{
    			top1=new MovieOccurrences();
    			top1.movieid=new String(key.toString());
    			top1.occurrences=occurrences;
    		}
    }
    
    protected void cleanup(Context context) throws IOException, InterruptedException
    {
    		// Emit the key associated with the top movie 
        	context.write(NullWritable.get(), new Text(top1.movieid+"_"+top1.occurrences));
    }
    
}
