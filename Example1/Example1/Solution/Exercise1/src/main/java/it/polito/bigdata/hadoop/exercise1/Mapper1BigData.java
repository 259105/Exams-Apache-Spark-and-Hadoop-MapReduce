package it.polito.bigdata.hadoop.exercise1;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper
 */
class Mapper1BigData extends Mapper<
					LongWritable , // Input key type
                    Text,         // Input value type
                    Text,         // Output key type
                    IntWritable> {// Output value type
	
    protected void map(
    		LongWritable  key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {
    	
    		String[] fields=value.toString().split(",");
    		
    		// fields[1] = movieid
    		// fields[2] = startTime
    		// fields[3] = endTime
    		
    		// Consider this line if and only if the duration is > 10
    		if (BigDataTime.computeDuration(fields[2], fields[3])>10) {
        		// Emit a pair (movieid,1)
            	context.write(new Text(fields[1]), new IntWritable(1));
    		}
    }
    
}
