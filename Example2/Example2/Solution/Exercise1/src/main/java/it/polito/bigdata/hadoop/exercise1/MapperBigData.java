package it.polito.bigdata.hadoop.exercise1;


import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper
 */

class MapperBigData extends Mapper<
					LongWritable , // Input key type
                    Text,         // Input value type
                    Text,         // Output key type
                    NullWritable> {// Output value type
	
    protected void map(
    		LongWritable  key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {
    	
    		String[] fields=value.toString().split(",");
    		
    		// fields[2] = PM10 value
    		double PM10value=Double.parseDouble(fields[2]);
    		
    		// Select the line if the value is greater than 45 or less than 0. 
    		if (PM10value>45 || PM10value<0) {
        		// Emit a pair (record,null)
            	context.write(new Text(value), NullWritable.get());
    		}
    }
    
}
