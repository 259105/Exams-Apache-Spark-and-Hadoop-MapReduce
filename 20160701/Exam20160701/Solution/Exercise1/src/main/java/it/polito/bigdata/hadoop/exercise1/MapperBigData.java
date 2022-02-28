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
                    FloatWritable> {// Output value type
	
    protected void map(
    		LongWritable  key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {
    	
    		String[] fields=value.toString().split(",");
    		
    		// fields[1] = bookid
    		// fields[3] = price
    		
       		// Emit a pair (bookid,price)
            context.write(new Text(fields[1]), new FloatWritable(Float.parseFloat(fields[3])));
    }
    
}
