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
                    IntWritable> {// Output value type
	
    protected void map(
    		LongWritable  key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {
    	
    		String[] fields=value.toString().split(",");
    
    		
    		// station1,Politecnico,ZoneA,Turin,30
    		
    		
    		// fields[2] = zone
    		// fields[3] = city
    		// fields[4] = totalNumberOfSlots
    		
    		String zone=fields[2];
    		String city=fields[3];
    		int totalNumberOfSlots=Integer.parseInt(fields[4]);
    		
       		// Emit a pair (zone,1) if the station is located in the city of Turin and 
    		// it is a large station
    		if (city.compareTo("Turin")==0 && totalNumberOfSlots>=20)
    		{
    			context.write(new Text(zone), new IntWritable(new Integer(1)));
    		}
    }
    
}
