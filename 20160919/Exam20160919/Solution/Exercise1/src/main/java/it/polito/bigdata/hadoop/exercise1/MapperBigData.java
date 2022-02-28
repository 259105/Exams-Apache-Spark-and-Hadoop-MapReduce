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
    
    		
    		// monitoringStationId,date,hour,minute,PM10value,PM2.5value
    		// station1,2016/03/07,17,15,29.2,15.1
   		
    		// fields[0] = monitoringStationId
    		// fields[1] = date
    		// fields[2] = hour
    		// fields[3] = minute
    		// fields[4] = PM10value
    		// fields[5] = PM2.5value
    		
    		String stationid=fields[0];
    		String year=fields[1].substring(0,4);
    		Double PM10=Double.valueOf(fields[4]);
    		Double PM25=Double.valueOf(fields[5]);

    		// Emit a pair (stationid,1) if the reading satisfies the 
    		// constraints year=2013 and PM10>PM2.5 
    		if (year.equals("2013")==true && PM10>PM25)
    		{
    			context.write(new Text(stationid), new IntWritable(new Integer(1)));
    		}
    }
    
}
