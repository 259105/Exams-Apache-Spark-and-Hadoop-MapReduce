package it.polito.bigdata.hadoop;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * Basic MapReduce Project - Mapper
 */
class MapperBigData extends Mapper<
                    LongWritable, // Input key type
                    Text,         // Input value type
                    NullWritable,         // Output key type
                    Text> {// Output value type

    private HashMap<String, Long> counters;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
    	// The number of distinct years is "small" ~20 years
    	// The local number of occurrences of each year can be stored inside Java variable   
        this.counters = new HashMap<>();
    }

    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

            String[] fields = value.toString().split(",");
            String year = fields[0].split("/")[0];

            // Update the number of local occurrences of the current year
            counters.put(year, counters.getOrDefault(year, (long) 0) + 1);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
    	// Emit for each year the number of its local occurrences  
    	// key: NullWritbale
    	// value: year + number of local occurrences 
        for(Entry<String, Long> entry : counters.entrySet()) {
            context.write(NullWritable.get(), new Text(entry.getKey() + "_" + entry.getValue()));
        }
    }
}
