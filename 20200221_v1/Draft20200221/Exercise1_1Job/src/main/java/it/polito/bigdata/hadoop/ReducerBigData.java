package it.polito.bigdata.hadoop;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Basic MapReduce Project - Reducer
 */
class ReducerBigData extends Reducer<
                NullWritable,           // Input key type
                Text,    // Input value type
                Text,           // Output key type
                LongWritable> {  // Output value type


    @Override
    protected void reduce(
        NullWritable key, // Input key type
        Iterable<Text> values, // Input value type
        Context context) throws IOException, InterruptedException {

        HashMap<String, Long> counters =new HashMap<>();;
    	
        // Compute the global number of occurrences of each year
        for(Text val : values) {
            String[] fields = val.toString().split("_");
            String year = fields[0];
            Long count = Long.parseLong(fields[1]);

            // Update the number of occurrences of the current year
            counters.put(year, counters.getOrDefault(year, (long) 0) + count);
        }
        
        
        long maxCount = -1;
        String maxYear = "";

        // Compute the maximum number of occurrences and the first year associated with that maximum value
        for(Entry<String, Long> entry : counters.entrySet()) {
            if(maxCount < entry.getValue() || (maxCount == entry.getValue() && entry.getKey().compareTo(maxYear) < 0)) {
                maxCount = entry.getValue();
                maxYear = entry.getKey();
            }
        }

        // Store the result
        context.write(new Text(maxYear), new LongWritable(maxCount));
    }
}
