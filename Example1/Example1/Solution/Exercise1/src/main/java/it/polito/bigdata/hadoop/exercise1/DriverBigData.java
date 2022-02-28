package it.polito.bigdata.hadoop.exercise1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Driver class.
 */
public class DriverBigData extends Configured implements Tool {

  @Override
  public int run(String[] args) throws Exception {

    Path inputPath;
    Path outputDir;
    int numberOfReducers;
	int exitCode;  
	
	// Parse the parameters
    numberOfReducers = Integer.parseInt(args[0]);
    inputPath = new Path(args[1]);
    outputDir = new Path(args[2]);

    Configuration conf = this.getConf();
    
    // The fist job is a word count job
    // It counts the number of occurrences of each movie in watchedmovies.txt 

    // Define a new job
    Job job = Job.getInstance(conf); 

    // Assign a name to the job
    job.setJobName("Exam 1 - Exercise 1 - count");
    
    // Set path of the input file/folder (if it is a folder, the job reads all the files in the specified folder) for this job
    FileInputFormat.addInputPath(job, inputPath);
    
    // Set path of the output folder for this job
    // A temporary folder
    FileOutputFormat.setOutputPath(job, new Path("temp/"));
    
    // Specify the class of the Driver for this job
    job.setJarByClass(DriverBigData.class);
    
    // Set input format
    job.setInputFormatClass(TextInputFormat.class);
    
    // Set job output format
    job.setOutputFormatClass(TextOutputFormat.class);
    
    // Set map class
    job.setMapperClass(Mapper1BigData.class);
    
    // Set map output key and value classes
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    
    // Set reduce class
    job.setReducerClass(Reducer1BigData.class);
        
    // Set reduce output key and value classes
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    // Set number of reducers
    job.setNumReduceTasks(numberOfReducers);
    
    // Execute the job and wait for completion
    if (job.waitForCompletion(true)==true)
    {
        // The second job select the top 1 movie in terms of number of occurrences 
    	// the movies in watchedmovies.txt 

        // Define a new job
        Job job2 = Job.getInstance(conf); 

        // Assign a name to the job
        job2.setJobName("Exam 1 - Exercise 1 - top 1");
        
        // Set path of the input file/folder (if it is a folder, the job reads all the files in the specified folder) for this job
        FileInputFormat.addInputPath(job2, new Path("temp/"));
        
        // Set path of the output folder for this job
        // A temporary folder
        FileOutputFormat.setOutputPath(job2, outputDir);
        
        // Specify the class of the Driver for this job
        job2.setJarByClass(DriverBigData.class);
        
        // Set input format
        job2.setInputFormatClass(KeyValueTextInputFormat.class);
        
        // Set job output format
        job2.setOutputFormatClass(TextOutputFormat.class);
        
        // Set map class
        job2.setMapperClass(Mapper2BigData.class);
        
        // Set map output key and value classes
        job2.setMapOutputKeyClass(NullWritable.class);
        job2.setMapOutputValueClass(Text.class);
        
        // Set reduce class
        job2.setReducerClass(Reducer2BigData.class);
            
        // Set reduce output key and value classes
        job2.setOutputKeyClass(NullWritable.class);
        job2.setOutputValueClass(Text.class);

        // Set number of reducers
        // The top-k pattern is characterized by num. reducers = 1 
        job2.setNumReduceTasks(1);
        
        // Execute the job and wait for completion
        if (job2.waitForCompletion(true)==true)
        	exitCode=0;
        else
        	exitCode=1;
    }
    else
    	exitCode=1;
    	
    return exitCode;
  }

  /** Main of the driver
   */
  
  public static void main(String args[]) throws Exception {
	// Exploit the ToolRunner class to "configure" and run the Hadoop application
    int res = ToolRunner.run(new Configuration(), new DriverBigData(), args);

    System.exit(res);
  }
}
