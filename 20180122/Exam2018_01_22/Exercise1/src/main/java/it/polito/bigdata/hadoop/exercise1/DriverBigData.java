package it.polito.bigdata.hadoop.exercise1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * MapReduce program
 */
public class DriverBigData extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {

		// Parse the parameters
		Path inputPath;
		Path outputDir;
		Path outputDir2;
		int numberOfReducers;
		int exitCode;

		// Parse the parameters
		numberOfReducers = Integer.parseInt(args[0]);
		inputPath = new Path(args[1]);
		outputDir = new Path(args[2]);
		outputDir2 = new Path(args[3]);

		Configuration conf = this.getConf();

		// Define a new job
		Job job = Job.getInstance(conf);

		// Assign a name to the job
		job.setJobName("Exercise #1 - Exam 2018/01/22");

		// Set path of the input file/folder (if it is a folder, the job reads
		// all the files in the specified folder) for this job
		FileInputFormat.addInputPath(job, inputPath);

		// Set path of the output folder for this job
		FileOutputFormat.setOutputPath(job, outputDir);

		// Specify the class of the Driver for this job
		job.setJarByClass(DriverBigData.class);

		// Set job input format
		job.setInputFormatClass(TextInputFormat.class);

		// Set job output format
		job.setOutputFormatClass(TextOutputFormat.class);

		// Set map class
		job.setMapperClass(MapperBigData.class);

		// Set map output key and value classes
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		// Set reduce class
		job.setReducerClass(ReducerBigData.class);

		// Set reduce output key and value classes
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// Set number of reducers
		job.setNumReduceTasks(numberOfReducers);

		// Execute the job and wait for completion
		if (job.waitForCompletion(true) == true) {
			// Execute the second job to filter duplicates
			Configuration conf2 = this.getConf();

			// Define a new job
			Job job2 = Job.getInstance(conf2);

			// Assign a name to the job
			job2.setJobName("Job #2");

			// Set path of the input file/folder
			FileInputFormat.addInputPath(job2, outputDir);

			// Set path of the output folder for this job
			FileOutputFormat.setOutputPath(job2, outputDir2);

			// Specify the class of the Driver for this job
			job2.setJarByClass(DriverBigData.class);

			// Set job2 input format
			job2.setInputFormatClass(TextInputFormat.class);

			// Set job2 output format
			job2.setOutputFormatClass(TextOutputFormat.class);

			// Set map class
			job2.setMapperClass(MapperBigData2.class);

			// Set map output key and value classes
			job2.setMapOutputKeyClass(Text.class);
			job2.setMapOutputValueClass(IntWritable.class);

			// Set reduce class
			job2.setReducerClass(ReducerBigData2.class);

			// Set reduce output key and value classes
			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(IntWritable.class);

			// Set number of reducers
			// There is one single reducer top compute the most purchased item
			job2.setNumReduceTasks(1);

			// Execute the job and wait for completion
			if (job2.waitForCompletion(true) == true)
				exitCode = 0;
			else
				exitCode = 1;
		} else
			exitCode = 1;

		return exitCode;
	}

	/**
	 * Main of the driver
	 */

	public static void main(String args[]) throws Exception {
		// Exploit the ToolRunner class to "configure" and run the Hadoop
		// application
		int res = ToolRunner.run(new Configuration(), new DriverBigData(), args);

		System.exit(res);
	}
}