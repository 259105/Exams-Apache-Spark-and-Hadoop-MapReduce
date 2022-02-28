package it.polito.bigdata.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
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

		Path inputPath;
		Path outputDir;
		int numberOfReducers;
		int exitCode;

		// Parse the parameters
		// Number of instances of the reducer class
		// Any number >=1. The reduce step can be parallelized
		numberOfReducers = 3;
		// Folder containing the input data
		inputPath = new Path(args[0]);
		// Output folder
		outputDir = new Path(args[1]);

		Configuration conf = this.getConf();

		Job job = Job.getInstance(conf);
		job.setJobName("Exam20210630 - MapReduce - Hadoop");

		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputDir);

		job.setJarByClass(DriverBigData.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapperClass(MapperBigData.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(ReducerBigData.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		job.setNumReduceTasks(numberOfReducers);

		if (job.waitForCompletion(true) == true)
			exitCode = 0;
		else
			exitCode = 1;

		return exitCode;
	}

	/**
	 * Main of the driver
	 */

	public static void main(String args[]) throws Exception {
		// Exploit the ToolRunner class to "configure" and run the Hadoop application
		int res = ToolRunner.run(new Configuration(), new DriverBigData(), args);

		System.exit(res);
	}
}