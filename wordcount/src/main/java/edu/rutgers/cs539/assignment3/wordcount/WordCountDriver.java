package edu.rutgers.cs539.assignment3.wordcount;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Word count job runner.
 *
 */
public class WordCountDriver 
{
    public static void main( String[] args )
    {
    	runWordCountJob(args[0], args[1]);
    }
    
    public static void runWordCountJob(String inputPath, String outputPath) {
		Configuration conf = new Configuration();
		Job job;
		try {
			job = Job.getInstance(conf, "word count");

			job.setJarByClass(WordCountDriver.class);
			job.setMapperClass(WordCountMapper.class);
			job.setCombinerClass(WordCountReducer.class); // Combiner is same as reducer here.
			job.setReducerClass(WordCountReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);

			FileInputFormat.addInputPath(job, new Path(inputPath));
			FileOutputFormat.setOutputPath(job, new Path(outputPath));
			job.waitForCompletion(true);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
