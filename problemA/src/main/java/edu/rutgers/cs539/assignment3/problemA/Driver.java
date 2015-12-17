package edu.rutgers.cs539.assignment3.problemA;

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
public class Driver 
{
    public static void main( String[] args )
    {
    	runChessDataParser(args[0], args[1]);
    }
    
    public static void runChessDataParser(String inputPath, String outputPath) {
		Configuration conf = new Configuration();
		Job job;
		try {
			job = Job.getInstance(conf, "pgn parser");

			job.setJarByClass(Driver.class);
			job.setInputFormatClass(PGNInputFormat.class);
			job.setMapperClass(ChessGameMapper.class);
			job.setMapOutputKeyClass(IntWritable.class);
			job.setMapOutputValueClass(Text.class);
			job.setCombinerClass(ChessGameCombiner.class);
			job.setReducerClass(ChessGameReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);

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
