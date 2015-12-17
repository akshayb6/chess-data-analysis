package edu.rutgers.cs539.assignment3.problemC;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Word count job runner.
 *
 */
public class Driver 
{
	public static final String OUTPUT_DIR = "OUTPUTDIR";

    private static NumberFormat nf = new DecimalFormat("00");
    
    public static void main( String[] args )
    {
    	runMoveCountFrequencyCalcJob(args[0], args[1] + nf.format(1));
    	runSortFrequencyJob(args[1] + nf.format(1), args[1] + nf.format(2));
    }
    
    public static void runMoveCountFrequencyCalcJob(String inputPath, String outputPath) {
		Configuration conf = new Configuration();
		Job job;
		try {
			conf.set(OUTPUT_DIR, outputPath);
			job = Job.getInstance(conf, "Calculate frequency of move counts");

			job.setJarByClass(Driver.class);
			job.setInputFormatClass(PGNInputFormat.class);
			job.setMapperClass(MoveCountFrequencyMapper.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(IntWritable.class);
			job.setCombinerClass(MoveCountFrequencyCombiner.class);
			job.setReducerClass(MoveCountFrequencyReducer.class);

			FileInputFormat.addInputPath(job, new Path(inputPath));
			FileOutputFormat.setOutputPath(job, new Path(outputPath));
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			
			job.waitForCompletion(true);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
    
    public static void runSortFrequencyJob(String inputPath, String outputPath) {
		Configuration conf = new Configuration();
		Job job;
		try {
			conf.set(OUTPUT_DIR, outputPath);
			job = Job.getInstance(conf, "Sort frequency");

			job.setJarByClass(Driver.class);
			job.setInputFormatClass(TextInputFormat.class);
			job.setMapperClass(SortFrequencyMapper.class);
			job.setMapOutputKeyClass(SortDataWritable.class);
			job.setMapOutputValueClass(IntWritable.class);
			job.setReducerClass(SortFrequencyReducer.class);
			job.setPartitionerClass(SortDataPartitioner.class);

			FileInputFormat.addInputPath(job, new Path(inputPath));
			FileOutputFormat.setOutputPath(job, new Path(outputPath));
			
			job.setOutputKeyClass(IntWritable.class);
			job.setOutputValueClass(IntWritable.class);
			
			job.waitForCompletion(true);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
    
    public static void runChessDataParser(String inputPath, String outputPath) {
		Configuration conf = new Configuration();
		Job job;
		try {
			conf.set(OUTPUT_DIR, outputPath);
			job = Job.getInstance(conf, "pgn parser C");

			job.setJarByClass(Driver.class);
			job.setInputFormatClass(PGNInputFormat.class);
			job.setMapperClass(ChessGameMapper.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(IntWritable.class);
			job.setCombinerClass(ChessGameCombiner.class);
			job.setReducerClass(ChessGameReducer.class);

			FileInputFormat.addInputPath(job, new Path(inputPath));
			FileOutputFormat.setOutputPath(job, new Path(outputPath));
			//MultipleOutputs.addNamedOutput(job, OUTPUT_DIR, TextOutputFormat.class, Text.class, IntWritable.class);
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
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
