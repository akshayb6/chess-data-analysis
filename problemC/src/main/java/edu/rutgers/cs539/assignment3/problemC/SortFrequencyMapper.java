package edu.rutgers.cs539.assignment3.problemC;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SortFrequencyMapper extends Mapper<LongWritable, Text, SortDataWritable, IntWritable> {

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] vals = value.toString().split("\\s+");
		int moveCount = Integer.parseInt(vals[0]);
		int frequency = Integer.parseInt(vals[1]);
		
		context.write(new SortDataWritable(frequency), new IntWritable(moveCount));
	}

}
