package edu.rutgers.cs539.assignment3.problemC;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SortFrequencyMapper extends Mapper<LongWritable, Text, SortDataWritable, IntWritable> {

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] vals = value.toString().split("\\s+");
		
		context.write(new SortDataWritable(vals[1]), new IntWritable(Integer.parseInt(vals[0])));
	}

}
