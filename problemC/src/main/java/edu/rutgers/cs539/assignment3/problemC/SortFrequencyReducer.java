package edu.rutgers.cs539.assignment3.problemC;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class SortFrequencyReducer extends Reducer<SortDataWritable, IntWritable, IntWritable, IntWritable> {

	public void reduce(SortDataWritable key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		
		while (values.iterator().hasNext()) {
			context.write(values.iterator().next(), new IntWritable(key.getFrequency()));
		}
	}
}
