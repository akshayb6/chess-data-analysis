package edu.rutgers.cs539.assignment3.problemA;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ChessGameCombiner extends Reducer<IntWritable, Text, IntWritable, Text> {

	public void reduce(IntWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		int gamesOfConcern = 0;
		int totalGames = 0;
		while (values.iterator().hasNext()) {
			String[] tmp = values.iterator().next().toString().split(",");
			gamesOfConcern += Integer.parseInt(tmp[0]);
			totalGames += Integer.parseInt(tmp[1]);
		}
		
		context.write(key, new Text(String.format("%d,%d", gamesOfConcern, totalGames)));
	}
}
