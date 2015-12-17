package edu.rutgers.cs539.assignment3.problemA;


import java.io.IOException;
import java.text.DecimalFormat;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ChessGameReducer extends Reducer<IntWritable, Text, Text, Text> {

    DecimalFormat formatter = new DecimalFormat("##.00");
    
	public void reduce(IntWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		int gamesOfConcern = 0;
		int totalGames = 0;
		while (values.iterator().hasNext()) {
			String[] tmp = values.iterator().next().toString().split(",");
			gamesOfConcern += Integer.parseInt(tmp[0]);
			totalGames += Integer.parseInt(tmp[1]);
		}
		
		double percentGamesOfConcern = (double)gamesOfConcern / totalGames;
		String result = gamesOfConcern + " " + formatter.format(percentGamesOfConcern);
		context.write(new Text(GameUtils.gameResultToString(key.get())), new Text(result));
	}

}
