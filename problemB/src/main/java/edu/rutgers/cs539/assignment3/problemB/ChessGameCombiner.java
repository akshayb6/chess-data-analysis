package edu.rutgers.cs539.assignment3.problemB;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ChessGameCombiner extends Reducer<PlayerWritable, Text, PlayerWritable, Text> {

	public void reduce(PlayerWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		int wins = 0;
		int loses = 0;
		int draw = 0;
		int unknown = 0;
		int totalGames = 0;
		while (values.iterator().hasNext()) {
			String[] tmp = values.iterator().next().toString().split(",");
			wins += Integer.parseInt(tmp[0]);
			loses += Integer.parseInt(tmp[1]);
			draw += Integer.parseInt(tmp[2]);
			unknown += Integer.parseInt(tmp[3]);
			totalGames += Integer.parseInt(tmp[4]);
		}
		
		context.write(key, new Text(String.format("%d,%d,%d,%d,%d", wins, loses, draw, unknown, totalGames)));
	}
}
