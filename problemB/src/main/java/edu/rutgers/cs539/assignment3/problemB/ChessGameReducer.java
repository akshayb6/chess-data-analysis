package edu.rutgers.cs539.assignment3.problemB;


import java.io.IOException;
import java.text.DecimalFormat;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ChessGameReducer extends Reducer<PlayerWritable, Text, Text, Text> {

    DecimalFormat formatter = new DecimalFormat("##.00");
    
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
		
		double percentWins = (double)wins / totalGames;
		double percentLoses = (double)loses / totalGames;
		double percentDraw = (double)draw / totalGames;
		
		String result = formatter.format(percentWins) + " " + formatter.format(percentLoses) + " " + formatter.format(percentDraw);
		context.write(new Text(key.toString()), new Text(result));
	}
}
