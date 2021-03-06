package edu.rutgers.cs539.assignment3.problemA;


import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import chesspresso.Chess;

/**
 * We send 0/1 based encoding for all game results
 * for e.g is black wins then we send 1,1 to black and 0,1 to others, if its a draw then we send 1,1 to draw and o,1 to others.
 * where integer before comma signifies the number of games concerned to key and after value signifies the total games.
 * @author ashish
 *
 */
public class ChessGameMapper extends Mapper<GameDataWritable,NullWritable, IntWritable, Text> {

	public void map(GameDataWritable key, NullWritable value, Context context) throws IOException, InterruptedException {

		// Format = <Of Concern to key?> , <Game counter>
		if (key.getGameResult() == Chess.RES_BLACK_WINS) {
			context.write(new IntWritable(Chess.RES_BLACK_WINS), new Text(String.format("%d,%d", 1, 1)));
			context.write(new IntWritable(Chess.RES_WHITE_WINS), new Text(String.format("%d,%d", 0, 1)));
			context.write(new IntWritable(Chess.RES_DRAW), new Text(String.format("%d,%d", 0, 1)));
			context.write(new IntWritable(Chess.RES_NOT_FINISHED), new Text(String.format("%d,%d", 0, 1)));
		} else if (key.getGameResult() == Chess.RES_WHITE_WINS) {
			context.write(new IntWritable(Chess.RES_BLACK_WINS), new Text(String.format("%d,%d", 0, 1)));
			context.write(new IntWritable(Chess.RES_WHITE_WINS), new Text(String.format("%d,%d", 1, 1)));
			context.write(new IntWritable(Chess.RES_DRAW), new Text(String.format("%d,%d", 0, 1)));
			context.write(new IntWritable(Chess.RES_NOT_FINISHED), new Text(String.format("%d,%d", 0, 1)));
		} else if (key.getGameResult() == Chess.RES_DRAW) {
			context.write(new IntWritable(Chess.RES_BLACK_WINS), new Text(String.format("%d,%d", 0, 1)));
			context.write(new IntWritable(Chess.RES_WHITE_WINS), new Text(String.format("%d,%d", 0, 1)));
			context.write(new IntWritable(Chess.RES_DRAW), new Text(String.format("%d,%d", 1, 1)));
			context.write(new IntWritable(Chess.RES_NOT_FINISHED), new Text(String.format("%d,%d", 0, 1)));
		} else {
			context.write(new IntWritable(Chess.RES_BLACK_WINS), new Text(String.format("%d,%d", 0, 1)));
			context.write(new IntWritable(Chess.RES_WHITE_WINS), new Text(String.format("%d,%d", 0, 1)));
			context.write(new IntWritable(Chess.RES_DRAW), new Text(String.format("%d,%d", 0, 1)));
			context.write(new IntWritable(Chess.RES_NOT_FINISHED), new Text(String.format("%d,%d", 1, 1)));
		}
	}
}
