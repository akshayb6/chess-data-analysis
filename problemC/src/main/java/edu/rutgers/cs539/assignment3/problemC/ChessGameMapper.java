package edu.rutgers.cs539.assignment3.problemC;


import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * We send <Plycount> <1> from mapper
 * .
 * @author ashish
 *
 */
public class ChessGameMapper extends Mapper<GameDataWritable,NullWritable, Text, IntWritable> {

	private static final IntWritable one = new IntWritable(1);
	public static final String TOTAL_COUNT = "TOTAL_COUNT";
	
	public void map(GameDataWritable key, NullWritable value, Context context) throws IOException, InterruptedException {

		context.write(new Text(Integer.toString(key.getNumberOfMoves())), one);
		context.write(new Text(TOTAL_COUNT), one);
	}
}
