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
public class MoveCountFrequencyMapper extends Mapper<GameDataWritable,NullWritable, Text, IntWritable> {

	private static final IntWritable one = new IntWritable(1);
	
	public void map(GameDataWritable key, NullWritable value, Context context) throws IOException, InterruptedException {

		context.write(new Text(Integer.toString(key.getNumberOfMoves())), one);
	}
}
