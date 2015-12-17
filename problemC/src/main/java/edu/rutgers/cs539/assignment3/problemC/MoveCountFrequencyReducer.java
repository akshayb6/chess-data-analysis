package edu.rutgers.cs539.assignment3.problemC;


import java.io.IOException;
import java.text.DecimalFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;

public class MoveCountFrequencyReducer extends Reducer<Text, IntWritable, Text, Text> {

	private long numGames = 0;
    DecimalFormat formatter = new DecimalFormat("##.00");
	
	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.Reducer#setup(org.apache.hadoop.mapreduce.Reducer.Context)
	 */
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		//super.setup(context);
		
		Configuration conf = context.getConfiguration();
		Cluster cluster = new Cluster(conf);
		Job currentJob = cluster.getJob(context.getJobID());

		//ystem.out.println("------------------------------------------------------------------------------------");
		//System.out.println("Counter = " + context.getCounter(GAMES_COUNTER.NUM_GAMES).getValue());
		numGames = currentJob.getCounters().findCounter(MoveCountFrequencyMapper.GAMES_COUNTER.NUM_GAMES).getValue();  
	}

	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
	
		int count = 0;
		while (values.iterator().hasNext()) {
			count += values.iterator().next().get();
		}
		
		Double result = ((double)count / numGames) * 100;
		context.write(key, new Text(formatter.format(result)));
	}
}
