package edu.rutgers.cs539.assignment3.problemC;


import java.io.IOException;
import java.text.DecimalFormat;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class ChessGameReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	public static final String OUTPUT_TOTAL = "outputTotal";
	private MultipleOutputs<Text, IntWritable> mos;
    DecimalFormat formatter = new DecimalFormat("##.00");
    
	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.Reducer#setup(org.apache.hadoop.mapreduce.Reducer.Context)
	 */
	@Override
	protected void setup(Reducer<Text, IntWritable, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		super.setup(context);
		
		mos = new MultipleOutputs<>(context);
	}

	public static String getOutputFilePath(String reducerOutputDir) {
		Path path = new Path(reducerOutputDir);
		path = path.getParent();

		System.out.println(path.toString());
		System.out.println(path.toUri());
		
		return path.toString() + "/" + OUTPUT_TOTAL;
	}
	
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		
		int count = 0;
		while (values.iterator().hasNext()) {
			count += values.iterator().next().get();
		}
		
		if (key.toString().compareTo(ChessGameMapper.TOTAL_COUNT) == 0) {
			String outputPath = context.getConfiguration().get(Driver.OUTPUT_DIR);
			System.out.println("Output dir : " + outputPath);
			mos.write(key, new IntWritable(count), getOutputFilePath(outputPath));
		} else {

			context.write(key, new IntWritable(count));
		}
		
		/*
		Configuration conf = context.getConfiguration();
        Cluster cluster = new Cluster(conf);
        Job currentJob = cluster.getJob(context.getJobID());
		System.out.println("Job ID = " + context.getJobID());
        
		long totalGames = context.getCounter(TaskCounter.MAP_OUTPUT_RECORDS).getValue();
		System.out.println("Numeber of games = " + totalGames);
		totalGames = context.getCounter(TaskCounter.class.getName(), TaskCounter.MAP_OUTPUT_RECORDS.toString()).getValue();
		System.out.println("Class: " + TaskCounter.class.getName() + " Counter : " + TaskCounter.MAP_OUTPUT_RECORDS.toString());
		System.out.println("Numeber of games = " + totalGames);
		
		totalGames = context.getCounter(TaskCounter.MAP_OUTPUT_RECORDS).getValue();
		System.out.println("Numeber of games = " + totalGames);
		totalGames = context.getCounter(TaskCounter.class.getName(), TaskCounter.MAP_OUTPUT_RECORDS.toString()).getValue();

		System.out.println("Numeber of games = " + context.getConfiguration().getInt("abc", 1));
		
		double percent = (double) count / totalGames;
		String result = formatter.format(percent) + "%";
		mos.wr
		context.write(key, new Text(result));*/
	}
}
