/**
 * 
 */
package edu.rutgers.cs539.assignment3.problemC;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author ashish
 *
 */
public class SortDataPartitioner extends Partitioner<SortDataWritable, IntWritable>  {

	public static final int PREDICTED_MAX_FREQ = 700;
	
	@Override
	public int getPartition(SortDataWritable key, IntWritable value, int numPartitions) {
		int frequency = key.getFrequency();
		int intervalSize = PREDICTED_MAX_FREQ / numPartitions;
		
		System.out.println("frequency = " + frequency + " numPartitions = " + numPartitions + " interval = " + intervalSize + " bucket = " + frequency / intervalSize);
		return frequency / intervalSize;
	}
}
