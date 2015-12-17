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

	@Override
	public int getPartition(SortDataWritable key, IntWritable value, int numPartitions) {
		Double percent = Double.parseDouble(key.getPercent());
		if (numPartitions >= 100) {
			return (int)Math.floor(percent);
		} else {

			int intervalSize = 100 / numPartitions;
			return (numPartitions - ((int)Math.floor(percent) / intervalSize) - 1);
		}
	}
}
