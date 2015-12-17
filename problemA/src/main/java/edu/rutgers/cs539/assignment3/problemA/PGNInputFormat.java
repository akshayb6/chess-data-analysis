/**
 * 
 */
package edu.rutgers.cs539.assignment3.problemA;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 * @author ashish
 *
 */
public class PGNInputFormat extends FileInputFormat<GameDataWritable,NullWritable> {

	@Override
	public RecordReader<GameDataWritable, NullWritable> createRecordReader(InputSplit arg0, TaskAttemptContext arg1)
			throws IOException, InterruptedException {
		return new PGNRecordReader();
	}

}
