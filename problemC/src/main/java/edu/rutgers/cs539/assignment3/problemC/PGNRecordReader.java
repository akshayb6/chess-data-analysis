/**
 * 
 */
package edu.rutgers.cs539.assignment3.problemC;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import chesspresso.game.Game;
import chesspresso.pgn.PGNReader;
import chesspresso.pgn.PGNSyntaxError;

/**
 * @author ashish
 *
 */
public class PGNRecordReader extends RecordReader<GameDataWritable, NullWritable> {
	private PGNReader pgnReader;
	private GameDataWritable key = new GameDataWritable();

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {

		// Retrieve configuration value
		Configuration job = context.getConfiguration();

		// Retrieve FileSplit details
		FileSplit fileSplit = (FileSplit) split;
		final Path file = fileSplit.getPath();
		FileSystem fs = file.getFileSystem(job);

		// Open FileSplit FSDataInputStream
		FSDataInputStream fileIn = fs.open(fileSplit.getPath());
        pgnReader = new PGNReader(fileIn, fileSplit.getPath().getName());
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {

		try {
			Game game = pgnReader.parseGame();
			if (game == null) {
				key = null;
				return false;
			}
			
			key.setGameResult(game.getResult());
			key.setNumberOfMoves(game.getCurrentPly());
			key.setBlackPlayer(game.getBlack());
			key.setWhitePlayer(game.getWhite());
			return true;
		} catch (Exception e) {
			return nextKeyValue();
		}
	}

	@Override
	public void close() throws IOException {
	}

	@Override
	public GameDataWritable getCurrentKey() throws IOException, InterruptedException {
		return key;
	}

	@Override
	public NullWritable getCurrentValue() throws IOException, InterruptedException {
		return NullWritable.get();
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return 0.0f;
	}
}
