/**
 * 
 */
package edu.rutgers.cs539.assignment3.problemC;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**
 * @author ashish
 *
 */
public class GameDataWritable implements WritableComparable<GameDataWritable> {

	private int gameResult; // Who won? Integer encoding as per chesspresso lib. B/W/Draw/Unknown
	private int numberOfMoves; // plyCount from pgn file
	private String blackPlayer; // Player id of black player
	private String whitePlayer; // Player id of white player
	
	public GameDataWritable() {
		
	}
	
	public GameDataWritable(int gameResult, int numberOfMoves, String blackPlayer, String whitePlayer) {
		super();
		this.gameResult = gameResult;
		this.numberOfMoves = numberOfMoves;
		this.blackPlayer = blackPlayer;
		this.whitePlayer = whitePlayer;
	}

	@Override
	public void readFields(DataInput input) throws IOException {
		gameResult = input.readInt();
		numberOfMoves = input.readInt();
		blackPlayer = input.readUTF();
		whitePlayer = input.readUTF();
	}

	@Override
	public void write(DataOutput output) throws IOException {
		output.writeInt(gameResult);
		output.writeInt(numberOfMoves);
		output.writeUTF(blackPlayer);
		output.writeUTF(whitePlayer);
	}

	@Override
	public int compareTo(GameDataWritable game) {
		return this.gameResult - game.gameResult;
	}

	/**
	 * @return the gameResult
	 */
	public int getGameResult() {
		return gameResult;
	}

	/**
	 * @param gameResult the gameResult to set
	 */
	public void setGameResult(int gameResult) {
		this.gameResult = gameResult;
	}

	/**
	 * @return the numberOfMoves
	 */
	public int getNumberOfMoves() {
		return numberOfMoves;
	}

	/**
	 * @param numberOfMoves the numberOfMoves to set
	 */
	public void setNumberOfMoves(int numberOfMoves) {
		this.numberOfMoves = numberOfMoves;
	}

	/**
	 * @return the blackPlayer
	 */
	public String getBlackPlayer() {
		return blackPlayer;
	}

	/**
	 * @param blackPlayer the blackPlayer to set
	 */
	public void setBlackPlayer(String blackPlayer) {
		this.blackPlayer = blackPlayer;
	}

	/**
	 * @return the whitePlayer
	 */
	public String getWhitePlayer() {
		return whitePlayer;
	}

	/**
	 * @param whitePlayer the whitePlayer to set
	 */
	public void setWhitePlayer(String whitePlayer) {
		this.whitePlayer = whitePlayer;
	}
}
