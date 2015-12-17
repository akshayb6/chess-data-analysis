/**
 * 
 */
package edu.rutgers.cs539.assignment3.problemB;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**
 * @author ashish
 *
 */
public class PlayerWritable implements WritableComparable<PlayerWritable> {

	private char bw;
	private String playerId;
	
	public PlayerWritable() {
		
	}
	
	public PlayerWritable(char bw, String playerId) {
		super();
		this.bw = bw;
		this.playerId = playerId;
	}

	@Override
	public void write(DataOutput out) throws IOException {

		out.writeChar(bw);
		out.writeUTF(playerId);
	}

	@Override
	public void readFields(DataInput in) throws IOException {

		bw = in.readChar();
		playerId = in.readUTF();
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + bw;
		result = prime * result + ((playerId == null) ? 0 : playerId.hashCode());
		return result;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		PlayerWritable other = (PlayerWritable) obj;
		if (bw != other.bw)
			return false;
		if (playerId == null) {
			if (other.playerId != null)
				return false;
		} else if (!playerId.equals(other.playerId))
			return false;
		return true;
	}

	@Override
	public int compareTo(PlayerWritable player) {
		if (this.playerId.compareTo(player.playerId) == 0) {
			return this.bw-player.bw;
		} else {
			return (this.playerId.compareTo(player.playerId));
		}
	}

	/**
	 * @return the bw
	 */
	public char getBw() {
		return bw;
	}

	/**
	 * @param bw the bw to set
	 */
	public void setBw(char bw) {
		this.bw = bw;
	}

	/**
	 * @return the playerId
	 */
	public String getPlayerId() {
		return playerId;
	}

	/**
	 * @param playerId the playerId to set
	 */
	public void setPlayerId(String playerId) {
		this.playerId = playerId;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		if (bw == 'B') {
			return playerId + " " + "Black";
		} else {
			return playerId + " " + "White";
		}
	}
}
