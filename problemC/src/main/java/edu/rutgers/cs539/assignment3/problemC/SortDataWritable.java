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
public class SortDataWritable implements WritableComparable<SortDataWritable> {

	private int frequency;
	
	public SortDataWritable() {
		
	}
	
	public SortDataWritable(int frequency) {
		super();
		this.frequency = frequency;
	}

	@Override
	public void write(DataOutput out) throws IOException {

		out.writeInt(frequency);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		frequency = in.readInt();
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + frequency;
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
		SortDataWritable other = (SortDataWritable) obj;
		if (frequency != other.frequency)
			return false;
		return true;
	}

	@Override
	public int compareTo(SortDataWritable data) {
		return data.frequency - this.frequency;
	}

	/**
	 * @return the frequency
	 */
	public int getFrequency() {
		return frequency;
	}

	/**
	 * @param frequency the frequency to set
	 */
	public void setFrequency(int frequency) {
		this.frequency = frequency;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return Integer.toString(frequency);
	}
}
