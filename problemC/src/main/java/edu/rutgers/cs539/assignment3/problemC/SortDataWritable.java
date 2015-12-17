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

	private String percent;
	
	public SortDataWritable() {
		
	}
	
	public SortDataWritable(String percent) {
		super();
		this.percent = percent;
	}

	@Override
	public void write(DataOutput out) throws IOException {

		out.writeUTF(percent);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		percent = in.readUTF();
	}

	@Override
	public int compareTo(SortDataWritable data) {
		double val1 = Double.parseDouble(percent);
		double val2 = Double.parseDouble(data.percent);
		if (val1==val2) {
			return 0;
		} else if (val2>val1) {
			return 1;
		} else{
			return -1;
		}
	}

	/**
	 * @return the percent
	 */
	public String getPercent() {
		return percent;
	}

	/**
	 * @param percent the percent to set
	 */
	public void setPercent(String percent) {
		this.percent = percent;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return percent;
	}
}
