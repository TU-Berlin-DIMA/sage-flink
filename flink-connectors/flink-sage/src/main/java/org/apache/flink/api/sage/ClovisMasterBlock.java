package org.apache.flink.api.sage;

import java.nio.ByteBuffer;

/**
 * Created by Clemens Lutz on 4/17/18.
 */
public class ClovisMasterBlock {

	private int numStreams;
	private int totalRecords;
	private int totalBlocks;

	public int getMasterBlockSize() {
		return 4 + 4 + 4;
	}

	public void write(ByteBuffer out) {
		out.putInt(numStreams);
		out.putInt(totalRecords);
		out.putInt(totalBlocks);
	}

	public void read(ByteBuffer in) {
		numStreams = in.getInt();
		totalRecords = in.getInt();
		totalBlocks = in.getInt();
	}

	public int getNumStreams() {
		return numStreams;
	}

	public void setNumStreams(int numStreams) {
		this.numStreams = numStreams;
	}

	public int getTotalRecords() {
		return totalRecords;
	}

	public void setTotalRecords(int totalRecords) {
		this.totalRecords = totalRecords;
	}

	public int getTotalBlocks() {
		return totalBlocks;
	}

	public void setTotalBlocks(int totalBlocks) {
		this.totalBlocks = totalBlocks;
	}
}
