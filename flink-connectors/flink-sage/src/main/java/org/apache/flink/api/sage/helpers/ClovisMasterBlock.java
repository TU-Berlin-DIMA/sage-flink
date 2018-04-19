package org.apache.flink.api.sage.helpers;

import java.nio.ByteBuffer;

/**
 * Created by Clemens Lutz on 4/17/18.
 */
public class ClovisMasterBlock {

	private long numStreams;
	private long totalRecords;
	private long totalBlocks;

	public int getMasterBlockSize() {
		return Long.BYTES + Long.BYTES + Long.BYTES;
	}

	public void write(ByteBuffer out) {
		out.putLong(numStreams);
		out.putLong(totalRecords);
		out.putLong(totalBlocks);
	}

	public void read(ByteBuffer in) {
		numStreams = in.getLong();
		totalRecords = in.getLong();
		totalBlocks = in.getLong();
	}

	public long getNumStreams() {
		return numStreams;
	}

	public void setNumStreams(long numStreams) {
		this.numStreams = numStreams;
	}

	public long getTotalRecords() {
		return totalRecords;
	}

	public void setTotalRecords(long totalRecords) {
		this.totalRecords = totalRecords;
	}

	public long getTotalBlocks() {
		return totalBlocks;
	}

	public void setTotalBlocks(long totalBlocks) {
		this.totalBlocks = totalBlocks;
	}
}
