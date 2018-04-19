package org.apache.flink.api.sage;

import org.apache.flink.api.common.io.statistics.BaseStatistics;

/**
 * Created by Clemens Lutz on 4/17/18.
 */
public class ClovisStatistics implements BaseStatistics {

	private long totalBytes;
	private long totalRecords;
	private int totalBlocks;
	private int blockSize;
	private int numStreams;

	ClovisStatistics() { }

	void setTotalBytes(long totalBytes) {
		this.totalBytes = totalBytes;
	}

	void setTotalRecords(long totalRecords) {
		this.totalRecords = totalRecords;
	}

	void setTotalBlocks(int totalBlocks) {
		this.totalBlocks = totalBlocks;
	}

	void setBlockSize(int blockSize) { this.blockSize = blockSize; }

	void setNumStreams(int numStreams) {
		this.numStreams = numStreams;
	}

	@Override
	public long getTotalInputSize() {
		return totalBytes;
	}

	@Override
	public long getNumberOfRecords() {
		return totalRecords;
	}

	@Override
	public float getAverageRecordWidth() {
		return totalBytes / totalRecords;
	}

	public int getTotalInputBlocks() { return totalBlocks; }

	public int getBlockSize() { return blockSize; }

	public int getNumStreams() {
		return numStreams;
	}
}
