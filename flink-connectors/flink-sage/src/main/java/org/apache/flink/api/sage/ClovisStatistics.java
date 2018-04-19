package org.apache.flink.api.sage;

import org.apache.flink.api.common.io.statistics.BaseStatistics;

/**
 * Created by Clemens Lutz on 4/17/18.
 */
public class ClovisStatistics implements BaseStatistics {

	private long totalBytes;
	private long totalRecords;
	private long totalBlocks;
	private int blockSize;
	private long numStreams;

	public ClovisStatistics() { }

	public void setTotalBytes(long totalBytes) {
		this.totalBytes = totalBytes;
	}

	public void setTotalRecords(long totalRecords) {
		this.totalRecords = totalRecords;
	}

	public void setTotalBlocks(long totalBlocks) {
		this.totalBlocks = totalBlocks;
	}

	public void setBlockSize(int blockSize) { this.blockSize = blockSize; }

	public void setNumStreams(long numStreams) {
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

	public long getTotalInputBlocks() { return totalBlocks; }

	public int getBlockSize() { return blockSize; }

	public long getNumStreams() {
		return numStreams;
	}
}
