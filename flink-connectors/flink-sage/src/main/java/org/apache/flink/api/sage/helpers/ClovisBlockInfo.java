package org.apache.flink.api.sage.helpers;

import java.nio.ByteBuffer;

/**
 * Created by Clemens Lutz on 4/19/18.
 */
public class ClovisBlockInfo {

	private long blockRecordCount;
	private long accumulatedRecordCount;
	private long firstRecordStartOffset;

	public int getInfoSize() {
		return Long.BYTES + Long.BYTES + Long.BYTES;
	}

	public void write(ByteBuffer out) {
		out.putLong(blockRecordCount);
		out.putLong(accumulatedRecordCount);
		out.putLong(firstRecordStartOffset);
	}

	public void read(ByteBuffer in) {
		blockRecordCount = in.getLong();
		accumulatedRecordCount = in.getLong();
		firstRecordStartOffset = in.getLong();
	}

	public long getBlockRecordCount() {
		return blockRecordCount;
	}

	public void setBlockRecordCount(long blockRecordCount) {
		this.blockRecordCount = blockRecordCount;
	}

	public long getAccumulatedRecordCount() {
		return accumulatedRecordCount;
	}

	public void setAccumulatedRecordCount(long accumulatedRecordCount) {
		this.accumulatedRecordCount = accumulatedRecordCount;
	}

	public long getFirstRecordStartOffset() {
		return firstRecordStartOffset;
	}

	public void setFirstRecordStartOffset(long firstRecordStartOffset) {
		this.firstRecordStartOffset = firstRecordStartOffset;
	}
}
