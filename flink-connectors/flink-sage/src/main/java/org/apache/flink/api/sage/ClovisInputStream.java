package org.apache.flink.api.sage;

import com.clovis.jni.pojo.ClovisBufVec;
import org.apache.flink.api.common.io.BlockInfo;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;

/**
 * Created by Clemens Lutz on 4/16/18.
 */
public class ClovisInputStream extends InputStream {

	private ClovisReader clovisReader;

	private int blockSize;
	private int maxPayloadSize;

	private transient int streamIndex;
	private transient ClovisBufVec currentBufVec;
	private transient ByteBuffer currentBlock;
	private transient int currentBlockIndex;

	private long blockRecordCount;
	private long totalRecordCount;
	private long firstRecordStartPos = NO_RECORD;

	public ClovisInputStream(ClovisReader clovisReader) {
		super();

		this.clovisReader = clovisReader;
	}

	public void open(long objectId, int blockSize) throws IOException {
		clovisReader.open(objectId, blockSize);

		this.streamIndex = 0;
		ArrayList<Integer> bufferIndexes = new ArrayList<>(1);
		bufferIndexes.add(streamIndex);
		clovisReader.scheduleRead(bufferIndexes);
		this.currentBufVec = clovisReader.getNextBuffer();
		this.currentBlockIndex = 0;
		currentBlock = currentBufVec.get(currentBlockIndex);

		BlockInfo blockInfo = new BlockInfo();
		this.blockSize = blockSize;
		this.maxPayloadSize = blockSize - blockInfo.getInfoSize();

		this.readInfo();
	}

	@Override
	public void close() throws IOException {
		clovisReader.freeBuffer(currentBufVec);
		currentBufVec = null;
		currentBlock = null;
		clovisReader.close();
	}

	@Override
	public int read() throws IOException {

		if (currentBlock.position() == maxPayloadSize) {
			currentBlockIndex++;

			if (currentBlockIndex >= BUFVEC_LENGTH) {
				clovisReader.freeBuffer(currentBufVec);
				currentBufVec = null;
				currentBlock = null;
				streamIndex++;
				ArrayList<Integer> bufferIndexes = new ArrayList<>(1);
				bufferIndexes.add(streamIndex);
				clovisReader.scheduleRead(bufferIndexes);
				currentBufVec = clovisReader.getNextBuffer();
				currentBlockIndex = 0;
			}

			currentBlock = currentBufVec.get(currentBlockIndex);
			this.readInfo();
		}

		byte b = currentBlock.get();
		return (int) b;
	}

	@Override
	public long skip(long n) throws IOException {
		return super.skip(n);
	}

	public long skipToFirstRecordInBlock() throws IOException {
		long skippedBytes = 0;

//		while (firstRecordStartPos == NO_RECORD) {
//			long skipBytes = blockSize - currentBlock.position();
//			skippedBytes += skip(skipBytes);
//		}
		if (firstRecordStartPos == NO_RECORD) {
			throw new IOException("Block does not contain any records");
		}

		if (currentBlock.position() < firstRecordStartPos) {
			long skipBytes = firstRecordStartPos - currentBlock.position();
			skippedBytes += skip(skipBytes);
		}

		return skippedBytes;
	}

	@Override
	public int available() throws IOException {
		return maxPayloadSize - currentBlock.position();
	}

	public boolean isRecordAvailable() throws IOException {
		return firstRecordStartPos == NO_RECORD;
	}

	public long recordsInBlock() throws IOException {
		return this.blockRecordCount;
	}

	private void readInfo() {
		BlockInfo blockInfo = new BlockInfo();

		// Read BlockInfo from end of block and reset to initial position
		int initPos = currentBlock.position();
		currentBlock.position(maxPayloadSize);
		blockInfo.setRecordCount(currentBlock.getLong());
		blockInfo.setAccumulatedRecordCount(currentBlock.getLong());
		blockInfo.setFirstRecordStart(currentBlock.getLong());
		currentBlock.position(initPos);

		blockRecordCount = blockInfo.getRecordCount();
		totalRecordCount = blockInfo.getAccumulatedRecordCount();
		firstRecordStartPos = blockInfo.getFirstRecordStart();
	}

	private static final int BUFVEC_LENGTH = 1;
	private static final int NO_RECORD = -1;
}
