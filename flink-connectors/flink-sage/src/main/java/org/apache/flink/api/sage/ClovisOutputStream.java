package org.apache.flink.api.sage;

import com.clovis.jni.pojo.ClovisBufVec;
import org.apache.flink.api.common.io.BlockInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;

/**
 * Created by Clemens Lutz on 4/13/18.
 */
public class ClovisOutputStream extends OutputStream {

	private static final Logger LOG = LoggerFactory.getLogger(ClovisOutputStream.class);

	private ClovisWriter clovisWriter;

	private int blockSize;
	private int maxPayloadSize;

	private transient int streamIndex;
	private transient ClovisBufVec currentBufVec;
	private transient ByteBuffer currentBlock;
	private transient int currentBlockIndex;

	private int blockRecordCount, totalRecordCount;
	private long firstRecordStartPos = NO_RECORD;

	ClovisOutputStream(ClovisWriter clovisWriter) {
		super();

		this.clovisWriter = clovisWriter;
	}

	public void open(long objectId, int blockSize) throws IOException {
		clovisWriter.open(objectId, blockSize);

		if (currentBufVec == null) {
			currentBufVec = clovisWriter.allocBuffer(BUFVEC_LENGTH);
			currentBlockIndex = 0;
		}

		if (currentBlock == null) {
			currentBlock = currentBufVec.get(currentBlockIndex);
			currentBlock.rewind();
		}

		BlockInfo blockInfo = new BlockInfo();
		this.blockSize = blockSize;
		this.maxPayloadSize = blockSize - blockInfo.getInfoSize();
		this.streamIndex = 0;
		this.totalRecordCount = 0;
	}

	@Override
	public void close() throws IOException {
		if (this.currentBlock.position() > 0) {
			this.writeInfo();

			ArrayList<Integer> bufferIndexes = new ArrayList<>(1);
			bufferIndexes.add(streamIndex);
			clovisWriter.scheduleWrite(bufferIndexes, currentBufVec);
			clovisWriter.writeFinish();
		}

		clovisWriter.close();
		currentBlock = null;
	}

	@Override
	public void flush() throws IOException {
	}

	@Override
	public void write(int i) throws IOException {

		byte b = (byte) (i & 0xFF);
		currentBlock.put(b);

		if (currentBlock.position() == this.maxPayloadSize) {
			this.writeInfo();
			currentBlockIndex++;

			if (currentBlockIndex >= BUFVEC_LENGTH) {
				ArrayList<Integer> bufferIndexes = new ArrayList<>(1);
				bufferIndexes.add(streamIndex);
				clovisWriter.scheduleWrite(bufferIndexes, currentBufVec);
				clovisWriter.writeFinish();
				streamIndex++;
				currentBufVec = clovisWriter.allocBuffer(BUFVEC_LENGTH);
				currentBlockIndex = 0;
			}

			currentBlock = currentBufVec.get(currentBlockIndex);
		}
	}

	public void startRecord() {
		if (this.firstRecordStartPos == NO_RECORD) {
			this.firstRecordStartPos = currentBlock.position();
		}
		this.blockRecordCount++;
		this.totalRecordCount++;
	}

	private void writeInfo() throws IOException {
		if (LOG.isDebugEnabled()) {
			LOG.debug("WriteInfo with " + blockRecordCount + " records in block, " + totalRecordCount +
				" total records and offset " + firstRecordStartPos + " bytes");
		}

		BlockInfo blockInfo = new BlockInfo();
		blockInfo.setRecordCount(this.blockRecordCount);
		blockInfo.setAccumulatedRecordCount(this.totalRecordCount);
		blockInfo.setFirstRecordStart(this.firstRecordStartPos == NO_RECORD ? 0 : this.firstRecordStartPos);

		// Write out BlockInfo at end of block
		currentBlock.position(maxPayloadSize);
		currentBlock.putLong(blockInfo.getRecordCount());
		currentBlock.putLong(blockInfo.getAccumulatedRecordCount());
		currentBlock.putLong(blockInfo.getFirstRecordStart());

		this.blockRecordCount = 0;
		this.firstRecordStartPos = NO_RECORD;
	}

	private static final int BUFVEC_LENGTH = 1;
	private static final int NO_RECORD = -1;
}
