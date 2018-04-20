/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.api.sage.helpers;

import com.clovis.jni.pojo.ClovisBufVec;
import org.apache.flink.api.sage.ClovisStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;

/**
 * Created by Clemens Lutz on 4/16/18.
 */
public class ClovisInputStream extends InputStream {

	private static final Logger LOG = LoggerFactory.getLogger(ClovisInputStream.class);

	private ClovisReader clovisReader;

	private int blockSize;
	private int maxPayloadSize;

	private transient int streamIndex;
	private transient ClovisBufVec currentBufVec;
	private transient ByteBuffer currentBlock;
	private transient int currentBlockIndex;

	private long blockRecordCount;
	private long accumulatedRecordCount;
	private long firstRecordStartPos = NO_RECORD;
	private long totalRecords = NO_RECORD;
	private long totalBytes;
	private long totalBlocks;
	private long numStreams;

	public ClovisInputStream(ClovisReader clovisReader) {
		super();

		this.clovisReader = clovisReader;
	}

	public void open(long objectId, int blockSize) throws IOException {
		clovisReader.open(objectId, blockSize);

		if (totalRecords == NO_RECORD) {
			readMaster();
		}

		if (totalBlocks > 0) {
			this.streamIndex = 1;
			ArrayList<Integer> bufferIndexes = new ArrayList<>(1);
			bufferIndexes.add(streamIndex);
			clovisReader.scheduleRead(bufferIndexes);
			this.currentBufVec = clovisReader.getNextBuffer();
			this.currentBlockIndex = 0;
			currentBlock = currentBufVec.get(currentBlockIndex);
			currentBlock.rewind();

			ClovisBlockInfo blockInfo = new ClovisBlockInfo();
			this.blockSize = blockSize;
			this.maxPayloadSize = blockSize - blockInfo.getInfoSize();

			this.readInfo();
		}
	}

	@Override
	public void close() throws IOException {
		clovisReader.freeBuffer(currentBufVec);
		currentBufVec = null;
		currentBlock = null;
		clovisReader.close();
	}

	public ClovisStatistics getStatistics(long objectId, int blockSize) throws IOException {
		if (totalRecords == NO_RECORD) {
			clovisReader.open(objectId, blockSize);
			readMaster();
			clovisReader.close();
		}

		ClovisStatistics statistics = new ClovisStatistics();
		statistics.setTotalBytes(totalBytes);
		statistics.setTotalRecords(totalRecords);
		statistics.setTotalBlocks(totalBlocks);
		statistics.setBlockSize(blockSize);
		statistics.setNumStreams(numStreams);

		return statistics;
	}

	@Override
	public int read() throws IOException {

		byte b = currentBlock.get();

		if (currentBlock.position() == maxPayloadSize) {
			currentBlockIndex++;

			// Note: streamIndex includes master block, but totalBlocks does not
			if (streamIndex + 1 < totalBlocks + 1) {
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
				currentBlock.rewind();
				this.readInfo();
			}
		}

		return (int) b;
	}

	@Override
	public long skip(long n) throws IOException {
		return super.skip(n);
	}

	public long skipBlocks(long blocks) throws IOException {
		return super.skip(blocks * maxPayloadSize);
	}

	public long skipToFirstRecordInBlock() throws IOException {
		long skippedBytes = 0;

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
		if (totalBlocks > 0 && streamIndex < totalBlocks + 1) {
			return maxPayloadSize - currentBlock.position();
		}
		else {
			return 0;
		}
	}

	public boolean isRecordAvailable() throws IOException {
		return totalRecords > 0 && firstRecordStartPos != NO_RECORD;
	}

	public long recordsInBlock() throws IOException {
		return this.blockRecordCount;
	}

	private void readInfo() {
		ClovisBlockInfo blockInfo = new ClovisBlockInfo();

		// Read BlockInfo from end of block and reset to initial position
		currentBlock.mark();
		currentBlock.position(maxPayloadSize);
		blockInfo.read(currentBlock);
		currentBlock.reset(); // Reset position to mark()

		blockRecordCount = blockInfo.getBlockRecordCount();
		accumulatedRecordCount = blockInfo.getAccumulatedRecordCount();
		firstRecordStartPos = blockInfo.getFirstRecordStartOffset();

		if (LOG.isDebugEnabled()) {
			LOG.debug("ReadInfo with " + blockRecordCount + " records in block, " + accumulatedRecordCount +
				" accumulated records and offset " + firstRecordStartPos + " bytes");
		}
	}

	private void readMaster() throws IOException {
		ClovisMasterBlock masterBlock = new ClovisMasterBlock();
		ArrayList<Integer> bufferIndexes = new ArrayList<>(1);
		bufferIndexes.add(MASTER_BLOCK_INDEX);
		clovisReader.scheduleRead(bufferIndexes);
		ClovisBufVec metaDataBufVec = clovisReader.getNextBuffer();
		ByteBuffer masterByteBuffer = metaDataBufVec.get(0);
		masterByteBuffer.rewind();
		masterBlock.read(masterByteBuffer);

		totalBlocks = masterBlock.getTotalBlocks();
		totalBytes = -1;
		totalRecords = masterBlock.getTotalRecords();
		numStreams = masterBlock.getNumStreams();

		clovisReader.freeBuffer(metaDataBufVec);
	}

	private static final int BUFVEC_LENGTH = 1;
	private static final int NO_RECORD = -1;
	private static final int MASTER_BLOCK_INDEX = 0;
}
