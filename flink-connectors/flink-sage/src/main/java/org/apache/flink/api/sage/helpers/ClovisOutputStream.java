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

	public ClovisOutputStream(ClovisWriter clovisWriter) {
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

		ClovisBlockInfo blockInfo = new ClovisBlockInfo();
		this.blockSize = blockSize;
		this.maxPayloadSize = blockSize - blockInfo.getInfoSize();
		this.streamIndex = 1;
		this.totalRecordCount = 0;
	}

	@Override
	public void close() throws IOException {
		ArrayList<Integer> bufferIndexes;

		if (this.currentBlock.position() > 0) {
			// Flush out currently open block
			this.writeInfo();
			bufferIndexes = new ArrayList<>(1);
			bufferIndexes.add(streamIndex);
			clovisWriter.scheduleWrite(bufferIndexes, currentBufVec);
			clovisWriter.writeFinish();
			streamIndex++;
		}

		// Gather metadata
		ClovisMasterBlock masterBlock = new ClovisMasterBlock();
		masterBlock.setNumStreams(1);
		masterBlock.setTotalBlocks(streamIndex - 1 /* Exclude master block */);
		masterBlock.setTotalRecords(totalRecordCount);

		// Write metadata blocks
		ClovisBufVec metaDataBufVec = clovisWriter.allocBuffer(1);
		ByteBuffer masterByteBuffer = metaDataBufVec.get(0);
		masterBlock.write(masterByteBuffer);
		bufferIndexes = new ArrayList<>(1);
		bufferIndexes.add(MASTER_BLOCK_INDEX);
		clovisWriter.scheduleWrite(bufferIndexes, metaDataBufVec);
		clovisWriter.writeFinish();

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

		ClovisBlockInfo blockInfo = new ClovisBlockInfo();
		blockInfo.setBlockRecordCount(this.blockRecordCount);
		blockInfo.setAccumulatedRecordCount(this.totalRecordCount);
		blockInfo.setFirstRecordStartOffset(this.firstRecordStartPos == NO_RECORD ? 0 : this.firstRecordStartPos);

		// Write out BlockInfo at end of block
		currentBlock.position(maxPayloadSize);
		blockInfo.write(currentBlock);

		this.blockRecordCount = 0;
		this.firstRecordStartPos = NO_RECORD;
	}

	private static final int BUFVEC_LENGTH = 1;
	private static final int NO_RECORD = -1;
	private static final int MASTER_BLOCK_INDEX = 0;
}
