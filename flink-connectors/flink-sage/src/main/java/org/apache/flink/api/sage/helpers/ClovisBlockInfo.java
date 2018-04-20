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
