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
 * Created by Clemens Lutz on 4/17/18.
 */
public class ClovisStreamBlock {

	private long streamNumber;
	private long streamRecords;
	private long streamBlocks;

	public int getStreamBlockSize() {
		return Long.BYTES + Long.BYTES + Long.BYTES;
	}

	public void write(ByteBuffer out) {
		out.putLong(streamNumber);
		out.putLong(streamRecords);
		out.putLong(streamBlocks);
	}

	public void read(ByteBuffer in) {
		streamNumber = in.getLong();
		streamRecords = in.getLong();
		streamBlocks = in.getLong();
	}

	public long getStreamNumber() {
		return streamNumber;
	}

	public void setStreamNumber(long streamNumber) {
		this.streamNumber = streamNumber;
	}

	public long getStreamRecords() {
		return streamRecords;
	}

	public void setStreamRecords(long streamRecords) {
		this.streamRecords = streamRecords;
	}

	public long getStreamBlocks() {
		return streamBlocks;
	}

	public void setStreamBlocks(long streamBlocks) {
		this.streamBlocks = streamBlocks;
	}
}
