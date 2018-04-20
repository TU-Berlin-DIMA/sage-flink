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
