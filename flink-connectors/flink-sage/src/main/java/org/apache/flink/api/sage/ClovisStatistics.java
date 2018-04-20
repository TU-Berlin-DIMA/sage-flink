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
