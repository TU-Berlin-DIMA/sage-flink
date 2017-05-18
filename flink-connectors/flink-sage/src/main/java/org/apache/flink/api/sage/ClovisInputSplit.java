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

import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.InputSplit;

import java.util.ArrayList;


/**
 * The {@link InputSplit} implementation which works with
 * Mero data blocks (buffers). Each input split comprises several
 * data blocks of a particular Mero object.
 *
 * TODO: Added the offset_buffers and related methods to this class
 */
public class ClovisInputSplit implements InputSplit {
	
	private static final long serialVersionUID = 1L;

	//private final int[] offset_buffers;
	private final ArrayList<Integer> offset_buffers;

	private final Path[] buffers;
	private int num;
	
	/**
	 * @param offset_buffers
	 * @param num - the number assigned to this {@link ClovisInputSplit}
	 * @param buffers - the array comprising the addresses (ids) to each block(buffer)
	 */
	public ClovisInputSplit(int[] offset_buffers, int num, Path[] buffers) {
		this.buffers = buffers;
		this.num = num;

		this.offset_buffers = null;
	}

	public ClovisInputSplit(int num, ArrayList<Integer> offset_buffers) {
		this.offset_buffers = offset_buffers;
		this.num = num;

		this.buffers = null;
	}

	@Override
	public int getSplitNumber() {
		return num;
	}
	
	public Path[] getBuffers() {
		return buffers;
	}

	public ArrayList<Integer> getOffset_buffers() { return offset_buffers; }
	
}
