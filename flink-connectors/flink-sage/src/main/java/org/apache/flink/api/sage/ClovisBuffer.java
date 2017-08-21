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

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;


/**
 * The buffer used for operating on records written to/read from
 * {@link ClovisBuffer}. Internally, it uses the {@link ByteBuffer}, 
 * but introduces additional method {@link ClovisBuffer#read(byte[])} 
 * used to read records delimited by the specified delimiter.
 *
 */
public class ClovisBuffer {
	
	private ByteBuffer byteBuffer;
	private transient int currOffset;
	private transient int currLen;
	
	public ClovisBuffer(int bufferSize) {
		byte[] bytes = new byte[bufferSize];
		this.byteBuffer = ByteBuffer.wrap(bytes);
	}
	
	public void clear() {
		byteBuffer.clear();
	}
	
	/**
	 * Write the record to the buffer
	 * @param record
	 * @return
	 */
	public boolean write(byte[] record) {
		try {
			byteBuffer.put(record);
			return true;
		} catch (BufferOverflowException e) {
			return false;
		}
	}
	
	/**
	 * Read the next record from the buffer, delimited by recordDelim
	 * @param recordDelim
	 * @return
	 */
	public boolean read(byte[] recordDelim) {
		
		if (!byteBuffer.hasRemaining()) {
			return false;
		}
		
		/* position of matching positions in the delimiter byte array */
		int i = 0;

		int startPos = byteBuffer.position();
		int count;
		
		while (byteBuffer.hasRemaining() && i < recordDelim.length) {
			if ((byteBuffer.get()) == recordDelim[i]) {
				i++;
			} else {
				i = 0;
			}

		}

		// check why we dropped out
		if (i == recordDelim.length) {
			count = byteBuffer.position() - startPos - recordDelim.length;
		} else {
			count = byteBuffer.limit() - startPos;
		}
		
		this.currOffset = startPos;
		this.currLen = count;
		
		return true;
	}
	
	public int getCurrentOffset() {
		return currOffset;
	}
	
	public int getCurrentLength() {
		return currLen;
	}
	
	public byte[] array() {
		return byteBuffer.array();
	}
	
	public void flip() {
		byteBuffer.flip();
	}
	
	public int limit() {
		return byteBuffer.limit();
	}
	
	public void setLimit(int limit) {
		byteBuffer.limit(limit);
	}

	public ByteBuffer getByteBuffer() {
		return this.byteBuffer;
	}
}
