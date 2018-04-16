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

import com.clovis.jni.pojo.ClovisBufVec;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializerBase;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;


/**
 * {@link InputFormat} implementation that enables the access to the Mero Storage. 
 * It defines the way Mero Object will be read by multiple workers, i.e. 
 * how the input splits will be formed and how the records will be read from each
 * particular input split.
 * 
 * This implementation is working with CSV data format.
 * 
 * Each InputSplit comprises {@link ClovisInputFormat#buffersPerSplit} Mero data blocks.
 *
 * @param <T> the type of the elements this InputFormat produces
 */
public class ClovisInputFormat<T> extends RichInputFormat<T, ClovisInputSplit> {
	
	private static final Logger LOG = LoggerFactory.getLogger(ClovisInputFormat.class);
	
	private static final long serialVersionUID = 1L;
	
	private static final Class<?>[] EMPTY_TYPES = new Class<?>[0];
	
	private static final byte[] DEFAULT_LINE_DELIMITER = {'\n'};

	private static final byte[] DEFAULT_FIELD_DELIMITER = new byte[] {','};

	private Class<?>[] fieldTypes = EMPTY_TYPES;

	protected transient Object[] parsedValues;

	private TupleSerializerBase<T> tupleSerializer;

	/**
	 * Maximum number of buffers in the read queue
	 */
	private static final int BUFFER_QUEUE_CAPACITY = 20;
	
	/**
	 * Maximum number of times the read task will be re-scheduled upon failure
	 */
	private static final int READ_RETRY_ATTEMPTS = 2;
	
	/**
	 * Periods the nextRecord method will wait to get the filled
	 * buffer from the queue between checking for the presence
	 * of failed ReadTasks
	 */
	private static final int BUFFER_WAIT_TIMEOUT_SEC = 5;
	
	private static final boolean[] EMPTY_INCLUDED = new boolean[0];

	private Integer buffersPerSplit;
	
	/**
	 * Blocking queue with buffers already filled
	 */
	private transient LinkedList<ClovisBuffer> fullBufferQueue;
	
	/**
	 * Holds clean buffers for reuse
	 */
	private transient LinkedList<ClovisBuffer> cleanBuffers;
	
	/**
	 * Asynchronous tasks executor
	 */
	// private transient ClovisThreadPoolExecutor executor;
	private transient ClovisReader clovisReader;
	private Configuration configParameters;

	/**
	 * The buffer this InputFormat currently reads from
	 */
	private transient ClovisBufVec currentData;
	private transient Iterator<ByteBuffer> dataIterator;
	private transient ByteBuffer currentBuffer;
	private transient int currentBufferOffset;
	
	/**
	 * Signalizes that all the splits were read
	 */
	private transient boolean end;
	
	/**
	 * The desired number of splits, as set by the configure() method.
	 */
	protected int numSplits = -1;

	/**
	 * Mero Object Properties
	 */
	private long meroObjectId;
	private String meroFilePath;
	private int meroBufferSize;
	private int meroChunkSize;

	private transient Iterator<Integer> inputSplitIterator;

	public ClovisInputFormat(long meroObjectId, String meroFilePath, int meroBufferSize, int meroChunkSize) {

		this.meroObjectId = meroObjectId;
		this.meroFilePath = meroFilePath;
		this.meroBufferSize = meroBufferSize;
		this.meroChunkSize = meroChunkSize;
		this.buffersPerSplit = 0;

		if (LOG.isDebugEnabled()) {
			LOG.debug("Constructor called");
		}
	}

	public ClovisInputFormat(long meroObjectId, String meroFilePath, int meroBufferSize, int meroChunkSize, int buffersPerSplit) {

		this.meroObjectId = meroObjectId;
		this.meroFilePath = meroFilePath;
		this.meroBufferSize = meroBufferSize;
		this.meroChunkSize = meroChunkSize;
		this.buffersPerSplit = buffersPerSplit;
	}

	/**
	 * Precedence given to parameters provided in the configuration file
	 *
	 * Mero Object related properties should be assigned either using {@link #ClovisInputFormat(long, String, int, int)}
	 * or the TaskConfig
	 *
	 * {@link #buffersPerSplit} is to be set using {@link #ClovisInputFormat(long, String, int, int, int)}, or
	 * {@link #setBuffersPerSplit(Integer)}, or the TaskConfig
	 *
	 * @param parameters The configuration with all parameters (note: not the Flink config but the TaskConfig).
	 */
	@Override
	public void configure(Configuration parameters) {

		if (LOG.isDebugEnabled()) {
			LOG.debug("Configure called");
		}

		// Store because we don't serialize clovisReader for transfer from Flink master to workers and must re-initialize
		configParameters = parameters;

		Integer buffPerSplit = parameters.getInteger(BUFFERS_PER_SPLIT_PARAMETER_KEY, -1);
		if (buffPerSplit > 0) {
			this.buffersPerSplit = buffPerSplit;
		} else if (buffersPerSplit == null) {
			throw new IllegalArgumentException("Number of buffers per split was not specified in input format, nor configuration.");
		}
	}

	@Override
	public BaseStatistics getStatistics(BaseStatistics cachedStatistics) throws IOException {
		return null;
	}

	/**
	 * @param minNumSplits The minimum desired number of splits. If fewer are created, some parallel
	 *                     instances may remain idle.
	 * @return
	 * @throws IOException
	 */
	@Override
	public ClovisInputSplit[] createInputSplits(int minNumSplits) throws IOException {
		if (minNumSplits < 1) {
			throw new IllegalArgumentException("Number of input splits has to be at least 1.");
		}
		
		// take the desired number of splits into account
		minNumSplits = Math.max(minNumSplits, this.numSplits);

		final List<ClovisInputSplit> inputSplits = new ArrayList<ClovisInputSplit>(minNumSplits);

		/**
		 * Deciding the total number of splits
		 */
		final int chunkSize = this.meroChunkSize;
		int numOfSplits = chunkSize/buffersPerSplit;
		if (chunkSize % buffersPerSplit > 0) {
			numOfSplits++;
		}

		if (numOfSplits < minNumSplits) {
			if (LOG.isWarnEnabled()) {
				LOG.warn("Impossible to produce " + minNumSplits + " splits");
			}
		}

		/**
		 * Allocating Mero Object offset values for each split previously created
		 */
		int splitNum = 0;
		int meroObjectOffset = 0;
		int meroSubObjectChunkSize = this.meroBufferSize;
		while (meroObjectOffset < this.meroBufferSize * this.meroChunkSize) {
			ArrayList<Integer> buffers = new ArrayList<>(buffersPerSplit);
			for (int i = 0; i < buffersPerSplit; i++) {
				buffers.add(meroObjectOffset);
				meroObjectOffset += meroSubObjectChunkSize;
			}

			ClovisInputSplit is = new ClovisInputSplit(splitNum++, buffers);
			inputSplits.add(is);

		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("Produced " + inputSplits.size() + " input splits");
		}

		return inputSplits.toArray(new ClovisInputSplit[inputSplits.size()]);
	}

	@Override
	public InputSplitAssigner getInputSplitAssigner(ClovisInputSplit[] inputSplits) {
		return new DefaultInputSplitAssigner(inputSplits);
	}

	/**
	 * @param split The split to be opened.
	 * @throws IOException
	 */
	@Override
	public void open(ClovisInputSplit split) throws IOException {

		//Iterator over the mero object offsets for the split received as a parameter
		this.inputSplitIterator = split.getMeroObjectOffsets().iterator();

		if (LOG.isDebugEnabled()) {
			LOG.debug("Opening input split " + split.getSplitNumber());
		}

		if (clovisReader == null) {
			clovisReader.setUserConfValues(configParameters);
			clovisReader = new ClovisReader();
		}
		clovisReader.open(meroObjectId, meroBufferSize);

		currentData = null;
		dataIterator = null;
		currentBuffer = null;
		currentBufferOffset = 0;

		this.end = false;
	}

	@Override
	public void close() throws IOException {
		if (currentData != null) {
			clovisReader.freeBuffer(currentData);
			currentData = null;
		}

		dataIterator = null;
		currentBuffer = null;
		currentBufferOffset = 0;

		clovisReader.close();
	}

	public void setFields(Class<?> ... fieldTypes) {
	}
	
	@Override
	public boolean reachedEnd() throws IOException {
		return end;
	}

	@Override
	public T nextRecord(T reuse) throws IOException {
		System.out.println("Read next record");
		if (LOG.isDebugEnabled()) {
			LOG.debug("Read next record");
		}

		//if we finished reading records from current buffer
		if (currentData == null || !dataIterator.hasNext()) {

			if (currentData != null) {
				clovisReader.freeBuffer(currentData);
				currentData = null;
				dataIterator = null;
			}

			if (inputSplitIterator.hasNext()) {
				ArrayList<Integer> bufferIndexes = new ArrayList<>(1);
				bufferIndexes.add(inputSplitIterator.next());
				clovisReader.scheduleRead(bufferIndexes);
			} else {
				return null;
			}

			currentData = clovisReader.getNextBuffer();
			dataIterator = currentData.iterator();

			if (LOG.isWarnEnabled() && !dataIterator.hasNext()) {
				LOG.warn("Mero returned empty ByteBuffer");
			}

			if (LOG.isDebugEnabled()) {
				LOG.debug("Clovis returned vector with " + currentData.getNumberOfBuffers() + " ByteBuffers");
			}

			currentBuffer = dataIterator.next();
			currentBufferOffset = 0;
		} else if (currentBufferOffset >= currentBuffer.limit()) {
			currentBuffer = dataIterator.next();
			currentBufferOffset = 0;
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("Parsing Clovis ByteBuffer at offset " + currentBufferOffset + "; buffer has " + currentBuffer.limit() + " bytes");
		}

		// Copy ByteBuffer into byte array. Maybe use ByteBuffer directly inside parseRecord()?
		// Note: ByteBuffer.array() not implemented for this type of ByteBuffer
		byte[] tmpBytes = new byte[currentBuffer.limit()];
		for (int i = 0; i < currentBuffer.limit(); ++i) {
			tmpBytes[i] = currentBuffer.get(i);
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("Copied Clovis ByteBuffer to byte array: " + new String(tmpBytes, currentBufferOffset, currentBuffer.limit()));
		}
		currentBufferOffset = currentBuffer.limit();

//		if (deserializer.parseRecord(parsedValues, tmpBytes, currentBufferOffset, 1 /*currentBuffer.limit() - currentBufferOffset */)) {
//			fillRecord(reuse, parsedValues);
//		} else {
//			return null;
//		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("Done deserializing next record");
		}

		return reuse;
	}
	
	/**
	 * Creates an object of type T from parsed field values
	 * @param reuse
	 * @param parsedValues - values of the fields in the tuple
	 * @return
	 */
	private T fillRecord(T reuse, Object[] parsedValues) {
		
		if (tupleSerializer == null)  {
			TypeInformation<T> typeInfo = TypeExtractor.getForObject(reuse);
			tupleSerializer = (TupleSerializerBase<T>) typeInfo.createSerializer(new ExecutionConfig());
		}
		
		return tupleSerializer.createOrReuseInstance(parsedValues, reuse);
	}
	
	public int getNumSplits() {
		return numSplits;
	}
	
	public void setNumSplits(int numSplits) {
		if (numSplits < -1 || numSplits == 0) {
			throw new IllegalArgumentException("The desired number of splits must be positive or -1 (= don't care).");
		}
		
		this.numSplits = numSplits;
	}

	public void setBuffersPerSplit(Integer buffersPerSplit) {
		this.buffersPerSplit = buffersPerSplit;
	}
	
	public Integer getBuffersPerSplit() {
		return buffersPerSplit;
	}

	/**
	 * ------------------------------------- Config Keys ------------------------------------------
	 */
	private static final String BUFFERS_PER_SPLIT_PARAMETER_KEY = "buffers.per.split";

	/**
	 * Utility Methods
	 */

	private static int max(int[] ints) {
		Preconditions.checkArgument(ints.length > 0);
		
		int max = ints[0];
		for (int i = 0; i < ints.length; i++) {
			max = Math.max(max, ints[i]);
		}
		return max;
	}

}
