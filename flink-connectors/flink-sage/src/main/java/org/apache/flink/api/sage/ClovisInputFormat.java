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

import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * {@link InputFormat} implementation that enables the access to the Mero Storage. 
 * It defines the way Mero Object will be read by multiple workers, i.e. 
 * how the input splits will be formed and how the records will be read from each
 * particular input split.
 *
 * @param <T> the type of the elements this InputFormat produces
 */
public class ClovisInputFormat<T> extends RichInputFormat<T, ClovisInputSplit> implements ResultTypeQueryable<T> {
	
	private static final Logger LOG = LoggerFactory.getLogger(ClovisInputFormat.class);
	
	private static final long serialVersionUID = 1L;

	private transient ClovisInputStream clovisInputStream;
	private transient DataInputViewStreamWrapper clovisDataInputView;
	private transient long recordsRead;

	private Configuration configParameters;

	private TypeInformation<T> resultType;
	private TypeSerializer<T> typeSerializer;

	/**
	 * Mero Object Properties
	 */
	private long meroObjectId;
	private String meroFilePath;
	private int meroBlockSize;

	private transient Iterator<Integer> inputSplitIterator;

	public ClovisInputFormat(TypeInformation<T> resultType, long meroObjectId, String meroFilePath, int meroBlockSize) {

		this.resultType = resultType;
		this.meroObjectId = meroObjectId;
		this.meroFilePath = meroFilePath;
		this.meroBlockSize = meroBlockSize;

		if (LOG.isDebugEnabled()) {
			LOG.debug("Constructor called");
		}
	}

	/**
	 * Precedence given to parameters provided in the configuration file
	 *
	 * @param parameters The configuration with all parameters (note: not the Flink config but the TaskConfig).
	 */
	@Override
	public void configure(Configuration parameters) {

		// Store because we don't serialize clovisReader for transfer from Flink master to workers and must re-initialize
		configParameters = parameters;
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

		final List<ClovisInputSplit> inputSplits = new ArrayList<ClovisInputSplit>(minNumSplits);

		ArrayList clovisSplitBlocks = new ArrayList<>(1);
		clovisSplitBlocks.add(0);
		inputSplits.add(new ClovisInputSplit(0, clovisSplitBlocks));

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

		if (typeSerializer == null) {
			this.typeSerializer = resultType.createSerializer(this.getRuntimeContext().getExecutionConfig());
//			throw new IOException("DataSet type unknown - must call setInputType()");
		}

		if (clovisInputStream == null) {
			ClovisWriter.setUserConfValues(configParameters);
			ClovisReader clovisReader = new ClovisReader();
			clovisInputStream = new ClovisInputStream(clovisReader);
			clovisDataInputView = null;
		}
		clovisInputStream.open(meroObjectId, meroBlockSize);

		if (clovisDataInputView == null) {
			clovisDataInputView = new DataInputViewStreamWrapper(clovisInputStream);
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("Opening input split " + split.getSplitNumber());
		}

		// TODO: Skip to first block of inputSplit. For now, just assume single split and skip to first record
		//Iterator over the mero object offsets for the split received as a parameter
		// this.inputSplitIterator = split.getMeroObjectOffsets().iterator();
		if (clovisInputStream.isRecordAvailable()) {
			clovisInputStream.skipToFirstRecordInBlock();
		}

		this.recordsRead = 0;
	}

	@Override
	public void close() throws IOException {
		if (clovisInputStream != null) {
			clovisInputStream.close();
		}
	}
	
	@Override
	public boolean reachedEnd() throws IOException {
		return recordsRead >= clovisInputStream.recordsInBlock();
	}

	@Override
	public T nextRecord(T record) throws IOException {
		System.out.println("Read next record");
		if (LOG.isDebugEnabled()) {
			LOG.debug("Read next record");
		}

		if (this.reachedEnd()) {
			return null;
		}
		record = typeSerializer.deserialize(record, clovisDataInputView);
		recordsRead++;

		if (LOG.isDebugEnabled()) {
			LOG.debug("Done deserializing next record");
		}

		return record;
	}

	@Override
	public TypeInformation<T> getProducedType() {
		return this.resultType;
	}
}
