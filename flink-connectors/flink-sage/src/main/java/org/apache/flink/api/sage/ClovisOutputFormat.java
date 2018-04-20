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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.InputTypeConfigurable;
import org.apache.flink.api.sage.helpers.ClovisOutputStream;
import org.apache.flink.api.sage.helpers.ClovisWriter;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * {@link OutputFormat} implementation that enables the access to the Mero Storage.  
 * It defines how the records will be written into the particular Mero object. 
 * It can work only with Tuple type and uses CSV data format.
 *
 * @param <T> - the type of Tuple the format works with
 */
public class ClovisOutputFormat<T> extends RichOutputFormat<T> implements InputTypeConfigurable {
	
	private static final long serialVersionUID = 1L;

	private static final Logger LOG = LoggerFactory.getLogger(ClovisOutputFormat.class);

	private static final WriteMode DEFAULT_WRITE_MODE = WriteMode.OVERWRITE;
	private WriteMode writeMode;

	private int flinkTaskNumber, numFlinkTasks;

	private transient ClovisOutputStream clovisOutputStream;
	private transient DataOutputViewStreamWrapper clovisDataOutputView;
	private Configuration configParameters;

	private TypeSerializer<T> typeSerializer;

	/**
	 * Mero Object Properties
	 */
	private long meroObjectId;
	private String meroFilePath;
	private int meroBlockSize;

	public ClovisOutputFormat(long meroObjectId, String meroFilePath, int meroBlockSize) {

		this.meroObjectId = meroObjectId;
		this.meroFilePath = meroFilePath;
		this.meroBlockSize = meroBlockSize;
	}

	/**
	 * @param parameters The configuration with all parameters.
	 */
	@Override
	public void configure(Configuration parameters) {
		// Store because we don't serialize clovisReader for transfer from Flink master to workers and must re-initialize
		configParameters = parameters;
	}

	public void setWriteMode(WriteMode writeMode) {
		this.writeMode = writeMode;
	}

	@Override
	public void open(int taskNumber, int numTasks) throws IOException {

		if (typeSerializer == null) {
			throw new IOException("DataSet type unknown - must call setInputType()");
		}

		this.flinkTaskNumber = taskNumber;
		this.numFlinkTasks = numTasks;

		if (clovisOutputStream == null) {
			ClovisWriter.setUserConfValues(configParameters);
			clovisOutputStream = new ClovisOutputStream(new ClovisWriter());
			clovisDataOutputView = null;
		}
		clovisOutputStream.open(meroObjectId, meroBlockSize);

		if (clovisDataOutputView == null) {
			clovisDataOutputView = new DataOutputViewStreamWrapper(clovisOutputStream);
		}
	}

	@Override
	public void close() throws IOException {
		if (clovisOutputStream != null) {
			clovisOutputStream.close();
		}
	}

	@Override
	public void writeRecord(T record) throws IOException {
		clovisOutputStream.startRecord();
		typeSerializer.serialize(record, clovisDataOutputView);
	}

	public void setInputType(TypeInformation<?> type, ExecutionConfig executionConfig) {
		this.typeSerializer = (TypeSerializer<T>)type.createSerializer(executionConfig);
	}
}
