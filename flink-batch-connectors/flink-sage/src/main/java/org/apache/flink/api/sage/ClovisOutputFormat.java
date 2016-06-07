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

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.IllegalCharsetNameException;
import java.nio.charset.UnsupportedCharsetException;

import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.FileSystem.WriteMode;

public class ClovisOutputFormat<T> extends RichOutputFormat<T> {
	
	private static final long serialVersionUID = 1L;
	
	private static StorageType DEFAULT_STORAGE_TYPE = StorageType.STORAGE_TYPE_1;
	private static WriteMode DEFAULT_WRITE_MODE = WriteMode.NO_OVERWRITE;
	private static final int NEWLINE = '\n';
	
	private Path path;
	private StorageType storageType;
	private WriteMode writeMode;
	
	private int taskNumber;
	private int numTasks;
	
	
	private int currentBufferNumber;
	private int bytesLeft;
	
	private transient Path actualFilePath;
	
	private String charsetName;
	
	private transient Charset charset;

	/**
	 * The output directory mode
	 */
//	private OutputDirectoryMode outputDirectoryMode;
	
	/** The stream to which the data is written; */
	protected transient FSDataOutputStream stream;
	
	/** Flag indicating whether this format actually created a file, which should be removed on cleanup. */
	private transient boolean fileCreated;
	
	public static enum StorageType {
		STORAGE_TYPE_1("1"),
		STORAGE_TYPE_2("2"),
		STORAGE_TYPE_3("3");
		
		private String name;
		
		private StorageType(String name) {
			this.name = name;
		}
		
		@Override
		public String toString() {
			return name;
		}
		
		public static StorageType fromString(String value) {
			switch (value) {
				case "1" :
					return StorageType.STORAGE_TYPE_1;
				case "2" :
					return StorageType.STORAGE_TYPE_2;
				case "3" :
					return StorageType.STORAGE_TYPE_3;
				default:
					return null;
			}
		}
	}
	
	/**
	 * The key under which the name of the target path is stored in the configuration. 
	 */
	public static final String FILE_PARAMETER_KEY = "flink.output.file";
	
	public static final String STORAGE_TYPE_PARAMETER_KEY = "storage.type";
	public static final int BUFFER_SIZE = 1024;
	
	public ClovisOutputFormat() {}
	
	public ClovisOutputFormat(Path path) {
		this.setPath(path);
	}

	public ClovisOutputFormat(Path path, StorageType storageType) {
		this.setPath(path);
		this.setStorageType(storageType);
	}
	
	public ClovisOutputFormat(StorageType storageType) {
		this.setStorageType(storageType);
	}

	@Override
	public void configure(Configuration parameters) {
		if (path == null) {
			String filePath = parameters.getString(FILE_PARAMETER_KEY, null);
			
			if (filePath == null) {
				throw new IllegalArgumentException("The output path has been specified neither via constructor/setters" +
						", nor via the Configuration.");
			}
			
			path = new Path(filePath);
		}
		
		if (storageType == null) {
			storageType = StorageType.fromString(parameters.getString(STORAGE_TYPE_PARAMETER_KEY, DEFAULT_STORAGE_TYPE.toString()));
			if (storageType == null) {
				storageType = DEFAULT_STORAGE_TYPE;
			}
		}
		
		if (writeMode == null) {
			writeMode = DEFAULT_WRITE_MODE;
		}
		
		if (charsetName == null) {
			charsetName = "UTF-8";
		}
	}
	
	private void initializePath(Path path) throws IOException {
		FileSystem fs = path.getFileSystem();
		// if this is a local file system, we need to initialize the local output directory here
		if (!fs.isDistributedFS()) {
			if(!fs.initOutPathLocalFS(path, writeMode, true)) {
				// output preparation failed! Cancel task.
				throw new IOException("Output directory '" + path.toString() + "' could not be created. Canceling task...");
			}
		}
	}

	@Override
	public void open(int taskNumber, int numTasks) throws IOException {
		if (taskNumber < 0 || numTasks < 1) {
			throw new IllegalArgumentException("TaskNumber: " + taskNumber + ", numTasks: " + numTasks);
		}
		
		this.taskNumber = taskNumber;
		this.numTasks = numTasks;
		this.currentBufferNumber = 0;
		this.bytesLeft = BUFFER_SIZE;
		
		path = path.suffix("/storage" + storageType.toString() + "/");
		
		FileSystem fs = path.getFileSystem();
		initializePath(path);
		
		try {
			this.charset = Charset.forName(charsetName);
		}
		catch (IllegalCharsetNameException e) {
			throw new IOException("The charset " + charsetName + " is not valid.", e);
		}
		catch (UnsupportedCharsetException e) {
			throw new IOException("The charset " + charsetName + " is not supported.", e);
		}
		
		// Suffix the path with the parallel instance index, if needed
		this.actualFilePath = path.suffix(getFileName(taskNumber, currentBufferNumber));

		// create output file
		this.stream = fs.create(this.actualFilePath, writeMode == WriteMode.OVERWRITE);
		
		// at this point, the file creation must have succeeded, or an exception has been thrown
		this.fileCreated = true;
	}
	
//	protected String getDirectoryFileName(int taskNumber) {
//		return Integer.toString(taskNumber + 1);
//	}
	
	private String getFileName(int taskNumber, int currentBufferNumber) {
		StringBuilder sb = new StringBuilder("/(");
		sb.append(taskNumber).append(",").append(currentBufferNumber).append(")");
		return sb.toString();
		
//		return "/"+ currentBufferNumber*numTasks + taskNumber;
	}

	@Override
	public void writeRecord(T record) throws IOException {
		byte[] bytes = record.toString().getBytes(charset);
		if (bytes.length > bytesLeft) {
			FileSystem fs = path.getFileSystem();
			final FSDataOutputStream s = this.stream;
			if (this.stream != null) {
				this.stream = null;
				s.close();
			}
			currentBufferNumber++;
			this.actualFilePath = path.suffix(getFileName(taskNumber, currentBufferNumber));
			this.stream = fs.create(this.actualFilePath, writeMode == WriteMode.OVERWRITE);
			this.bytesLeft = BUFFER_SIZE;
		}
		if (this.stream == null) {
			throw new IOException("File " + actualFilePath + "could not be created");
		}
		this.stream.write(bytes);
		this.stream.write(NEWLINE);
		
		this.bytesLeft -= bytes.length;
	}

	@Override
	public void close() throws IOException {
		final FSDataOutputStream s = this.stream;
		if (s != null) {
			this.stream = null;
			s.close();
		}		
	}

	public StorageType getStorageType() {
		return storageType;
	}

	public void setStorageType(StorageType storageType) {
		this.storageType = storageType;
	}

	public Path getPath() {
		return path;
	}

	public void setPath(Path path) {
		if (path == null) {
			throw new NullPointerException();
		}
		this.path = path;
	}
	
//	public void setOutputDirectoryMode(OutputDirectoryMode mode) {
//		if (mode == null) {
//			throw new NullPointerException();
//		}
//		
//		this.outputDirectoryMode = mode;
//	}
//	
//	public OutputDirectoryMode getOutputDirectoryMode() {
//		return this.outputDirectoryMode;
//	}
	
	
	public WriteMode getWriteMode() {
		return writeMode;
	}

	public void setWriteMode(WriteMode writeMode) {
		this.writeMode = writeMode;
	}

	public String getCharsetName() {
		return charsetName;
	}

	public void setCharsetName(String charsetName) {
		this.charsetName = charsetName;
	}

}
