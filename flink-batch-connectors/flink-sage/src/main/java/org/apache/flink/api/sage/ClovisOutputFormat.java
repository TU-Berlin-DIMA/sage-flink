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
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;

import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.types.StringValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;

/**
 * {@link OutputFormat} implementation that enables the access to the Mero Storage.  
 * It defines how the records will be written into the particular Mero object. 
 * It can work only with Tuple type and uses CSV data format.
 * 
 * On creation {@link ClovisOutputFormat} accepts the flag {@link ClovisOutputFormat#storageType}
 * which is an indication of the preferred storage layer the object should be written to.
 * 
 * It writes the records into the {@link ClovisOutputFormat#currentBuffer} until it is full.
 * When the buffer is full it initiates the asynchronous {@link WriteTask}, which persists 
 * the buffer to the storage.
 *
 * @param <T> - the type of Tuple the format works with
 */
public class ClovisOutputFormat<T extends Tuple> extends RichOutputFormat<T> {
	
	private static final long serialVersionUID = 1L;
	private static final Logger LOG = LoggerFactory.getLogger(ClovisOutputFormat.class);
	
	private static final StorageType DEFAULT_STORAGE_TYPE = StorageType.STORAGE_TYPE_1;
	private static final WriteMode DEFAULT_WRITE_MODE = WriteMode.NO_OVERWRITE;
	private static final byte[] DEFAULT_LINE_DELIMITER = {'\n'};
	private static final byte[] DEFAULT_FIELD_DELIMITER = new byte[] {','};
	
	/**
	 * Maximum number of buffers in the write queue
	 */
	private static final int BUFFER_QUEUE_CAPACITY = 20;
	
	/**
	 * Maximum number of times the read task will be re-scheduled upon failure
	 */
	private static final int WRITE_RETRY_ATTEMPTS = 2;
	
	/**
	 * The period to wait for termination of all write tasks on the closure of InputFormat
	 */
	private static final int WRITE_TASKS_TERMINATION_TIMEOUT_SEC = 5;
	
	/**
	 * Periods the nextRecord method will wait to get the next
	 * clean buffer from the queue between checking for the presence
	 * of failed WriteTasks
	 */
	private static final int BUFFER_WAIT_TIMEOUT_SEC = 5;
	
	private Path path;
	
	/**
	 * The preferable storage type for this OutputFormat
	 */
	private StorageType storageType;
	private WriteMode writeMode;
	private boolean allowNullValues = true;
	private boolean quoteStrings = false;
	
	private byte[] fieldDelim = DEFAULT_FIELD_DELIMITER;
	private byte[] recordDelim = DEFAULT_LINE_DELIMITER;
	
	/**
	 * Number of current task
	 */
	private int taskNumber;
	
	/**
	 * The number of the buffer currently written into
	 */
	private int currentBufferNumber;
	
	/**
	 * The path (i.e. Object name) written into
	 */
	private transient Path actualFilePath;
	
	private String charsetName;
	
	private transient Charset charset;

	private transient ClovisThreadPoolExecutor executor;
	
	/**
	 * The queue holding the clean buffers to be filled and given to WriteTasks for persistance
	 */
	private transient BlockingQueue<ClovisBuffer> queue;
	
	/**
	 * The buffer we currently write to
	 */
	private transient ClovisBuffer currentBuffer;
	
	/**
	 * Holds the existing storage type
	 * Used to indicate (set a flag) which storage is 
	 * preferable for current OutputFormat to write to
	 *
	 */
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
		this.currentBufferNumber = 0;
		
		//Initialize the path (taking into account the preferable storage type)
		path = path.suffix("/storage" + storageType.toString() + "/");
		
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

		this.executor = new ClovisThreadPoolExecutor(0, Integer.MAX_VALUE, 60L,
				TimeUnit.SECONDS, new SynchronousQueue<Runnable>(),
				WRITE_RETRY_ATTEMPTS);
		
		//if the queue was already created - just reuse it
		if (this.queue == null) {
			this.queue = new ArrayBlockingQueue<ClovisBuffer>(BUFFER_QUEUE_CAPACITY);
			
			for (int i = 0; i < BUFFER_QUEUE_CAPACITY; i++) {
				queue.add(new ClovisBuffer());
			}
		}
	}
	
	private String getFileName(int taskNumber, int currentBufferNumber) {
		StringBuilder sb = new StringBuilder("/(");
		sb.append(taskNumber).append(",").append(currentBufferNumber).append(")");
		return sb.toString();
	}

	@Override
	public void writeRecord(T element) throws IOException {
		
		if (executor.hasFailedTasks()) {
			throw new IOException("Buffer could not be persisted");
		}
		
		int numFields = element.getArity();
		
		StringBuilder sb = new StringBuilder();

		for (int i = 0; i < numFields; i++) {
			Object v = element.getField(i);
			if (v != null) {
				if (i != 0) {
					sb.append(new String(fieldDelim, Charsets.UTF_8));
				}

				if (quoteStrings) {
					if (v instanceof String || v instanceof StringValue) {
						sb.append('"');
						sb.append(v.toString());
						sb.append('"');
					} else {
						sb.append(v.toString());
					}
				} else {
					sb.append(v.toString());
				}
			} else {
				if (this.allowNullValues) {
					if (i != 0) {
						sb.append(new String(fieldDelim, Charsets.UTF_8));
					}
				} else {
					throw new IOException("Cannot write tuple with <null> value at position: " + i);
				}
			}
		}

		// add the record delimiter
		sb.append(new String(recordDelim, Charsets.UTF_8));
		
		//Record (bytes) to be written into the buffer
		byte[] bytes = sb.toString().getBytes(charset);
		
		if (currentBuffer == null || !currentBuffer.write(bytes)) {
			
			//If the buffer is full - create a new write task
			if (currentBuffer != null) {
				WriteTask task = new WriteTask(currentBuffer, queue, actualFilePath);
				executor.execute(task);
				
				currentBufferNumber++;
				this.actualFilePath = path.suffix(getFileName(taskNumber, currentBufferNumber));
			}
			
			currentBuffer = null;
			try {
				//Wait for a new clean buffer from the blocking queue
				//if none available immediately - wait BUFFER_WAIT_TIMEOUT_SEC
				//in a loop while also checking the presence of execution errors
				while (currentBuffer == null) {
					currentBuffer = queue.poll(BUFFER_WAIT_TIMEOUT_SEC, TimeUnit.SECONDS);
					if (executor.hasFailedTasks()) {
						throw new IOException("Could not obtain the free buffer from the system");
					}
				}
				currentBuffer.write(bytes);
			} catch (InterruptedException e) {
				throw new IOException("Could not obtain the free buffer from the system");
			}
		}
	}

	@Override
	public void close() throws IOException {
		
		//Create the last write task of what is in the buffer at the moment
		if (currentBuffer != null) {
			WriteTask task = new WriteTask(currentBuffer, queue, actualFilePath);
			executor.execute(task);
			currentBuffer = null;
		}
		
		try {
			executor.shutdown();
			//Wait WRITE_TASKS_TERMINATION_TIMEOUT_SEC for the tasks to finish
			executor.awaitTermination((long)WRITE_TASKS_TERMINATION_TIMEOUT_SEC, TimeUnit.SECONDS);
			if (executor.hasFailedTasks()) {
				throw new IOException("Buffers could not be persisted");
			}
		} catch (InterruptedException e) {
			throw new IOException(e);
		}
		
		if (!executor.isTerminated()) {
			throw new IOException("Buffers could not be persisted");
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
	
	/**
	 * Configures the format to either allow null values (writing an empty field),
	 * or to throw an exception when encountering a null field.
	 * <p>
	 * by default, null values are allowed.
	 *
	 * @param allowNulls Flag to indicate whether the output format should accept null values.
	 */
	public void setAllowNullValues(boolean allowNulls) {
		this.allowNullValues = allowNulls;
	}
	
	/**
	 * Configures whether the output format should quote string values. String values are fields
	 * of type {@link java.lang.String} and {@link org.apache.flink.types.StringValue}, as well as
	 * all subclasses of the latter.
	 * <p>
	 * By default, strings are not quoted.
	 *
	 * @param quoteStrings Flag indicating whether string fields should be quoted.
	 */
	public void setQuoteStrings(boolean quoteStrings) {
		this.quoteStrings = quoteStrings;
	}
	
	public void setFieldDelimiter(byte[] delimiter) {
		if (delimiter == null) {
			throw new IllegalArgumentException("Delimiter must not be null");
		}

		this.fieldDelim = delimiter;
	}

	public void setFieldDelimiter(char delimiter) {
		setFieldDelimiter(String.valueOf(delimiter));
	}

	public void setFieldDelimiter(String delimiter) {
		this.fieldDelim = delimiter.getBytes(Charsets.UTF_8);
	}
	
	public void setRecordDelimiter(byte[] delimiter) {
		if (delimiter == null) {
			throw new IllegalArgumentException("Delimiter must not be null");
		}

		this.recordDelim = delimiter;
	}

	public void setRecordDelimiter(char delimiter) {
		setRecordDelimiter(String.valueOf(delimiter));
	}

	public void setRecordDelimiter(String delimiter) {
		this.recordDelim = delimiter.getBytes(Charsets.UTF_8);
	}
	
	class WriteTask implements ClovisAsyncTask {
		
		private BlockingQueue<ClovisBuffer> queue;
		private ClovisBuffer buffer;
		private FSDataOutputStream stream;
		private int retryAttempt;
		
		public WriteTask(ClovisBuffer buffer, BlockingQueue<ClovisBuffer> queue, Path path) throws IOException {
			this.queue = queue;
			this.buffer = buffer;
			FileSystem fs = path.getFileSystem();
			this.stream = fs.create(path, writeMode == WriteMode.OVERWRITE);
			setRetryAttempt(0);
		}

		@Override
		public void run() {
			try {
				//flip the buffer to be available for writing
				buffer.flip();
				//write the bytes
				stream.write(buffer.array(), 0, buffer.limit());
				//close the stream and put the clean buffer into the blocking queue
				cleanup();
			} catch (Exception e) {
				buffer.flip();
				throw new RuntimeException(e);
			}
			
		}
		
		public void cleanup() throws InterruptedException, IOException {
			buffer.clear();
			queue.put(buffer);
			final FSDataOutputStream s = this.stream;
			if (s != null) {
				this.stream = null;
				s.close();
			}
		}

		public int getRetryAttempt() {
			return retryAttempt;
		}

		public void setRetryAttempt(int retryAttempt) {
			this.retryAttempt = retryAttempt;
		}
	}
}