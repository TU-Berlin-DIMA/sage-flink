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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;

import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.api.common.io.ParseException;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializerBase;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.types.parser.FieldParser;
import org.apache.flink.types.parser.StringParser;
import org.apache.flink.types.parser.StringValueParser;
import org.apache.flink.util.InstantiationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;

public class ClovisInputFormat<T> extends RichInputFormat<T, ClovisInputSplit> {
	
	private static final Logger LOG = LoggerFactory.getLogger(ClovisInputFormat.class);
	
	private static final long serialVersionUID = 1L;
	
	private static final Class<?>[] EMPTY_TYPES = new Class<?>[0];
	
	private static final byte[] DEFAULT_LINE_DELIMITER = {'\n'};
	private static final byte[] DEFAULT_FIELD_DELIMITER = new byte[] {','};
	private static final byte BACKSLASH = 92;
	private static final int BUFFER_QUEUE_CAPACITY = 20;
	private static final int READ_RETRY_ATTEMPTS = 2;
	private static final int BUFFER_WAIT_TIMEOUT_SEC = 5;
	
	private static final boolean[] EMPTY_INCLUDED = new boolean[0];
	
	private byte[] fieldDelim = DEFAULT_FIELD_DELIMITER;
	private byte[] recordDelim = DEFAULT_LINE_DELIMITER;
	
	private Class<?>[] fieldTypes = EMPTY_TYPES;
	
	protected boolean[] fieldIncluded = EMPTY_INCLUDED;
	
	private Path path;
	
	private Integer buffersPerSplit;
	
	private boolean lenient;
	private boolean quotedStringParsing = false;
	private byte quoteCharacter;
	
	private transient FieldParser<?>[] fieldParsers;
	protected transient Object[] parsedValues;
	
	private TupleSerializerBase<T> tupleSerializer;

	private transient BlockingQueue<ClovisBuffer> fullBufferQueue;
	private transient LinkedList<ClovisBuffer> cleanBuffers;
	private transient ClovisThreadPoolExecutor executor;
	
	private transient ClovisBuffer currentBuffer;
	
	private transient Iterator<Path> splitsIterator;
	private transient boolean end;
	
	/**
	 * The desired number of splits, as set by the configure() method.
	 */
	protected int numSplits = -1;
	
	public ClovisInputFormat() {}
	
	public ClovisInputFormat(Path filePath) {
		if (filePath == null) {
			throw new IllegalArgumentException("File path may not be null.");
		}
		
		this.path = filePath;
	}
	
	public ClovisInputFormat(Path filePath, int buffersPerSplit) {
		if (filePath == null) {
			throw new IllegalArgumentException("File path may not be null.");
		}
		
		this.path = filePath;
		this.buffersPerSplit = buffersPerSplit;
	}
	
	@Override
	public void configure(Configuration parameters) {
		
		String filePath = parameters.getString(FILE_PARAMETER_KEY, null);
		if (filePath != null) {
			try {
				this.path = new Path(filePath);
			}
			catch (RuntimeException rex) {
				throw new RuntimeException("Could not create a valid URI from the given file path name: " + rex.getMessage()); 
			}
		}
		else if (this.path == null) {
			throw new IllegalArgumentException("File path was not specified in input format, nor configuration."); 
		}
		
		Integer buffPerSplit = parameters.getInteger(BUFFERS_PER_SPLIT_PARAMETER_KEY, -1); 
		if (buffPerSplit > 0) {
			this.buffersPerSplit = buffPerSplit;
		} else if (buffersPerSplit == null) {
			throw new IllegalArgumentException("Number of buffers per split was not specified in input format, nor configuration.");
		}
		
		if (fieldTypes.length < 1) {
			throw new IllegalArgumentException("Field types are not configured");
		}
	}

	@Override
	public BaseStatistics getStatistics(BaseStatistics cachedStatistics) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ClovisInputSplit[] createInputSplits(int minNumSplits) throws IOException {
		if (minNumSplits < 1) {
			throw new IllegalArgumentException("Number of input splits has to be at least 1.");
		}
		
		// take the desired number of splits into account
		minNumSplits = Math.max(minNumSplits, this.numSplits);
		
		final Path path = this.path;
		final List<ClovisInputSplit> inputSplits = new ArrayList<ClovisInputSplit>(minNumSplits);

		// get all the files that are involved in the splits
		List<FileStatus> files = new ArrayList<FileStatus>();

		final FileSystem fs = path.getFileSystem();
		final FileStatus pathFile = fs.getFileStatus(path);

		if (pathFile.isDir()) {
			addFilesInDir(path, files, true);
		} else {
			files.add(pathFile);
		}
		
		int bufferNum = files.size();
		int numOfSplits = bufferNum/buffersPerSplit;
		if (bufferNum % buffersPerSplit > 0) {
			numOfSplits++;
		}
		
		if (numOfSplits < minNumSplits) {
			if (LOG.isWarnEnabled()) {
				LOG.warn("Impossible to produce " + minNumSplits + " splits");
			}
		}
		
		int splitNum = 0;
		Iterator<FileStatus> it = files.iterator();
		while (it.hasNext()) {
			ArrayList<Path> buffers = new ArrayList<Path>(buffersPerSplit);
			for (int i = 0; i < buffersPerSplit; i++) {
				if (it.hasNext()) {
					buffers.add(it.next().getPath());
				}
			}
			ClovisInputSplit is = new ClovisInputSplit(splitNum++, buffers.toArray(new Path[buffers.size()]));
			inputSplits.add(is);
		}
		return inputSplits.toArray(new ClovisInputSplit[inputSplits.size()]);
	}
	
	/**
	 * Enumerate all files in the directory and recursive if enumerateNestedFiles is true.
	 * @return the total length of accepted files.
	 */
	private long addFilesInDir(Path path, List<FileStatus> files, boolean logExcludedFiles)
			throws IOException {
		final FileSystem fs = path.getFileSystem();

		long length = 0;

		for(FileStatus file: fs.listStatus(path)) {
			if (!file.isDir()) {
				if(acceptFile(file)) {
					files.add(file);
					length += file.getLen();
				} else {
					if (logExcludedFiles && LOG.isDebugEnabled()) {
						LOG.debug("Directory "+file.getPath().toString()+" did not pass the file-filter and is excluded.");
					}
				}
			}
		}
		return length;
	}
	
	/**
	 * A simple hook to filter files and directories from the input.
	 * The method may be overridden. Hadoop's FileInputFormat has a similar mechanism and applies the
	 * same filters by default.
	 * 
	 * @param fileStatus The file status to check.
	 * @return true, if the given file or directory is accepted
	 */
	protected boolean acceptFile(FileStatus fileStatus) {
		final String name = fileStatus.getPath().getName();
		return !name.startsWith("_") && !name.startsWith(".");
	}

	@Override
	public InputSplitAssigner getInputSplitAssigner(ClovisInputSplit[] inputSplits) {
		return new DefaultInputSplitAssigner(inputSplits);
	}

	@Override
	public void open(ClovisInputSplit split) throws IOException {
		
		this.splitsIterator = Arrays.asList(split.getBuffers()).iterator();

		if (LOG.isDebugEnabled()) {
			LOG.debug("Opening input split " + split.getSplitNumber());
		}
		
		// instantiate the parsers
		FieldParser<?>[] parsers = new FieldParser<?>[fieldTypes.length];
		
		for (int i = 0; i < fieldTypes.length; i++) {
			if (fieldTypes[i] != null) {
				Class<? extends FieldParser<?>> parserType = FieldParser.getParserForType(fieldTypes[i]);
				if (parserType == null) {
					throw new RuntimeException("No parser available for type '" + fieldTypes[i].getName() + "'.");
				}

				FieldParser<?> p = InstantiationUtil.instantiate(parserType, FieldParser.class);

				if (this.quotedStringParsing) {
					if (p instanceof StringParser) {
						((StringParser)p).enableQuotedStringParsing(this.quoteCharacter);
					} else if (p instanceof StringValueParser) {
						((StringValueParser)p).enableQuotedStringParsing(this.quoteCharacter);
					}
				}

				parsers[i] = p;
			}
		}
		this.fieldParsers = parsers;
		
		// create the value holders
		this.parsedValues = new Object[fieldParsers.length];
		for (int i = 0; i < fieldParsers.length; i++) {
			this.parsedValues[i] = fieldParsers[i].createValue();
		}

		this.executor = new ClovisThreadPoolExecutor(0, Integer.MAX_VALUE, 60L,
				TimeUnit.SECONDS, new SynchronousQueue<Runnable>(),
				READ_RETRY_ATTEMPTS);
		
		if (fullBufferQueue == null) {
			this.fullBufferQueue = new ArrayBlockingQueue<ClovisBuffer>(BUFFER_QUEUE_CAPACITY);
		}
		
		if (this.cleanBuffers == null) {
			cleanBuffers = new LinkedList<ClovisBuffer>();
			for (int i = 0; i < BUFFER_QUEUE_CAPACITY; i++) {
				this.cleanBuffers.add(new ClovisBuffer());
			}
		}
		
		for (int i = 0; i < BUFFER_QUEUE_CAPACITY; i++) {
			if (splitsIterator.hasNext()) {
				executor.execute(new ReadTask(cleanBuffers.pollLast(), fullBufferQueue, splitsIterator.next()));
			}
		}
		
		this.end = false;
		
	}
	
	@Override
	public boolean reachedEnd() throws IOException {
		return end;
	}

	@Override
	public T nextRecord(T reuse) throws IOException {
		if (currentBuffer == null || !currentBuffer.read(recordDelim)) {
			try {
				if (currentBuffer != null)  {
					if (splitsIterator.hasNext()) {
						executor.execute(new ReadTask(currentBuffer, fullBufferQueue, splitsIterator.next()));
					} else {
						cleanBuffers.add(currentBuffer);
						if (cleanBuffers.size() == BUFFER_QUEUE_CAPACITY) {
							currentBuffer = null;
							end = true;
							return null;
						}
					}
				}
				currentBuffer = null;
				while (currentBuffer == null) {
					currentBuffer = fullBufferQueue.poll(BUFFER_WAIT_TIMEOUT_SEC, TimeUnit.SECONDS);
					if (executor.hasFailedTasks()) {
						throw new IOException("Could not fill the buffer");
					}
				}
				return nextRecord(reuse);
			} catch (InterruptedException e) {
				throw new IOException("Could not fill the buffer");
			}
		}
		
		if (parseRecord(parsedValues, currentBuffer.array(), currentBuffer.getCurrentOffset(), currentBuffer.getCurrentLength())) {
			fillRecord(reuse, parsedValues);
		} else {
			return null;
		}
		return reuse;
	}
	
	private T fillRecord(T reuse, Object[] parsedValues) {
		
		if (tupleSerializer == null)  {
			TypeInformation<T> typeInfo = TypeExtractor.getForObject(reuse);
			tupleSerializer = (TupleSerializerBase<T>) typeInfo.createSerializer(this.getRuntimeContext().getExecutionConfig());
		}
		
		return tupleSerializer.createOrReuseInstance(parsedValues, reuse);
	}
	

	@Override
	public void close() throws IOException {
		executor.shutdown();
		if (executor.hasFailedTasks()) {
			throw new IOException("Read was not successful");
		}
	}
	
	public void setFields(boolean[] includedMask, Class<?>[] fieldTypes) {
		Preconditions.checkNotNull(includedMask);
		Preconditions.checkNotNull(fieldTypes);

		ArrayList<Class<?>> types = new ArrayList<Class<?>>();

		// check if types are valid for included fields
		int typeIndex = 0;
		for (int i = 0; i < includedMask.length; i++) {

			if (includedMask[i]) {
				if (typeIndex > fieldTypes.length - 1) {
					throw new IllegalArgumentException("Missing type for included field " + i + ".");
				}
				Class<?> type = fieldTypes[typeIndex++];

				if (type == null) {
					throw new IllegalArgumentException("Type for included field " + i + " should not be null.");
				} else {
					// check if we support parsers for this type
					if (FieldParser.getParserForType(type) == null) {
						throw new IllegalArgumentException("The type '" + type.getName() + "' is not supported for the CSV input format.");
					}
					types.add(type);
				}
			}
		}

		this.fieldTypes = types.toArray(new Class<?>[types.size()]);
		this.fieldIncluded = includedMask;
	}
	
	public void setFields(Class<?> ... fieldTypes) {
		if (fieldTypes == null) {
			throw new IllegalArgumentException("Field types must not be null.");
		}
		
		this.fieldIncluded = new boolean[fieldTypes.length];
		ArrayList<Class<?>> types = new ArrayList<Class<?>>();
		
		// check if we support parsers for these types
		for (int i = 0; i < fieldTypes.length; i++) {
			Class<?> type = fieldTypes[i];
			
			if (type != null) {
				if (FieldParser.getParserForType(type) == null) {
					throw new IllegalArgumentException("The type '" + type.getName() + "' is not supported for the CSV input format.");
				}
				types.add(type);
				fieldIncluded[i] = true;
			}
		}

		this.fieldTypes = types.toArray(new Class<?>[types.size()]);
	}
	
	public void setFields(int[] sourceFieldIndices, Class<?>[] fieldTypes) {
		Preconditions.checkNotNull(sourceFieldIndices);
		Preconditions.checkNotNull(fieldTypes);
		Preconditions.checkArgument(sourceFieldIndices.length == fieldTypes.length,
			"Number of field indices and field types must match.");

		for (int i : sourceFieldIndices) {
			if (i < 0) {
				throw new IllegalArgumentException("Field indices must not be smaller than zero.");
			}
		}

		int largestFieldIndex = Ints.max(sourceFieldIndices);
		this.fieldIncluded = new boolean[largestFieldIndex + 1];
		ArrayList<Class<?>> types = new ArrayList<Class<?>>();

		// check if we support parsers for these types
		for (int i = 0; i < fieldTypes.length; i++) {
			Class<?> type = fieldTypes[i];

			if (type != null) {
				if (FieldParser.getParserForType(type) == null) {
					throw new IllegalArgumentException("The type '" + type.getName()
						+ "' is not supported for the CSV input format.");
				}
				types.add(type);
				fieldIncluded[sourceFieldIndices[i]] = true;
			}
		}

		this.fieldTypes = types.toArray(new Class<?>[types.size()]);
	}
	
	private boolean parseRecord(Object[] holders, byte[] bytes, int offset, int numBytes) throws ParseException {
		
		boolean[] fieldIncluded = this.fieldIncluded;
		
		int startPos = offset;
		final int limit = offset + numBytes;
		
		for (int field = 0, output = 0; field < fieldIncluded.length; field++) {
			
			// check valid start position
			if (startPos >= limit) {
				if (lenient) {
					return false;
				} else {
					throw new ParseException("Row too short: " + new String(bytes, offset, numBytes));
				}
			}
			
			if (fieldIncluded[field]) {
				// parse field
				@SuppressWarnings("unchecked")
				FieldParser<Object> parser = (FieldParser<Object>) this.fieldParsers[output];
				Object reuse = holders[output];
				startPos = parser.parseField(bytes, startPos, limit, this.fieldDelim, reuse);
				holders[output] = parser.getLastResult();
				
				// check parse result
				if (startPos < 0) {
					// no good
					if (lenient) {
						return false;
					} else {
						String lineAsString = new String(bytes, offset, numBytes);
						throw new ParseException("Line could not be parsed: '" + lineAsString + "'\n"
								+ "ParserError " + parser.getErrorState() + " \n"
								+ "Expect field types: "+fieldTypesToString() + " \n"
								+ "in file: " + path);
					}
				}
				output++;
			}
			else {
				// skip field
				startPos = skipFields(bytes, startPos, limit, this.fieldDelim);
				if (startPos < 0) {
					if (!lenient) {
						String lineAsString = new String(bytes, offset, numBytes);
						throw new ParseException("Line could not be parsed: '" + lineAsString+"'\n"
								+ "Expect field types: "+fieldTypesToString()+" \n"
								+ "in file: "+ path);
					}
				}
			}
		}
		return true;
	}
	
	protected int skipFields(byte[] bytes, int startPos, int limit, byte[] delim) {

		int i = startPos;

		final int delimLimit = limit - delim.length + 1;

		if (quotedStringParsing && bytes[i] == quoteCharacter) {

			// quoted string parsing enabled and field is quoted
			// search for ending quote character, continue when it is escaped
			i++;

			while (i < limit && (bytes[i] != quoteCharacter || bytes[i-1] == BACKSLASH)){
				i++;
			}
			i++;

			if (i == limit) {
				// we are at the end of the record
				return limit;
			} else if ( i < delimLimit && FieldParser.delimiterNext(bytes, i, delim)) {
				// we are not at the end, check if delimiter comes next
				return i + delim.length;
			} else {
				// delimiter did not follow end quote. Error...
				return -1;
			}
		} else {
			// field is not quoted
			while(i < delimLimit && !FieldParser.delimiterNext(bytes, i, delim)) {
				i++;
			}

			if (i >= delimLimit) {
				// no delimiter found. We are at the end of the record
				return limit;
			} else {
				// delimiter found.
				return i + delim.length;
			}
		}
	}
	
	private String fieldTypesToString() {
		StringBuilder string = new StringBuilder();
		string.append(this.fieldTypes[0].toString());

		for (int i = 1; i < this.fieldTypes.length; i++) {
			string.append(", ").append(this.fieldTypes[i]);
		}
		
		return string.toString();
	}

	public void setFilePath(Path filePath) {
		if (filePath == null) {
			throw new IllegalArgumentException("File path may not be null.");
		}
		
		this.path = filePath;
	}
	
	public void setFilePath(String filePath) {
		setFilePath(new Path(filePath));
	}
	
	public void setLenient(boolean lenient) {
		this.lenient = lenient;
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
	
	public void enableQuotedStringParsing(char quoteCharacter) {
		quotedStringParsing = true;
		this.quoteCharacter = (byte)quoteCharacter;
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
	
	public void setBuffersPerSplit(Integer buffersPerSplit) {
		this.buffersPerSplit = buffersPerSplit;
	}
	
	public Integer getBuffersPerSplit() {
		return buffersPerSplit;
	}
	
	public class ReadTask implements ClovisAsyncTask {
		
		private BlockingQueue<ClovisBuffer> queue;
		private ClovisBuffer buffer;
		private FSDataInputStream stream;
		private int retryAttempt;
		
		public ReadTask(ClovisBuffer buffer, BlockingQueue<ClovisBuffer> queue, Path path) throws IOException {
			this.queue = queue;
			this.buffer = buffer;
			final FileSystem fs = FileSystem.get(path.toUri());
			this.stream = fs.open(path);
			this.retryAttempt = 0;
		}

		@Override
		public void run() {
			try {
				this.buffer.clear();
				int read = this.stream.read(this.buffer.array());
				if (read == -1) {
					throw new RuntimeException("Buffer could not be filled");
				} else {
					buffer.flip();
					buffer.setLimit(read);
					queue.put(buffer);
					cleanup();
				}
			} catch (IOException | InterruptedException e) {
				throw new RuntimeException(e);
			}
		}

		@Override
		public void cleanup() throws InterruptedException, IOException {
			final FSDataInputStream s = this.stream;
			if (s != null) {
				this.stream = null;
				s.close();
			}
		}

		@Override
		public int getRetryAttempt() {
			return retryAttempt;
		}

		@Override
		public void setRetryAttempt(int retryAttempt) {
			this.retryAttempt = retryAttempt;
		}
	}
	
	
	// ============================================================================================
	//  Parameterization via configuration
	// ============================================================================================
	
	// ------------------------------------- Config Keys ------------------------------------------
	
	/**
	 * The config parameter which defines the input file path.
	 */
	private static final String FILE_PARAMETER_KEY = "input.file.path";
	
	private static final String BUFFERS_PER_SPLIT_PARAMETER_KEY = "buffers.per.split";

}
